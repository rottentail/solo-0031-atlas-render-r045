#!/usr/bin/env python3
"""Verify the Atlas Render container lifecycle contract with real processes.

A container runtime is not required. The script reproduces the contract that
the image and compose file describe:

  * the code directory is read-only; only the state volume is writable;
  * the process runs as the invoking (non-root) user;
  * stopping (SIGTERM) and force-killing (SIGKILL) leave accepted jobs,
    receipts and artifacts available after the next start;
  * a second instance pointed at the same state volume fails safely;
  * an unwritable state volume is reported as not-ready while the process
    stays alive and keeps retrying.

The exit status is 0 only when every check passes. Each step prints one JSON
line so the transcript can be attached to deployment records.

No container runtime was available in this environment; this is a process-level
proof of the same contract, not a substitute for a runtime-specific smoke test.
"""

import argparse
import http.client
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
EXIT_LOCKED = 3


def scene_document(width=320, height=240):
    return {
        "width": width,
        "height": height,
        "background": "#F5F1E8",
        "layers": [
            {
                "id": "plate",
                "kind": "rectangle",
                "width": width * 0.7,
                "height": height * 0.6,
                "transform": {"x": 20, "y": 20, "scaleX": 1, "scaleY": 1, "degrees": 3},
                "opacity": 1,
                "fill": {
                    "kind": "linear",
                    "color": "#145A70",
                    "endColor": "#32A5A0",
                    "from": {"x": 0, "y": 0},
                    "to": {"x": width, "y": height},
                },
            },
            {
                "id": "accent",
                "kind": "ellipse",
                "width": width * 0.4,
                "height": height * 0.4,
                "transform": {"x": width * 0.35, "y": height * 0.3, "scaleX": 1, "scaleY": 1},
                "opacity": 0.85,
                "fill": {"kind": "solid", "color": "#E87E4F"},
            },
        ],
    }


def heavy_document():
    # Enough work to still be running when the process is signalled.
    doc = scene_document(1500, 1500)
    doc["layers"] = [
        {
            "id": f"sheet-{i}",
            "kind": "rectangle",
            "width": 1500,
            "height": 1500,
            "transform": {"scaleX": 1, "scaleY": 1},
            "opacity": 0.9,
            "fill": {
                "kind": "linear",
                "color": "#101820",
                "endColor": "#F2AA4C",
                "from": {"x": 0, "y": 0},
                "to": {"x": 1500, "y": 1500},
            },
        }
        for i in range(12)
    ]
    return doc


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class Server:
    def __init__(self, binary, code_root, state_path, log_path, addr="127.0.0.1"):
        self.binary = binary
        self.code_root = code_root
        self.state_path = state_path
        self.log_path = log_path
        self.port = free_port()
        self.addr = f"{addr}:{self.port}"
        self.proc = None
        self.log = None

    @property
    def url(self):
        host = "127.0.0.1" if self.addr.startswith("0.0.0.0") else self.addr.split(":")[0]
        return f"http://{host}:{self.port}"

    def start(self, wait=True, timeout=20):
        self.log = open(self.log_path, "ab")
        env = dict(
            os.environ,
            ATLAS_ADDR=self.addr,
            ATLAS_STATE=str(self.state_path),
        )
        env.pop("GOCACHE", None)
        # Run from the read-only code tree, as the image entrypoint does.
        self.proc = subprocess.Popen(
            [str(self.binary)], cwd=str(self.code_root), env=env,
            stdout=self.log, stderr=self.log,
        )
        if wait:
            self.wait_live(timeout)
        return self

    def request(self, method, path, body=None, status=200, headers=None):
        hdr = dict(headers or {})
        data = None
        if isinstance(body, (dict, list)):
            data = json.dumps(body).encode()
            hdr["Content-Type"] = "application/json"
        elif body is not None:
            data = body
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=15)
        conn.request(method, path, body=data, headers=hdr)
        response = conn.getresponse()
        payload = response.read()
        code = response.status
        ctype = response.getheader("Content-Type", "")
        conn.close()
        if code != status:
            raise AssertionError(
                f"{method} {path}: expected {status}, got {code}: {payload[:400]!r}")
        if ctype.startswith("application/json"):
            return json.loads(payload)
        return payload

    def wait_live(self, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise AssertionError(f"server exited during startup: {self.tail_log()}")
            try:
                self.request("GET", "/livez")
                return
            except (OSError, AssertionError):
                time.sleep(0.03)
        raise AssertionError(f"liveness deadline exceeded: {self.tail_log()}")

    def wait_ready(self, timeout=30):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                self.request("GET", "/readyz")
                return
            except AssertionError as error:
                last = error
                time.sleep(0.1)
        raise AssertionError(f"readiness deadline exceeded: {last}: {self.tail_log()}")

    def wait_job(self, job_id, wanted=("succeeded",), timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            job = self.request("GET", "/v1/jobs/" + job_id)
            if job["state"] in wanted:
                return job
            if job["state"] in ("failed", "cancelled") and job["state"] not in wanted:
                raise AssertionError(f"unexpected job outcome: {job}")
            time.sleep(0.02)
        raise AssertionError(f"job deadline exceeded: {job_id}")

    def stop(self, abrupt=False, timeout=30):
        if not self.proc or self.proc.poll() is not None:
            return self.proc.returncode if self.proc else None
        if abrupt:
            self.proc.send_signal(signal.SIGKILL)
        else:
            self.proc.send_signal(signal.SIGTERM)
        try:
            code = self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            code = self.proc.wait(timeout=10)
            raise AssertionError(f"process did not stop in {timeout}s")
        if self.log:
            self.log.close()
            self.log = None
        return code

    def check(self, mode, timeout="2s"):
        env = dict(os.environ, ATLAS_ADDR=self.addr)
        result = subprocess.run(
            [str(self.binary), "check", "-mode", mode, "-timeout", timeout],
            env=env, capture_output=True, text=True, timeout=15,
        )
        return result.returncode, result.stdout.strip(), result.stderr.strip()

    def tail_log(self, limit=2000):
        try:
            data = Path(self.log_path).read_text(errors="replace")
        except OSError:
            return ""
        return data[-limit:]


def log_step(step, ok=True, **details):
    record = {"step": step, "ok": bool(ok)}
    record.update(details)
    print(json.dumps(record, ensure_ascii=False), flush=True)


def create_artifact(server, scene_id="emblem", document=None, key="render"):
    document = document or scene_document()
    server.request("PUT", f"/v1/scenes/{scene_id}",
                   {"name": "container check", "expectedRevision": 0, "document": document})
    scene = server.request("GET", f"/v1/scenes/{scene_id}")
    snapshot = server.request(
        "POST", f"/v1/scenes/{scene_id}/snapshots",
        {"expectedRevision": scene["revision"]}, status=201)
    job = server.request(
        "POST", "/v1/jobs",
        {"snapshotId": snapshot["id"], "scale": 1}, status=202,
        headers={"Idempotency-Key": key})
    return snapshot, job


def submit(server, snapshot, key, scale=1):
    return server.request(
        "POST", "/v1/jobs", {"snapshotId": snapshot["id"], "scale": scale},
        status=202, headers={"Idempotency-Key": key})


def download_digest(server, artifact_id):
    import hashlib
    data = server.request("GET", "/v1/artifacts/" + artifact_id)
    assert data[:8] == b"\x89PNG\r\n\x1a\n", "artifact is not a PNG"
    return hashlib.sha256(data).hexdigest(), len(data)


def verify(binary, workdir):
    workdir = Path(workdir)
    code_root = workdir / "image"
    volume = workdir / "volume"
    logs = workdir / "logs"
    for path in (code_root, volume, logs):
        path.mkdir(parents=True, exist_ok=True)

    # --- 1. Read-only code tree, writable independent volume ---------------
    binary_path = code_root / "atlas"
    shutil.copy2(binary, binary_path)
    binary_path.chmod(0o0555)
    code_root.chmod(0o0555)
    before = sorted(p.name for p in code_root.iterdir())
    try:
        (code_root / "probe").write_text("x")
        code_tree_read_only = False
    except OSError:
        code_tree_read_only = True
    assert code_tree_read_only, "test setup failed: code tree is writable"
    log_step("code_tree_read_only", True, path=str(code_root), entries=before)
    assert volume.stat().st_uid == os.getuid(), "volume must belong to the runtime user"

    state_path = volume / "state.json"
    vol_ok = os.access(volume, os.W_OK)
    assert vol_ok, "state volume is not writable by the runtime user"
    log_step("state_volume_writable", True, path=str(volume))

    # --- 2. First start: submit work, stop gracefully ----------------------
    first = Server(binary_path, code_root, state_path, logs / "first.log")
    first.start()
    live_code, live_out, _ = first.check("live")
    ready_code, ready_out, _ = first.check("ready")
    assert live_code == 0 and ready_code == 0, (live_out, ready_out)
    log_step("probes_ready", True, live=json.loads(live_out), ready=json.loads(ready_out))

    snapshot, fast_job = create_artifact(first, key="receipt-fast")
    fast_job = first.wait_job(fast_job["id"])
    fast_digest, fast_bytes = download_digest(first, fast_job["artifactId"])
    replay = submit(first, snapshot, "receipt-fast")
    assert replay["id"] == fast_job["id"], "receipt did not replay the original job"
    log_step("accepted_work", True, job=fast_job["id"], artifact=fast_job["artifactId"],
             pngBytes=fast_bytes, digest=fast_digest)

    # A second snapshot of the heavy document provides the interrupted work.
    heavy = heavy_document()
    first.request("PUT", "/v1/scenes/heavy",
                  {"name": "heavy", "expectedRevision": 0, "document": heavy})
    heavy_scene = first.request("GET", "/v1/scenes/heavy")
    heavy_snapshot = first.request(
        "POST", "/v1/scenes/heavy/snapshots",
        {"expectedRevision": heavy_scene["revision"]}, status=201)
    reference = first.wait_job(submit(first, heavy_snapshot, "receipt-reference")["id"])
    reference_digest, _ = download_digest(first, reference["artifactId"])

    interrupted = submit(first, heavy_snapshot, "receipt-sigterm")
    first.wait_job(interrupted["id"], wanted=("running",))
    exit_code = first.stop(abrupt=False)
    assert exit_code == 0, f"graceful stop exited {exit_code}"
    log_step("graceful_stop", True, exitCode=exit_code, job=interrupted["id"])

    stored = json.loads(state_path.read_text())
    assert Path(str(state_path) + ".lock").exists(), "lock file missing"
    assert "receipt-fast" in stored["receipts"], "receipt not in persisted state"
    assert stored["jobs"][interrupted["id"]]["state"] == "running", \
        f"interrupted job state: {stored['jobs'][interrupted['id']]['state']}"
    log_step("state_durable_after_stop", True,
             receipts=len(stored["receipts"]), jobs=len(stored["jobs"]),
             snapshots=len(stored["snapshots"]))

    # --- 3. Restart: accepted work and receipts still usable ---------------
    second = Server(binary_path, code_root, state_path, logs / "second.log")
    second.start()
    second.wait_ready()
    recovered = second.wait_job(interrupted["id"])
    recovered_digest, _ = download_digest(second, recovered["artifactId"])
    assert recovered["attempt"] == 1, "recovery must not increase attempt"
    assert recovered_digest == reference_digest, "recovered render changed pixels"
    again = submit(second, snapshot, "receipt-fast")
    assert again["id"] == fast_job["id"], "receipt lost across restart"
    assert again["state"] == "succeeded", "replayed job lost its result"
    old = second.request("GET", "/v1/jobs/" + fast_job["id"])
    assert old["state"] == "succeeded" and old["artifactId"] == fast_job["artifactId"]
    old_digest, _ = download_digest(second, fast_job["artifactId"])
    assert old_digest == fast_digest, "artifact bytes changed across restart"
    log_step("restart_preserves_work", True, job=recovered["id"],
             attempt=recovered["attempt"], digest=recovered_digest)

    # --- 4. Second instance on the same volume fails safely ----------------
    conflict = Server(binary_path, code_root, state_path, logs / "conflict.log")
    conflict.start(wait=False)
    try:
        code = conflict.proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        conflict.proc.kill()
        raise AssertionError("second instance did not fail; it kept running")
    conflict_text = conflict.tail_log()
    assert code == EXIT_LOCKED, f"second instance exit code {code}, log: {conflict_text}"
    assert "lock" in conflict_text.lower(), "second instance did not explain the lock"
    # The owner must be unaffected.
    assert second.request("GET", "/readyz")["status"] == "ready"
    ready_code, _, _ = second.check("ready")
    assert ready_code == 0, "owner stopped being ready after a competing start"
    log_step("second_instance_refused", True, exitCode=code)

    # --- 5. SIGKILL leaves state recoverable -------------------------------
    killed = submit(second, heavy_snapshot, "receipt-sigkill")
    second.wait_job(killed["id"], wanted=("running",))
    # Record the committed queue before the abrupt stop.
    before_kill = json.loads(state_path.read_text())
    assert killed["id"] in before_kill["jobs"]
    assert before_kill["jobs"][killed["id"]]["state"] == "running"
    code = second.stop(abrupt=True)
    assert code == -signal.SIGKILL, f"unexpected SIGKILL exit status {code}"
    # A killed writer leaves only temporary files, never a half-written state.
    leftovers = sorted(p.name for p in volume.iterdir() if p.name.startswith(".atlas-write-"))
    log_step("abrupt_stop", True, exitCode=code, staleWrites=leftovers)

    third = Server(binary_path, code_root, state_path, logs / "third.log")
    third.start()
    third.wait_ready()
    assert not [p for p in volume.iterdir() if p.name.startswith(".atlas-write-")], \
        "stale temporary files were not cleaned on start"
    killed_done = third.wait_job(killed["id"])
    killed_digest, _ = download_digest(third, killed_done["artifactId"])
    assert killed_digest == reference_digest, "render after SIGKILL changed pixels"
    assert submit(third, snapshot, "receipt-fast")["id"] == fast_job["id"]
    log_step("restart_after_kill", True, job=killed_done["id"], digest=killed_digest)
    third.stop()

    # --- 6. Unwritable volume: alive but not ready -------------------------
    locked_volume = workdir / "locked-volume"
    locked_volume.mkdir()
    locked_volume.chmod(0o555)
    try:
        # The parent is not writable either, so even creating the state
        # directory must fail without taking the process down.
        degraded = Server(binary_path, code_root, locked_volume / "nested" / "state.json",
                          logs / "degraded.log")
        degraded.start()
        live_code, live_out, _ = degraded.check("live")
        ready_code, ready_out, _ = degraded.check("ready")
        assert live_code == 0, f"liveness failed on unwritable volume: {live_out}"
        assert ready_code == 1, f"readiness passed on unwritable volume: {ready_out}"
        ready_body = json.loads(ready_out)["body"]
        assert ready_body.get("status") == "not_ready", ready_body
        assert ready_body.get("reason") == "state_unusable", ready_body
        # A request must return a typed 503, not a crash or a generic 500.
        rejected = degraded.request("GET", "/v1/status", status=503)
        assert rejected["code"] == "state_unusable", rejected
        time.sleep(2.5)
        assert degraded.proc.poll() is None, "degraded process exited instead of retrying"
        log_step("degraded_alive_not_ready", True,
                 live=json.loads(live_out), ready=ready_body)

        # Making the volume usable lets the same process become ready, without
        # a restart and without losing the lock it never held.
        locked_volume.chmod(0o755)
        degraded.wait_ready(timeout=30)
        document = scene_document()
        degraded.request("PUT", "/v1/scenes/recovered",
                         {"name": "recovered", "expectedRevision": 0, "document": document})
        assert degraded.request("GET", "/v1/scenes/recovered")["name"] == "recovered"
        assert (locked_volume / "nested" / "state.json").exists(), "state was not created after recovery"
        log_step("degraded_recovers", True)
        assert degraded.stop() == 0
    finally:
        locked_volume.chmod(0o755)

    # --- 7. Volume contents and code tree are as expected ------------------
    assert sorted(p.name for p in code_root.iterdir()) == before, \
        "the runtime modified the read-only code tree"
    volume_entries = sorted(p.name for p in volume.iterdir())
    assert "state.json" in volume_entries and "state.json.lock" in volume_entries
    log_step("volume_layout", True, entries=volume_entries)

    print(json.dumps({"ok": True, "checks": 9}, ensure_ascii=False), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", help="prebuilt atlas binary (default: build it)")
    parser.add_argument("--workdir", help="directory for the simulated image and volume")
    args = parser.parse_args()
    if os.geteuid() == 0:
        print(json.dumps({"ok": False, "error": "run as a normal user; root bypasses permission checks"}))
        return 2
    temporary = None
    if args.workdir:
        workdir = Path(args.workdir)
        workdir.mkdir(parents=True, exist_ok=True)
    else:
        temporary = tempfile.TemporaryDirectory(prefix="atlas-container-")
        workdir = Path(temporary.name)
    binary = args.binary
    if not binary:
        binary = workdir / "atlas-build"
        subprocess.run(["go", "build", "-o", str(binary), "./cmd/atlas"],
                       cwd=ROOT, check=True, timeout=120)
    try:
        verify(binary, workdir)
        return 0
    except AssertionError as error:
        print(json.dumps({"ok": False, "error": str(error)}, ensure_ascii=False), flush=True)
        return 1
    finally:
        if temporary:
            temporary.cleanup()


if __name__ == "__main__":
    sys.exit(main())
