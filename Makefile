.PHONY: build vet test run demo container-check image

build:
	go build ./...
vet:
	go vet ./...
test:
	go test ./internal/persist ./internal/lifecycle ./internal/engine
run:
	go run ./cmd/atlas
demo:
	python3 scripts/walkthrough.py --workflow submit-render --out var/demo
# Verify read-only code tree, state volume, SIGTERM/SIGKILL recovery, the
# second-instance lock failure, and the alive-but-not-ready permission path.
container-check:
	go build -o /tmp/atlas-lifecycle-build ./cmd/atlas
	python3 -I scripts/container_lifecycle.py --binary /tmp/atlas-lifecycle-build
image:
	docker build -f deploy/Dockerfile -t atlas-render:local .
