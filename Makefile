.PHONY: test test-race

test:
	CGO_ENABLED=1 go test ./...

test-race:
	CGO_ENABLED=1 go test -race ./...
