.PHONY: test cover vet fmt lint build example

test:
	go test ./... -race -count=1

cover:
	go test ./... -race -count=1 -coverprofile=coverage.txt -covermode=atomic
	go tool cover -func=coverage.txt | tail -1

vet:
	go vet ./...

fmt:
	gofmt -l -w .

lint: fmt vet

build:
	go build ./...

example:
	go build ./example/basic
