alias b := build

build:
    go build -o jot

fmt:
    gofmt -w .