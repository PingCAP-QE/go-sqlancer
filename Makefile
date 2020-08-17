GOARCH := $(if $(GOARCH),$(GOARCH),amd64)
GO=GO15VENDOREXPERIMENT="1" CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) GO111MODULE=on go
GOTEST=GO15VENDOREXPERIMENT="1" CGO_ENABLED=1 GO111MODULE=on go test # go race detector requires cgo
VERSION   := $(if $(VERSION),$(VERSION),latest)

GOBUILD=$(GO) build

default: sqlancer mutasql

sqlancer:
	$(GOBUILD) $(GOMOD) -o bin/go-sqlancer cmd/sqlancer/*.go

mutasql:
	$(GOBUILD) $(GOMOD) -o bin/go-mutasql cmd/mutasql/*.go

fmt:
	go fmt ./...

test:
	go test ./...

tidy:
	@echo "go mod tidy"
	GO111MODULE=on go mod tidy
	@git diff --exit-code -- go.mod
