BINARY := replicron
GO ?= go
MODULE ?= github.com/Cikouyanqu/replicron

.PHONY: build test vet lint fmt docker clean set-module

build:
	$(GO) build -trimpath -ldflags "-s -w" -o $(BINARY) .

test:
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

lint:
	golangci-lint run

fmt:
	gofmt -l -w .

docker:
	docker build -t replicron .

# Retarget the module path before publishing to GitHub (the local module is
# a bare name so the repo builds anywhere without knowing the owner).
set-module:
	$(GO) mod edit -module $(MODULE)
	find . -name '*.go' -not -path './.git/*' -exec sed -i 's#"replicron/#"$(MODULE)/#g' {} +

clean:
	rm -f $(BINARY) $(BINARY).exe
