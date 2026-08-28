GO ?= go

.PHONY: test update lint build clean help

test: ## 单测 + golden vectors
	$(GO) test ./... -count=1 -race -timeout 5m

update: ## 重新生成 golden vectors（协议用例变更时）
	$(GO) test ./frame -update

lint: ## gofmt + go vet
	@out="$$(gofmt -l . | grep -v '^third_party/' || true)"; \
	if [ -n "$$out" ]; then echo "$$out"; exit 1; fi
	$(GO) vet ./...

build: ## 构建
	$(GO) build ./...

clean: ## 清理
	rm -rf bin

help: ## 帮助
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-10s %s\n", $$1, $$2}'
