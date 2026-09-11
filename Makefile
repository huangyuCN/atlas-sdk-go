GO ?= go

.PHONY: test update lint comment-lint build clean help

test: ## 单测 + golden vectors
	$(GO) test ./... -count=1 -race -timeout 5m

update: ## 重新生成 golden vectors（已迁至 atlas 主仓，此处仅校验消费）
	$(GO) test ./frame -count=1

lint: ## gofmt + go vet + Go doc 注释规范
	@out="$$(gofmt -l . | grep -v '^third_party/' || true)"; \
	if [ -n "$$out" ]; then echo "$$out"; exit 1; fi
	$(GO) vet ./...
	$(GO) run ./scripts/go-comment-lint .

comment-lint: ## Go doc 注释规范检查（首词=声明名等，详见 docs/go-comments.md；违规退出码 1）
	$(GO) run ./scripts/go-comment-lint .

build: ## 构建
	$(GO) build ./...

clean: ## 清理
	rm -rf bin

help: ## 帮助
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-10s %s\n", $$1, $$2}'
