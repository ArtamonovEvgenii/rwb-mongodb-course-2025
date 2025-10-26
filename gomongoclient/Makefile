-include local.env
export

BIN_NAME=gomongoclient


# HELP =================================================================================================================
# This will output the help for each task
# thanks to https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
.PHONY: help
help: ## display this help screen
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-35s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)


.PHONY: bin
bin: ### build service
	$(info build $(BIN_NAME) ...)
	@go build -o $(BIN_NAME) .


.PHONY: run
run: bin ### run service in local environment
	$(info run $(BIN_NAME) ...)
	@./$(BIN_NAME)


.PHONY: build-docker
build-docker: ### build docker image
#	@docker rmi gomongoclient:1.0
	@docker build -t gomongoclient:1.0 .
