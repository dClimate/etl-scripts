# Default SSH public key path
SSH_KEY_PATH ?= $(HOME)/.ssh/id_ed25519.pub

# Read SSH public key content
SSH_PUBLIC_KEY := $(shell cat $(SSH_KEY_PATH) 2>/dev/null)

image: Dockerfile
	@if [ -z "$(SSH_PUBLIC_KEY)" ]; then \
		echo "Error: No SSH public key found at $(SSH_KEY_PATH)"; \
		echo "Please ensure the file exists or specify a different path:"; \
		echo "  make SSH_KEY_PATH=/path/to/your/key.pub"; \
		exit 1; \
	fi
	docker build --build-arg SSH_PUBLIC_KEY="$(SSH_PUBLIC_KEY)" -t cluster-node:latest ./

spinup: image
	# Container 1
	docker run -d -p 2221:22 -p 4001:4001 -p 8081:8080 -p 5001:5001 -p 9001:9094 --name cluster-node-1 cluster-node:latest

	# Container 2
	docker run -d -p 2222:22 -p 4002:4001 -p 8082:8080 -p 5002:5001 -p 9002:9094 --name cluster-node-2 cluster-node:latest

	# Container 3
	docker run -d -p 2223:22 -p 4003:4001 -p 8083:8080 -p 5003:5001 -p 9003:9094 --name cluster-node-3 cluster-node:latest

clean:
	docker rm -f cluster-node-1
	docker rm -f cluster-node-2
	docker rm -f cluster-node-3
	docker rmi cluster-node:latest

help:
	@echo "Available targets:"
	@echo "  clean             - Stop and remove containers, remove built images"
	@echo "  image             - Build images"
	@echo "  spinup            - Create 3 containers from the built image"
	@echo "  help              - Show this help message"
	@echo ""
	@echo "Options:"
	@echo "  SSH_KEY_PATH     - Path to SSH public key (default: ~/.ssh/id_ed25519.pub)"
	@echo "                     Example: make SSH_KEY_PATH=~/.ssh/my_custom_key.pub"

.PHONY: spinup clean help
