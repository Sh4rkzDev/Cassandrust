default: build

build:
	@echo "Building images..."
	@docker compose build

start:
	@echo "Initializing cluster..."
	@docker compose up -d

stop:
	@echo "Stopping cluster..."
	@for container in $(shell docker compose ps -q); do \
		timestamp=$$(date +"%Y-%m-%d %H:%M:%S"); \
		docker exec $$container sh -c "echo 'Node shutting down at $$timestamp\n\n' >> /proc/1/fd/1"; \
	done
	@docker compose stop

start-%:
	@echo "Starting node $*..."
	@docker compose start $*

stop-%:
	@echo "Stopping node $*..."
	@docker compose stop $*

destroy:
	@echo "Destroying the cluster..."
	@docker compose down -v

test:
	@echo "Running tests..."
	@cargo test

help:
	@echo "Available commands:"
	@echo "  make build    - Build node image"
	@echo "  make start    - Initialize the cluster"
	@echo "  make stop     - Stop the cluster"
	@echo "  make start-%  - Start a specific node"
	@echo "  make stop-%   - Stop a specific node"
	@echo "  make destroy  - Stop and destroy the cluster"
	@echo "  make test     - Run tests"
