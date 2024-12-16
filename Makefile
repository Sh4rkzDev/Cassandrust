default: build

build:
	@echo "Building images..."
	docker compose build

start:
	@echo "Initializing cluster..."
	docker compose up -d

stop:
	@echo "Stoping cluster..."
	docker compose stop

destroy:
	@echo "Destroying the cluster..."
	docker compose down -v

test:
	@echo "Running tests..."
	cargo test

help:
	@echo "Available commands:"
	@echo "  make build    - Build node image"
	@echo "  make start    - Initialize the cluster"
	@echo "  make stop     - Stop the cluster"
	@echo "  make destroy  - Stop and destroy the cluster"
	@echo "  make test     - Run tests"
