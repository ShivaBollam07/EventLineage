.PHONY: help build run stop clean test demo

# Default target
help:
	@echo "EventLineage - Message Lineage and Causality Engine"
	@echo ""
	@echo "Usage:"
	@echo "  make build     - Build all Docker images"
	@echo "  make run       - Start all services"
	@echo "  make stop      - Stop all services"
	@echo "  make clean     - Remove all containers and volumes"
	@echo "  make demo      - Run demo script to create sample chain"
	@echo "  make test      - Run all tests"
	@echo "  make logs      - Tail logs from all services"
	@echo ""
	@echo "Development:"
	@echo "  make dev-api   - Run API server locally (requires Go)"
	@echo "  make dev-ui    - Run UI dev server (requires Node.js)"
	@echo ""

# Build all Docker images
build:
	docker-compose build

# Start all services
run:
	docker-compose up -d
	@echo ""
	@echo "Services starting..."
	@echo "  - UI:     http://localhost:3000"
	@echo "  - API:    http://localhost:8080"
	@echo "  - Neo4j:  http://localhost:7474 (neo4j/eventlineage)"
	@echo "  - Kafka:  localhost:9092"
	@echo ""
	@echo "Run 'make demo' to create a sample chain"

# Stop all services
stop:
	docker-compose down

# Remove all containers, volumes, and images
clean:
	docker-compose down -v --rmi local
	@echo "Cleaned up all containers and volumes"

# Tail logs
logs:
	docker-compose logs -f

# Run demo script
demo:
	@echo "Waiting for services to be ready..."
	@sleep 5
	cd scripts && python demo.py

# Run tests
test:
	cd backend && go test ./...
	cd sdk/python && pip install -e . && pytest
	cd sdk/java && mvn test

# Local development - API
dev-api:
	cd backend && go run ./cmd/server

# Local development - UI
dev-ui:
	cd ui && npm install && npm run dev

# Install Python SDK locally
install-sdk-python:
	cd sdk/python && pip install -e .

# Build Java SDK
build-sdk-java:
	cd sdk/java && mvn clean package
