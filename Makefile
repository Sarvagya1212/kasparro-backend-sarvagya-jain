.PHONY: help up down restart logs test test-cov clean migrate seed

# Default target
.DEFAULT_GOAL := help

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[0;33m
RED := \033[0;31m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)Kasparro ETL Backend System$(NC)"
	@echo "$(YELLOW)Available commands:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  $(GREEN)%-15s$(NC) %s\n", $1, $2}'

up: ## Start all services
	@echo "$(BLUE)Starting services...$(NC)"
	docker-compose up -d --build
	@echo "$(GREEN)Services started successfully!$(NC)"
	@echo "API available at: http://localhost:8000"
	@echo "Health check: http://localhost:8000/health"

down: ## Stop all services
	@echo "$(YELLOW)Stopping services...$(NC)"
	docker-compose down
	@echo "$(GREEN)Services stopped.$(NC)"

restart: down up ## Restart all services

logs: ## View logs from all services
	docker-compose logs -f

logs-api: ## View API service logs only
	docker-compose logs -f api

logs-db: ## View database logs only
	docker-compose logs -f postgres

test: ## Run test suite
	@echo "$(BLUE)Running tests...$(NC)"
	docker-compose exec api pytest tests/ -v
	@echo "$(GREEN)Tests completed.$(NC)"

test-cov: ## Run tests with coverage report
	@echo "$(BLUE)Running tests with coverage...$(NC)"
	docker-compose exec api pytest tests/ -v --cov=. --cov-report=html --cov-report=term
	@echo "$(GREEN)Coverage report generated in htmlcov/$(NC)"

test-local: ## Run tests locally (without Docker)
	@echo "$(BLUE)Running tests locally...$(NC)"
	pytest tests/ -v
	@echo "$(GREEN)Tests completed.$(NC)"

migrate: ## Run database migrations
	@echo "$(BLUE)Running migrations...$(NC)"
	docker-compose exec api alembic upgrade head
	@echo "$(GREEN)Migrations completed.$(NC)"

migrate-create: ## Create a new migration (use: make migrate-create MSG="description")
	@echo "$(BLUE)Creating migration: $(MSG)$(NC)"
	docker-compose exec api alembic revision --autogenerate -m "$(MSG)"

seed: ## Seed database with test data
	@echo "$(BLUE)Seeding database...$(NC)"
	docker-compose exec api python scripts/seed_data.py
	@echo "$(GREEN)Database seeded.$(NC)"

run-etl: ## Manually trigger ETL pipeline
	@echo "$(BLUE)Running ETL pipeline...$(NC)"
	docker-compose exec api python scripts/run_etl.py
	@echo "$(GREEN)ETL completed.$(NC)"

shell: ## Open a shell in the API container
	docker-compose exec api /bin/bash

db-shell: ## Open PostgreSQL shell
	docker-compose exec postgres psql -U etl_user -d etl_db

clean: ## Remove containers, volumes, and generated files
	@echo "$(YELLOW)Cleaning up...$(NC)"
	docker-compose down -v
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name "htmlcov" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name ".coverage" -delete 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@echo "$(GREEN)Cleanup completed.$(NC)"

format: ## Format code with black
	@echo "$(BLUE)Formatting code...$(NC)"
	docker-compose exec api black .
	@echo "$(GREEN)Code formatted.$(NC)"

lint: ## Run linting checks
	@echo "$(BLUE)Running linters...$(NC)"
	docker-compose exec api flake8 .
	docker-compose exec api mypy .
	@echo "$(GREEN)Linting completed.$(NC)"

check: format lint test ## Format, lint, and test

ps: ## Show running containers
	docker-compose ps

stats: ## Show container resource usage
	docker stats $(shell docker-compose ps -q)