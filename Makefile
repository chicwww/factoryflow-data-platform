.PHONY: setup start stop test lint clean

setup: ## Install Python dependencies in a local virtual environment
	python3 -m venv .venv
	. .venv/bin/activate && pip install --upgrade pip
	. .venv/bin/activate && pip install -r requirements.txt

start: ## Start local services defined in docker-compose.yml
	docker compose up -d

stop: ## Stop local services
	docker compose down

test: ## Run the Python test suite
	. .venv/bin/activate && pytest -v

lint: ## Run Ruff static analysis
	. .venv/bin/activate && ruff check src tests

clean: ## Remove caches, virtual environment, and Docker volumes
	rm -rf .venv .pytest_cache .ruff_cache
	docker compose down -v
