.PHONY: help install install-dev format lint type-check test clean run-import

help:
	@echo "Available commands:"
	@echo "  make install      Install production dependencies"
	@echo "  make install-dev  Install all dependencies including dev"
	@echo "  make format       Format code with black"
	@echo "  make lint         Run linting with ruff"
	@echo "  make type-check   Run type checking with mypy"
	@echo "  make test         Run tests"
	@echo "  make clean        Remove cache files"
	@echo "  make run-import   Run movie import script"

install:
	@which python | grep -q venv || echo "⚠️ Warning: You are not in a virtualenv"
	pip install -e .

install-dev:
	pip install -e ".[dev]"

format:
	black src/ tests/ scripts/

lint:
	ruff check src/ tests/ scripts/

type-check:
	mypy src/

test:
	pytest

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".coverage" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".mypy_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +

run-import:
	python scripts/import_movies.py

run-validate:
	python scripts/validate_data.py

update-circuits:
	python scripts/update_cinema_circuits.py