.PHONY: help install install-dev test lint format clean run

# Default target
.DEFAULT_GOAL := help

# Help target
help:
	@echo "Account Log Cleaner - Development Commands"
	@echo ""
	@echo "Available targets:"
	@echo "  install       - Install production dependencies"
	@echo "  install-dev   - Install development dependencies"
	@echo "  test          - Run tests with coverage"
	@echo "  lint          - Run code quality checks (flake8, mypy)"
	@echo "  format        - Format code with black and isort"
	@echo "  clean         - Clean up build artifacts and cache"
	@echo "  run           - Run the application"
	@echo "  help          - Show this help message"

# Install production dependencies
install:
	pip install -r requirements.txt

# Install development dependencies
install-dev: install
	pip install black flake8 mypy pytest pytest-cov isort

# Run tests
test:
	pytest tests/ -v --cov=app --cov-report=html
	@echo "Coverage report generated in htmlcov/index.html"

# Run linting
lint:
	@echo "Running flake8..."
	flake8 app/ tests/ --max-line-length=100
	@echo "Running mypy..."
	mypy app/ --ignore-missing-imports

# Format code
format:
	@echo "Running black..."
	black app/ tests/
	@echo "Running isort..."
	isort app/ tests/

# Clean up
clean:
	@echo "Cleaning up..."
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .mypy_cache/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "Clean completed"

# Run application
run:
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found. Please create .env from .env.example"; \
		echo "cp .env.example .env"; \
		exit 1; \
	fi
	python main.py
