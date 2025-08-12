# Makefile for simple_queue Python project
# Uses virtual environment for all operations

# Variables
VENV_DIR = ./venv
PYTHON = $(VENV_DIR)/bin/python
PIP = $(VENV_DIR)/bin/pip
PYTEST = $(VENV_DIR)/bin/pytest
BLACK = $(VENV_DIR)/bin/black
FLAKE8 = $(VENV_DIR)/bin/flake8
MYPY = $(VENV_DIR)/bin/mypy

# Source directories
SRC_DIR = simple_queue
TEST_DIR = tests
EXAMPLES_DIR = examples

# Default target
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  venv        - Create and setup virtual environment"
	@echo "  install     - Install dependencies in venv"
	@echo "  install-dev - Install dev dependencies in venv"
	@echo "  test        - Run tests with coverage"
	@echo "  lint        - Run all linting (flake8 + mypy)"
	@echo "  format      - Format code with black"
	@echo "  format-check- Check if code is formatted"
	@echo "  clean       - Clean up generated files"
	@echo "  all         - Run format, lint, and test"

# Create virtual environment
.PHONY: venv
venv:
	@if [ ! -d "$(VENV_DIR)" ]; then \
		echo "Creating virtual environment..."; \
		virtualenv $(VENV_DIR); \
	else \
		echo "Virtual environment already exists"; \
	fi

# Install production dependencies
.PHONY: install
install: venv
	@echo "Installing production dependencies..."
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt

# Install development dependencies
.PHONY: install-dev
install-dev: install
	@echo "Installing development dependencies..."
	$(PIP) install -r test-requirements.txt
	$(PIP) install black flake8 mypy
	$(PIP) install -e .

# Run tests with coverage
.PHONY: test
test: venv
	@echo "Running tests with coverage..."
	$(PYTEST) $(TEST_DIR) --cov=$(SRC_DIR) --cov-report=term-missing --cov-report=html -v

# Run linting (flake8 + mypy)
.PHONY: lint
lint: lint-flake8 lint-mypy

# Run flake8 linting
.PHONY: lint-flake8
lint-flake8: venv
	@echo "Running flake8 linting..."
	$(FLAKE8) $(SRC_DIR) $(TEST_DIR) $(EXAMPLES_DIR) --max-line-length=88 --extend-ignore=E203,W503

# Run mypy type checking
.PHONY: lint-mypy
lint-mypy: venv
	@echo "Running mypy type checking..."
	$(MYPY) $(SRC_DIR) --ignore-missing-imports

# Format code with black
.PHONY: format
format: venv
	@echo "Formatting code with black..."
	$(BLACK) $(SRC_DIR) $(TEST_DIR) $(EXAMPLES_DIR)

# Check if code is formatted
.PHONY: format-check
format-check: venv
	@echo "Checking code formatting..."
	$(BLACK) --check $(SRC_DIR) $(TEST_DIR) $(EXAMPLES_DIR)

# Run all checks (format, lint, test)
.PHONY: all
all: format lint test
	@echo "All checks completed successfully!"

# Clean up generated files
.PHONY: clean
clean:
	@echo "Cleaning up..."
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage
	rm -rf $(SRC_DIR).egg-info
	rm -rf build
	rm -rf dist
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Development setup (install everything needed)
.PHONY: dev-setup
dev-setup: install-dev
	@echo "Development environment setup complete!"
