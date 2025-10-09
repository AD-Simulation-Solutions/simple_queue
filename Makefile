# Makefile for simple_queue Python project
# Uses virtual environment for all operations

# Variables
VENV_DIR = ./venv
PYTHON = $(VENV_DIR)/bin/python
PIP = $(VENV_DIR)/bin/pip
PYTEST = $(PYTHON) -m pytest
AUTOPEP8 = $(PYTHON) -m autopep8
FLAKE8 = $(PYTHON) -m flake8
MYPY = $(PYTHON) -m mypy
PYLINT = $(PYTHON) -m pylint

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
	@echo "  lint        - Run all linting (flake8, pylint, mypy)"
	@echo "  format      - Format code with autopep8"
	@echo "  format-check- Check if code is formatted"
	@echo "  integration-test - Run integration tests with Redis"
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
	$(PIP) install -r dev-requirements.txt
	$(PIP) install -e .

# Run tests with coverage
.PHONY: test
test: venv
	@echo "Running tests..."
	$(PYTEST) $(TEST_DIR) --cov=$(SRC_DIR) --cov-report=html --cov-report=term

# Run linting (flake8, pylint, mypy)
.PHONY: lint
lint: venv
	@echo "Running flake8 linting..."
	$(FLAKE8) $(SRC_DIR) $(TEST_DIR) $(EXAMPLES_DIR)
	@echo "Running pylint linting..."
	$(PYLINT) $(SRC_DIR) $(EXAMPLES_DIR)
	@echo "Running mypy type checking..."
	$(MYPY) $(SRC_DIR) --ignore-missing-imports

# Format code with autopep8
.PHONY: format
format: venv
	@echo "Formatting code with autopep8..."
	$(AUTOPEP8) --in-place --recursive $(SRC_DIR) $(TEST_DIR) $(EXAMPLES_DIR)

# Check if code is formatted
.PHONY: format-check
format-check: venv
	@echo "Checking code formatting..."
	$(AUTOPEP8) --diff --recursive $(SRC_DIR) $(TEST_DIR) $(EXAMPLES_DIR)

# Run all checks (format, lint, test)
.PHONY: all
all: format lint test

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
