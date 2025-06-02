##########################################
#
# Makefile to build the funland data engineering project
#
##########################################

PROJECT_NAME = funland-data-engineering-pipeline
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PIP :=pip

# create python interpreter enviorment.
create-environment:
	@echo ">>> Creating python virtual enviorment for: $(PROJECT_NAME)..."
	@echo ">>> Checking python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up virtualenvironment..."
	( \
		$(PIP) install -q virtualenv virtualenvwrapper; \
		virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Activate virtualenv
ACTIVATE_ENV := source venv/bin/activate

# Helper to run commands inside venv
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

# Install all requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r requirements.txt)

##################################################################
# Quality, Security & Testing

# Run bandit (security scanner) on every python file
security-test:
	$(call execute_in_env, bandit -lll -r */*.py)

# Run black (code formatter)
run-black:
	$(call execute_in_env, black ./src ./test)

# Run flake8 (code linter)
lint:
	$(call execute_in_env, flake8 --max-line-length=120 src test)

# Run tests
unit-test:
	$(call execute_in_env, PYTHONPATH=$(PYTHONPATH) pytest --testdox -vvrP)

# Run coverage check and create a coverage.txt file
check-coverage:
	$(call execute_in_env, coverage report -m > coverage.txt && rm -f .coverage)

# Vulnerability check
audit:
	$(call execute_in_env, pip-audit)

# Run all tests and checks in one
run-checks: run-black lint security-test unit-test check-coverage audit