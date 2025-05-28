#############################
#
# Makefile to build the funland data engineering project
#
#############################

PROJECT_NAME = funland-data-engineering-pipeline
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PIP :=pip

# create python interpreter enviorment.
create-enviorment:
	@echo ">>> About to create enviorment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
		$(PIP) install -q virtualenv virtualenvwrapper; \
		virtual venv --python=$(PYTHON_INTERPRETER); \
	)

# Activate virtualenv
ACTIVATE_ENV := source venv/bin/Activate

define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

# Install production requirements
requirements : create-environment
	$(call execute_in_env, $(PIP) install -r requirements.txt)

# Install development tools (black, bandit )
dev-setup:
	$(call execute_in_env, $(PIP) install -r dev-requirements.txt)