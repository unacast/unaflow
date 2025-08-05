.SILENT: ;
.DEFAULT_GOAL := help
VENV_FOLDER ?=.venv
AIRFLOW_VERSION ?= 2.10.5
PYTHON_VERSION=$(shell python --version | cut -d " " -f 2 | cut -d "." -f 1-2)
CONSTRAINT_URL=https://raw.githubusercontent.com/apache/airflow/constraints-$(AIRFLOW_VERSION)/constraints-$(PYTHON_VERSION).txt

.PHONY: help
help: ## List all described targets available
	awk -F ':|##' '/^[^\t].+:.*##/ { printf "\033[36m%-28s\033[0m -%s\n", $$1, $$NF }' $(MAKEFILE_LIST) | sort
	echo "AIRFLOW_VERSION: $(AIRFLOW_VERSION)"
	echo "PYTHON_VERSION: $(PYTHON_VERSION)"

.PHONY: package
package: ## Make a python package
	rm -rf dist
	python setup.py sdist --formats=gztar,zip bdist_wheel
	twine check dist/*
	echo ""
	echo Package created at `ls dist/*.tar.gz`

.PHONY: clear-venv
clear-venv: ## Clear the virtual environment
	rm -rf $(VENV_FOLDER)

.PHONY: venv
venv: ## Create a virtual environment folder for Code-completion and tests inside your IDE
	python -m venv $(VENV_FOLDER); \
	source $(VENV_FOLDER)/bin/activate; \
	export AIRFLOW_GPL_UNIDECODE="yes"; \
	pip install "apache-airflow==$(AIRFLOW_VERSION)" -r requirements.txt -c $(CONSTRAINT_URL);

.PHONY: flake8
flake8: ## Run flake8 lint
	source .venv/bin/activate && flake8 unaflow

.PHONY: run
run: ## Run examples locally
	mkdir -p /tmp/temp-python-path/
	mkdir -p /tmp/temp-airflow-unaflow/
	rm -rf /tmp/temp-python-path/unaflow
	touch /tmp/temp-python-path/__init__.py
	ln -s ${PWD}/unaflow /tmp/temp-python-path/unaflow
	source .venv/bin/activate && AIRFLOW_HOME=/tmp/temp-airflow-unaflow/ \
		AIRFLOW__CORE__DAGS_FOLDER=${PWD}/example/dags \
		AIRFLOW__CORE__LOAD_EXAMPLES=false \
		PYTHONPATH=/tmp/temp-python-path/ \
		airflow standalone

.PHONY: clear-run
clear-run: ## Clear the run folder
	rm -rf /tmp/temp-python-path/
	rm -rf /tmp/temp-airflow-unaflow/