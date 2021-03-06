.SILENT: ;
.DEFAULT_GOAL := help
VENV_FOLDER ?=.venv

.PHONY: help
help: ## List all described targets available
	awk -F ':|##' '/^[^\t].+:.*##/ { printf "\033[36m%-28s\033[0m -%s\n", $$1, $$NF }' $(MAKEFILE_LIST) | sort

.PHONY: package
package: ## Make a python package
	rm -rf dist
	python setup.py sdist --formats=gztar,zip bdist_wheel
	twine check dist/*
	echo ""
	echo Package created at `ls dist/*.tar.gz`

.PHONY: venv
venv: ## Create a virtual environment folder for Code-completion and tests inside your IDE
	virtualenv -p python3 $(VENV_FOLDER); \
	source $(VENV_FOLDER)/bin/activate; \
	export AIRFLOW_GPL_UNIDECODE="yes"; \
	pip install -r requirements.txt;

.PHONY: flake8
flake8: ## Run flake8 lint
	flake8 unaflow

