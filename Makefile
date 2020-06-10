.SILENT: ;
.DEFAULT_GOAL := help
# You can override this if you would like staged files in local testing
# For example like "make package PACKAGE_GIT_REF=`git stash create`"
PACKAGE_GIT_REF=HEAD
VENV_FOLDER ?=.venv

.PHONY: help
help: ## List all described targets available
	awk -F ':|##' '/^[^\t].+:.*##/ { printf "\033[36m%-28s\033[0m -%s\n", $$1, $$NF }' $(MAKEFILE_LIST) | sort

.PHONY: package
package: ## Make a python package
	mkdir -p output
	git archive --format=zip --output=output/unaflow.zip $(PACKAGE_GIT_REF):src
	echo Archive created at output/unaflow.zip

.PHONY: venv
venv: ## Create a virtual environment folder for Code-completion and tests inside your IDE
	virtualenv -p python3 $(VENV_FOLDER); \
	source $(VENV_FOLDER)/bin/activate; \
	export AIRFLOW_GPL_UNIDECODE="yes"; \
	pip install -r requirements.txt;

.PHONY: flake8
flake8:
	flake8 src

