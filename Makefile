.SILENT: ;
BROKKR_PLUGINS = help/help@v0.3.5
.DEFAULT_GOAL := help
PACKAGE_VERSION=
PACKAGE_GIT_REF=HEAD
VENV_FOLDER ?=.venv

package: ## Make a python package
	mkdir -p output
	$(if $(value PACKAGE_VERSION),, $(error PACKAGE_VERSION environment variable is not set.))
	git archive --format=zip --output=output/unaflow-$(PACKAGE_VERSION).zip $(PACKAGE_GIT_REF):src

venv: ## Create a virtual environment folder for Code-completion and tests inside your IDE
	virtualenv -p python3 $(VENV_FOLDER); \
	source $(VENV_FOLDER)/bin/activate; \
	export AIRFLOW_GPL_UNIDECODE="yes"; \
	pip install -r requirements.txt;

flake8:
	flake8 src

-include ./brokkr/brokkr.mk

