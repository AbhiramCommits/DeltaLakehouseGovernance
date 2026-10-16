SHELL := /bin/bash
.DEFAULT_GOAL := help

PY      ?= .venv/bin/python
PIP     ?= .venv/bin/pip
VENV    := .venv
export PYTHONPATH := src

# CI uses DLG_SCALE=small to keep the data-tests job under a few minutes.
SCALE   ?= full
RESULTS := results

.PHONY: help setup gen-data bronze silver gold stream quality governance \
        agent-eval serve test lint baseline-update all clean

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

setup: ## Create the virtualenv and install the project (editable) with dev extras
	python3.11 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -e ".[dev]"
	@$(PY) -c "import dlg; print('dlg', dlg.__version__, 'ready')"

gen-data: ## Generate the synthetic landing zone (set SCALE=small for CI)
	$(PY) -m dlg.cli gen-data --scale $(SCALE)

bronze: ## Ingest landing files into bronze Delta tables
	$(PY) -m dlg.cli bronze

silver: ## Cleanse, type, dedup and MERGE landing data into silver Delta tables
	$(PY) -m dlg.cli silver

gold: ## Build the gold marts from silver
	$(PY) -m dlg.cli gold

stream: ## Run the incremental Structured Streaming ingest once (availableNow)
	$(PY) -m dlg.cli stream --once

quality: ## Run DQ expectations, SLA checks and observability snapshot
	$(PY) -m dlg.cli quality-run
	$(PY) -m dlg.cli sla-check
	$(PY) -m dlg.cli observe

governance: ## Validate the catalog, rebuild lineage, apply policies
	$(PY) -m dlg.cli catalog-validate
	$(PY) -m dlg.cli policies-apply
	$(PY) -m dlg.cli lineage

agent-eval: ## Run the versioned agent eval suite
	$(PY) -m dlg.cli agent-eval

serve: ## Serve the agentic analytics API (uvicorn, port 8000)
	$(PY) -m uvicorn dlg.agent.service:app --host 127.0.0.1 --port 8000

test: ## Run the pytest suite with coverage
	$(PY) -m pytest --cov=dlg --cov-report=term-missing

lint: ## Ruff lint + format check
	$(PY) -m ruff check src tests
	$(PY) -m ruff format --check src tests

baseline-update: ## Recompute the regression baseline from the current gold tables
	$(PY) -m dlg.cli baseline-update

all: gen-data bronze silver gold governance quality stream agent-eval test ## Full pipeline end to end
	@echo "== full pipeline complete =="

clean: ## Remove local runtime artefacts (lakehouse, landing, checkpoints)
	rm -rf lakehouse landing _checkpoints spark-warehouse derby.log metastore_db
