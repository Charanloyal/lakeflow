PY ?= python3
CTL = $(PY) scripts/lakeflowctl.py
PROFILE ?=
PROFILE_ARG = $(if $(PROFILE),--profile $(PROFILE),)

.PHONY: help bootstrap up down clean demo test integration-test benchmark status doctor logs migrate lint maintenance backfill

help: ## List targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "}; {printf "  %-18s %s\n", $$1, $$2}'

bootstrap: ## Check prerequisites, generate .env secrets, pull and build images
	$(CTL) $(PROFILE_ARG) bootstrap

up: ## Start the platform for the selected profile (PROFILE=8gb|16gb) and wait for health
	$(CTL) $(PROFILE_ARG) up

demo: ## Guided CLI demo: insert, update, crash Spark, delete, verify uniqueness
	$(CTL) $(PROFILE_ARG) demo

test: ## Unit tests (stdlib) + API and PySpark tests in containers
	$(CTL) $(PROFILE_ARG) test

integration-test: ## Integration + end-to-end tests against the running stack
	$(CTL) $(PROFILE_ARG) integration-test

benchmark: ## Deterministic benchmark; raw JSON lands in benchmarks/results/
	$(CTL) $(PROFILE_ARG) benchmark

down: ## Stop containers (keeps data volumes)
	$(CTL) $(PROFILE_ARG) down

clean: ## Stop containers and delete all data volumes
	$(CTL) $(PROFILE_ARG) down --volumes

status: ## Container status
	$(CTL) $(PROFILE_ARG) status

doctor: ## Prerequisite and health diagnostics
	$(CTL) $(PROFILE_ARG) doctor

logs: ## Tail logs (SERVICE=spark)
	$(CTL) $(PROFILE_ARG) logs $(SERVICE)

migrate: ## Apply source schema migrations (contract v2 channel column)
	$(CTL) $(PROFILE_ARG) migrate

maintenance: ## Compact, expire snapshots and remove orphan files now (same code as the Airflow DAG)
	$(CTL) $(PROFILE_ARG) maintenance

backfill: ## Debezium incremental snapshot: make backfill CONTRACT=orders [KEYS="uuid ..."]
	$(CTL) $(PROFILE_ARG) backfill $(or $(CONTRACT),orders) $(if $(KEYS),--keys $(KEYS),)

lint: ## Python lint and format check
	ruff check . && ruff format --check .
