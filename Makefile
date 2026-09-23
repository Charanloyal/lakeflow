.PHONY: help config up down restart status logs clean verify

help:
	@echo "========================================================================"
	@echo " Data Platform Lab - Operational Commands"
	@echo "========================================================================"
	@echo " make config     - Validate and render docker-compose configuration"
	@echo " make up         - Start all platform services in background"
	@echo " make down       - Stop all platform services"
	@echo " make restart    - Restart all platform services"
	@echo " make status     - Check status and health of all containers"
	@echo " make logs       - Tail logs from all services"
	@echo " make verify     - Run automated health probes across all services"
	@echo " make clean      - Stop containers and remove persistent volumes"
	@echo "========================================================================"

config:
	docker compose config

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose down && docker compose up -d

status:
	docker compose ps

logs:
	docker compose logs -f

test:
	python -m unittest discover -s tests -p "test_*.py" -v

demo:
	python scripts/demo.py

benchmark:
	python apps/benchmarks/benchmark_engine.py

verify:
	python scripts/verify_health.py

clean:
	docker compose down -v --remove-orphans
