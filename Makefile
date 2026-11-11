.PHONY: help venv build up down restart rebuild logs

help:
	@grep -E '^[a-zA-Z_-]+:' $(MAKEFILE_LIST) | cut -d: -f1 | grep -v '^\.PHONY$$' | sort

venv: 
	uv venv
	uv pip install -r requirements.txt

build: 
	docker compose build

up: 
	docker compose up -d

down:
	docker compose down

restart: ## restart a service e.g make restart s=producer
	docker compose restart $(s)

rebuild: 
	docker compose build $(s)
	docker compose up -d --force-recreate $(s)

logs:
	docker compose logs -f --tail 100 $(s)


.DEFAULT_GOAL := help
