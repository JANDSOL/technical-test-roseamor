.PHONY: up down app eda dq etl reset-db clean-docker

up:
	docker compose up -d --build

down:
	docker compose down

app:
	docker compose up -d --build web

eda:
	docker compose exec python python scripts/eda_console.py $(FLAGS)

dq:
	docker compose exec python python scripts/data_quality.py $(FLAGS)

etl:
	docker compose exec python python scripts/etl_incremental_cdc.py

reset-db:
	docker compose down -v
	docker compose up -d --build

clean-docker:
	docker compose down -v --rmi local --remove-orphans
