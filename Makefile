.PHONY: up down restart logs test lint refresh clean

# Start all services
up:
	docker-compose up -d

# Stop all services
down:
	docker-compose down

# Restart all services
restart:
	docker-compose down && docker-compose up -d

# Tail logs
logs:
	docker-compose logs -f

# Run tests (assumes postgres is running on 5433)
test:
	python3 -m pytest tests/ -v

# Basic syntax validation for local Python sources
lint:
	python3 -m compileall scripts tests api

# Run pipeline refresh locally
refresh:
	POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=5433 POSTGRES_DB=airflow POSTGRES_USER=airflow POSTGRES_PASSWORD=airflow API_URL=http://127.0.0.1:8000/api/shipments TIERS_CSV_PATH=data/customer_tiers.csv python3 scripts/extract_shipments.py && \
	POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=5433 POSTGRES_DB=airflow POSTGRES_USER=airflow POSTGRES_PASSWORD=airflow TIERS_CSV_PATH=data/customer_tiers.csv python3 scripts/extract_customer_tiers.py && \
	POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=5433 POSTGRES_DB=airflow POSTGRES_USER=airflow POSTGRES_PASSWORD=airflow python3 scripts/transform_data.py && \
	POSTGRES_HOST=127.0.0.1 POSTGRES_PORT=5433 POSTGRES_DB=airflow POSTGRES_USER=airflow POSTGRES_PASSWORD=airflow python3 scripts/load_analytics.py

# Clean up docker volumes
clean:
	docker-compose down -v
