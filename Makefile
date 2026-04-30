
NETWORK_NAME=ocean_network

create_network:
	@if ! docker network ls --format '{{.Name}}' | grep -q '^$(NETWORK_NAME)'; then \
		echo "Creating Docker network $(NETWORK_NAME)..."; \
		docker network create $(NETWORK_NAME); \
	else \
		echo "Docker network $(NETWORK_NAME) already exists."; \
	fi

run_postgres: create_network
	docker-compose -f docker-compose.postgres.yml up

run_postgres_test: create_network
	docker-compose -f docker-compose.postgres.test.yml up

start_postgres_test: create_network
	docker-compose -f docker-compose.postgres.test.yml up -d

shell:
	docker-compose run --rm -it ocean_db_client bash

delete_volume:
	docker volume rm oceandb_postgres_data

build_image:
	docker build -f docker_build/Dockerfile -t ocean_db_client:latest .

psql:
	docker exec -it postgres psql -h localhost -p 5432 -U postgres -d ocean2

.PHONY: format lint check start_postgres_test test test-db-create test-db-ingest test-along-track test-eddy-nearalong

format:
	black src/OceanDB
	isort src/OceanDB

lint:
	flake8 src/OceanDB

check: format lint

test: start_postgres_test
	pytest

test-db-create: start_postgres_test
	pytest tests/database/test_create.py

test-db-ingest: start_postgres_test
	pytest tests/database/test_ingest.py

test-along-track: start_postgres_test
	pytest tests/along_track/test_spatiotemporal_queries.py

test-eddy-nearalong: start_postgres_test
	pytest tests/eddy/test_eddy_points_nearalong_track.py
