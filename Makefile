
ifneq (,$(wildcard .env))
include .env
export
endif

NETWORK_NAME=ocean_network

ifneq ("$(wildcard .env)","")
include .env
export
endif

create_network:
	@if ! docker network ls --format '{{.Name}}' | grep -q '^$(NETWORK_NAME)'; then \
		echo "Creating Docker network $(NETWORK_NAME)..."; \
		docker network create $(NETWORK_NAME); \
	else \
		echo "Docker network $(NETWORK_NAME) already exists."; \
	fi

run_postgres: create_network
	@if [ -z "$(POSTGRES_DATA_DIRECTORY)" ]; then \
		echo "POSTGRES_DATA_DIRECTORY is not set."; \
		echo "Set it in .env or your shell before running make run_postgres."; \
		exit 1; \
	fi
	docker-compose -f docker-compose.postgres.yml up

run_postgres_test: create_network
	docker-compose -f docker-compose.postgres.test.yml up

start_postgres_test: create_network
	docker-compose -f docker-compose.postgres.test.yml up -d

shell:
	docker-compose run --rm -it ocean_db_client bash

delete_volume:
	@echo "OceanDB now uses a bind-mounted PostgreSQL data directory configured in .env."
	@echo "Remove the old named volume manually if you still have it:"
	@echo "docker volume rm oceandb_postgres_data"

build_image:
	docker build -f docker_build/Dockerfile -t ocean_db_client:latest .

psql:
#<<<<<<< Updated upstream
#	psql "host=$(POSTGRES_HOST) port=$(POSTGRES_PORT) user=$(POSTGRES_USERNAME) password=$(POSTGRES_PASSWORD) dbname=$(POSTGRES_DATABASE)"
#=======
	docker exec -it postgres psql -h $(POSTGRES_HOST) -p $(POSTGRES_PORT) -U $(POSTGRES_USERNAME) -d $(POSTGRES_DATABASE)
#>>>>>>> Stashed changes

.PHONY: format lint check start_postgres_test test test-db-create test-db-ingest test-along-track test-eddy-nearalong

format:
	black src/OceanDB
	isort src/OceanDB
	black tests/
	isort tests/

lint:
	flake8 src/OceanDB

check: format lint

test: start_postgres_test
	PYTHONPATH=src:. pytest

test-db-create: start_postgres_test
	PYTHONPATH=src:. pytest tests/database/test_create.py

test-db-ingest: start_postgres_test
	PYTHONPATH=src:. pytest tests/database/test_ingest.py

test-along-track: start_postgres_test
	PYTHONPATH=src:. pytest tests/along_track/test_spatiotemporal_queries.py

test-eddy-nearalong: start_postgres_test
	PYTHONPATH=src:. pytest tests/eddy/test_eddy_points_nearalong_track.py
