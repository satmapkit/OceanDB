
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

shell:
	docker-compose run --rm -it ocean_db_client bash

build_image:
	docker build -f build/Dockerfile -t ocean_db_client:latest .

psql:
	docker exec -it postgres psql -h localhost -p 5432 -U postgres -d ocean_db