docker-build:
	docker compose build

docker-run-after-build:
	docker compose up --build

docker-run:
	make docker-down && docker compose up

docker-run_detach:
	make docker-down && docker compose up -d

docker-down:
	docker compose down --remove-orphans

docker-down-volumes:
	docker compose down --volumes --remove-orphans

docker-clean:
	docker compose down --rmi="all" --volumes

example-csv-jdd-to-postgres:
	cargo run --package lib-etl --example csv_jdd_to_postgres

example-csv-hdd-to-postgres:
	cargo run  --package lib-etl --example csv_hdd_to_postgres

example-transform-jdd-normalisation:
	cargo run --package lib-etl --example transform_jdd_normalisation

example-transform-hdd-deduplication:
	cargo run --package lib-etl --example transform_hdd_deduplication

example-csv-jdd-to-kafka:
	cargo run --package lib-etl --example csv_jdd_to_kafka

example-kafka-jdd-to-mongo:
	cargo run --package lib-etl --example kafka_jdd_to_mongo
