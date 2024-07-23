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
	cargo run --example csv_jdd_to_postgres

example-transform-jdd-normalisation:
	cargo run --example transform_jdd_normalisation

example-csv-to-kafka:
	cargo run --example csv_to_kafka

example-kafka-to-mongo:
	cargo run --example kafka_to_mongo
