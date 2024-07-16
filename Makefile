build:
	docker compose build

build_and_run:
	docker compose up --build

run:
	docker compose up

run_detach:
	docker compose up -d
