.PHONY: help init-env up down clean

help:
	@echo "Usage:"
	@echo "  make init-env      # create .env from template (if missing)"
	@echo "  make up            # start infrastructure in background"
	@echo "  make up-build      # start infrastructure in background, after reassembling the images"
	@echo "  make down          # stop all services (without removing data)"
	@echo "  make clean         # stop services and remove volumes/orphaned containers"

init-env:
	@test -f .env || cp .env.example .env

up:
	docker compose up -d

up-build:
	docker compose up -d --build

down:
	docker compose down

clean:
	docker compose down -v --remove-orphans

