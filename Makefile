.PHONY: up down restart logs ps init venv install

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

logs:
	docker compose logs -f

ps:
	docker compose ps

init:
	cp .env.example .env
	@echo "────────────────────────────────────"
	@echo "  .env создан из .env.example"
	@echo "  Заполни пароли: nano .env"
	@echo "  Затем запусти:  make install && make up"
	@echo "────────────────────────────────────"

venv:
	python3.12 -m venv .venv
	@echo "Активируй: source .venv/bin/activate"

install:
	pip install -r requirements.txt