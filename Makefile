.PHONY: up down restart logs ps init venv install

up:
	cd infra && docker compose up -d

down:
	cd infra && docker compose down

restart:
	cd infra && docker compose restart

logs:
	cd infra && docker compose logs -f

ps:
	cd infra && docker compose ps

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