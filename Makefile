.PHONY: up down restart logs ps setup init venv install test coverage lint fmt

# ── Первый запуск ─────────────────────────────────────────────────────────────

# Полный цикл с нуля: создать .env → собрать образ → поднять стек
setup: init
	docker compose up --build -d
	@echo "Ждём завершения инициализации Airflow…"
	@docker compose wait airflow-init || true
	@echo "────────────────────────────────────────────────"
	@echo " ✓ Готово!"
	@echo " Airflow UI : http://localhost:8080"
	@echo " ClickHouse : http://localhost:8123"
	@echo " PostgreSQL : localhost:5432"
	@echo "────────────────────────────────────────────────"

# Создать .env и сгенерировать криптографические ключи (если .env ещё нет)
init:
	@[ -f .env ] && echo ".env уже существует — пропускаю. Удали .env чтобы сбросить." || { \
		cp .env.example .env && \
		FERNET=$$(python3 -c "import base64,os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())") && \
		SECRET=$$(python3 -c "import secrets; print(secrets.token_hex(32))") && \
		sed -i "s|^AIRFLOW_FERNET_KEY=$$|AIRFLOW_FERNET_KEY=$$FERNET|" .env && \
		sed -i "s|^AIRFLOW_SECRET_KEY=$$|AIRFLOW_SECRET_KEY=$$SECRET|" .env && \
		echo "✓ .env создан, ключи сгенерированы автоматически"; \
	}

# ── Docker ────────────────────────────────────────────────────────────────────

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

# ── Dev setup ─────────────────────────────────────────────────────────────────

venv:
	python3 -m venv .venv
	@echo "Активируй: source .venv/bin/activate"

install:
	pip install -e ".[dev]"

# ── Tests & quality ───────────────────────────────────────────────────────────

test:
	pytest tests/ -v

test-dag:
	pytest tests/test_dag.py -v

coverage:
	pytest tests/ --cov=dags/manual_excel_loader --cov-report=term-missing --cov-report=html
	@echo "HTML-отчёт: htmlcov/index.html"

lint:
	ruff check dags/ tests/

fmt:
	ruff format dags/ tests/
