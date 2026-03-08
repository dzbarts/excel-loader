.PHONY: up down restart logs ps init venv install test coverage lint fmt

# ── Docker ───────────────────────────────────────────────────────────────────

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

# ── Dev setup ────────────────────────────────────────────────────────────────

init:
	cp .env.example .env
	@echo "────────────────────────────────────"
	@echo " .env создан из .env.example"
	@echo " Заполни пароли: nano .env"
	@echo " Затем запусти: make install && make up"
	@echo "────────────────────────────────────"

venv:
	python3 -m venv .venv
	@echo "Активируй: source .venv/bin/activate"

install:
	pip install -e ".[dev]"

# ── Tests & quality ──────────────────────────────────────────────────────────

test:
	pytest tests/ -v

# DAG-тесты не требуют запущенного Airflow
test-dag:
	pytest tests/test_dag.py -v

# Покрытие — создаёт HTML-отчёт в htmlcov/
coverage:
	pytest tests/ --cov=src/manual_excel_loader --cov-report=term-missing --cov-report=html
	@echo "HTML-отчёт: htmlcov/index.html"

lint:
	ruff check src/ tests/ dags/

fmt:
	ruff format src/ tests/ dags/