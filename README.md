# de-stack-template

Шаблон локального DE-окружения: ClickHouse + PostgreSQL + Airflow + Python 3.12

> ⚠️ Шаблон рассчитан на Linux / WSL2. На нативном Windows не работает напрямую.

---

## Требования
| Что | Версия |
|-----|--------|
| Windows | 10 (21H2+) или 11 |
| WSL2 + Ubuntu | 22.04+ |
| Docker Engine | 24+ |
| Python | 3.12 |

---

## Быстрый старт (если окружение уже настроено)
```bash
# 1. Клонировать шаблон
git clone git@github.com:dzbarts/de-stack-template.git my-new-project
cd my-new-project

# 2. Отвязать от шаблона — своя git-история
rm -rf .git && git init && git add . && git commit -m "init"

# 3. Поднять окружение
make init                      # создаёт .env из .env.example
nano .env                      # заполни пароли
make venv                      # создаёт виртуальное окружение Python
source .venv/bin/activate      # активировать окружение
make install                   # устанавливает пакеты
make up                        # поднимает все сервисы
```

### Проверить что всё работает
| Сервис | URL | Логин |
|--------|-----|-------|
| Airflow | http://localhost:8080 | admin / пароль из .env |
| ClickHouse | http://localhost:8123/ping | — |
| PostgreSQL | localhost:5432 | admin / пароль из .env |

---

## Стек
| Сервис | Версия | Порт |
|--------|--------|------|
| ClickHouse | 24.3 | 8123, 9000 |
| PostgreSQL | 16 | 5432 |
| Airflow | 2.9.1 | 8080 |
| Python | 3.12 | — |

---

## Структура проекта
```
de-project/
├── infra/
│   └── compose.yaml        # инфраструктура
├── dags/                   # Airflow DAGs
├── src/
│   ├── extractors/         # забираем данные
│   ├── transformers/       # трансформации
│   └── loaders/            # загружаем в хранилище
├── tests/                  # тесты
├── .env.example            # шаблон переменных (коммитится)
├── .env                    # реальные секреты (не коммитится)
├── requirements.txt        # зависимости Python
└── Makefile                # команды управления
```

---

## Makefile команды
| Команда | Действие |
|---------|----------|
| `make up` | поднять все сервисы |
| `make down` | остановить сервисы |
| `make logs` | логи всех сервисов |
| `make ps` | статус сервисов |
| `make init` | создать .env из .env.example |
| `make venv` | создать виртуальное окружение |
| `make install` | установить Python-пакеты |

---

## Установка с нуля (Windows → WSL2)

Нужно сделать **один раз** на новой машине.

### 1 — Включить WSL2

Открой PowerShell от имени администратора:
```powershell
wsl --install
```

Перезагрузи компьютер. Ubuntu установится автоматически.

> Если WSL уже был установлен:
> ```powershell
> wsl --update
> wsl --set-default-version 2
> ```

### 2 — Установить Docker Engine

В терминале Ubuntu:
```bash
sudo apt remove docker docker-engine docker.io containerd runc
sudo apt update
sudo apt install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) \
  signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io \
  docker-buildx-plugin docker-compose-plugin
sudo service docker start
sudo usermod -aG docker $USER
```

Закрой терминал. В PowerShell:
```powershell
wsl --shutdown
```

Открой Ubuntu снова, проверь:
```bash
docker run hello-world
```

### 3 — Установить Python 3.12
```bash
sudo apt update
sudo apt install -y python3.12 python3.12-venv python3-pip
```

### 4 — Настроить Git и SSH
```bash
git config --global user.name "Твоё Имя"
git config --global user.email "твой@email.com"
git config --global pull.rebase true
ssh-keygen -t ed25519 -C "твой@email.com"
cat ~/.ssh/id_ed25519.pub
```

Добавь ключ на GitHub:
`Settings → SSH and GPG keys → New SSH key`

### 5 — VS Code (опционально)

1. Скачай [VS Code](https://code.visualstudio.com) на Windows
2. Установи расширение **Remote - WSL**
3. Открывай проекты: `code .` из терминала Ubuntu