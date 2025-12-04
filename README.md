# Нагрузочное тестирование Сбер Process Mining (SPM)

## Обзор проекта

Проект для нагрузочного тестирования Process Mining функционала. Система имитирует поведение пользователей при загрузке данных, создании потоков обработки и генерации дашбордов Process Mining.

## Архитектура

```
pm_volume-tests/
├── common/                      # Общие утилиты
│   ├── api.py                  # Базовый API класс с методами ETL
│   ├── auth.py                 # Аутентификация пользователей
│   ├── clickhouse_monitor.py   # Мониторинг ClickHouse метрик
│   ├── csv_utils.py            # Работа с CSV файлами
│   ├── managers.py             # Менеджеры потоков и пользователей
│   └── metrics.py              # Prometheus метрики
├── logs/                       # Логи тестирования
├── scenarios/                  # Сценарии нагрузочного тестирования
│   ├── load_test.py           # Базовый сценарий загрузки
│   ├── process_metrics.py     # Process Mining дашборды
│   ├── tc_load_001_baseline.py # Baseline тест (1 пользователь)
│   └── tc_load_002_concurrent.py # Concurrent тест (3 пользователя)
├── .env                        # Переменные окружения
├── config.py                   # Загрузчик конфигурации
├── config_multi.yaml          # Основная конфигурация
├── config_ift.yaml            # Альтернативная конфигурация
├── locustfile.py              # Главный файл Locust
├── requirements.txt           # Зависимости Python
└── prometheus.yml             # Конфигурация Prometheus
```

## Сценарии тестирования

### TC-LOAD-001: Baseline тест
**Цель:** Установить эталонные метрики производительности для одного пользователя.

```bash
# Запуск
locust -f locustfile.py --tags tc_load_001 -u 1 -r 1 --host=https://your-superset.com
```

Метрики:
- Время загрузки CSV (500MB файл)
- Длительность DAG #1 (импорт в ClickHouse) - SLA < 5 мин
- Длительность DAG #2 (PM Dashboard) - SLA < 3 мин
- Время загрузки дашборда - SLA < 3 сек

### TC-LOAD-002: Concurrent тест

**Цель:** Проверить систему при параллельной работе 3 пользователей.

```bash
# Запуск
locust -f locustfile.py --tags tc_load_002 -u 3 -r 3 --host=https://your-superset.com
```

**SLA критерии:** Не более +50% от baseline метрик.

### Другие сценарии:
- **load_test**: Базовый сценарий многопользовательской загрузки
- **process_metrics**: Создание Process Mining дашбордов
- **mixed**: Комбинированный сценарий

## Быстрый старт

### 1. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 2. Настройка окружения

Создайте файл `.env` на основе примера:

```env
# Базовые настройки
BASE_URL=https://your-superset-instance.com
CSV_FILE_PATH=./process_log.csv

# Учетные данные
USER1_PASSWORD=change_me_1

# ClickHouse
CLICKHOUSE_ENABLED=true
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=default

# Параметры тестирования
MAX_ITERATIONS=1
```

### 3. Подготовка тестовых данных

Создайте CSV файл с тестовыми данными Process Mining:

```bash
#Генерация файла
# Запуск с предопределенными конфигурациями
python3.11 data/PM_file-generator/main.py --config 20GB
python3.11 data/PM_file-generator/main.py --config 30GB
python3.11 data/PM_file-generator/main.py --config 50GB

# Запуск с кастомным размером
python3.11 data/PM_file-generator/main.py --size 5
python3.11 data/PM_file-generator/main.py --size 2.5

# С указанием выходной директории
python3.11 data/PM_file-generator/main.py --config 10GB --output ./my_data/
```

### 4. Запуск тестов

**Baseline тест (один пользователь):**

```bash
locust -f locustfile.py --tags tc_load_001 -u 1 -r 1 --headless -t 30m
```

**Concurrent тест (три пользователя):**

```bash
locust -f locustfile.py --tags tc_load_002 -u 3 -r 3 --headless -t 30m
```

**С указанием конкретного сценария:**

```bash
# Использование переменной окружения
export LOCUST_SCENARIO=tc_load_001
locust -f locustfile.py -u 1 -r 1

# Или через параметр
locust -f locustfile.py --scenario tc_load_002 -u 3 -r 3
```

**С веб-интерфейсом Locust:**

```bash
locust -f locustfile.py --tags tc_load_001 --host=https://your-superset.com
# Откройте http://localhost:8089
```

## Конфигурация

### Основные файлы конфигурации:

1. `config_multi.yaml` - Конфигурация для мультистендов
2. `config_ift.yaml` - Конфигурация для IFT-стендов
3. `.env` - Переменные окружения с секретами

### Ключевые настройки:

```yaml
# В config_multi.yaml или config_ift.yaml
api:
  base_url: FROM_ENV  # Подставляется из BASE_URL в .env
  flow_endpoint: "/etl/api/v1/flow/"

upload_control:
  timeout_small: 300    # 5 минут для небольших файлов
  timeout_large: 3600   # 60 минут для больших файлов
  pm_timeout: 3600      # 1 час для Process Mining

max_iterations: FROM_ENV  # Количество итераций на пользователя

clickhouse:
  enabled: true
  host: FROM_ENV  # CLICKHOUSE_HOST в .env
  port: FROM_ENV  # CLICKHOUSE_PORT в .env (default: 8123)
  user: FROM_ENV  # CLICKHOUSE_USER в .env (default: default)
  password: FROM_ENV  # CLICKHOUSE_PASSWORD в .env
  monitoring_interval: 10  # Интервал сбора метрик в секундах
```

## Мониторинг и метрики

### Prometheus метрики

Система экспортирует метрики на порт 9090:

```bash
# Запуск Prometheus
prometheus --config.file=prometheus.yml

# Просмотр метрик
curl http://localhost:9090/metrics
```

**Основные метрики:**
- `superset_loadtest_requests_total` - количество запросов
- `superset_loadtest_request_duration_seconds` - время выполнения
- `superset_loadtest_chunk_uploads_total` - загрузки чанков
- `superset_loadtest_flow_creations_total` - создание потоков
- `superset_loadtest_active_users` - активные пользователи

### ClickHouse мониторинг

Сбор метрик из `system.tables` ClickHouse:
- Активные запросы
- Использование памяти
- Количество вставленных строк
- HTTP соединения

## Генерация отчетов

После завершения тестов автоматически генерируются отчеты:

**Для TC-LOAD-001:**
```
./logs/tc_load_001_report_20241205_143022.txt
```

**Для TC-LOAD-002:**
```
./logs/tc_load_002_report_20241205_150045.txt
```

**Отчет включает:**
- Агрегированные метрики производительности
- Сравнение с baseline (для TC-LOAD-002)
- SLA валидацию
- ClickHouse метрики
- Рекомендации по оптимизации

## Расширенные сценарии использования

### Тестирование с разными размерами файлов

1. Измените `CSV_FILE_PATH` в `.env`
2. Обновите `baseline_metrics` в `config_multi.yaml` / `config_ift.yaml`
3. Запустите тест

### Пользовательские сценарии

```python
# В scenarios/ создайте новый файл
from common.api import Api
from locust import task

class CustomScenario(Api):
    @task
    def custom_flow(self):
        # Ваша логика тестирования
        pass
```

### Распределенное тестирование

```bash
# Master node
locust -f locustfile.py --master --host=https://your-superset.com

# Worker nodes
locust -f locustfile.py --worker --master-host=192.168.1.100
```

## Устранение неполадок

### Распространенные проблемы:

1. **Ошибка аутентификации**
   - Проверьте `BASE_URL` в `.env`
   - Убедитесь в правильности учетных данных
   - Проверьте доступность Superset инстанса

2. **CSV файл не найден**
   - Укажите полный путь к файлу в `CSV_FILE_PATH`
   - Проверьте права доступа к файлу
   - Убедитесь что файл имеет правильный формат

3. **ClickHouse недоступен**
   - Проверьте `CLICKHOUSE_HOST` и `CLICKHOUSE_PORT`
   - Убедитесь что ClickHouse работает
   - Проверьте сетевые настройки

4. **Тест не останавливается**
   - Проверьте настройки `max_iterations`
   - Убедитесь что все пользователи завершают итерации
   - Проверьте логи в папке `logs/`

## Логирование

Логи сохраняются в формате:
```
./logs/locust_test_2024-12-05.log
```

Уровень логирования настраивается через:

```yaml
# В config_multi.yaml
log_verbose: true
log_debug: false
log_level: "INFO"
```

## Разработка

### Добавление нового теста

1. Создайте класс в `scenarios/`
2. Наследуйтесь от `common.api.Api`
3. Реализуйте методы `@task`
4. Зарегистрируйте в `locustfile.py`
5. Добавьте в конфигурацию сценариев

**Пример:**

```python
# scenarios/tc_load_003_highload.py
from common.api import Api
from locust import task

class TC_LOAD_003_HighLoad(Api):
    @task
    def highload_scenario(self):
        # Логика тестирования высокой нагрузки
        pass
```

```python
# locustfile.py (добавьте)
register_tasks({
    'tc_load_003': TC_LOAD_003_HighLoad
})
```

```yaml
# config_multi.yaml (добавьте)
scenarios:
  tc_load_003:
    - tc_load_003
```

```groovy
# Jenkinsfile (добавьте)
parameters {
    extendedChoice(
        name: 'SELECTED_SCENARIOS',
        type: 'PT_CHECKBOX',
        value: 'tc_load_003'
        defaultValue: '',
        description: 'Выберите сценарии для запуска'
    )
```

## Контакты и поддержка

- Репозиторий тестов:
- Confluence:
- Документация Superset: https://superset.apache.org/
- Документация Locust: https://docs.locust.io/

## Лицензия

Проект предназначен для внутреннего использования.
