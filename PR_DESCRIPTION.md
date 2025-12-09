# Add TC-LOAD-003: Peak Concurrent Load Test

## 📋 Описание

Добавлен новый сценарий нагрузочного тестирования **TC-LOAD-003: Peak Concurrent Load Test** для симуляции максимальной пиковой нагрузки на систему.

## 🎯 Цель теста

Проверить работу системы при максимальной нагрузке с двумя типами пользователей:
- **5 Heavy Users**: выполняют полный ETL pipeline (CSV Upload → DAG#1 → DAG#2)
- **3 Light Users**: работают с Superset UI (открытие дашбордов, фильтры, экспорт)

## 🏗️ Архитектура

### Два класса пользователей

**TC_LOAD_003_Heavy** (ETL операции):
- Загрузка CSV файлов
- Запуск DAG #1 (ClickHouse Import)
- Запуск DAG #2 (PM Dashboard Creation)
- Открытие дашборда
- Регистрация дашборда в DashboardPool для Light users

**TC_LOAD_003_Light** (Superset UI):
- Ожидание появления дашбордов от Heavy users
- Цикличная работа с UI:
  - Открытие дашбордов (weight=5, 50%)
  - Применение фильтров (weight=3, 30%)
  - Экспорт данных (weight=2, 20%)

### Координация

**DashboardPool** - потокобезопасный механизм координации:
- Heavy users регистрируют созданные дашборды
- Light users берут случайные дашборды из пула
- Light users ждут появления дашбордов (timeout: 10 минут)

### Метрики

**TestMetricsCollector003** собирает метрики от обоих типов:
- Heavy: CSV upload, DAG#1, DAG#2, dashboard load times
- Light: dashboard opens, filter applies, exports, response times

## 📁 Изменённые файлы

### Новые файлы:
- `scenarios/tc_load_003_peak.py` - основной код сценария (1113 строк)

### Обновлённые файлы:
- `locustfile.py` - импорт и регистрация задач, startup banner с baseline метриками
- `config.py` - добавлены сценарии в fallback config
- `config_multi.yaml` - добавлены сценарии
- `config_ift.yaml` - добавлены сценарии

## 🚀 Использование

### Полный тест (рекомендуется):
```bash
LOCUST_SCENARIO=tc_load_003 locust -f locustfile.py --users 8
```

### Только Heavy users (для отладки):
```bash
LOCUST_SCENARIO=tc_load_003_heavy locust -f locustfile.py --users 5
```

### Только Light users (для отладки):
```bash
LOCUST_SCENARIO=tc_load_003_light locust -f locustfile.py --users 3
```

## 📊 Измеряемые метрики

### Heavy Users (ETL):
- CSV Upload Time (min/max/avg)
- DAG #1 Duration (min/max/avg)
- DAG #2 Duration (min/max/avg)
- Dashboard Load Time
- Success Rate

### Light Users (Superset UI):
- Dashboard Load Time (min/max/avg/p95/p99)
- Filter Application Time
- Data Export Time
- Total Operations Count
- Superset Response Time

### System-wide:
- ClickHouse concurrent queries
- ClickHouse latency (p50/p95/p99)
- HTTP метрики (RPS, failures, response times)

## ✅ Критерии успеха

### Heavy Users:
- ✓ Success rate > 95%
- ✓ DAG#1 time < baseline × 2
- ✓ DAG#2 time < baseline × 2

### Light Users:
- ✓ Superset response time < 10s
- ✓ No service crashes

## ⚠️ Текущий статус

**Концептуальная структура с заглушками**

Light users используют `time.sleep()` для симуляции операций.

**TODO:**
- [ ] Добавить реальные Superset API endpoints для Light users
- [ ] Протестировать на реальных данных
- [ ] Собрать baseline метрики для больших файлов

## 📝 Commits

- `7df4d9c` - Add TC-LOAD-003: Peak Concurrent Load Test
- `a049e77` - Add TC-LOAD-003 configuration to all config files
- `c254c6a` - Update locustfile.py
- `728ea6c` - Add baseline metrics display for TC-LOAD-003 startup banner

## 🔗 Related

Продолжение работы над нагрузочными тестами:
- TC-LOAD-001: Baseline Load Test (single user)
- TC-LOAD-002: Concurrent Load Test (3 users)
- TC-LOAD-003: Peak Concurrent Load Test (5 heavy + 3 light) ← **этот PR**
