# Улучшенная система отчетности

## 🎯 Обзор

Новая unified reporting система предоставляет:

- **Детальные метрики производительности** с percentiles (P50, P90, P95, P99)
- **Error tracking и categorization** (retriable vs permanent errors)
- **SLO compliance tracking** с автоматической валидацией
- **Множественные форматы экспорта** (Text, JSON, CSV)
- **Умные рекомендации** на основе анализа данных
- **Per-user breakdown** для concurrent тестов
- **HTTP request tracking** с детальной статистикой

## 📦 Компоненты

### `MetricsCollector`
Центральный класс для сбора метрик:

```python
from common.report_engine import MetricsCollector

collector = MetricsCollector(test_name="TC-LOAD-001")

# Define SLOs
collector.define_slo("dag1_duration", threshold=300, comparison="less_than")

# Register test runs
collector.register_test_run({
    'success': True,
    'username': 'userNT_1',
    'dag1_duration': 240.5,
    'dag2_duration': 165.3,
    # ... other metrics
})

# Register errors
collector.register_error({
    'type': 'NetworkError',
    'endpoint': 'Upload chunk',
    'retriable': True
})
```

### `ReportGenerator`
Генерирует отчеты в различных форматах:

```python
from common.report_engine import ReportGenerator

generator = ReportGenerator(collector)

# Text report (для консоли/логов)
text_report = generator.generate_text_report()
print(text_report)

# JSON report (для автоматизации)
json_report = generator.generate_json_report()

# CSV report (для Excel/анализа)
csv_report = generator.generate_csv_report()

# Сохранить все форматы
saved_files = generator.save_reports(output_dir="./logs")
```

## 🚀 Быстрый старт

### 1. Базовое использование

```python
from common.report_engine import MetricsCollector, ReportGenerator
import time

# Создать collector
collector = MetricsCollector(test_name="MY_TEST")

# Определить SLOs
collector.define_slo("response_time", threshold=3.0, comparison="less_than")

# Начало теста
collector.set_test_times(time.time())

# Ваш тест...
collector.register_test_run({
    'success': True,
    'response_time': 2.5,
    # ... другие метрики
})

# Конец теста
collector.set_test_times(collector.test_start_time, time.time())

# Генерация отчетов
generator = ReportGenerator(collector)
generator.save_reports()
```

### 2. Интеграция с Locust

```python
from locust import events
from common.report_engine import MetricsCollector, ReportGenerator

_collector = MetricsCollector(test_name="TC-LOAD-001")

@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    import time
    _collector.set_test_times(time.time())
    _collector.define_slo("dag1_duration", 300, "less_than")

@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    import time
    _collector.set_test_times(_collector.test_start_time, time.time())

    # Собрать Locust метрики
    stats = environment.stats
    _collector.locust_metrics = {
        'total_requests': stats.total.num_requests,
        'total_failures': stats.total.num_failures,
        'percentile_95': stats.total.get_response_time_percentile(0.95),
        # ...
    }

    # Генерация отчетов
    generator = ReportGenerator(_collector)
    generator.save_reports()
```

## 📊 Отчеты

### Text Report

```
================================================================================
TC-LOAD-001 - DETAILED REPORT
================================================================================
Generated: 2025-12-07 14:30:15

TEST SUMMARY
--------------------------------------------------
Test Name: TC-LOAD-001
Duration: 1825.5s (30.4 min)
Total Runs: 10
Successful: 10 (100.0%)
Failed: 0

PERFORMANCE METRICS
--------------------------------------------------

DAG #1 Duration (ClickHouse Import):
  Count: 10 runs
  Mean: 245.30s
  Median (P50): 242.50s
  Min: 220.10s | Max: 285.40s
  Std Dev: 18.25s
  Percentiles:
    P75: 255.20s
    P90: 272.30s
    P95: 279.60s
    P99: 284.50s
  Baseline: 240.00s | Difference: +2.2%

SLO COMPLIANCE
--------------------------------------------------
dag1_duration (< 300): 100.0% compliance ✓ PASS
  Compliant: 10/10 runs

ERROR ANALYSIS
--------------------------------------------------
Total Errors: 5
Total Warnings: 2

Error Types:
  - NetworkError: 3 occurrences (RETRIABLE)
    Affected endpoints: Upload chunk
  - RateLimitError: 2 occurrences (RETRIABLE)
    Affected endpoints: Create flow

RECOMMENDATIONS
--------------------------------------------------
⚠ High variance in dag2_duration (std dev: 35.2s, 21.5% of mean).
   Consider investigating performance inconsistency.

✓ No critical issues detected. Performance within expected parameters.
```

### JSON Report

```json
{
  "metadata": {
    "report_format": "json",
    "generated_at": "2025-12-07T14:30:15",
    "test_name": "TC-LOAD-001"
  },
  "statistics": {
    "summary": {
      "total_runs": 10,
      "successful_runs": 10,
      "failed_runs": 0,
      "success_rate": 100.0
    },
    "performance": {
      "dag1_duration": {
        "count": 10,
        "mean": 245.30,
        "median": 242.50,
        "p95": 279.60,
        "p99": 284.50
      }
    },
    "slo_compliance": {
      "dag1_duration": {
        "threshold": 300,
        "compliance_rate": 100.0,
        "passed": true
      }
    }
  }
}
```

### CSV Report

```csv
timestamp,success,username,dag1_duration,dag2_duration,total_duration
2025-12-07T14:00:00,True,userNT_1,242.5,165.3,450.2
2025-12-07T14:08:00,True,userNT_1,238.1,172.5,455.8
...
```

## 🔧 Расширенные возможности

### Error Tracking

```python
# Регистрация ошибки
collector.register_error({
    'type': 'NetworkError',          # Тип ошибки
    'endpoint': 'Upload chunk 15',   # Где произошла
    'message': 'Timeout',            # Сообщение
    'retriable': True,               # Можно ли ретраить
    'status_code': None,             # HTTP код (если есть)
    'retry_attempt': 2               # Номер попытки
})

# Регистрация warning
collector.register_warning({
    'type': 'PerformanceWarning',
    'message': 'Slow response',
    'value': 5.2,
    'threshold': 3.0
})
```

### HTTP Request Tracking

```python
# Регистрация HTTP запроса
collector.register_http_request({
    'method': 'POST',
    'endpoint': '/api/v1/flow/',
    'status_code': 200,
    'duration': 0.245  # секунды
})
```

### Baseline Comparison

```python
# Для TC-LOAD-002 и выше - сравнение с baseline
collector.set_baseline_metrics({
    'csv_upload': 45.0,
    'dag1_duration': 240.0,
    'dag2_duration': 160.0
})

# Отчет автоматически покажет отклонение от baseline
```

### Custom SLOs

```python
# Определить собственные SLOs
collector.define_slo("custom_metric", threshold=100, comparison="less_than")
collector.define_slo("throughput", threshold=1000, comparison="greater_than")

# При генерации отчета будет проверен compliance
```

## 📈 Метрики и Percentiles

Система автоматически вычисляет:

- **P50 (медиана)**: 50% запросов быстрее этого значения
- **P75**: 75% запросов быстрее
- **P90**: 90% запросов быстрее
- **P95**: 95% запросов быстрее (типичный SLO target)
- **P99**: 99% запросов быстрее
- **P99.9**: 99.9% запросов быстрее
- **Mean, Min, Max**: Средне, минимум, максимум
- **Std Dev**: Стандартное отклонение

## 🎓 Примеры

Смотрите `common/report_examples.py` для полных примеров:

- Базовое использование
- Интеграция с Locust
- Error tracking
- Baseline comparison
- Percentile tracking

## 🔄 Миграция с старой системы

### Было (TC-LOAD-001):

```python
class TestMetricsCollector:
    def __init__(self):
        self.test_runs = []

    def register_test_run(self, metrics):
        self.test_runs.append(metrics)

    def generate_summary(self):
        # Ручная агрегация...
```

### Стало:

```python
from common.report_engine import MetricsCollector, ReportGenerator

collector = MetricsCollector(test_name="TC-LOAD-001")
collector.define_slo("dag1_duration", 300, "less_than")

# ... в тесте
collector.register_test_run(metrics)

# ... в конце
generator = ReportGenerator(collector)
generator.save_reports()
```

## 💡 Best Practices

1. **Определяйте SLOs в начале теста**
   ```python
   collector.define_slo("dag1_duration", 300, "less_than")
   ```

2. **Регистрируйте все ошибки с контекстом**
   ```python
   collector.register_error({
       'type': 'NetworkError',
       'endpoint': endpoint_name,
       'retriable': True
   })
   ```

3. **Используйте baseline для regression testing**
   ```python
   collector.set_baseline_metrics(baseline_from_tc_load_001)
   ```

4. **Сохраняйте все форматы отчетов**
   ```python
   generator.save_reports()  # Text, JSON, CSV
   ```

5. **Анализируйте percentiles, не только средние значения**
   - P95/P99 показывают worst-case performance
   - Mean может скрывать outliers

## 🐛 Troubleshooting

**Проблема**: Отчеты не генерируются

**Решение**: Проверьте, что:
- Директория `./logs` существует
- Есть права на запись
- `collector.test_runs` не пустой

**Проблема**: JSON не сериализуется

**Решение**: Используйте `default=str` при ручном json.dumps:
```python
json.dumps(data, default=str)
```

**Проблема**: SLO compliance всегда False

**Решение**: Проверьте имена метрик - должны совпадать:
```python
collector.define_slo("dag1_duration", ...)  # Имя SLO
collector.register_test_run({'dag1_duration': 240})  # То же имя в метриках
```

## 📚 Дополнительная информация

- См. `common/report_engine.py` для полной документации API
- См. `common/report_examples.py` для примеров использования
- См. существующие тесты `scenarios/tc_load_001_baseline.py` для интеграции
