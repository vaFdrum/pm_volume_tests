"""
Locust Load Testing - Main File
Универсальный locustfile для всех сценариев
"""

import locust.runners

locust.runners.MASTER_HEARTBEAT_TIMEOUT = 900
locust.runners.HEARTBEAT_INTERVAL = 750

from locust import HttpUser, between, events


from config import CONFIG, register_tasks, load_multiple_tasks_config
from scenarios.load_test import LoadFlow
from scenarios.process_metrics import ProcessMetricsCalculator
from scenarios.tc_load_001_baseline import TC_LOAD_001_Baseline
from scenarios.tc_load_002_concurrent import TC_LOAD_002_Concurrent
from scenarios.tc_load_003_peak import TC_LOAD_003_Heavy, TC_LOAD_003_Light

# Регистрируем все доступные задачи
register_tasks({
    'load_test': LoadFlow,
    'process_metrics': ProcessMetricsCalculator,
    'tc_load_001': TC_LOAD_001_Baseline,
    'tc_load_002': TC_LOAD_002_Concurrent,
    'tc_load_003_heavy': TC_LOAD_003_Heavy,
    'tc_load_003_light': TC_LOAD_003_Light
})

_tasks = load_multiple_tasks_config()


class SupersetUser(HttpUser):
    host = CONFIG["api"]["base_url"]
    wait_time = between(min_wait=1, max_wait=5)
    tasks = _tasks


@events.test_start.add_listener
def on_test_start_universal(environment, **kwargs):
    """
    Универсальный listener для всех TC-LOAD тестов
    Автоматически определяет какой тест запущен
    """

    # Проверяем какой тест в tasks
    if TC_LOAD_001_Baseline in SupersetUser.tasks:
        print("\n" + "=" * 80)
        print("TC-LOAD-001: BASELINE LOAD TEST STARTED")
        print("=" * 80)
        print(f"Configuration:")
        print(f"  - Test Type: Baseline (single user)")
        print(f"  - CSV File: {CONFIG.get('csv_file_path', 'N/A')}")
        print(
            f"  - ClickHouse Monitoring: {'Enabled' if CONFIG.get('clickhouse', {}).get('enabled', False) else 'Disabled'}")
        print("=" * 80 + "\n")

    elif TC_LOAD_002_Concurrent in SupersetUser.tasks:
        print("\n" + "=" * 80)
        print("TC-LOAD-002: CONCURRENT LOAD TEST STARTED (3 USERS)")
        print("=" * 80)
        print(f"Configuration:")
        print(f"  - Test Type: Concurrent (3 users)")
        print(f"  - CSV File: {CONFIG.get('csv_file_path', 'N/A')}")
        print(
            f"  - ClickHouse Monitoring: {'Enabled' if CONFIG.get('clickhouse', {}).get('enabled', False) else 'Disabled'}")

        # Показываем baseline info для TC-LOAD-002
        baseline_config = CONFIG.get('baseline_metrics', {})
        if baseline_config:
            try:
                import os
                csv_path = CONFIG.get("csv_file_path", "")
                if csv_path and os.path.exists(csv_path):
                    size_mb = os.path.getsize(csv_path) / (1024 * 1024)

                    # Ищем ближайший baseline
                    selected_baseline = None
                    min_diff = float('inf')

                    for key, baseline in baseline_config.items():
                        baseline_size = baseline.get('file_size_mb', 0)
                        diff = abs(size_mb - baseline_size)
                        if diff < min_diff:
                            min_diff = diff
                            selected_baseline = baseline

                    if selected_baseline:
                        print(f"  - Baseline: {selected_baseline.get('file_size_mb', 0)} MB "
                              f"(DAG#1: {selected_baseline.get('dag1_duration', 0):.0f}s, "
                              f"DAG#2: {selected_baseline.get('dag2_duration', 0):.0f}s)")
                    else:
                        print(f"  - Baseline: Not found for {size_mb:.1f} MB file")
            except Exception as e:
                print(f"  - Baseline: Error loading ({e})")
        else:
            print(f"  - Baseline: Not configured (add to config_multi.yaml)")

        print("=" * 80 + "\n")

    elif TC_LOAD_003_Heavy in SupersetUser.tasks or TC_LOAD_003_Light in SupersetUser.tasks:
        print("\n" + "=" * 80)
        print("TC-LOAD-003: PEAK CONCURRENT LOAD TEST STARTED")
        print("=" * 80)
        print(f"Configuration:")
        print(f"  - Test Type: Peak Concurrent")
        print(f"  - Heavy Users: 5 (ETL Pipeline)")
        print(f"  - Light Users: 3 (Superset UI)")
        print(f"  - CSV File: {CONFIG.get('csv_file_path', 'N/A')}")
        print(
            f"  - ClickHouse Monitoring: {'Enabled' if CONFIG.get('clickhouse', {}).get('enabled', False) else 'Disabled'}")

        # Показываем baseline info для TC-LOAD-003
        baseline_config = CONFIG.get('baseline_metrics', {})
        if baseline_config:
            try:
                import os
                csv_path = CONFIG.get("csv_file_path", "")
                if csv_path and os.path.exists(csv_path):
                    size_mb = os.path.getsize(csv_path) / (1024 * 1024)

                    # Ищем ближайший baseline
                    selected_baseline = None
                    min_diff = float('inf')

                    for key, baseline in baseline_config.items():
                        baseline_size = baseline.get('file_size_mb', 0)
                        diff = abs(size_mb - baseline_size)
                        if diff < min_diff:
                            min_diff = diff
                            selected_baseline = baseline

                    if selected_baseline:
                        print(f"  - Baseline: {selected_baseline.get('file_size_mb', 0)} MB "
                              f"(DAG#1: {selected_baseline.get('dag1_duration', 0):.0f}s, "
                              f"DAG#2: {selected_baseline.get('dag2_duration', 0):.0f}s)")
                    else:
                        print(f"  - Baseline: Not found for {size_mb:.1f} MB file")
            except Exception as e:
                print(f"  - Baseline: Error loading ({e})")
        else:
            print(f"  - Baseline: Not configured (add to config_multi.yaml)")

        print("=" * 80 + "\n")

    # Добавляй новые тесты здесь:
    # elif TC_LOAD_XXX_XXX in SupersetUser.tasks:
    #     print("TC-LOAD-XXX banner...")

    else:
        # Fallback для неизвестных тестов
        print("\n" + "=" * 80)
        print("LOAD TEST STARTED")
        print("=" * 80)
        print(f"  - CSV File: {CONFIG.get('csv_file_path', 'N/A')}")
        print("=" * 80 + "\n")
