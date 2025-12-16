"""
TC-LOAD-002: Concurrent Load Test (3 Users)
Параллельная загрузка 3 пользователями - средняя нагрузка
"""

import logging
import random
import time
import urllib3
from datetime import datetime
from typing import Optional, List, Dict
from threading import Lock

from locust import task, between, events

from common.auth import establish_session
from common.api.load_api import LoadApi
from common.csv_utils import count_chunks, count_csv_lines
from common.managers import UserPool
from common.clickhouse_monitor import ClickHouseMonitor
from common.report_engine import MetricsCollector, ReportGenerator  # 🆕 Новая система отчетности
from config import CONFIG

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ============================================================================
# 🆕 ENHANCED REPORTING SYSTEM
# ============================================================================
# Создаем глобальный collector для TC-LOAD-002 (Concurrent Test)
# Используем новую систему с автоматическими percentiles, SLO tracking и baseline comparison
_metrics_collector = MetricsCollector(test_name="TC-LOAD-002")


# ============================================================================
# 📊 SLO DEFINITIONS FOR TC-LOAD-002 (Concurrent Test)
# ============================================================================
# TC-LOAD-002 проверяет производительность при параллельной работе 3 пользователей
# SLO критерий из README.md: "Не более +50% от baseline метрик"
#
# ⚙️ КАК НАСТРОИТЬ ПОСЛЕ ПОЛУЧЕНИЯ РЕАЛЬНЫХ ДАННЫХ:
#
# ШАГИ НАСТРОЙКИ:
# 1. Запустите TC-LOAD-001 и получите baseline метрики
# 2. Посмотрите в отчете TC-LOAD-001 значения P95 для каждой метрики
# 3. Установите SLO для TC-LOAD-002 = P95_baseline * 1.5 (добавляем 50% как в README)
# 4. Обновите значения ниже
#
# Пример расчета:
#   TC-LOAD-001 отчет показывает "DAG #1 P95: 280.5s"
#   TC-LOAD-002 SLO = 280.5 * 1.5 = 420.75 ≈ 425 секунд
#   Это означает: при 3 параллельных пользователях допустимо замедление до +50%
#
# 📌 ВАЖНО: Сначала запустите TC-LOAD-001 для получения baseline!
# ============================================================================

# SLO #1: DAG #1 Duration для Concurrent теста
# 📝 Описание: Время импорта CSV в ClickHouse при 3 параллельных пользователях
# 🎯 Обновлено: 2025-12-16 на основе нового baseline TC-LOAD-001
# 📊 Baseline из TC-LOAD-001: 55.6s P95 (среднее из 8 запусков)
# ✏️ Как изменить: threshold = (P95 из TC-LOAD-001) * 1.5
_metrics_collector.define_slo(
    name="dag1_duration",
    threshold=84.0,                  # P95_baseline (55.6s) × 1.5 = 83.4s ≈ 84.0s
    comparison="less_than"
)

# SLO #2: DAG #2 Duration для Concurrent теста
# 📝 Описание: Время создания PM дашборда при 3 параллельных пользователях
# 🎯 Обновлено: 2025-12-16 на основе нового baseline TC-LOAD-001
# 📊 Baseline из TC-LOAD-001: 106.4s P95 (среднее из 8 запусков)
# ✏️ Как изменить: threshold = (P95 из TC-LOAD-001) * 1.5
_metrics_collector.define_slo(
    name="dag2_duration",
    threshold=159.0,                 # P95_baseline (106.4s) × 1.5 = 159.6s ≈ 159.0s
    comparison="less_than"
)

# SLO #3: Dashboard Load для Concurrent теста
# 📝 Описание: Время загрузки дашборда при 3 параллельных пользователях
# 🎯 Обновлено: 2025-12-16 на основе нового baseline TC-LOAD-001
# 📊 ВАЖНО: Метрика крайне нестабильна в baseline!
#    - Реальный P95 baseline: 1.3s
#    - Диапазон baseline: 0.6s-8.4s (14× вариация!)
#    - Консервативный baseline: 2.5s (1.92× от реального P95)
#    - Эффективный множитель от реального P95: 2.92× (вместо стандартных 1.5×)
# ✏️ Обоснование: Высокая нестабильность + concurrent нагрузка требуют мягкого порога
_metrics_collector.define_slo(
    name="dashboard_duration",
    threshold=3.8,                   # Real P95 (1.3s) → Safe baseline (2.5s) × 1.5 = 3.75s ≈ 3.8s
    comparison="less_than"
)

# SLO #4: CSV Upload Time для Concurrent теста
# 📝 Описание: Время загрузки CSV файла при 3 параллельных пользователях
# 🎯 Обновлено: 2025-12-16 на основе нового baseline TC-LOAD-001
# 📊 Baseline из TC-LOAD-001: 68.2s P95 (среднее из 8 запусков, исключены аномалии)
_metrics_collector.define_slo(
    name="csv_upload_duration",
    threshold=102.0,                 # P95_baseline (68.2s) × 1.5 = 102.3s ≈ 102.0s
    comparison="less_than"
)

# SLO #5: Total Scenario Duration для Concurrent теста
# 📝 Описание: Полное время выполнения сценария при 3 параллельных пользователях
# 🎯 Обновлено: 2025-12-16 на основе нового baseline TC-LOAD-001
# 📊 Baseline из TC-LOAD-001: 227.9s P95 (среднее из 8 запусков)
_metrics_collector.define_slo(
    name="total_duration",
    threshold=342.0,                 # P95_baseline (227.9s) × 1.5 = 341.85s ≈ 342.0s
    comparison="less_than"
)

# ============================================================================
# 📊 BASELINE METRICS SETUP
# ============================================================================
# Baseline метрики автоматически загружаются из config_multi.yaml
# См. секцию 'baseline_metrics' в config файле
#
# После запуска TC-LOAD-001 обновите config_multi.yaml:
# baseline_metrics:
#   "500mb":
#     csv_upload: <значение из TC-LOAD-001>
#     dag1_duration: <значение из TC-LOAD-001>
#     dag2_duration: <значение из TC-LOAD-001>
#     dashboard_load: <значение из TC-LOAD-001>
# ============================================================================


def get_metrics_collector_002() -> MetricsCollector:
    """Возвращает глобальный metrics collector для TC-LOAD-002"""
    return _metrics_collector


class TC_LOAD_002_Concurrent(LoadApi):
    """
    TC-LOAD-002: Concurrent Load Test

    Сценарий:
    - 3 пользователя одновременно загружают CSV
    - Каждый запускает DAG #1 (ClickHouse import)
    - Каждый запускает DAG #2 (PM dashboard)
    - Каждый открывает свой дашборд

    Цель: Проверить работу системы при параллельной работе нескольких пользователей
    """

    wait_time = between(min_wait=1, max_wait=3)

    def __init__(self, parent):
        super().__init__(parent)
        self.user_id = f"concurrent_user_{random.randint(10000, 99999)}"
        self.session_id = f"concurrent_{random.randint(1000, 9999)}"
        self.logged_in = False
        self.session_valid = False
        self.total_chunks = count_chunks(CONFIG["csv_file_path"], CONFIG["chunk_size"])
        self.total_lines = count_csv_lines(CONFIG["csv_file_path"])
        self.worker_id = 0
        self.username = None
        self.password = None
        self.flow_id = None
        self.pm_flow_id = None

        # ClickHouse мониторинг (только первый пользователь инициализирует)
        self.ch_monitor: Optional[ClickHouseMonitor] = None
        self._init_clickhouse_monitor()

        # Метрики для отчёта
        self.test_start_time = None
        self.csv_upload_duration = 0
        self.dag1_duration = 0
        self.dag2_duration = 0
        self.dashboard_duration = 0
        self.total_duration = 0

    def _init_clickhouse_monitor(self):
        """Инициализирует ClickHouse монитор если включен (только первый пользователь)"""
        ch_config = CONFIG.get("clickhouse", {})

        if not ch_config.get("enabled", False):
            self.log("[TC-LOAD-002] ClickHouse monitoring disabled")
            return

        # Проверяем, не инициализирован ли уже
        if get_metrics_collector_002().clickhouse_monitor is not None:
            self.log("[TC-LOAD-002] ClickHouse monitor already initialized by another user")
            return

        try:
            self.ch_monitor = ClickHouseMonitor(
                host=ch_config.get("host", "localhost"),
                port=ch_config.get("port", 8123),
                user=ch_config.get("user", "default"),
                password=ch_config.get("password", ""),
                monitoring_interval=ch_config.get("monitoring_interval", 10)
            )

            if self.ch_monitor.check_connection():
                self.log("[TC-LOAD-002] ClickHouse monitor initialized successfully")
                # Регистрируем в глобальном collector
                get_metrics_collector_002().set_clickhouse_monitor(self.ch_monitor)
            else:
                self.log("[TC-LOAD-002] ClickHouse connection failed, monitoring disabled", logging.WARNING)
                self.ch_monitor = None

        except Exception as e:
            self.log(f"[TC-LOAD-002] Failed to initialize ClickHouse monitor: {e}", logging.ERROR)
            self.ch_monitor = None

    def _format_file_size(self) -> str:
        """Форматирует размер файла для отчёта"""
        try:
            import os
            csv_path = CONFIG.get("csv_file_path", "")
            if csv_path and os.path.exists(csv_path):
                size_bytes = os.path.getsize(csv_path)
                size_mb = size_bytes / (1024 * 1024)
                return f"{size_mb:.1f} MB"
        except Exception:
            pass
        return "N/A"

    def _log_msg(self, message: str, level=logging.INFO):
        """Helper для упрощения логирования с автоматическим префиксом [TC-LOAD-002][username]"""
        self.log(f"[TC-LOAD-002][{self.username}] {message}", level)

    def _register_failure(self, reason: str):
        """
        Регистрирует неудачное выполнение сценария в метриках
        Используется для всех early returns чтобы правильно считать success rate
        """
        get_metrics_collector_002().register_test_run({
            'success': False,
            'username': self.username,
            'error': reason,
            'csv_upload_duration': self.csv_upload_duration,
            'dag1_duration': self.dag1_duration,
            'dag2_duration': self.dag2_duration,
        })
        self._log_msg(f"Scenario failed: {reason}", logging.ERROR)

    def establish_session(self):
        """Establish user session with authentication"""
        success = establish_session(
            client=self.client,
            username=self.username,
            password=self.password,
            session_id=self.session_id,
            log_function=self.log
        )

        if success:
            self.logged_in = True
            self.session_valid = True
            self.log(f"[TC-LOAD-002] Authentication successful for {self.username}")
        else:
            self.log("[TC-LOAD-002] Authentication failed", logging.ERROR)
            self.interrupt()

    def on_start(self):
        """Initialize concurrent test"""
        runner = getattr(self, "environment", None)
        if runner:
            runner = getattr(runner, "runner", None)
            self.worker_id = getattr(runner, "worker_id", 0) if runner else 0

        creds = UserPool.get_credentials()
        self.username = creds["username"]
        self.password = creds["password"]
        self.client.verify = False

        self.establish_session()
        self.log(f"[TC-LOAD-002] Concurrent test started for user: {self.username}")

        # Устанавливаем время старта в глобальном collector
        get_metrics_collector_002().set_test_times(time.time(), time.time())

        # Стартуем ClickHouse мониторинг (только первый пользователь)
        if self.ch_monitor:
            self.ch_monitor.collect_baseline()
            self.ch_monitor.start_monitoring()

    def on_stop(self):
        """Clean up when user stops"""
        self.log(f"[TC-LOAD-002] User {self.username} stopped")

    @task
    def run_concurrent_scenario(self):
        """
        Основной сценарий TC-LOAD-002:
        - CSV Upload
        - DAG #1: File import to ClickHouse
        - DAG #2: PM dashboard creation
        - Dashboard interaction
        """

        if not self.logged_in:
            self.establish_session()
            if not self.logged_in:
                self._register_failure("authentication_failed")
                return

        self._log_msg("Starting concurrent scenario")
        self.test_start_time = time.time()
        scenario_start = time.time()

        try:
            # ========== PHASE 1: CSV Upload & File Import Flow ==========
            self._log_msg("[PHASE 1] CSV Upload & File Import")
            phase1_start = time.time()

            # 1. Создание flow для загрузки файла
            flow_name, flow_id = self._create_flow(worker_id=self.worker_id)
            self.flow_id = flow_id

            if not flow_id:
                self._register_failure("flow_creation_failed")
                return

            self._log_msg(f"File flow created: {flow_name} (ID: {flow_id})")

            # 2. Получение параметров DAG
            target_connection, target_schema = self._get_dag_import_params(flow_id)
            if not target_connection or not target_schema:
                self._register_failure("missing_dag_parameters")
                return

            # 3. Обновление flow перед загрузкой
            update_resp = self._update_flow(
                flow_id,
                flow_name,
                target_connection,
                target_schema,
                file_uploaded=False,
                count_chunks_val=self.total_chunks,
            )
            if not update_resp or not update_resp.ok:
                self._register_failure("flow_update_failed")
                return

            # 4. Получение ID базы данных пользователя
            db_id = self._get_user_database_id()
            if not db_id:
                self._register_failure("user_database_not_found")
                return

            if self.total_chunks == 0:
                self._register_failure("no_chunks_to_upload")
                return

            timeout = (
                CONFIG["upload_control"]["timeout_large"]
                if self.total_chunks > CONFIG["upload_control"]["chunk_threshold"]
                else CONFIG["upload_control"]["timeout_small"]
            )

            # 5. Начало загрузки
            csv_upload_start = time.time()
            if not self._start_file_upload(flow_id, db_id, target_schema, self.total_chunks, timeout):
                self._register_failure("start_file_upload_failed")
                return

            # 6. Загрузка чанков
            uploaded_chunks = self._upload_chunks(flow_id, db_id, target_schema, self.total_chunks)
            csv_upload_duration = time.time() - csv_upload_start
            self.csv_upload_duration = csv_upload_duration
            self._log_msg(f"CSV upload completed: {uploaded_chunks}/{self.total_chunks} chunks in {csv_upload_duration:.2f}s")

            # 7. Финализация загрузки
            if not self._finalize_file_upload(flow_id, uploaded_chunks, timeout):
                self._register_failure("finalize_file_upload_failed")
                return

            # ========== DAG #1: File Processing (ClickHouse Import) ==========
            self._log_msg("[PHASE 2] DAG #1: ClickHouse Import")
            dag1_start = time.time()

            # 8. Начало обработки файла
            file_run_id = self._start_file_processing(
                flow_id, target_connection, target_schema, self.total_chunks, timeout
            )
            if not file_run_id:
                self._register_failure("start_file_processing_failed")
                return

            # 9. Мониторинг статуса обработки файла
            file_processing_start = time.time()
            success = self._monitor_processing_status(
                file_run_id, timeout, flow_id, db_id, target_schema,
                self.total_lines, file_processing_start, is_pm_flow=False
            )

            if not success:
                self._register_failure("dag1_processing_failed")
                return

            dag1_duration = time.time() - dag1_start
            self.dag1_duration = dag1_duration
            phase1_duration = time.time() - phase1_start
            self._log_msg(f"DAG #1 completed in {dag1_duration:.2f}s")
            self._log_msg(f"[PHASE 1] Completed in {phase1_duration:.2f}s")

            # ========== PHASE 2: Process Mining Flow ==========
            self._log_msg("[PHASE 3] DAG #2: Process Mining Dashboard")
            phase2_start = time.time()

            # 10. Получаем параметры для PM блока
            source_connection, source_schema = self._get_dag_pm_params(flow_id)
            if not all([source_connection, source_schema]):
                self._register_failure("missing_pm_dag_parameters")
                return

            # 11. Создаем PM flow
            table_name = f"Tube_{flow_id}"
            pm_flow_name, pm_flow_id = self._create_pm_flow(
                worker_id=self.worker_id,
                source_connection=source_connection,
                source_schema=source_schema,
                table_name=table_name,
                base_flow_name=flow_name
            )

            if not pm_flow_id:
                self._register_failure("pm_flow_creation_failed")
                return

            self.pm_flow_id = pm_flow_id
            self._log_msg(f"PM Flow created: {pm_flow_name} (ID: {pm_flow_id})")

            # 12. Запускаем Process Mining flow (DAG #2)
            dag2_start = time.time()
            pm_run_id = self._start_pm_flow(
                pm_flow_id, source_connection, source_schema, table_name
            )

            if not pm_run_id:
                self._register_failure("start_pm_flow_failed")
                return

            # 13. Мониторинг статуса Process Mining
            pm_timeout = CONFIG["upload_control"]["pm_timeout"]
            pm_result = self._monitor_processing_status(
                pm_run_id, pm_timeout, pm_flow_id, is_pm_flow=True
            )

            if not (isinstance(pm_result, dict) and pm_result.get("success")):
                self._register_failure("dag2_processing_failed")
                return

            dag2_duration = time.time() - dag2_start
            self.dag2_duration = dag2_duration
            self._log_msg(f"DAG #2 completed in {dag2_duration:.2f}s")

            # ========== PHASE 3: Dashboard Interaction ==========
            self._log_msg("[PHASE 4] Dashboard Interaction")

            # 14. Получаем block_run_ids и открываем дашборд
            block_run_ids = pm_result.get("block_run_ids", {})
            target_block_id = "spm_dashboard_creation_v_0_2[0]"
            block_run_id = block_run_ids.get(target_block_id)

            if block_run_id:
                # Получаем URL дашборда из артефактов
                dashboard_url = self._get_dashboard_url_from_artefacts(
                    pm_flow_id=pm_flow_id,
                    block_id=target_block_id,
                    block_run_id=block_run_id,
                    run_id=pm_run_id
                )

                if dashboard_url:
                    # Открываем дашборд
                    dashboard_start = time.time()
                    dashboard_loaded = self._open_dashboard(dashboard_url)
                    dashboard_duration = time.time() - dashboard_start
                    self.dashboard_duration = dashboard_duration

                    if dashboard_loaded:
                        self._log_msg(f"Dashboard loaded in {dashboard_duration:.2f}s: {dashboard_url}")
                    else:
                        self._log_msg("Failed to load dashboard", logging.WARNING)
                else:
                    self._log_msg("Could not retrieve dashboard URL", logging.WARNING)
            else:
                self._log_msg(f"block_run_id not found for {target_block_id}", logging.WARNING)

            phase2_duration = time.time() - phase2_start
            self._log_msg(f"[PHASE 3] Completed in {phase2_duration:.2f}s")

            # ========== Scenario Complete ==========
            total_duration = time.time() - scenario_start
            self.total_duration = total_duration
            self._log_msg(
                f"Concurrent scenario completed successfully in {total_duration:.2f}s "
                f"(CSV: {self.csv_upload_duration:.2f}s, DAG#1: {self.dag1_duration:.2f}s, DAG#2: {self.dag2_duration:.2f}s)"
            )

            # ========== Регистрируем метрики в глобальном collector ==========
            get_metrics_collector_002().register_test_run({
                'success': True,
                'username': self.username,
                'flow_id': self.flow_id,
                'pm_flow_id': self.pm_flow_id,
                'csv_upload_duration': self.csv_upload_duration,
                'dag1_duration': self.dag1_duration,
                'dag2_duration': self.dag2_duration,
                'dashboard_duration': self.dashboard_duration,
                'total_duration': self.total_duration,
                'file_size': self._format_file_size(),
                'total_lines': self.total_lines,
                'total_chunks': self.total_chunks,
            })

            # Обновляем время окончания
            get_metrics_collector_002().set_test_times(self.test_start_time, time.time())

        except Exception as e:
            self._log_msg(f"Unexpected error in concurrent scenario: {str(e)}", logging.ERROR)

            # Регистрируем failed run
            get_metrics_collector_002().register_test_run({
                'success': False,
                'username': self.username,
                'error': str(e),
            })


# ========== Locust Event Listeners ==========

@events.test_stop.add_listener
def on_test_stop_002(environment, **kwargs):
    """Вызывается при завершении TC-LOAD-002 - генерируем общий отчёт"""

    # Проверяем что TC-LOAD-002 запущен
    try:
        from locustfile import SupersetUser
        if TC_LOAD_002_Concurrent not in SupersetUser.tasks:
            return  # Этот тест не запущен, пропускаем
    except Exception:
        return  # Если не можем проверить - пропускаем (другой тест запущен)

    collector = get_metrics_collector_002()

    # ============================================================================
    # 📊 ЗАГРУЗКА BASELINE METRICS
    # ============================================================================
    # Загружаем baseline метрики из config_multi.yaml для сравнения
    # Это позволяет увидеть отклонение от TC-LOAD-001 baseline
    # ============================================================================
    if collector.baseline_metrics is None:
        baseline_config = CONFIG.get('baseline_metrics', {})
        if baseline_config:
            try:
                import os
                csv_path = CONFIG.get("csv_file_path", "")
                if csv_path and os.path.exists(csv_path):
                    size_mb = os.path.getsize(csv_path) / (1024 * 1024)

                    # Ищем ближайший baseline по размеру файла
                    selected_baseline = None
                    min_diff = float('inf')

                    for key, baseline in baseline_config.items():
                        baseline_size = baseline.get('file_size_mb', 0)
                        diff = abs(size_mb - baseline_size)
                        if diff < min_diff:
                            min_diff = diff
                            selected_baseline = baseline

                    if selected_baseline:
                        collector.set_baseline_metrics(selected_baseline)
                        print(f"[TC-LOAD-002] Loaded baseline metrics from config: {selected_baseline}")
            except Exception as e:
                print(f"[TC-LOAD-002] Warning: Could not load baseline metrics: {e}")

    # Останавливаем ClickHouse мониторинг если есть
    if collector.clickhouse_monitor:
        collector.clickhouse_monitor.stop_monitoring()
        collector.clickhouse_monitor.collect_final()

    # Собираем Locust stats для RPS и Response Time
    stats = environment.stats
    locust_metrics = {
        'total_rps': stats.total.current_rps if stats.total.num_requests > 0 else 0,
        'total_requests': stats.total.num_requests,
        'total_failures': stats.total.num_failures,
        'median_response_time': stats.total.median_response_time,
        'avg_response_time': stats.total.avg_response_time,
        'percentile_95': stats.total.get_response_time_percentile(0.95),
        'percentile_99': stats.total.get_response_time_percentile(0.99),
    }
    collector.locust_metrics = locust_metrics

    # ============================================================================
    # 🆕 ГЕНЕРАЦИЯ ENHANCED ОТЧЕТОВ
    # ============================================================================
    # Используем новую систему отчетности с:
    # - Автоматическими percentiles (P50, P75, P90, P95, P99)
    # - SLO compliance tracking
    # - Baseline comparison (отклонение от TC-LOAD-001)
    # - Error analysis
    # - Smart recommendations
    # - Per-user breakdown
    # - Multiple formats: Text, JSON, CSV
    # ============================================================================

    # Генерируем отчеты с помощью ReportGenerator
    generator = ReportGenerator(collector)

    # Выводим текстовый отчет в консоль
    text_report = generator.generate_text_report()
    print("\n" + text_report)

    # Сохраняем все форматы отчетов (Text, JSON, CSV)
    try:
        saved_files = generator.save_reports(output_dir="./logs")
        print(f"\n[TC-LOAD-002] ✓ Successfully saved {len(saved_files)} report files:")
        for filepath in saved_files:
            print(f"  - {filepath}")
        print()
    except Exception as e:
        print(f"\n[TC-LOAD-002] ✗ Failed to save reports: {e}\n")
