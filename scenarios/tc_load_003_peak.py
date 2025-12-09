"""
TC-LOAD-003: Peak Concurrent Load Test
5 Heavy Users (ETL) + 3 Light Users (Superset UI)

Цель: Симуляция максимальной пиковой нагрузки
- Heavy users: параллельная загрузка CSV, DAG#1, DAG#2 (без координации)
- Light users: работа с готовыми дашбордами (фильтры, экспорт, навигация)
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
from common.api import Api
from common.csv_utils import count_chunks, count_csv_lines
from common.managers import UserPool
from common.clickhouse_monitor import ClickHouseMonitor
from config import CONFIG

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ============================================================================
# СЕКЦИЯ 1: ГЛОБАЛЬНОЕ СОСТОЯНИЕ (КООРДИНАЦИЯ)
# ============================================================================

class DashboardPool:
    """
    Общий пул дашбордов для координации между Heavy и Light users

    Как работает:
    - Heavy users после создания дашборда → добавляют в пул
    - Light users → берут случайный дашборд из пула для работы
    - Потокобезопасно (threading.Lock)
    """

    def __init__(self):
        self.lock = Lock()
        self.dashboards: List[tuple] = []  # [(url, owner_username, created_timestamp)]

    def add(self, url: str, owner: str):
        """Heavy user регистрирует созданный дашборд"""
        with self.lock:
            self.dashboards.append((url, owner, time.time()))
            print(f"[DashboardPool] Added dashboard from {owner}: {url}")

    def get_random(self) -> Optional[str]:
        """Light user берёт случайный дашборд для работы"""
        with self.lock:
            if self.dashboards:
                return random.choice(self.dashboards)[0]  # возвращаем URL
        return None

    def has_dashboards(self) -> bool:
        """Проверка: есть ли хотя бы один дашборд"""
        with self.lock:
            return len(self.dashboards) > 0

    def count(self) -> int:
        """Количество доступных дашбордов"""
        with self.lock:
            return len(self.dashboards)

    def wait_until_available(self, timeout=600) -> bool:
        """
        Light user ждёт появления дашбордов от Heavy users
        Возвращает True если дашборды появились, False если таймаут
        """
        start = time.time()
        while not self.has_dashboards():
            if time.time() - start > timeout:
                return False  # Таймаут
            time.sleep(5)  # Проверяем каждые 5 секунд
        return True


# Глобальный singleton
_dashboard_pool_003 = DashboardPool()


def get_dashboard_pool_003() -> DashboardPool:
    """Возвращает глобальный DashboardPool"""
    return _dashboard_pool_003


# ============================================================================
# СЕКЦИЯ 2: СБОРЩИК МЕТРИК
# ============================================================================

class TestMetricsCollector003:
    """
    Собирает метрики от двух типов пользователей:
    - Heavy: ETL операции (CSV, DAG#1, DAG#2)
    - Light: Superset UI (dashboard load, filters, export)
    """

    def __init__(self):
        self.lock = Lock()

        # Heavy users метрики
        self.heavy_runs: List[Dict] = []

        # Light users метрики
        self.light_runs: List[Dict] = []

        # Общие метрики
        self.test_start_time = None
        self.test_end_time = None
        self.ch_monitor: Optional[ClickHouseMonitor] = None
        self.locust_metrics: Optional[Dict] = None

    def register_heavy_run(self, metrics: Dict):
        """Heavy user регистрирует результаты ETL"""
        with self.lock:
            self.heavy_runs.append(metrics)

    def register_light_run(self, metrics: Dict):
        """Light user регистрирует результаты UI взаимодействия"""
        with self.lock:
            self.light_runs.append(metrics)

    def set_test_times(self, start_time: float, end_time: float):
        """Устанавливает время начала и конца теста"""
        with self.lock:
            if self.test_start_time is None:
                self.test_start_time = start_time
            self.test_end_time = end_time

    def set_clickhouse_monitor(self, monitor: ClickHouseMonitor):
        """Устанавливает ClickHouse монитор"""
        with self.lock:
            if self.ch_monitor is None:
                self.ch_monitor = monitor

    def generate_summary(self) -> str:
        """
        Генерирует финальный отчёт с тремя секциями:
        1. Heavy Users Performance (ETL Pipeline)
        2. Light Users Performance (Superset UI)
        3. System Resources & Validation
        """

        with self.lock:
            if not self.heavy_runs and not self.light_runs:
                return "\n[TC-LOAD-003] No test runs completed\n"

            # ========== HEAVY USERS METRICS ==========
            total_heavy = len(self.heavy_runs)
            successful_heavy = sum(1 for r in self.heavy_runs if r.get('success', False))
            failed_heavy = total_heavy - successful_heavy

            # CSV Upload
            csv_times = [r['csv_upload_duration'] for r in self.heavy_runs if 'csv_upload_duration' in r]
            csv_avg = sum(csv_times) / len(csv_times) if csv_times else 0
            csv_min = min(csv_times) if csv_times else 0
            csv_max = max(csv_times) if csv_times else 0

            # DAG #1
            dag1_times = [r['dag1_duration'] for r in self.heavy_runs if 'dag1_duration' in r]
            dag1_avg = sum(dag1_times) / len(dag1_times) if dag1_times else 0
            dag1_min = min(dag1_times) if dag1_times else 0
            dag1_max = max(dag1_times) if dag1_times else 0

            # DAG #2
            dag2_times = [r['dag2_duration'] for r in self.heavy_runs if 'dag2_duration' in r]
            dag2_avg = sum(dag2_times) / len(dag2_times) if dag2_times else 0
            dag2_min = min(dag2_times) if dag2_times else 0
            dag2_max = max(dag2_times) if dag2_times else 0

            # Dashboard Open (Heavy)
            dash_heavy_times = [r['dashboard_duration'] for r in self.heavy_runs if 'dashboard_duration' in r]
            dash_heavy_avg = sum(dash_heavy_times) / len(dash_heavy_times) if dash_heavy_times else 0

            # Total duration (Heavy)
            total_times = [r['total_duration'] for r in self.heavy_runs if 'total_duration' in r]
            total_avg = sum(total_times) / len(total_times) if total_times else 0

            # ========== LIGHT USERS METRICS ==========
            total_light = len(self.light_runs)

            # Aggregate light operations
            total_opens = sum(r.get('dashboard_opens', 0) for r in self.light_runs)
            total_filters = sum(r.get('filter_applies', 0) for r in self.light_runs)
            total_exports = sum(r.get('exports', 0) for r in self.light_runs)

            # Dashboard load times (Light)
            all_load_times = []
            for r in self.light_runs:
                all_load_times.extend(r.get('dashboard_load_times', []))

            load_avg = sum(all_load_times) / len(all_load_times) if all_load_times else 0
            load_min = min(all_load_times) if all_load_times else 0
            load_max = max(all_load_times) if all_load_times else 0

            # Percentiles
            if all_load_times:
                sorted_times = sorted(all_load_times)
                p95_idx = int(len(sorted_times) * 0.95)
                p99_idx = int(len(sorted_times) * 0.99)
                load_p95 = sorted_times[p95_idx] if p95_idx < len(sorted_times) else sorted_times[-1]
                load_p99 = sorted_times[p99_idx] if p99_idx < len(sorted_times) else sorted_times[-1]
            else:
                load_p95 = load_p99 = 0

            # Filter times
            all_filter_times = []
            for r in self.light_runs:
                all_filter_times.extend(r.get('filter_times', []))
            filter_avg = sum(all_filter_times) / len(all_filter_times) if all_filter_times else 0

            # Export times
            all_export_times = []
            for r in self.light_runs:
                all_export_times.extend(r.get('export_times', []))
            export_avg = sum(all_export_times) / len(all_export_times) if all_export_times else 0

            # ========== TEST DURATION ==========
            test_duration = self.test_end_time - self.test_start_time if self.test_start_time and self.test_end_time else 0

            # ========== BUILD REPORT ==========
            lines = [
                "",
                "=" * 80,
                "TC-LOAD-003: PEAK CONCURRENT LOAD TEST REPORT",
                "=" * 80,
                "",
                "ИНФОРМАЦИЯ О ТЕСТЕ",
                "-" * 50,
                f"Дата проведения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                f"Окружение: {CONFIG.get('api', {}).get('base_url', 'N/A')}",
                f"Тип теста: Peak Concurrent Load (5 Heavy + 3 Light)",
                f"Длительность теста: {test_duration:.2f}s ({test_duration/60:.1f} min)",
                "",
                "КОНФИГУРАЦИЯ ТЕСТА",
                "-" * 50,
                f"Heavy Users: 5 (CSV Upload → DAG#1 → DAG#2 → Dashboard)",
                f"Light Users: 3 (Dashboard interaction, filters, export)",
                f"Synchronization: None (all users work independently)",
                "",

                # === HEAVY USERS SECTION ===
                "HEAVY USERS PERFORMANCE (ETL Pipeline)",
                "=" * 80,
                f"Total Heavy Runs: {total_heavy}",
                f"Successful: {successful_heavy} ({successful_heavy/total_heavy*100:.1f}%)" if total_heavy > 0 else "Successful: 0",
                f"Failed: {failed_heavy} ({failed_heavy/total_heavy*100:.1f}%)" if failed_heavy > 0 else "",
                "",
                "CSV Upload Time:",
                f"  Среднее: {csv_avg:.2f}s",
                f"  Мин: {csv_min:.2f}s | Макс: {csv_max:.2f}s",
                "",
                "DAG #1 Duration (ClickHouse Import):",
                f"  Среднее: {dag1_avg:.2f}s ({dag1_avg/60:.1f} min)",
                f"  Мин: {dag1_min:.2f}s | Макс: {dag1_max:.2f}s",
                "",
                "DAG #2 Duration (PM Dashboard Creation):",
                f"  Среднее: {dag2_avg:.2f}s ({dag2_avg/60:.1f} min)",
                f"  Мін: {dag2_min:.2f}s | Макс: {dag2_max:.2f}s",
                "",
                "Dashboard Open (Heavy users):",
                f"  Среднее: {dash_heavy_avg:.2f}s",
                "",
                "Total Scenario Duration (Heavy):",
                f"  Среднее: {total_avg:.2f}s ({total_avg/60:.1f} min)",
                "",

                # === LIGHT USERS SECTION ===
                "LIGHT USERS PERFORMANCE (Superset UI)",
                "=" * 80,
                f"Total Light Users: {total_light}",
                f"Total Operations: {total_opens + total_filters + total_exports}",
                f"  - Dashboard Opens: {total_opens}",
                f"  - Filter Applications: {total_filters}",
                f"  - Data Exports: {total_exports}",
                "",
                "Dashboard Load Time (Light users):",
                f"  Среднее: {load_avg:.2f}s",
                f"  Мин: {load_min:.2f}s | Макс: {load_max:.2f}s",
                f"  P95: {load_p95:.2f}s | P99: {load_p99:.2f}s",
                "",
                "Filter Application Time:",
                f"  Среднее: {filter_avg:.2f}s",
                "",
                "Data Export Time:",
                f"  Среднее: {export_avg:.2f}s",
                "",
            ]

            # === LOCUST METRICS ===
            if self.locust_metrics:
                lm = self.locust_metrics
                lines.extend([
                    "HTTP МЕТРИКИ (Locust Stats)",
                    "-" * 50,
                    f"Total Requests: {lm.get('total_requests', 0):,}",
                    f"Total Failures: {lm.get('total_failures', 0):,}",
                    f"RPS (средний): {lm.get('total_rps', 0):.2f} req/s",
                    f"Response Time (средний): {lm.get('avg_response_time', 0):.0f} ms",
                    f"Response Time (медиана): {lm.get('median_response_time', 0):.0f} ms",
                    f"Response Time (P95): {lm.get('percentile_95', 0):.0f} ms",
                    f"Response Time (P99): {lm.get('percentile_99', 0):.0f} ms",
                    "",
                ])

            # === SLA VALIDATION ===
            success_rate = (successful_heavy / total_heavy * 100) if total_heavy > 0 else 0
            success_pass = success_rate > 95

            superset_responsive = sum(1 for t in all_load_times if t < 10)
            superset_total = len(all_load_times)
            superset_pass = (superset_responsive / superset_total * 100) if superset_total > 0 else 0

            lines.extend([
                "SLA VALIDATION",
                "-" * 50,
                "Heavy Users (ETL):",
                f"  ✓ Success rate > 95%: {success_rate:.1f}% ({'PASS' if success_pass else 'FAIL'})",
                f"  ⚠ DAG#1 < baseline × 2: [Manual verification required]",
                f"  ⚠ DAG#2 < baseline × 2: [Manual verification required]",
                "",
                "Light Users (Superset UI):",
                f"  ✓ Response time < 10s: {superset_responsive}/{superset_total} ({superset_pass:.1f}%) {'PASS' if superset_pass > 95 else 'FAIL'}",
                f"  ✓ No service crashes: [Manual verification required]",
                "",
            ])

            # === CLICKHOUSE METRICS ===
            if self.ch_monitor:
                lines.append(self.ch_monitor.format_summary_report())
            else:
                lines.extend([
                    "CLICKHOUSE МЕТРИКИ",
                    "-" * 50,
                    "[ClickHouse monitoring disabled or unavailable]",
                    "",
                ])

            # === SYSTEM RESOURCES ===
            lines.extend([
                "РЕСУРСЫ СИСТЕМЫ (Peak Load)",
                "-" * 50,
                "CPU (Airflow Worker): [manual monitoring required]",
                "Memory (Airflow Worker): [manual monitoring required]",
                "CPU (ClickHouse): [manual monitoring required]",
                "Memory (ClickHouse): [manual monitoring required]",
                "CPU (Superset): [manual monitoring required]",
                "Memory (Superset): [manual monitoring required]",
                "Airflow Queue Depth: [manual monitoring required]",
                "ClickHouse Concurrent Queries: [manual monitoring required]",
                "",
                "=" * 80,
            ])

            return "\n".join(lines)


# Глобальный collector
_metrics_collector_003 = TestMetricsCollector003()


def get_metrics_collector_003() -> TestMetricsCollector003:
    """Возвращает глобальный metrics collector для TC-LOAD-003"""
    return _metrics_collector_003


# ============================================================================
# СЕКЦИЯ 3: HEAVY USER CLASS (ETL Operations)
# ============================================================================

class TC_LOAD_003_Heavy(Api):
    """
    Heavy ETL operations - 5 параллельных пользователей

    Сценарий:
    1. CSV Upload
    2. DAG #1 (ClickHouse Import) + ожидание завершения
    3. DAG #2 (PM Dashboard Creation) + ожидание завершения
    4. Open Dashboard (проверка что работает)
    5. Регистрация дашборда в DashboardPool для Light users

    Особенности:
    - Работает НЕЗАВИСИМО от других Heavy users (нет синхронизации)
    - Каждый в своём темпе
    - Регистрирует результаты в MetricsCollector
    """

    wait_time = between(min_wait=1, max_wait=3)

    def __init__(self, parent):
        super().__init__(parent)
        self.user_id = f"heavy_user_{random.randint(10000, 99999)}"
        self.session_id = f"heavy_{random.randint(1000, 9999)}"

        # Стандартные поля
        self.logged_in = False
        self.session_valid = False
        self.username = None
        self.password = None
        self.flow_id = None
        self.pm_flow_id = None
        self.worker_id = 0

        # CSV конфигурация
        self.total_chunks = count_chunks(CONFIG["csv_file_path"], CONFIG["chunk_size"])
        self.total_lines = count_csv_lines(CONFIG["csv_file_path"])

        # ClickHouse мониторинг (только первый инициализирует)
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
        """
        Инициализация ClickHouse монитора
        Только первый Heavy user инициализирует, остальные пропускают
        """
        ch_config = CONFIG.get("clickhouse", {})

        if not ch_config.get("enabled", False):
            self.log("[TC-LOAD-003][Heavy] ClickHouse monitoring disabled")
            return

        # Проверяем, не инициализирован ли уже
        if get_metrics_collector_003().ch_monitor is not None:
            self.log("[TC-LOAD-003][Heavy] ClickHouse monitor already initialized by another user")
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
                self.log("[TC-LOAD-003][Heavy] ClickHouse monitor initialized successfully")
                # Регистрируем в глобальном collector
                get_metrics_collector_003().set_clickhouse_monitor(self.ch_monitor)
            else:
                self.log("[TC-LOAD-003][Heavy] ClickHouse connection failed, monitoring disabled", logging.WARNING)
                self.ch_monitor = None

        except Exception as e:
            self.log(f"[TC-LOAD-003][Heavy] Failed to initialize ClickHouse monitor: {e}", logging.ERROR)
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
            self.log(f"[TC-LOAD-003][Heavy] Authentication successful for {self.username}")
        else:
            self.log("[TC-LOAD-003][Heavy] Authentication failed", logging.ERROR)
            self.interrupt()

    def on_start(self):
        """Инициализация Heavy user"""
        runner = getattr(self, "environment", None)
        if runner:
            runner = getattr(runner, "runner", None)
            self.worker_id = getattr(runner, "worker_id", 0) if runner else 0

        creds = UserPool.get_credentials()
        self.username = creds["username"]
        self.password = creds["password"]
        self.client.verify = False

        self.establish_session()
        self.log(f"[TC-LOAD-003][Heavy] User {self.username} started")

        # Устанавливаем время старта в глобальном collector
        get_metrics_collector_003().set_test_times(time.time(), time.time())

        # Стартуем ClickHouse мониторинг (только первый пользователь)
        if self.ch_monitor:
            self.ch_monitor.collect_baseline()
            self.ch_monitor.start_monitoring()

    def on_stop(self):
        """Clean up when user stops"""
        self.log(f"[TC-LOAD-003][Heavy] User {self.username} stopped")

    @task
    def heavy_etl_scenario(self):
        """
        ОСНОВНОЙ СЦЕНАРИЙ HEAVY USER

        Полный ETL pipeline без синхронизации с другими users:
        CSV Upload → DAG#1 → DAG#2 → Dashboard → Register for Light users
        """

        if not self.logged_in:
            self.establish_session()
            if not self.logged_in:
                self.log("[TC-LOAD-003][Heavy] Failed to establish session", logging.ERROR)
                return

        self.log(f"[TC-LOAD-003][Heavy][{self.username}] Starting ETL scenario")
        self.test_start_time = time.time()
        scenario_start = time.time()

        try:
            # ========== PHASE 1: CSV Upload & File Import Flow ==========
            self.log(f"[TC-LOAD-003][Heavy][{self.username}][PHASE 1] CSV Upload & File Import")
            phase1_start = time.time()

            # 1. Создание flow для загрузки файла
            flow_name, flow_id = self._create_flow(worker_id=self.worker_id)
            self.flow_id = flow_id

            if not flow_id:
                self.log(f"[TC-LOAD-003][Heavy][{self.username}] Failed to create flow", logging.ERROR)
                return

            self.log(f"[TC-LOAD-003][Heavy][{self.username}] File flow created: {flow_name} (ID: {flow_id})")

            # 2. Получение параметров DAG
            target_connection, target_schema = self._get_dag_import_params(flow_id)
            if not target_connection or not target_schema:
                self.log(f"[TC-LOAD-003][Heavy][{self.username}] Missing DAG parameters", logging.ERROR)
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
                self.log(f"[TC-LOAD-003][Heavy][{self.username}] Failed to update flow before upload", logging.ERROR)
                return

            # 4. Получение ID базы данных пользователя
            db_id = self._get_user_database_id()
            if not db_id:
                self.log(f"[TC-LOAD-003][Heavy][{self.username}] User database not found", logging.ERROR)
                return

            if self.total_chunks == 0:
                self.log(f"[TC-LOAD-003][Heavy][{self.username}] No chunks to upload", logging.WARNING)
                return

            timeout = (
                CONFIG["upload_control"]["timeout_large"]
                if self.total_chunks > CONFIG["upload_control"]["chunk_threshold"]
                else CONFIG["upload_control"]["timeout_small"]
            )

            # 5. Начало загрузки
            csv_upload_start = time.time()
            if not self._start_file_upload(flow_id, db_id, target_schema, self.total_chunks, timeout):
                return

            # 6. Загрузка чанков
            uploaded_chunks = self._upload_chunks(flow_id, db_id, target_schema, self.total_chunks)
            csv_upload_duration = time.time() - csv_upload_start
            self.csv_upload_duration = csv_upload_duration
            self.log(f"[TC-LOAD-003][Heavy][{self.username}] CSV upload completed: {uploaded_chunks}/{self.total_chunks} chunks in {csv_upload_duration:.2f}s")

            # 7. Финализация загрузки
            if not self._finalize_file_upload(flow_id, uploaded_chunks, timeout):
                return

            # ========== DAG #1: File Processing (ClickHouse Import) ==========
            self.log(f"[TC-LOAD-003][Heavy][{self.username}][PHASE 2] DAG #1: ClickHouse Import")
            dag1_start = time.time()

            # 8. Начало обработки файла
            file_run_id = self._start_file_processing(
                flow_id, target_connection, target_schema, self.total_chunks, timeout
            )
            if not file_run_id:
                return

            # 9. Мониторинг статуса обработки файла
            file_processing_start = time.time()
            success = self._monitor_processing_status(
                file_run_id, timeout, flow_id, db_id, target_schema,
                self.total_lines, file_processing_start, is_pm_flow=False
            )

            if not success:
                self.log(f"[TC-LOAD-003][Heavy][{self.username}] DAG #1 processing failed", logging.ERROR)
                return

            dag1_duration = time.time() - dag1_start
            self.dag1_duration = dag1_duration
            phase1_duration = time.time() - phase1_start
            self.log(f"[TC-LOAD-003][Heavy][{self.username}] DAG #1 completed in {dag1_duration:.2f}s")
            self.log(f"[TC-LOAD-003][Heavy][{self.username}][PHASE 1] Completed in {phase1_duration:.2f}s")

            # ========== PHASE 2: Process Mining Flow ==========
            self.log(f"[TC-LOAD-003][Heavy][{self.username}][PHASE 3] DAG #2: Process Mining Dashboard")
            phase2_start = time.time()

            # 10. Получаем параметры для PM блока
            source_connection, source_schema = self._get_dag_pm_params(flow_id)
            if not all([source_connection, source_schema]):
                self.log(f"[TC-LOAD-003][Heavy][{self.username}] Missing PM DAG parameters", logging.ERROR)
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
                self.log(f"[TC-LOAD-003][Heavy][{self.username}] Failed to create Process Mining flow", logging.ERROR)
                return

            self.pm_flow_id = pm_flow_id
            self.log(f"[TC-LOAD-003][Heavy][{self.username}] PM Flow created: {pm_flow_name} (ID: {pm_flow_id})")

            # 12. Запускаем Process Mining flow (DAG #2)
            dag2_start = time.time()
            pm_run_id = self._start_pm_flow(
                pm_flow_id, source_connection, source_schema, table_name
            )

            if not pm_run_id:
                self.log(f"[TC-LOAD-003][Heavy][{self.username}] Failed to start Process Mining flow", logging.ERROR)
                return

            # 13. Мониторинг статуса Process Mining
            pm_timeout = CONFIG["upload_control"]["pm_timeout"]
            pm_result = self._monitor_processing_status(
                pm_run_id, pm_timeout, pm_flow_id, is_pm_flow=True
            )

            if not (isinstance(pm_result, dict) and pm_result.get("success")):
                self.log(f"[TC-LOAD-003][Heavy][{self.username}] DAG #2 processing failed", logging.ERROR)
                return

            dag2_duration = time.time() - dag2_start
            self.dag2_duration = dag2_duration
            self.log(f"[TC-LOAD-003][Heavy][{self.username}] DAG #2 completed in {dag2_duration:.2f}s")

            # ========== PHASE 3: Dashboard Interaction ==========
            self.log(f"[TC-LOAD-003][Heavy][{self.username}][PHASE 4] Dashboard Interaction")

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
                        self.log(f"[TC-LOAD-003][Heavy][{self.username}] Dashboard loaded in {dashboard_duration:.2f}s: {dashboard_url}")

                        # ✅ КЛЮЧЕВОЙ МОМЕНТ: Регистрируем дашборд для Light users!
                        get_dashboard_pool_003().add(dashboard_url, self.username)
                        self.log(f"[TC-LOAD-003][Heavy][{self.username}] Dashboard registered for Light users (total: {get_dashboard_pool_003().count()})")
                    else:
                        self.log(f"[TC-LOAD-003][Heavy][{self.username}] Failed to load dashboard", logging.WARNING)
                else:
                    self.log(f"[TC-LOAD-003][Heavy][{self.username}] Could not retrieve dashboard URL", logging.WARNING)
            else:
                self.log(f"[TC-LOAD-003][Heavy][{self.username}] block_run_id not found for {target_block_id}", logging.WARNING)

            phase2_duration = time.time() - phase2_start
            self.log(f"[TC-LOAD-003][Heavy][{self.username}][PHASE 3] Completed in {phase2_duration:.2f}s")

            # ========== Scenario Complete ==========
            total_duration = time.time() - scenario_start
            self.total_duration = total_duration
            self.log(
                f"[TC-LOAD-003][Heavy][{self.username}] ETL completed successfully in {total_duration:.2f}s "
                f"(CSV: {self.csv_upload_duration:.2f}s, DAG#1: {self.dag1_duration:.2f}s, DAG#2: {self.dag2_duration:.2f}s)"
            )

            # ========== Регистрируем метрики в глобальном collector ==========
            get_metrics_collector_003().register_heavy_run({
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
            get_metrics_collector_003().set_test_times(self.test_start_time, time.time())

        except Exception as e:
            self.log(f"[TC-LOAD-003][Heavy][{self.username}] Unexpected error in ETL scenario: {str(e)}", logging.ERROR)

            # Регистрируем failed run
            get_metrics_collector_003().register_heavy_run({
                'success': False,
                'username': self.username,
                'error': str(e),
            })


# ============================================================================
# СЕКЦИЯ 4: LIGHT USER CLASS (Superset UI Operations)
# ============================================================================

class TC_LOAD_003_Light(Api):
    """
    Light Superset UI operations - 3 пользователя

    Сценарий:
    1. Ждут появления дашбордов от Heavy users
    2. Работают в ЦИКЛЕ:
       - Открывают дашборды
       - Применяют фильтры (ЗАГЛУШКА)
       - Переключаются между графиками (ЗАГЛУШКА)
       - Экспортируют данные (ЗАГЛУШКА)
    3. Создают нагрузку на Superset UI

    Особенности:
    - Стартуют сразу, но ЖДУТ дашборды
    - Работают в цикле (не одна итерация, а continuous load)
    - Измеряют Superset response time

    TODO: Заменить заглушки на реальные Superset API endpoints когда они будут готовы
    """

    wait_time = between(min_wait=2, max_wait=5)  # Пауза между действиями

    def __init__(self, parent):
        super().__init__(parent)
        self.user_id = f"light_user_{random.randint(10000, 99999)}"
        self.session_id = f"light_{random.randint(1000, 9999)}"

        self.logged_in = False
        self.session_valid = False
        self.username = None
        self.password = None

        # Счётчики для метрик
        self.dashboard_opens = 0
        self.filter_applies = 0
        self.exports = 0

        # Время операций
        self.dashboard_load_times = []
        self.filter_times = []
        self.export_times = []

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
            self.log(f"[TC-LOAD-003][Light] Authentication successful for {self.username}")
        else:
            self.log("[TC-LOAD-003][Light] Authentication failed", logging.ERROR)
            self.interrupt()

    def on_start(self):
        """
        Инициализация Light user

        ВАЖНО: Ждём появления дашбордов от Heavy users!
        """

        # 1. Авторизация
        creds = UserPool.get_credentials()
        self.username = creds["username"]
        self.password = creds["password"]
        self.client.verify = False

        self.establish_session()

        if not self.logged_in:
            self.log(f"[TC-LOAD-003][Light][{self.username}] Failed to authenticate", logging.ERROR)
            self.interrupt()
            return

        # 2. Ждём дашборды
        self.log(f"[TC-LOAD-003][Light][{self.username}] Waiting for dashboards from Heavy users...")

        if not get_dashboard_pool_003().wait_until_available(timeout=600):
            self.log(f"[TC-LOAD-003][Light][{self.username}] Timeout: No dashboards available after 10 min", logging.WARNING)
            self.interrupt()
            return

        self.log(f"[TC-LOAD-003][Light][{self.username}] Dashboards available ({get_dashboard_pool_003().count()})! Starting UI operations")

    def on_stop(self):
        """
        Завершение работы Light user
        Регистрируем финальные метрики
        """

        if self.dashboard_opens > 0:
            get_metrics_collector_003().register_light_run({
                'username': self.username,
                'dashboard_opens': self.dashboard_opens,
                'filter_applies': self.filter_applies,
                'exports': self.exports,
                'dashboard_load_times': self.dashboard_load_times,
                'filter_times': self.filter_times,
                'export_times': self.export_times,
            })

        self.log(f"[TC-LOAD-003][Light][{self.username}] Stopped. Operations: "
                 f"{self.dashboard_opens} opens, {self.filter_applies} filters, {self.exports} exports")

    @task(weight=5)
    def open_and_explore_dashboard(self):
        """
        ЗАДАЧА 1: Открыть дашборд и поработать с ним

        Симулирует реального пользователя:
        - Открывает дашборд
        - Ждёт загрузки всех компонентов
        - Измеряет время отклика

        Weight=5: самая частая операция

        TODO: Заменить заглушку на реальный GET запрос к Superset
        """

        dashboard_url = get_dashboard_pool_003().get_random()

        if not dashboard_url:
            self.log(f"[TC-LOAD-003][Light][{self.username}] No dashboards in pool", logging.WARNING)
            return

        self.log(f"[TC-LOAD-003][Light][{self.username}] Opening dashboard: {dashboard_url}")

        start_time = time.time()

        # === ЗАГЛУШКА: Здесь будет реальный API call ===
        # TODO: Раскомментировать когда будут готовы endpoints
        # response = self.client.get(
        #     dashboard_url,
        #     name="[Light] Open Dashboard",
        #     catch_response=True
        # )
        #
        # if response.ok:
        #     load_time = time.time() - start_time
        #     self.dashboard_load_times.append(load_time)
        #     self.dashboard_opens += 1
        #
        #     if load_time < 10:
        #         response.success()
        #     else:
        #         response.failure(f"Dashboard load too slow: {load_time:.2f}s")
        # else:
        #     response.failure(f"Failed to load dashboard: {response.status_code}")

        # ЗАГЛУШКА: симуляция загрузки
        time.sleep(random.uniform(0.5, 2.0))
        load_time = time.time() - start_time
        self.dashboard_load_times.append(load_time)
        self.dashboard_opens += 1

        self.log(f"[TC-LOAD-003][Light][{self.username}] Dashboard loaded in {load_time:.2f}s")

    @task(weight=3)
    def apply_filters_and_refresh(self):
        """
        ЗАДАЧА 2: Применить фильтры к дашборду

        Симулирует аналитика, который:
        - Выбирает дашборд
        - Применяет фильтры (даты, категории, etc.)
        - Ждёт пересчёта данных

        Weight=3: средняя частота

        TODO: Заменить заглушку на реальный POST запрос с фильтрами
        """

        dashboard_url = get_dashboard_pool_003().get_random()

        if not dashboard_url:
            return

        self.log(f"[TC-LOAD-003][Light][{self.username}] Applying filters to dashboard")

        start_time = time.time()

        # === ЗАГЛУШКА: Здесь будет POST запрос с фильтрами ===
        # TODO: Раскомментировать когда будут готовы endpoints
        # filter_payload = {
        #     "date_range": "last_7_days",
        #     "category": "example"
        # }
        #
        # response = self.client.post(
        #     f"{dashboard_url}/api/filter",
        #     json=filter_payload,
        #     name="[Light] Apply Filters",
        #     catch_response=True
        # )

        # ЗАГЛУШКА: симуляция
        time.sleep(random.uniform(0.3, 1.5))
        filter_time = time.time() - start_time
        self.filter_times.append(filter_time)
        self.filter_applies += 1

        self.log(f"[TC-LOAD-003][Light][{self.username}] Filters applied in {filter_time:.2f}s")

    @task(weight=2)
    def export_dashboard_data(self):
        """
        ЗАДАЧА 3: Экспортировать данные

        Симулирует пользователя, который:
        - Выбирает дашборд
        - Запрашивает экспорт (CSV/Excel)
        - Ждёт генерации файла

        Weight=2: самая редкая (но тяжёлая) операция

        TODO: Заменить заглушку на реальный GET/POST запрос на экспорт
        """

        dashboard_url = get_dashboard_pool_003().get_random()

        if not dashboard_url:
            return

        self.log(f"[TC-LOAD-003][Light][{self.username}] Exporting data from dashboard")

        start_time = time.time()

        # === ЗАГЛУШКА: Здесь будет GET/POST запрос на экспорт ===
        # TODO: Раскомментировать когда будут готовы endpoints
        # response = self.client.get(
        #     f"{dashboard_url}/api/export?format=csv",
        #     name="[Light] Export Data",
        #     catch_response=True
        # )

        # ЗАГЛУШКА: симуляция (экспорт дольше)
        time.sleep(random.uniform(1.0, 3.0))
        export_time = time.time() - start_time
        self.export_times.append(export_time)
        self.exports += 1

        self.log(f"[TC-LOAD-003][Light][{self.username}] Export completed in {export_time:.2f}s")


# ============================================================================
# СЕКЦИЯ 5: LOCUST EVENT LISTENERS
# ============================================================================

@events.test_start.add_listener
def on_test_start_003(environment, **kwargs):
    """
    Вызывается при старте TC-LOAD-003
    Выводит баннер с информацией о тесте
    """

    # Проверяем что TC-LOAD-003 запущен
    try:
        from locustfile import SupersetUser
        if TC_LOAD_003_Heavy not in SupersetUser.tasks and TC_LOAD_003_Light not in SupersetUser.tasks:
            return  # Тест не запущен
    except Exception:
        pass

    print("\n" + "=" * 80)
    print("TC-LOAD-003: PEAK CONCURRENT LOAD TEST STARTED")
    print("=" * 80)
    print(f"Configuration:")
    print(f"  - Test Type: Peak Concurrent")
    print(f"  - Heavy Users: 5 (ETL Pipeline - CSV → DAG#1 → DAG#2 → Dashboard)")
    print(f"  - Light Users: 3 (Superset UI - open, filters, export)")
    print(f"  - Synchronization: None (all users work independently)")
    print(f"  - CSV File: {CONFIG.get('csv_file_path', 'N/A')}")
    print(f"  - ClickHouse Monitoring: {'Enabled' if CONFIG.get('clickhouse', {}).get('enabled', False) else 'Disabled'}")
    print("=" * 80 + "\n")


@events.test_stop.add_listener
def on_test_stop_003(environment, **kwargs):
    """
    Вызывается при завершении TC-LOAD-003
    Генерирует и выводит финальный отчёт
    """

    # Проверяем что TC-LOAD-003 запущен
    try:
        from locustfile import SupersetUser
        if TC_LOAD_003_Heavy not in SupersetUser.tasks and TC_LOAD_003_Light not in SupersetUser.tasks:
            return  # Тест не запущен
    except Exception:
        pass

    collector = get_metrics_collector_003()

    # Останавливаем ClickHouse мониторинг
    if collector.ch_monitor:
        collector.ch_monitor.stop_monitoring()
        collector.ch_monitor.collect_final()

    # Собираем Locust stats
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

    # Генерируем summary
    summary = collector.generate_summary()
    print(summary)

    # Сохраняем в файл
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = f"./logs/tc_load_003_report_{timestamp}.txt"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(summary)
        print(f"\n[TC-LOAD-003] Report saved to: {report_path}\n")
    except Exception as e:
        print(f"\n[TC-LOAD-003] Failed to save report: {e}\n")
