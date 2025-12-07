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
from common.api import Api
from common.csv_utils import count_chunks, count_csv_lines
from common.managers import UserPool
from common.clickhouse_monitor import ClickHouseMonitor
from config import CONFIG

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class TestMetricsCollector002:
    """
    Глобальный сборщик метрик для TC-LOAD-002.
    Агрегирует данные от нескольких параллельных пользователей.
    """

    def __init__(self):
        self.lock = Lock()
        self.test_runs: List[Dict] = []
        self.test_start_time = None
        self.test_end_time = None
        self.ch_monitor: Optional[ClickHouseMonitor] = None
        self.baseline_metrics = None
        self.locust_metrics: Optional[Dict] = None

    def set_baseline_metrics(self, baseline: Dict):
        """Устанавливает baseline метрики для сравнения"""
        with self.lock:
            self.baseline_metrics = baseline

    def register_test_run(self, metrics: Dict):
        """Регистрирует результаты одного test run"""
        with self.lock:
            self.test_runs.append(metrics)

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

    def _calculate_baseline_comparison(self, metric_name: str, values: List[float]) -> Dict:
        """Вычисляет сравнение с baseline и SLA validation"""
        if not values or not self.baseline_metrics:
            return {'avg': 0, 'sla_pass': 0, 'sla_total': 0, 'baseline_diff': 'N/A'}

        avg_value = sum(values) / len(values)
        baseline_value = self.baseline_metrics.get(metric_name, 0)

        if baseline_value > 0:
            # SLA: не более +50% от baseline
            max_allowed = baseline_value * 1.5
            sla_pass = sum(1 for v in values if v <= max_allowed)
            diff_percent = ((avg_value - baseline_value) / baseline_value) * 100

            return {
                'avg': avg_value,
                'baseline': baseline_value,
                'max_allowed': max_allowed,
                'sla_pass': sla_pass,
                'sla_total': len(values),
                'baseline_diff': f"{diff_percent:+.1f}%"
            }

        return {
            'avg': avg_value,
            'sla_pass': len(values),
            'sla_total': len(values),
            'baseline_diff': 'N/A'
        }

    def generate_summary(self) -> str:
        """Генерирует итоговый отчёт по всем test runs"""
        with self.lock:
            if not self.test_runs:
                return "\n[TC-LOAD-002] No test runs completed\n"

            # Агрегируем метрики
            total_runs = len(self.test_runs)
            successful_runs = sum(1 for r in self.test_runs if r.get('success', False))
            failed_runs = total_runs - successful_runs

            # Группируем по пользователям
            users = {}
            for run in self.test_runs:
                username = run.get('username', 'unknown')
                if username not in users:
                    users[username] = []
                users[username].append(run)

            # CSV Upload
            csv_times = [r['csv_upload_duration'] for r in self.test_runs if 'csv_upload_duration' in r]
            csv_stats = self._calculate_baseline_comparison('csv_upload', csv_times)
            csv_min = min(csv_times) if csv_times else 0
            csv_max = max(csv_times) if csv_times else 0

            # DAG #1
            dag1_times = [r['dag1_duration'] for r in self.test_runs if 'dag1_duration' in r]
            dag1_stats = self._calculate_baseline_comparison('dag1_duration', dag1_times)
            dag1_min = min(dag1_times) if dag1_times else 0
            dag1_max = max(dag1_times) if dag1_times else 0

            # DAG #2
            dag2_times = [r['dag2_duration'] for r in self.test_runs if 'dag2_duration' in r]
            dag2_stats = self._calculate_baseline_comparison('dag2_duration', dag2_times)
            dag2_min = min(dag2_times) if dag2_times else 0
            dag2_max = max(dag2_times) if dag2_times else 0

            # Dashboard
            dash_times = [r['dashboard_duration'] for r in self.test_runs if 'dashboard_duration' in r]
            dash_stats = self._calculate_baseline_comparison('dashboard_load', dash_times)
            dash_min = min(dash_times) if dash_times else 0
            dash_max = max(dash_times) if dash_times else 0

            # Total duration
            total_times = [r['total_duration'] for r in self.test_runs if 'total_duration' in r]
            total_avg = sum(total_times) / len(total_times) if total_times else 0

            # Время теста
            test_duration = self.test_end_time - self.test_start_time if self.test_start_time and self.test_end_time else 0

            # Получаем первый run для конфигурации
            first_run = self.test_runs[0]

            lines = [
                "",
                "=" * 80,
                "TC-LOAD-002: CONCURRENT LOAD TEST REPORT (3 USERS)",
                "=" * 80,
                "",
                "ИНФОРМАЦИЯ О ТЕСТЕ",
                "-" * 50,
                f"Дата проведения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                f"Окружение: {CONFIG.get('api', {}).get('base_url', 'N/A')}",
                f"Тип теста: Concurrent Load Test (3 users)",
                f"Длительность теста: {test_duration:.2f}s ({test_duration/60:.1f} min)",
                f"Всего запусков: {total_runs}",
                f"Успешных: {successful_runs} ({successful_runs/total_runs*100:.1f}%)",
                f"Неудачных: {failed_runs} ({failed_runs/total_runs*100:.1f}%)" if failed_runs > 0 else "",
                f"Параллельных пользователей: {len(users)}",
                "",
                "КОНФИГУРАЦИЯ ТЕСТА",
                "-" * 50,
                f"Количество пользователей: 3 (concurrent)",
                f"Размер файла: {first_run.get('file_size', 'N/A')}",
                f"Количество строк: {first_run.get('total_lines', 0):,}",
                f"Chunks: {first_run.get('total_chunks', 0)}",
                "",
                "РЕЗУЛЬТАТЫ ПРОИЗВОДИТЕЛЬНОСТИ",
                "-" * 50,
                f"CSV Upload Time:",
                f"  Среднее: {csv_stats['avg']:.2f}s",
                f"  Мин: {csv_min:.2f}s | Макс: {csv_max:.2f}s",
                f"  Baseline: {csv_stats.get('baseline', 'N/A')}s | Отклонение: {csv_stats['baseline_diff']}",
                "",
                f"DAG #1 Duration (ClickHouse Import):",
                f"  Среднее: {dag1_stats['avg']:.2f}s ({dag1_stats['avg']/60:.1f} min)",
                f"  Мин: {dag1_min:.2f}s | Макс: {dag1_max:.2f}s",
                f"  Baseline: {dag1_stats.get('baseline', 'N/A')}s | Отклонение: {dag1_stats['baseline_diff']}",
                "",
                f"DAG #2 Duration (PM Dashboard):",
                f"  Среднее: {dag2_stats['avg']:.2f}s ({dag2_stats['avg']/60:.1f} min)",
                f"  Мін: {dag2_min:.2f}s | Макс: {dag2_max:.2f}s",
                f"  Baseline: {dag2_stats.get('baseline', 'N/A')}s | Отклонение: {dag2_stats['baseline_diff']}",
                "",
                f"Dashboard Load:",
                f"  Среднее: {dash_stats['avg']:.2f}s",
                f"  Мин: {dash_min:.2f}s | Макс: {dash_max:.2f}s",
                f"  Baseline: {dash_stats.get('baseline', 'N/A')}s | Отклонение: {dash_stats['baseline_diff']}",
                "",
                f"Total Scenario Duration:",
                f"  Среднее: {total_avg:.2f}s ({total_avg/60:.1f} min)",
                "",
            ]

            # Per-user breakdown
            lines.extend([
                "РЕЗУЛЬТАТЫ ПО ПОЛЬЗОВАТЕЛЯМ",
                "-" * 50,
            ])

            for username, user_runs in sorted(users.items()):
                user_dag1 = [r['dag1_duration'] for r in user_runs if 'dag1_duration' in r]
                user_dag2 = [r['dag2_duration'] for r in user_runs if 'dag2_duration' in r]

                if user_dag1 and user_dag2:
                    lines.extend([
                        f"{username}:",
                        f"  Запусков: {len(user_runs)}",
                        f"  DAG #1 avg: {sum(user_dag1)/len(user_dag1):.2f}s",
                        f"  DAG #2 avg: {sum(user_dag2)/len(user_dag2):.2f}s",
                        "",
                    ])

            # Locust HTTP метрики (если доступны)
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

            # SLA Validation
            lines.extend([
                "SLA VALIDATION (не более +50% от baseline)",
                "-" * 50,
                f"DAG #1: {dag1_stats['sla_pass']}/{dag1_stats['sla_total']} прошли "
                f"({'✓ PASS' if dag1_stats['sla_pass'] == dag1_stats['sla_total'] else '✗ FAIL'})",
                f"DAG #2: {dag2_stats['sla_pass']}/{dag2_stats['sla_total']} прошли "
                f"({'✓ PASS' if dag2_stats['sla_pass'] == dag2_stats['sla_total'] else '✗ FAIL'})",
                "",
            ])

            # Добавляем ClickHouse метрики если есть
            if self.ch_monitor:
                lines.append(self.ch_monitor.format_summary_report())
            else:
                lines.extend([
                    "CLICKHOUSE МЕТРИКИ",
                    "-" * 50,
                    "[ClickHouse monitoring disabled or unavailable]",
                    "",
                ])

            lines.extend([
                "РЕСУРСЫ СИСТЕМЫ",
                "-" * 50,
                "CPU (Airflow Worker): [manual input required]",
                "Memory (Airflow Worker): [manual input required]",
                "CPU (ClickHouse): [manual input required]",
                "Memory (ClickHouse): [manual input required]",
                "CPU (Superset): [manual input required]",
                "Memory (Superset): [manual input required]",
                "Airflow Queue: [manual monitoring required]",
                "Superset UI Response: [manual monitoring required]",
                "",
                "=" * 80,
            ])

            return "\n".join(lines)


# Глобальный collector для TC-LOAD-002
_test_metrics_collector_002 = TestMetricsCollector002()


def get_metrics_collector_002() -> TestMetricsCollector002:
    """Возвращает глобальный metrics collector для TC-LOAD-002"""
    return _test_metrics_collector_002


class TC_LOAD_002_Concurrent(Api):
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
        if get_metrics_collector_002().ch_monitor is not None:
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
                self.log("[TC-LOAD-002] Failed to establish session", logging.ERROR)
                return

        self.log(f"[TC-LOAD-002][{self.username}] Starting concurrent scenario")
        self.test_start_time = time.time()
        scenario_start = time.time()

        try:
            # ========== PHASE 1: CSV Upload & File Import Flow ==========
            self.log(f"[TC-LOAD-002][{self.username}][PHASE 1] CSV Upload & File Import")
            phase1_start = time.time()

            # 1. Создание flow для загрузки файла
            flow_name, flow_id = self._create_flow(worker_id=self.worker_id)
            self.flow_id = flow_id

            if not flow_id:
                self.log(f"[TC-LOAD-002][{self.username}] Failed to create flow", logging.ERROR)
                return

            self.log(f"[TC-LOAD-002][{self.username}] File flow created: {flow_name} (ID: {flow_id})")

            # 2. Получение параметров DAG
            target_connection, target_schema = self._get_dag_import_params(flow_id)
            if not target_connection or not target_schema:
                self.log(f"[TC-LOAD-002][{self.username}] Missing DAG parameters", logging.ERROR)
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
                self.log(f"[TC-LOAD-002][{self.username}] Failed to update flow before upload", logging.ERROR)
                return

            # 4. Получение ID базы данных пользователя
            db_id = self._get_user_database_id()
            if not db_id:
                self.log(f"[TC-LOAD-002][{self.username}] User database not found", logging.ERROR)
                return

            if self.total_chunks == 0:
                self.log(f"[TC-LOAD-002][{self.username}] No chunks to upload", logging.WARNING)
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
            self.log(f"[TC-LOAD-002][{self.username}] CSV upload completed: {uploaded_chunks}/{self.total_chunks} chunks in {csv_upload_duration:.2f}s")

            # 7. Финализация загрузки
            if not self._finalize_file_upload(flow_id, uploaded_chunks, timeout):
                return

            # ========== DAG #1: File Processing (ClickHouse Import) ==========
            self.log(f"[TC-LOAD-002][{self.username}][PHASE 2] DAG #1: ClickHouse Import")
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
                self.log(f"[TC-LOAD-002][{self.username}] DAG #1 processing failed", logging.ERROR)
                return

            dag1_duration = time.time() - dag1_start
            self.dag1_duration = dag1_duration
            phase1_duration = time.time() - phase1_start
            self.log(f"[TC-LOAD-002][{self.username}] DAG #1 completed in {dag1_duration:.2f}s")
            self.log(f"[TC-LOAD-002][{self.username}][PHASE 1] Completed in {phase1_duration:.2f}s")

            # ========== PHASE 2: Process Mining Flow ==========
            self.log(f"[TC-LOAD-002][{self.username}][PHASE 3] DAG #2: Process Mining Dashboard")
            phase2_start = time.time()

            # 10. Получаем параметры для PM блока
            source_connection, source_schema = self._get_dag_pm_params(flow_id)
            if not all([source_connection, source_schema]):
                self.log(f"[TC-LOAD-002][{self.username}] Missing PM DAG parameters", logging.ERROR)
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
                self.log(f"[TC-LOAD-002][{self.username}] Failed to create Process Mining flow", logging.ERROR)
                return

            self.pm_flow_id = pm_flow_id
            self.log(f"[TC-LOAD-002][{self.username}] PM Flow created: {pm_flow_name} (ID: {pm_flow_id})")

            # 12. Запускаем Process Mining flow (DAG #2)
            dag2_start = time.time()
            pm_run_id = self._start_pm_flow(
                pm_flow_id, source_connection, source_schema, table_name
            )

            if not pm_run_id:
                self.log(f"[TC-LOAD-002][{self.username}] Failed to start Process Mining flow", logging.ERROR)
                return

            # 13. Мониторинг статуса Process Mining
            pm_timeout = CONFIG["upload_control"]["pm_timeout"]
            pm_result = self._monitor_processing_status(
                pm_run_id, pm_timeout, pm_flow_id, is_pm_flow=True
            )

            if not (isinstance(pm_result, dict) and pm_result.get("success")):
                self.log(f"[TC-LOAD-002][{self.username}] DAG #2 processing failed", logging.ERROR)
                return

            dag2_duration = time.time() - dag2_start
            self.dag2_duration = dag2_duration
            self.log(f"[TC-LOAD-002][{self.username}] DAG #2 completed in {dag2_duration:.2f}s")

            # ========== PHASE 3: Dashboard Interaction ==========
            self.log(f"[TC-LOAD-002][{self.username}][PHASE 4] Dashboard Interaction")

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
                        self.log(f"[TC-LOAD-002][{self.username}] Dashboard loaded in {dashboard_duration:.2f}s: {dashboard_url}")
                    else:
                        self.log(f"[TC-LOAD-002][{self.username}] Failed to load dashboard", logging.WARNING)
                else:
                    self.log(f"[TC-LOAD-002][{self.username}] Could not retrieve dashboard URL", logging.WARNING)
            else:
                self.log(f"[TC-LOAD-002][{self.username}] block_run_id not found for {target_block_id}", logging.WARNING)

            phase2_duration = time.time() - phase2_start
            self.log(f"[TC-LOAD-002][{self.username}][PHASE 3] Completed in {phase2_duration:.2f}s")

            # ========== Scenario Complete ==========
            total_duration = time.time() - scenario_start
            self.total_duration = total_duration
            self.log(
                f"[TC-LOAD-002][{self.username}] Concurrent scenario completed successfully in {total_duration:.2f}s "
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
            self.log(f"[TC-LOAD-002][{self.username}] Unexpected error in concurrent scenario: {str(e)}", logging.ERROR)

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
        pass  # Если не можем проверить - продолжаем

    collector = get_metrics_collector_002()

    # Загружаем baseline метрики если ещё не загружены
    if collector.baseline_metrics is None:
        baseline_config = CONFIG.get('baseline_metrics', {})
        if baseline_config:
            try:
                import os
                csv_path = CONFIG.get("csv_file_path", "")
                if csv_path and os.path.exists(csv_path):
                    size_mb = os.path.getsize(csv_path) / (1024 * 1024)

                    # Ищем ближайший baseline по размеру
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
            except Exception as e:
                pass

    # Останавливаем ClickHouse мониторинг если есть
    if collector.ch_monitor:
        collector.ch_monitor.stop_monitoring()
        collector.ch_monitor.collect_final()

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

    # Генерируем и выводим общий summary
    summary = collector.generate_summary()
    print(summary)

    # Сохраняем в файл
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = f"./logs/tc_load_002_report_{timestamp}.txt"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(summary)
        print(f"\n[TC-LOAD-002] Report saved to: {report_path}\n")
    except Exception as e:
        print(f"\n[TC-LOAD-002] Failed to save report: {e}\n")
