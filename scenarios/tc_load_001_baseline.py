"""
TC-LOAD-001: Baseline Load Test Scenario
Базовый сценарий одного пользователя для установки baseline метрик
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
from common.API.load_api import Api
from common.csv_utils import count_chunks, count_csv_lines
from common.managers import UserPool
from common.clickhouse_monitor import ClickHouseMonitor
from common.report_engine import MetricsCollector, ReportGenerator  # 🆕 Новая система отчетности
from config import CONFIG

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ============================================================================
# 🆕 ENHANCED REPORTING SYSTEM
# ============================================================================
# Создаем глобальный collector для сбора метрик
# Используем новую систему с автоматическими percentiles, SLO tracking и multi-format export
_metrics_collector = MetricsCollector(test_name="TC-LOAD-001")


# ============================================================================
# 📊 SLO DEFINITIONS (Service Level Objectives)
# ============================================================================
# SLO = конкретные измеримые цели для производительности системы
# Формат: collector.define_slo(metric_name, threshold, comparison)
#
# ⚙️ КАК НАСТРОИТЬ ПОСЛЕ ПОЛУЧЕНИЯ РЕАЛЬНЫХ ДАННЫХ:
# 1. Запустите TC-LOAD-001 первый раз
# 2. Посмотрите отчет в ./logs/tc_load_001_report_*.txt
# 3. Найдите секцию "PERFORMANCE METRICS" -> смотрите P95 (95-й перцентиль)
# 4. Установите threshold = P95 * 1.2 (добавляем 20% запас)
# 5. Обновите значения ниже
#
# Пример расчета:
#   Если в отчете видите "P95: 285.3s" для dag1_duration
#   То threshold = 285.3 * 1.2 = 342.36 ≈ 350 секунд
#
# ============================================================================

# SLO #1: DAG #1 Duration (ClickHouse Import)
# 📝 Описание: Время импорта CSV данных в ClickHouse
# 🎯 Текущий порог: 300 секунд (5 минут) - из README.md
# 📊 Где смотреть реальные данные: отчет -> "DAG #1 Duration" -> "P95"
# ✏️ Как изменить: замените 300 на реальное значение P95 * 1.2
_metrics_collector.define_slo(
    name="dag1_duration",           # Имя метрики (НЕ МЕНЯТЬ!)
    threshold=300,                   # ⬅️ ИЗМЕНИТЬ на основе P95 из отчета
    comparison="less_than"           # Должно быть МЕНЬШЕ порога
)

# SLO #2: DAG #2 Duration (PM Dashboard Creation)
# 📝 Описание: Время создания Process Mining дашборда
# 🎯 Текущий порог: 180 секунд (3 минуты) - из README.md
# 📊 Где смотреть реальные данные: отчет -> "DAG #2 Duration" -> "P95"
# ✏️ Как изменить: замените 180 на реальное значение P95 * 1.2
_metrics_collector.define_slo(
    name="dag2_duration",           # Имя метрики (НЕ МЕНЯТЬ!)
    threshold=180,                   # ⬅️ ИЗМЕНИТЬ на основе P95 из отчета
    comparison="less_than"           # Должно быть МЕНЬШЕ порога
)

# SLO #3: Dashboard Load Time
# 📝 Описание: Время загрузки дашборда в браузере
# 🎯 Текущий порог: 3 секунды - из README.md
# 📊 Где смотреть реальные данные: отчет -> "Dashboard Load Time" -> "P95"
# ✏️ Как изменить: замените 3 на реальное значение P95 * 1.2
_metrics_collector.define_slo(
    name="dashboard_duration",      # Имя метрики (НЕ МЕНЯТЬ!)
    threshold=3,                     # ⬅️ ИЗМЕНИТЬ на основе P95 из отчета
    comparison="less_than"           # Должно быть МЕНЬШЕ порога
)

# 💡 ДОПОЛНИТЕЛЬНЫЕ SLO (опционально):
# Раскомментируйте если нужны дополнительные проверки

# SLO #4: CSV Upload Time
# _metrics_collector.define_slo(
#     name="csv_upload_duration",
#     threshold=60,                   # ⬅️ Установите на основе данных
#     comparison="less_than"
# )

# SLO #5: Total Scenario Duration
# _metrics_collector.define_slo(
#     name="total_duration",
#     threshold=600,                  # ⬅️ Установите на основе данных (10 минут)
#     comparison="less_than"
# )

# ============================================================================
# 📌 ВАЖНО:
# - После первого запуска с текущими порогами - проверьте отчет
# - Если SLO FAIL - это нормально, порог слишком строгий
# - Настройте пороги на основе реальных P95 значений
# - Target compliance: >= 95% (т.е. 95% запусков должны укладываться в SLO)
# ============================================================================


def get_metrics_collector() -> MetricsCollector:
    """Возвращает глобальный metrics collector"""
    return _metrics_collector


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class TC_LOAD_001_Baseline(Api):
    """
    TC-LOAD-001: Baseline Load Test

    Сценарий:
    1. Загрузка CSV файла (500 МБ)
    2. Запуск DAG #1 (загрузка в ClickHouse)
    3. Ожидание завершения DAG #1
    4. Запуск DAG #2 (создание датасета и дашборда)
    5. Ожидание завершения DAG #2
    6. Автоматическая генерация дашборда
    7. Открытие и взаимодействие с дашбордом

    Цель: Установить baseline показатели производительности для одного пользователя
    """

    wait_time = between(min_wait=1, max_wait=3)

    def __init__(self, parent):
        super().__init__(parent)
        self.user_id = f"baseline_user_{random.randint(10000, 99999)}"
        self.session_id = f"baseline_{random.randint(1000, 9999)}"
        self.logged_in = False
        self.session_valid = False
        self.total_chunks = count_chunks(CONFIG["csv_file_path"], CONFIG["chunk_size"])
        self.total_lines = count_csv_lines(CONFIG["csv_file_path"])
        self.worker_id = 0
        self.username = None
        self.password = None
        self.flow_id = None
        self.pm_flow_id = None

        # ClickHouse мониторинг
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
        """Инициализирует ClickHouse монитор если включен"""
        ch_config = CONFIG.get("clickhouse", {})

        if not ch_config.get("enabled", False):
            self._log_msg("ClickHouse monitoring disabled")
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
                self._log_msg("ClickHouse monitor initialized successfully")
                # Регистрируем в глобальном collector
                get_metrics_collector().set_clickhouse_monitor(self.ch_monitor)
            else:
                self._log_msg("ClickHouse connection failed, monitoring disabled", logging.WARNING)
                self.ch_monitor = None

        except Exception as e:
            self._log_msg(f"Failed to initialize ClickHouse monitor: {e}", logging.ERROR)
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
        """Helper для упрощения логирования с автоматическим префиксом [TC-LOAD-001]"""
        self.log(f"[TC-LOAD-001] {message}", level)

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
            self._log_msg(f"Authentication successful for {self.username}")
        else:
            self._log_msg("Authentication failed", logging.ERROR)
            self.interrupt()

    def on_start(self):
        """Initialize baseline test"""
        runner = getattr(self, "environment", None)
        if runner:
            runner = getattr(runner, "runner", None)
            self.worker_id = getattr(runner, "worker_id", 0) if runner else 0

        creds = UserPool.get_credentials()
        self.username = creds["username"]
        self.password = creds["password"]
        self.client.verify = False

        self.establish_session()
        self._log_msg("Baseline test started")

        # Устанавливаем время старта в глобальном collector
        get_metrics_collector().set_test_times(time.time(), time.time())

        # Стартуем ClickHouse мониторинг
        if self.ch_monitor:
            self.ch_monitor.collect_baseline()
            self.ch_monitor.start_monitoring()

    def on_stop(self):
        """Clean up when user stops"""
        # Останавливаем ClickHouse мониторинг
        if self.ch_monitor:
            self.ch_monitor.stop_monitoring()
            self.ch_monitor.collect_final()

        self._log_msg("Baseline test stopped")

    @task
    def run_baseline_scenario(self):
        """
        Основной сценарий TC-LOAD-001:
        - CSV Upload
        - DAG #1: File import to ClickHouse
        - DAG #2: PM dashboard creation
        - Dashboard interaction
        """

        if not self.logged_in:
            self.establish_session()
            if not self.logged_in:
                self._log_msg("Failed to establish session", logging.ERROR)
                return

        self._log_msg("Starting baseline scenario")
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
                self._log_msg("Failed to create flow", logging.ERROR)
                return

            self._log_msg(f"File flow created: {flow_name} (ID: {flow_id})")

            # 2. Получение параметров DAG
            target_connection, target_schema = self._get_dag_import_params(flow_id)
            if not target_connection or not target_schema:
                self._log_msg("Missing DAG parameters", logging.ERROR)
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
                self._log_msg("Failed to update flow before upload", logging.ERROR)
                return

            # 4. Получение ID базы данных пользователя
            db_id = self._get_user_database_id()
            if not db_id:
                self._log_msg("User database not found", logging.ERROR)
                return

            if self.total_chunks == 0:
                self._log_msg("No chunks to upload", logging.WARNING)
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
            self._log_msg(f"CSV upload completed: {uploaded_chunks}/{self.total_chunks} chunks in {csv_upload_duration:.2f}s")

            # 7. Финализация загрузки
            if not self._finalize_file_upload(flow_id, uploaded_chunks, timeout):
                return

            # ========== DAG #1: File Processing (ClickHouse Import) ==========
            self._log_msg("[PHASE 2] DAG #1: ClickHouse Import")
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
                self._log_msg("DAG #1 processing failed", logging.ERROR)
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
                self._log_msg("Missing PM DAG parameters", logging.ERROR)
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
                self._log_msg("Failed to create Process Mining flow", logging.ERROR)
                return

            self.pm_flow_id = pm_flow_id
            self._log_msg(f"PM Flow created: {pm_flow_name} (ID: {pm_flow_id})")

            # 12. Запускаем Process Mining flow (DAG #2)
            dag2_start = time.time()
            pm_run_id = self._start_pm_flow(
                pm_flow_id, source_connection, source_schema, table_name
            )

            if not pm_run_id:
                self._log_msg("Failed to start Process Mining flow", logging.ERROR)
                return

            # 13. Мониторинг статуса Process Mining
            pm_timeout = CONFIG["upload_control"]["pm_timeout"]
            pm_result = self._monitor_processing_status(
                pm_run_id, pm_timeout, pm_flow_id, is_pm_flow=True
            )

            if not (isinstance(pm_result, dict) and pm_result.get("success")):
                self._log_msg("DAG #2 processing failed", logging.ERROR)
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
                f"Baseline scenario completed successfully in {total_duration:.2f}s "
                f"(CSV: {self.csv_upload_duration:.2f}s, DAG#1: {self.dag1_duration:.2f}s, DAG#2: {self.dag2_duration:.2f}s)"
            )

            # ========== Регистрируем метрики в глобальном collector ==========
            get_metrics_collector().register_test_run({
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
            get_metrics_collector().set_test_times(self.test_start_time, time.time())

        except Exception as e:
            self._log_msg(f"Unexpected error in baseline scenario: {str(e)}", logging.ERROR)

            # Регистрируем failed run
            get_metrics_collector().register_test_run({
                'success': False,
                'username': self.username,
                'error': str(e),
            })


# ========== Locust Event Listeners ==========

@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Вызывается при завершении теста - генерируем общий отчёт"""

    # Проверяем что TC-LOAD-001 запущен
    try:
        from locustfile import SupersetUser
        if TC_LOAD_001_Baseline not in SupersetUser.tasks:
            return  # Этот тест не запущен, пропускаем
    except Exception:
        return  # Если не можем проверить - пропускаем (другой тест запущен)

    collector = get_metrics_collector()

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
    # - Error analysis
    # - Smart recommendations
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
        print(f"\n[TC-LOAD-001] ✓ Successfully saved {len(saved_files)} report files:")
        for filepath in saved_files:
            print(f"  - {filepath}")
        print()
    except Exception as e:
        print(f"\n[TC-LOAD-001] ✗ Failed to save reports: {e}\n")
