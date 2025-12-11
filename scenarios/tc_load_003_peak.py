"""
TC-LOAD-003: Peak Concurrent Load Test
5 Heavy Users (ETL) + 3 Light Users (Superset UI)

Цель: Симуляция максимальной пиковой нагрузки
- Heavy users: параллельная загрузка CSV, DAG#1, DAG#2 (без координации)
- Light users: работа с готовыми дашбордами (фильтры, экспорт, навигация)
"""

import logging
import os
import random
import threading
import time
import urllib3
from datetime import datetime
from typing import Optional, List, Dict
from threading import Lock, Event

from locust import task, between, events

from common.auth import establish_session
from common.api.load_api import LoadApi
from common.api.object_api import ChartApi
from common.csv_utils import count_chunks, count_csv_lines
from common.managers import UserPool
from common.clickhouse_monitor import ClickHouseMonitor
from common.report_engine import MetricsCollector, ReportGenerator  # 🆕 Unified reporting system
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
    - Потокобезопасно (threading.Lock + threading.Event)
    """

    def __init__(self):
        self.lock = Lock()
        self.dashboards: List[tuple] = []  # [(url, owner_username, created_timestamp)]
        self.event = Event()  # Для эффективного ожидания появления дашбордов

    def add(self, url: str, owner: str):
        """Heavy user регистрирует созданный дашборд"""
        with self.lock:
            self.dashboards.append((url, owner, time.time()))
            self.event.set()  # Сигнализируем ожидающим Light users
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
        Использует threading.Event для эффективного ожидания
        Возвращает True если дашборды появились, False если таймаут
        """
        return self.event.wait(timeout)


# Глобальный singleton
_dashboard_pool_003 = DashboardPool()


def get_dashboard_pool_003() -> DashboardPool:
    """Возвращает глобальный DashboardPool"""
    return _dashboard_pool_003


# ============================================================================
# СЕКЦИЯ 2: UNIFIED METRICS COLLECTOR
# ============================================================================
# Используем MetricsCollector из report_engine.py для унифицированной отчётности
# Один collector собирает метрики от обоих типов пользователей:
# - Heavy users: ETL операции (с SLO validation и baseline comparison)
# - Light users: Superset UI операции (только статистика)
# ============================================================================

_metrics_collector_003 = MetricsCollector(test_name="TC-LOAD-003")


# ============================================================================
# 📊 SLO DEFINITIONS FOR TC-LOAD-003 (Peak Load Test)
# ============================================================================
# TC-LOAD-003 проверяет производительность при ПИКОВОЙ нагрузке (5 Heavy + 3 Light)
# SLO критерий из README.md: "Не более ×2 от baseline метрик"
#
# ⚙️ КАК НАСТРОИТЬ ПОСЛЕ ПОЛУЧЕНИЯ BASELINE:
#
# ШАГИ НАСТРОЙКИ:
# 1. Запустите TC-LOAD-001 и получите baseline метрики
# 2. Посмотрите в отчете TC-LOAD-001 значения P95 для каждой метрики
# 3. Установите SLO для TC-LOAD-003 = P95_baseline * 2.0 (удвоение допустимо при пике)
# 4. Обновите значения ниже
#
# Пример расчета:
#   TC-LOAD-001 отчет показывает "DAG #1 P95: 280.5s"
#   TC-LOAD-003 SLO = 280.5 * 2.0 = 561 секунд
#   Это означает: при пиковой нагрузке допустимо замедление до ×2
#
# 📌 ВАЖНО: SLO применяются ТОЛЬКО к Heavy users (ETL операциям)!
# Light users - это дополнительная нагрузка, их метрики собираются только для статистики
# ============================================================================

# SLO #1: DAG #1 Duration для Peak теста (Heavy users only)
# 📝 Описание: Время импорта CSV в ClickHouse при 5 Heavy + 3 Light пользователях
# 🎯 Текущий порог: 600 секунд (300s baseline * 2.0)
# 📊 Baseline из TC-LOAD-001: 300s (из README.md)
# ✏️ Как изменить: threshold = (P95 из TC-LOAD-001) * 2.0
_metrics_collector_003.define_slo(
    name="dag1_duration",
    threshold=600,                   # ⬅️ ИЗМЕНИТЬ: P95_baseline * 2.0
    comparison="less_than"
)

# SLO #2: DAG #2 Duration для Peak теста (Heavy users only)
# 📝 Описание: Время создания PM дашборда при 5 Heavy + 3 Light пользователях
# 🎯 Текущий порог: 360 секунд (180s baseline * 2.0)
# 📊 Baseline из TC-LOAD-001: 180s (из README.md)
# ✏️ Как изменить: threshold = (P95 из TC-LOAD-001) * 2.0
_metrics_collector_003.define_slo(
    name="dag2_duration",
    threshold=360,                   # ⬅️ ИЗМЕНИТЬ: P95_baseline * 2.0
    comparison="less_than"
)

# SLO #3: Dashboard Load для Peak теста (Heavy users only)
# 📝 Описание: Время загрузки дашборда при 5 Heavy + 3 Light пользователях
# 🎯 Текущий порог: 6.0 секунд (3s baseline * 2.0)
# 📊 Baseline из TC-LOAD-001: 3s (из README.md)
# ✏️ Как изменить: threshold = (P95 из TC-LOAD-001) * 2.0
_metrics_collector_003.define_slo(
    name="dashboard_duration",
    threshold=6.0,                   # ⬅️ ИЗМЕНИТЬ: P95_baseline * 2.0
    comparison="less_than"
)

# ============================================================================
# 📊 BASELINE METRICS SETUP
# ============================================================================
# Baseline метрики автоматически загружаются из config_multi.yaml
# См. секцию 'baseline_metrics' в config файле
#
# Применяется ТОЛЬКО к Heavy users!
# Light users не сравниваются с baseline - это просто доп. нагрузка
# ============================================================================


def get_metrics_collector_003() -> MetricsCollector:
    """Возвращает глобальный metrics collector для TC-LOAD-003"""
    return _metrics_collector_003


# ============================================================================
# СЕКЦИЯ 3: HEAVY USER CLASS (ETL Operations)
# ============================================================================

class TC_LOAD_003_Heavy(LoadApi):
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
        Thread-safe с использованием Lock для предотвращения race condition
        """
        ch_config = CONFIG.get("clickhouse", {})

        if not ch_config.get("enabled", False):
            self.log("[TC-LOAD-003][Heavy] ClickHouse monitoring disabled")
            return

        # Проверяем, не инициализирован ли уже (с Lock для thread-safety)
        collector = get_metrics_collector_003()
        with collector.lock:
            if collector.clickhouse_monitor is not None:
                self.log("[TC-LOAD-003][Heavy] ClickHouse monitor already initialized by another user")
                return

            # Инициализируем только если монитора еще нет
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
                    # Регистрируем в глобальном collector (уже внутри Lock)
                    collector.clickhouse_monitor = self.ch_monitor
                else:
                    self.log("[TC-LOAD-003][Heavy] ClickHouse connection failed, monitoring disabled", logging.WARNING)
                    self.ch_monitor = None

            except Exception as e:
                self.log(f"[TC-LOAD-003][Heavy] Failed to initialize ClickHouse monitor: {e}", logging.ERROR)
                self.ch_monitor = None

    def _format_file_size(self) -> str:
        """Форматирует размер файла для отчёта"""
        try:
            csv_path = CONFIG.get("csv_file_path", "")
            if csv_path and os.path.exists(csv_path):
                size_bytes = os.path.getsize(csv_path)
                size_mb = size_bytes / (1024 * 1024)
                return f"{size_mb:.1f} MB"
        except Exception:
            pass
        return "N/A"

    def _log_msg(self, message: str, level=logging.INFO):
        """Helper для упрощения логирования с автоматическим префиксом [TC-LOAD-003][Heavy][username]"""
        self.log(f"[TC-LOAD-003][Heavy][{self.username}] {message}", level)

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

    def _register_failure(self, reason: str):
        """
        Регистрирует неудачное выполнение сценария в метриках
        Используется для всех early returns чтобы правильно считать success rate
        """
        get_metrics_collector_003().register_test_run({
            'success': False,
            'user_type': 'heavy',
            'username': self.username,
            'error': reason,
        })
        self._log_msg(f"Scenario failed: {reason}", logging.ERROR)

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
                self._register_failure("authentication_failed")
                return

        self._log_msg("Starting ETL scenario")
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

                        # ✅ КЛЮЧЕВОЙ МОМЕНТ: Регистрируем дашборд для Light users!
                        get_dashboard_pool_003().add(dashboard_url, self.username)
                        self._log_msg(f"Dashboard registered for Light users (total: {get_dashboard_pool_003().count()})")
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
                f"ETL completed successfully in {total_duration:.2f}s "
                f"(CSV: {self.csv_upload_duration:.2f}s, DAG#1: {self.dag1_duration:.2f}s, DAG#2: {self.dag2_duration:.2f}s)"
            )

            # ========== Регистрируем метрики в глобальном collector ==========
            get_metrics_collector_003().register_test_run({
                'success': True,
                'user_type': 'heavy',
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
            self._log_msg(f"Unexpected error in ETL scenario: {str(e)}", logging.ERROR)

            # Регистрируем failed run
            get_metrics_collector_003().register_test_run({
                'success': False,
                'user_type': 'heavy',
                'username': self.username,
                'error': str(e),
            })


# ============================================================================
# СЕКЦИЯ 4: LIGHT USER CLASS (Superset UI Operations)
# ============================================================================

class TC_LOAD_003_Light(LoadApi):
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

        # ChartApi для создания чартов
        self.chart_api: Optional[ChartApi] = None

        # Счётчики для метрик
        self.dashboard_opens = 0
        self.chart_creates = 0
        self.exports = 0

        # Время операций
        self.dashboard_load_times = []
        self.chart_create_times = []
        self.export_times = []

    def _log_msg(self, message: str, level=logging.INFO):
        """Helper для упрощения логирования с автоматическим префиксом [TC-LOAD-003][Light][username]"""
        self.log(f"[TC-LOAD-003][Light][{self.username}] {message}", level)

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
            self._log_msg("Failed to authenticate", logging.ERROR)
            self.interrupt()
            return

        # 2. Инициализируем ChartApi
        self.chart_api = ChartApi(self.client, self.log)

        # 3. Ждём дашборды
        self._log_msg("Waiting for dashboards from Heavy users...")

        if not get_dashboard_pool_003().wait_until_available(timeout=600):
            self._log_msg(f"Timeout: No dashboards available after 10 min", logging.WARNING)
            self.interrupt()
            return

        self._log_msg(f"Dashboards available ({get_dashboard_pool_003().count()})! Starting UI operations")

    def on_stop(self):
        """
        Завершение работы Light user
        Регистрируем финальные метрики (агрегированные за весь тест)
        """

        if self.dashboard_opens > 0 or self.chart_creates > 0:
            get_metrics_collector_003().register_test_run({
                'user_type': 'light',
                'username': self.username,
                'dashboard_opens': self.dashboard_opens,
                'chart_creates': self.chart_creates,
                'exports': self.exports,
                'dashboard_load_times': self.dashboard_load_times,
                'chart_create_times': self.chart_create_times,
                'export_times': self.export_times,
            })

        self._log_msg(f"Stopped. Operations: "
                 f"{self.dashboard_opens} opens, {self.chart_creates} charts, {self.exports} exports")

    @task(weight=5)
    def open_and_explore_dashboard(self):
        """
        ЗАДАЧА 1: Открыть дашборд и поработать с ним

        Симулирует реального пользователя:
        - Открывает дашборд
        - Ждёт загрузки всех компонентов
        - Измеряет время отклика

        Weight=5: самая частая операция
        """

        dashboard_url = get_dashboard_pool_003().get_random()

        if not dashboard_url:
            self._log_msg("No dashboards in pool", logging.WARNING)
            return

        start_time = time.time()

        # Используем метод из базового класса LoadApi
        success = self._open_dashboard(dashboard_url)
        load_time = time.time() - start_time

        if success:
            self.dashboard_load_times.append(load_time)
            self.dashboard_opens += 1
            self._log_msg(f"Dashboard loaded in {load_time:.2f}s")
        else:
            self._log_msg(f"Failed to load dashboard: {dashboard_url}", logging.WARNING)

    @task(weight=3)
    def create_chart(self):
        """
        ЗАДАЧА 2: Создать чарт на дашборде

        Симулирует аналитика, который:
        - Выбирает дашборд
        - Получает datasource_id из информации о дашборде
        - Создаёт новый чарт (table, histogramChart или supersetGraph)

        Weight=3: средняя частота
        """

        dashboard_url = get_dashboard_pool_003().get_random()

        if not dashboard_url:
            self._log_msg("No dashboards in pool for chart creation", logging.WARNING)
            return

        if not self.chart_api:
            self._log_msg("ChartApi not initialized", logging.ERROR)
            return

        start_time = time.time()

        # Получаем информацию о дашборде (включая datasource_id)
        dashboard_info = self.chart_api.get_dashboard_info(dashboard_url)

        if not dashboard_info or not dashboard_info.get('datasource_id'):
            self._log_msg(f"Could not get datasource_id from dashboard: {dashboard_url}", logging.WARNING)
            return

        datasource_id = dashboard_info['datasource_id']
        self._log_msg(f"Creating chart for datasource_id={datasource_id}")

        # Создаём и сохраняем чарт (случайный тип)
        success, chart_id = self.chart_api.create_and_save_chart(datasource_id)
        create_time = time.time() - start_time

        if success:
            self.chart_create_times.append(create_time)
            self.chart_creates += 1
            self._log_msg(f"Chart created in {create_time:.2f}s (chart_id={chart_id})")
        else:
            self._log_msg(f"Failed to create chart for datasource_id={datasource_id}", logging.WARNING)

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

        self._log_msg("Exporting data from dashboard")

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

        self._log_msg(f"Export completed in {export_time:.2f}s")


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
    Генерирует unified отчёт используя ReportGenerator
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
    if collector.clickhouse_monitor:
        collector.clickhouse_monitor.stop_monitoring()
        collector.clickhouse_monitor.collect_final()

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

    # ============================================================================
    # 🆕 ГЕНЕРАЦИЯ ENHANCED ОТЧЕТОВ
    # ============================================================================
    # Используем новую систему отчетности с:
    # - Автоматическими percentiles (P50, P75, P90, P95, P99)
    # - SLO compliance tracking (только для Heavy users)
    # - Baseline comparison (только для Heavy users)
    # - Separate sections для Heavy и Light users
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
        print(f"\n[TC-LOAD-003] ✓ Successfully saved {len(saved_files)} report files:")
        for filepath in saved_files:
            print(f"  - {filepath}")
    except Exception as e:
        print(f"\n[TC-LOAD-003] ✗ Failed to save reports: {e}")

    print("\n" + "=" * 80)
    print("[TC-LOAD-003] Peak Concurrent Load Test completed")
    print("=" * 80 + "\n")
