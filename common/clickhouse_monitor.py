"""
ClickHouse Monitoring для TC-LOAD-001
Сбор метрик из ClickHouse во время нагрузочного тестирования
"""

import logging
import requests
from typing import Dict, List, Optional, Any
from threading import Thread, Event
from datetime import datetime

logger = logging.getLogger(__name__)


class ClickHouseMonitor:
    """
    Мониторинг ClickHouse метрик

    Собирает метрики в 3 точках:
    - Начало теста (baseline)
    - Периодически во время теста (фоновый сбор)
    - Конец теста (финальные значения)
    """

    def __init__(
            self,
            host: str,
            port: int = 8123,
            user: str = "default",
            password: str = "",
            monitoring_interval: int = 10
    ):
        """
        Args:
            host: ClickHouse хост
            port: HTTP порт (обычно 8123)
            user: Имя пользователя
            password: Пароль
            monitoring_interval: Интервал сбора метрик в секундах
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.monitoring_interval = monitoring_interval
        self.base_url = f"http://{host}:{port}"

        # Хранилище метрик
        self.baseline_metrics: Optional[Dict] = None
        self.periodic_metrics: List[Dict] = []
        self.final_metrics: Optional[Dict] = None

        # Для фонового мониторинга
        self._monitoring_thread: Optional[Thread] = None
        self._stop_event = Event()
        self._monitoring_active = False

    def _execute_query(self, query: str) -> Optional[str]:
        """Выполняет запрос к ClickHouse"""
        try:
            params = {
                "query": query,
                "user": self.user,
                "password": self.password
            }

            response = requests.get(
                self.base_url,
                params=params,
                timeout=10
            )

            if response.status_code == 200:
                return response.text.strip()
            else:
                logger.warning(f"ClickHouse query failed: {response.status_code}")
                return None

        except Exception as e:
            logger.error(f"Error executing ClickHouse query: {e}")
            return None

    def _collect_system_metrics(self) -> Dict[str, Any]:
        """Собирает метрики из system.metrics"""
        metrics = {}

        # Основные метрики
        metric_names = [
            'Query',  # Количество выполняющихся запросов
            'Merge',  # Количество активных слияний
            'MemoryTracking',  # Использование памяти
            'BackgroundPoolTask',  # Фоновые задачи
            'HTTPConnection',  # HTTP соединения
        ]

        for metric_name in metric_names:
            query = f"SELECT value FROM system.metrics WHERE metric = '{metric_name}'"
            result = self._execute_query(query)
            if result:
                try:
                    metrics[metric_name] = int(result)
                except ValueError:
                    metrics[metric_name] = result

        return metrics

    def _collect_system_events(self) -> Dict[str, Any]:
        """Собирает счётчики событий из system.events"""
        events = {}

        # Интересующие события
        event_names = [
            'Query',  # Всего запросов
            'SelectQuery',  # SELECT запросы
            'InsertQuery',  # INSERT запросы
            'InsertedRows',  # Вставленные строки
            'InsertedBytes',  # Вставленные байты
            'FailedQuery',  # Неудачные запросы
            'QueryTimeMicroseconds',  # Время выполнения запросов
        ]

        for event_name in event_names:
            query = f"SELECT value FROM system.events WHERE event = '{event_name}'"
            result = self._execute_query(query)
            if result:
                try:
                    events[event_name] = int(result)
                except ValueError:
                    events[event_name] = result

        return events

    def _collect_running_processes(self) -> Dict[str, Any]:
        """Собирает информацию об активных запросах из system.processes"""
        processes = {}

        # Количество активных запросов
        query = "SELECT count() FROM system.processes"
        result = self._execute_query(query)
        if result:
            try:
                processes['active_queries'] = int(result)
            except ValueError:
                processes['active_queries'] = 0

        # Типы активных запросов
        query = """
        SELECT 
            query_kind,
            count() as count
        FROM system.processes
        GROUP BY query_kind
        """
        result = self._execute_query(query)
        if result:
            processes['by_type'] = result

        return processes

    def _collect_all_metrics(self) -> Dict[str, Any]:
        """Собирает все метрики"""
        return {
            'timestamp': datetime.now().isoformat(),
            'system_metrics': self._collect_system_metrics(),
            'system_events': self._collect_system_events(),
            'processes': self._collect_running_processes()
        }

    def check_connection(self) -> bool:
        """Проверяет доступность ClickHouse"""
        try:
            result = self._execute_query("SELECT 1")
            return result == "1"
        except Exception as e:
            logger.error(f"ClickHouse connection check failed: {e}")
            return False

    def collect_baseline(self):
        """Собирает baseline метрики в начале теста"""
        logger.info("[ClickHouse] Collecting baseline metrics...")
        self.baseline_metrics = self._collect_all_metrics()
        logger.info("[ClickHouse] Baseline metrics collected")

    def start_monitoring(self):
        """Запускает фоновый мониторинг"""
        if self._monitoring_active:
            logger.warning("[ClickHouse] Monitoring already active")
            return

        self._stop_event.clear()
        self._monitoring_active = True
        self._monitoring_thread = Thread(target=self._monitoring_loop, daemon=True)
        self._monitoring_thread.start()
        logger.info(f"[ClickHouse] Background monitoring started (interval: {self.monitoring_interval}s)")

    def _monitoring_loop(self):
        """Цикл фонового мониторинга"""
        while not self._stop_event.is_set():
            try:
                metrics = self._collect_all_metrics()
                self.periodic_metrics.append(metrics)
                logger.debug(f"[ClickHouse] Periodic metrics collected ({len(self.periodic_metrics)} samples)")
            except Exception as e:
                logger.error(f"[ClickHouse] Error in monitoring loop: {e}")

            # Ждём интервал или stop event
            self._stop_event.wait(self.monitoring_interval)

    def stop_monitoring(self):
        """Останавливает фоновый мониторинг"""
        if not self._monitoring_active:
            logger.warning("[ClickHouse] Monitoring not active")
            return

        self._stop_event.set()
        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=5)

        self._monitoring_active = False
        logger.info(f"[ClickHouse] Background monitoring stopped ({len(self.periodic_metrics)} samples collected)")

    def collect_final(self):
        """Собирает финальные метрики в конце теста"""
        logger.info("[ClickHouse] Collecting final metrics...")
        self.final_metrics = self._collect_all_metrics()
        logger.info("[ClickHouse] Final metrics collected")

    def get_summary(self) -> Dict[str, Any]:
        """
        Возвращает summary метрик для отчёта

        Returns:
            Словарь с агрегированными метриками
        """
        summary = {
            'baseline': self.baseline_metrics,
            'final': self.final_metrics,
            'periodic_samples': len(self.periodic_metrics)
        }

        # Вычисляем дельты между baseline и final
        if self.baseline_metrics and self.final_metrics:
            baseline_events = self.baseline_metrics.get('system_events', {})
            final_events = self.final_metrics.get('system_events', {})

            summary['deltas'] = {
                'total_queries': final_events.get('Query', 0) - baseline_events.get('Query', 0),
                'select_queries': final_events.get('SelectQuery', 0) - baseline_events.get('SelectQuery', 0),
                'insert_queries': final_events.get('InsertQuery', 0) - baseline_events.get('InsertQuery', 0),
                'inserted_rows': final_events.get('InsertedRows', 0) - baseline_events.get('InsertedRows', 0),
                'inserted_bytes': final_events.get('InsertedBytes', 0) - baseline_events.get('InsertedBytes', 0),
                'failed_queries': final_events.get('FailedQuery', 0) - baseline_events.get('FailedQuery', 0),
            }

        # Пиковые значения из periodic метрик
        if self.periodic_metrics:
            summary['peak_values'] = self._calculate_peaks()

        return summary

    def _calculate_peaks(self) -> Dict[str, Any]:
        """Вычисляет пиковые значения из периодических метрик"""
        peaks = {
            'max_active_queries': 0,
            'max_memory': 0,
        }

        for metrics in self.periodic_metrics:
            system_metrics = metrics.get('system_metrics', {})
            processes = metrics.get('processes', {})

            # Максимум активных запросов
            active = processes.get('active_queries', 0)
            if active > peaks['max_active_queries']:
                peaks['max_active_queries'] = active

            # Максимум памяти
            memory = system_metrics.get('MemoryTracking', 0)
            if memory > peaks['max_memory']:
                peaks['max_memory'] = memory

        return peaks

    def format_summary_report(self) -> str:
        """Форматирует summary для отчёта"""
        summary = self.get_summary()

        lines = [
            "\nCLICKHOUSE МЕТРИКИ",
            "-" * 50,
        ]

        # Дельты
        if 'deltas' in summary:
            deltas = summary['deltas']
            lines.extend([
                f"Всего запросов: {deltas['total_queries']}",
                f"  SELECT: {deltas['select_queries']}",
                f"  INSERT: {deltas['insert_queries']}",
                f"Вставлено строк: {deltas['inserted_rows']:,}",
                f"Вставлено байт: {deltas['inserted_bytes']:,}",
                f"Неудачных запросов: {deltas['failed_queries']}",
            ])

        # Пиковые значения
        if 'peak_values' in summary:
            peaks = summary['peak_values']
            lines.extend([
                f"\nПиковые значения:",
                f"  Максимум активных запросов: {peaks['max_active_queries']}",
                f"  Максимум памяти: {peaks['max_memory']:,} bytes",
            ])

        # Финальное состояние
        if summary['final']:
            final_metrics = summary['final'].get('system_metrics', {})
            lines.extend([
                f"\nФинальное состояние:",
                f"  Активных запросов: {final_metrics.get('Query', 0)}",
                f"  HTTP соединений: {final_metrics.get('HTTPConnection', 0)}",
            ])

        return "\n".join(lines)


# Глобальный instance для использования в тестах
_monitor: Optional[ClickHouseMonitor] = None


def get_monitor() -> Optional[ClickHouseMonitor]:
    """Возвращает глобальный instance монитора"""
    return _monitor


def initialize_monitor(config: Dict) -> ClickHouseMonitor:
    """
    Инициализирует глобальный монитор

    Args:
        config: Конфигурация ClickHouse из CONFIG

    Returns:
        Инициализированный монитор
    """
    global _monitor

    if _monitor is not None:
        logger.warning("[ClickHouse] Monitor already initialized")
        return _monitor

    _monitor = ClickHouseMonitor(
        host=config.get("host", "localhost"),
        port=config.get("port", 8123),
        user=config.get("user", "default"),
        password=config.get("password", ""),
        monitoring_interval=config.get("monitoring_interval", 10)
    )

    # Проверяем подключение
    if not _monitor.check_connection():
        logger.error("[ClickHouse] Failed to connect to ClickHouse")
        _monitor = None
        raise ConnectionError("Cannot connect to ClickHouse")

    logger.info("[ClickHouse] Monitor initialized successfully")
    return _monitor


def cleanup_monitor():
    """Очищает глобальный монитор"""
    global _monitor

    if _monitor and _monitor._monitoring_active:
        _monitor.stop_monitoring()

    _monitor = None
