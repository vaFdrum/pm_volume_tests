"""Manager classes for flows and users"""
import threading
from threading import Lock

from config import CONFIG


class FlowManager:
    _lock = Lock()
    _counter = 0

    @classmethod
    def get_next_id(cls, worker_id=0):
        with cls._lock:
            cls._counter += 1
            return worker_id * 100000 + cls._counter


class UserPool:
    _lock = Lock()
    _index = 0

    @classmethod
    def get_credentials(cls):
        with cls._lock:
            creds = CONFIG["users"][cls._index % len(CONFIG["users"])]
            cls._index += 1
            return creds


class StopManager:
    """
    Менеджер для контроля выполнения:
    - Отслеживает сколько пользователей завершили все свои итерации
    - Останавливает тест когда ВСЕ пользователи завершили
    """
    _instance = None
    _lock = threading.Lock()
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not StopManager._initialized:
            self._lock = threading.Lock()
            self._iterations_per_user = CONFIG.get("max_iterations", 1)
            self._total_users = 1
            self._completed_users = 0
            self._user_iterations = {}
            self._should_stop = False
            self._stop_called = False
            StopManager._initialized = True

    def setup_scenario(self, total_users):
        """Настройка сценария при запуске теста"""
        with self._lock:
            if total_users is None:
                total_users = 1

            self._total_users = total_users
            self._completed_users = 0
            self._user_iterations = {}
            self._should_stop = False
            self._stop_called = False
            self._iterations_per_user = CONFIG.get("max_iterations", 1)

    def user_completed_iteration(self, user_id):
        """
        Пользователь завершил одну итерацию
        Возвращает: (user_finished, global_stop)
        """
        with self._lock:
            if user_id not in self._user_iterations:
                self._user_iterations[user_id] = 0

            self._user_iterations[user_id] += 1

            user_finished = self._user_iterations[user_id] >= self._iterations_per_user

            if user_finished:
                self._completed_users += 1

            global_stop = self._completed_users >= self._total_users

            if global_stop and not self._should_stop:
                self._should_stop = True
            return user_finished, global_stop

    def should_stop(self):
        """Должен ли весь тест остановиться"""
        with self._lock:
            return self._should_stop and not self._stop_called

    def set_stop_called(self):
        with self._lock:
            self._stop_called = True

    def is_stop_called(self):
        with self._lock:
            return self._stop_called

    def get_stats(self):
        with self._lock:
            total_iterations = sum(self._user_iterations.values())
            return {
                "completed_users": self._completed_users,
                "total_users": self._total_users,
                "iterations_per_user": self._iterations_per_user,
                "total_iterations": total_iterations,
                "user_iterations": dict(self._user_iterations),
                "should_stop": self._should_stop,
                "stop_called": self._stop_called
            }


stop_manager = StopManager()
