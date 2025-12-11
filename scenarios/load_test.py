"""Locust tasks module for Superset ETL flow testing"""

import logging
import random
import time
import urllib3

from locust import task, between

from common.auth import establish_session
from common.api.load_api import Api
from common.csv_utils import count_chunks, count_csv_lines
from common.managers import UserPool, stop_manager
from common.metrics import (
    ACTIVE_USERS,
    SESSION_STATUS,
    EXPECTED_ROWS,
    start_metrics_server,
)
from config import CONFIG

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

start_metrics_server(CONFIG.get("metrics_port", 9090))


class LoadFlow(Api):
    """ETL flow load testing task set"""

    wait_time = between(min_wait=1, max_wait=5)

    def __init__(self, parent):
        super().__init__(parent)
        self.user_id = f"user_{random.randint(10000, 99999)}"
        self.session_id = f"{random.randint(1000, 9999)}"
        self.user_stop_triggered = False
        self.global_stop_triggered = False
        self.logged_in = False
        self.session_valid = False
        self.total_chunks = count_chunks(CONFIG["csv_file_path"], CONFIG["chunk_size"])
        self.total_lines = count_csv_lines(CONFIG["csv_file_path"])
        self.worker_id = 0
        self.username = None
        self.password = None
        self.flow_id = None
        self.user_iteration_count = 0
        self.max_user_iterations = CONFIG.get("max_iterations", 1)

        # Устанавливаем метрику ожидаемых строк
        EXPECTED_ROWS.set(self.total_lines)

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
            ACTIVE_USERS.inc()
            SESSION_STATUS.labels(username=self.username).set(1)
            self.log(f"Authentication successful for {self.username}")
        else:
            self.log("Authentication failed", logging.ERROR)
            self.interrupt()

    def on_start(self):
        """Initialize user session and credentials"""
        if not hasattr(self.user.environment, 'stop_manager_initialized'):
            try:
                import sys
                total_users = 1
                for i, arg in enumerate(sys.argv):
                    if arg == '-u' and i + 1 < len(sys.argv):
                        try:
                            total_users = int(sys.argv[i + 1])
                            break
                        except (ValueError, IndexError):
                            pass

                stop_manager.setup_scenario(total_users)
                self.user.environment.stop_manager_initialized = True

                max_iterations = CONFIG.get("max_iterations", 1)
                print(f"\n=== TEST CONFIGURATION ===")
                print(f"Users: {total_users}")
                print(f"Iterations per user: {max_iterations}")
                print(f"Total iterations needed: {total_users * max_iterations}")
                print("==========================\n")
            except Exception as e:
                print(f"Error initializing stop manager: {e}")
                stop_manager.setup_scenario(1)
                self.user.environment.stop_manager_initialized = True

        if self.user_stop_triggered or self.global_stop_triggered:
            return

        if stop_manager.is_stop_called():
            self.global_stop_triggered = True
            return

        if stop_manager.should_stop():
            self._safe_stop_runner("Global stop signal detected in on_start.")
            return

        runner = getattr(self, "environment", None)
        if runner:
            runner = getattr(runner, "runner", None)
            self.worker_id = getattr(runner, "worker_id", 0) if runner else 0

        creds = UserPool.get_credentials()
        self.username = creds["username"]
        self.password = creds["password"]
        self.client.verify = False
        self.establish_session()

        self.log(f"User started. Max iterations: {self.max_user_iterations}")

    def on_stop(self):
        """Clean up metrics when user stops"""
        if self.logged_in:
            ACTIVE_USERS.dec()
            SESSION_STATUS.labels(username=self.username).set(0)

        self.log(f"User stopping. Completed {self.user_iteration_count} iterations")

    def _safe_stop_runner(self, message):
        """Безопасная остановка runner с защитой от повторных срабатываний"""
        if self.user_stop_triggered or self.global_stop_triggered:
            return

        self.global_stop_triggered = True
        self.log(f"Global stop: {message}")

        if hasattr(self, 'environment') and hasattr(self.environment, 'runner'):
            runner = self.environment.runner
            if not getattr(runner, 'stopped', False):
                stop_manager.set_stop_called()
                runner.stop()
                print(f"Test stopped: {message}")

    def _complete_iteration(self, success=True):
        """Завершение итерации и проверка условий остановки"""
        try:
            user_finished, global_stop = stop_manager.user_completed_iteration(self.user_id)

            stats = stop_manager.get_stats()
            status = "SUCCESS" if success else "FAILED"

            self.log(f"Iteration {status}. Progress: {stats['completed_users']}/{stats['total_users']} users completed")

            if global_stop:
                self._safe_stop_runner(f"All {stats['total_users']} users completed their iterations")
            elif user_finished:
                self.user_stop_triggered = True
                self.log(f"User completed all {self.max_user_iterations} iterations - stopping this user")
                self.interrupt()
        except Exception as e:
            self.log(f"Error in _complete_iteration: {e}", logging.ERROR)
            self.user_stop_triggered = True
            self.interrupt()

    @task
    def create_and_upload_flow(self):
        """Основная задача: создание и загрузка flow"""

        if self.user_stop_triggered:
            self.log("User already completed all iterations - skipping")
            return

        if self.global_stop_triggered:
            return

        if stop_manager.is_stop_called():
            self.global_stop_triggered = True
            return

        if stop_manager.should_stop():
            self._safe_stop_runner("Global stop detected at task start")
            return

        if self.user_iteration_count >= self.max_user_iterations:
            self.user_stop_triggered = True
            self.log(f"User reached iteration limit {self.max_user_iterations} - stopping")
            self.interrupt()
            return

        self.user_iteration_count += 1
        self.log(f"Starting iteration {self.user_iteration_count}/{self.max_user_iterations}")

        flow_processing_start = time.time()

        if not self.logged_in:
            self.establish_session()
            if not self.logged_in:
                self.log("Failed to establish session", logging.ERROR)
                self._complete_iteration(success=False)
                return

        self.log("Starting flow creation and upload process")

        try:
            # 1. Создание flow
            flow_name, flow_id = self._create_flow(worker_id=self.worker_id)
            self.flow_id = flow_id

            if not flow_id:
                self.log("Failed to create flow", logging.ERROR)
                self._complete_iteration(success=False)
                return

            self.log(f"Flow created: {flow_name} (ID: {flow_id})")

            # 2. Получение параметров DAG
            target_connection, target_schema = self._get_dag_import_params(flow_id)
            if not target_connection or not target_schema:
                self.log("Missing DAG parameters", logging.ERROR)
                self._complete_iteration(success=False)
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
                self.log("Failed to update flow before upload", logging.ERROR)
                self._complete_iteration(success=False)
                return

            # 4. Получение ID базы данных пользователя
            db_id = self._get_user_database_id()
            if not db_id:
                self.log("User database not found", logging.ERROR)
                self._complete_iteration(success=False)
                return

            if self.total_chunks == 0:
                self.log("No chunks to upload", logging.WARNING)
                self._complete_iteration(success=False)
                return

            timeout = (
                CONFIG["upload_control"]["timeout_large"]
                if self.total_chunks > CONFIG["upload_control"]["chunk_threshold"]
                else CONFIG["upload_control"]["timeout_small"]
            )

            # 5. Начало загрузки
            if not self._start_file_upload(flow_id, db_id, target_schema, self.total_chunks, timeout):
                self._complete_iteration(success=False)
                return

            # 6. Загрузка чанков
            uploaded_chunks = self._upload_chunks(flow_id, db_id, target_schema, self.total_chunks)
            self.log(f"Chunk upload completed: {uploaded_chunks}/{self.total_chunks} chunks")

            # 7. Финализация загрузки
            if not self._finalize_file_upload(flow_id, uploaded_chunks, timeout):
                self._complete_iteration(success=False)
                return

            # 8. Начало обработки
            run_id = self._start_file_processing(flow_id, target_connection, target_schema,
                                                 self.total_chunks, timeout)
            if not run_id:
                self._complete_iteration(success=False)
                return

            # 9. Мониторинг статуса обработки
            success = self._monitor_processing_status(
                run_id, timeout, flow_id, db_id, target_schema,
                self.total_lines, flow_processing_start
            )

            self._complete_iteration(success=success)

        except Exception as e:
            self.log(f"Unexpected error in flow processing: {str(e)}", logging.ERROR)
            self._complete_iteration(success=False)
