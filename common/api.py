"""Base api classes with reusable methods"""

import copy
import logging
import time
from datetime import datetime
from urllib.parse import quote

from locust import SequentialTaskSet

from common.csv_utils import split_csv_generator
from common.managers import FlowManager, stop_manager
from common.metrics import (
    REQUEST_COUNT,
    REQUEST_DURATION,
    FLOW_CREATIONS,
    CHUNK_UPLOADS,
    CHUNKS_IN_PROGRESS,
    UPLOAD_PROGRESS,
    CHUNK_UPLOAD_DURATION,
    DB_ROW_COUNT,
    COUNT_VALIDATION_RESULT,
    FLOW_PROCESSING_DURATION,
)
from config import CONFIG


class Api(SequentialTaskSet):
    def __init__(self, parent):
        super().__init__(parent)
        self.username = None
        self.password = None
        self.session_id = None
        self.logged_in = False
        self.session_valid = False

    def log(self, message, level=logging.INFO):
        """Logging with session context"""
        if not CONFIG.get("log_verbose") and level not in [
            logging.ERROR,
            logging.CRITICAL,
        ]:
            return

        level_name = logging.getLevelName(level)
        timestamp = datetime.now().strftime("%Y-%m-%d")

        # Формируем контекст для лога
        iteration_info = ""
        if hasattr(self, 'user_iteration_count') and hasattr(self, 'max_user_iterations'):
            iteration_info = f"[Iter {self.user_iteration_count}/{self.max_user_iterations}]"

        log_message = (
            f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S,%f')[:-3]} - "
            f"SupersetLoadTest - {level_name} - "
            f"[User {self.username or 'N/A'}][Session {self.session_id}]{iteration_info} {message}\n"
        )

        if level >= logging.WARNING or CONFIG.get("log_verbose"):
            print(log_message, end="")

        try:
            log_filename = f"./logs/locust_test_{timestamp}.log"
            with open(log_filename, "a", encoding="utf-8") as file:
                file.write(log_message)
                file.flush()
        except Exception as error:
            print(f"Log file error: {error}")

    def _retry_request(self, method, url, name, **kwargs):
        """Retry mechanism with timeouts and metrics"""
        timeout = kwargs.pop("timeout", CONFIG["request_timeout"])
        start_time = time.time()

        for attempt in range(CONFIG["max_retries"]):
            try:
                kwargs["timeout"] = timeout
                with method(url, name=name, catch_response=True, **kwargs) as response:
                    if response.status_code < 400:
                        # Записываем метрики успешного запроса
                        duration = time.time() - start_time
                        REQUEST_DURATION.labels(
                            method=method.__name__.upper(), endpoint=name
                        ).observe(duration)
                        REQUEST_COUNT.labels(
                            method=method.__name__.upper(),
                            endpoint=name,
                            status=response.status_code,
                        ).inc()
                        return response

                    elif 400 <= response.status_code < 500:
                        self.log(
                            f"Client error {response.status_code} from {name}",
                            logging.WARNING,
                        )
                        # Записываем метрики ошибки клиента
                        REQUEST_COUNT.labels(
                            method=method.__name__.upper(),
                            endpoint=name,
                            status=response.status_code,
                        ).inc()
                        response.failure(f"Client error: {response.status_code}")
                        return response

                    else:
                        self.log(
                            f"Server error {response.status_code} from {name}, attempt {attempt + 1}",
                            logging.WARNING,
                        )
                        # Записываем метрики ошибки сервера
                        REQUEST_COUNT.labels(
                            method=method.__name__.upper(),
                            endpoint=name,
                            status=response.status_code,
                        ).inc()

            except Exception as e:
                self.log(
                    f"Request {name} attempt {attempt + 1} failed: {str(e)}",
                    logging.WARNING,
                )
                # Записываем метрики ошибки запроса
                REQUEST_COUNT.labels(
                    method=method.__name__.upper(), endpoint=name, status="error"
                ).inc()

            if attempt < CONFIG["max_retries"] - 1:
                delay = CONFIG["retry_delay"] * (2 ** attempt)
                time.sleep(min(delay, 10))

        self.log(f"All attempts for {name} failed", logging.ERROR)
        return None

    def _get_user_database_id(self):
        """Get user's database ID by username pattern"""
        resp = self._retry_request(
            self.client.get,
            url="/api/v1/database/",
            name="Get databases list",
            timeout=15,
        )
        if not resp or not resp.ok:
            return None

        normalized_username = str(self.username).replace("_", "")
        expected_pattern = f"SberProcessMiningDB_{normalized_username}"

        # Сначала ищем точное совпадение
        for db in resp.json().get("result", []):
            db_name = db.get("database_name", "")
            if db_name.startswith(expected_pattern):
                return db.get("id")

        # Если не нашли, ищем частичное совпадение
        for db in resp.json().get("result", []):
            db_name = db.get("database_name", "")
            if ("SberProcessMiningDB" in db_name and
                    normalized_username in db_name):
                return db.get("id")

        return None

    def _create_flow(self, worker_id=0):
        """Create a new flow"""
        flow_id = FlowManager.get_next_id(worker_id=worker_id)
        flow_name = f"Tube_{flow_id}"
        flow_data = copy.deepcopy(CONFIG["flow_template"])
        flow_data["label"] = flow_name

        resp = self._retry_request(
            self.client.post,
            CONFIG["api"]["flow_endpoint"],
            name="Create flow",
            json=flow_data,
            timeout=20,
        )

        if not resp or not resp.ok:
            FLOW_CREATIONS.labels(status="failed").inc()
            return None, None

        new_flow_id = resp.json().get("id")
        FLOW_CREATIONS.labels(status="success").inc()
        return flow_name, new_flow_id

    def _get_dag_import_params(self, flow_id):
        """Get DAG file import parameters for a flow"""
        url = (
            f"/etl/api/v1/flow/dag_params/v2/spm_file_loader_v2?"
            f"q=(active:!f,block_id:0,enum_limit:20,flow_id:{flow_id})"
        )
        resp = self._retry_request(
            self.client.get, url, name="Get DAG file import parameters", timeout=15
        )
        if not resp or not resp.ok:
            return None, None

        target_connection = target_schema = None
        for item in resp.json().get("result", []):
            if item[0] == "target_connection":
                target_connection = item[1]["value"]
            elif item[0] == "target_schema":
                target_schema = item[1]["value"]

        return target_connection, target_schema

    def _update_flow(
            self,
            flow_id,
            flow_name,
            target_connection,
            target_schema,
            file_uploaded=False,
            count_chunks_val=0
    ):
        """Update flow configuration"""
        update_data = copy.deepcopy(CONFIG["flow_template"])
        update_data["label"] = flow_name
        update_data["config_inactive"]["blocks"] = [
            {
                "block_id": CONFIG["block"]["block_id"],
                "config": {
                    "date_convert": True,
                    "default_timezone": "Europe/Moscow",
                    "delimiter": ",",
                    "encoding": "UTF-8",
                    "file_type": "CSV",
                    "if_exists": "replace",
                    "is_config_valid": True,
                    "skip_rows": 0,
                    "target_connection": target_connection,
                    "target_schema": target_schema,
                    "target_table": f"Tube_{flow_id}",
                    "fileUploaded": file_uploaded,
                    "upload_id": f"{flow_id}_{CONFIG['block']['block_id']}",
                    "count_chunks": str(count_chunks_val),
                    "preview": {},
                    "columns": CONFIG["update_columns"],
                },
                "dag_id": CONFIG["block"]["dag_id"],
                "id": CONFIG["block"]["block_id"],
                "is_deprecated": False,
                "label": "Импорт данных из файла",
                "number": 1,
                "parent_ids": [],
                "status": "deferred",
                "type": CONFIG["block"]["dag_id"],
                "x": 152,
                "y": 0,
            }
        ]

        return self._retry_request(
            self.client.put,
            url=f"{CONFIG['api']['flow_endpoint']}{flow_id}",
            name="Update flow config",
            json=update_data,
            timeout=20,
        )

    def _upload_chunks(self, flow_id, db_id, target_schema, total_chunks):
        """Upload CSV chunks to server with progress tracking"""
        uploaded_chunks = 0
        chunk_timeout = 30

        # Увеличиваем счетчик активных загрузок
        CHUNKS_IN_PROGRESS.inc()

        try:
            for chunk in split_csv_generator(CONFIG["csv_file_path"], CONFIG["chunk_size"]):
                if not chunk or not chunk["chunk_text"]:
                    continue

                chunk_start_time = time.time()
                success = False

                for attempt in range(CONFIG["max_retries"]):
                    try:
                        data_payload = {
                            "upload_id": f"{flow_id}_{CONFIG['block']['block_id']}",
                            "database_id": str(db_id),
                            "schema": target_schema,
                            "table_name": f"Tube_{flow_id}",
                            "part_num": str(chunk["chunk_number"]),
                            "total_chunks": str(total_chunks),
                            "block_id": CONFIG["block"]["block_id"],
                            "flow_id": str(flow_id),
                        }
                        files_payload = {
                            "file": (
                                f"chunk_{chunk['chunk_number']}.csv",
                                chunk["chunk_text"],
                                "text/csv",
                            )
                        }

                        resp = self._retry_request(
                            self.client.post,
                            url="/etl/api/v1/file/upload",
                            name=f"Upload chunk {chunk['chunk_number']}",
                            data=data_payload,
                            files=files_payload,
                            timeout=chunk_timeout,
                        )

                        if resp and resp.ok:
                            uploaded_chunks += 1
                            success = True

                            # Записываем метрики успешной загрузки
                            chunk_duration = time.time() - chunk_start_time
                            CHUNK_UPLOAD_DURATION.observe(chunk_duration)
                            CHUNK_UPLOADS.labels(
                                flow_id=str(flow_id), status="success"
                            ).inc()

                            # Обновляем прогресс
                            progress = (uploaded_chunks / total_chunks) * 100
                            UPLOAD_PROGRESS.labels(flow_id=str(flow_id)).set(progress)

                            self.log(
                                f"Chunk {chunk['chunk_number']}/{total_chunks} uploaded"
                            )
                            break

                    except Exception as e:
                        self.log(
                            f"Chunk {chunk['chunk_number']} upload failed: {str(e)}",
                            logging.WARNING,
                        )
                        CHUNK_UPLOADS.labels(
                            flow_id=str(flow_id), status="failed"
                        ).inc()

                    if not success and attempt < CONFIG["max_retries"] - 1:
                        time.sleep(CONFIG["retry_delay"] * (attempt + 1))

                if not success:
                    self.log(
                        f"Failed to upload chunk {chunk['chunk_number']} "
                        f"after {CONFIG['max_retries']} attempts",
                        logging.ERROR,
                    )

        finally:
            # Уменьшаем счетчик активных загрузок
            CHUNKS_IN_PROGRESS.dec()

        return uploaded_chunks

    def _start_file_upload(self, flow_id, db_id, target_schema, total_chunks, timeout):
        """Start file upload process"""
        start_data = {
            "upload_id": f"{flow_id}_{CONFIG['block']['block_id']}",
            "database_id": str(db_id),
            "table_name": f"Tube_{flow_id}",
            "schema": target_schema,
            "flow_id": str(flow_id),
            "block_id": CONFIG["block"]["block_id"],
            "total_chunks": str(total_chunks),
        }

        start_resp = self._retry_request(
            self.client.post,
            url="/etl/api/v1/file/start_upload",
            name="Start file upload",
            json=start_data,
            timeout=timeout,
        )

        if not start_resp or not start_resp.ok:
            self.log("Failed to start file upload", logging.ERROR)
            return False

        self.log("File upload started successfully", logging.INFO)
        return True

    def _finalize_file_upload(self, flow_id, uploaded_chunks, timeout):
        """Finalize file upload"""
        finalize_data = {
            "count_chunks": uploaded_chunks,
            "upload_id": f"{flow_id}_{CONFIG['block']['block_id']}",
        }

        finalize_resp = self._retry_request(
            self.client.post,
            url="/etl/api/v1/file/finalize",
            name="Finalize file upload",
            json=finalize_data,
            timeout=timeout,
        )

        if not finalize_resp or not finalize_resp.ok:
            self.log("Failed to finalize file upload", logging.ERROR)
            return False

        self.log("File upload finalized successfully")
        return True

    def _start_file_processing(self, flow_id, target_connection, target_schema, total_chunks, timeout):
        """Start file processing"""
        final_data = {
            "flow_id": int(flow_id),
            "block_id": CONFIG["block"]["block_id"],
            "config": {
                **CONFIG["upload_settings"],
                "count_chunks": str(total_chunks),
                "fileUploaded": False,
                "target_connection": target_connection,
                "target_schema": target_schema,
                "target_table": f"Tube_{flow_id}",
                "upload_id": f"{flow_id}_{CONFIG['block']['block_id']}",
                "preview": {},
                "columns": CONFIG["upload_columns"],
            },
        }

        final_resp = self._retry_request(
            self.client.post,
            url="/etl/api/v1/file/start",
            name="Final file",
            json=final_data,
            timeout=timeout,
        )

        if not final_resp or not final_resp.ok:
            self.log("Failed to start file processing", logging.ERROR)
            return None

        run_id = final_resp.json().get("run_id")
        if not run_id:
            self.log("No run_id in response", logging.ERROR)
            return None

        return run_id

    def _monitor_processing_status(self, run_id, timeout, flow_id, db_id=None, target_schema=None,
                                   total_lines=None, flow_processing_start=None, is_pm_flow=None):
        """Universal method to monitor processing status with auto-detection"""

        # Автоматически определяем тип потока если не указан явно
        if is_pm_flow is None:
            run_id_str = str(run_id)
            # PM run_id обычно содержат специфичные маркеры
            pm_markers = ["__manual__", "__scheduled__", "spm_dashboard_creation"]
            is_pm_flow = any(marker in run_id_str for marker in pm_markers)

        # Определяем endpoint в зависимости от типа потока
        encoded_string = quote(str(run_id))
        if is_pm_flow:
            status_url = f"/etl/api/v1/flow/status/{encoded_string}"
            status_name = "PM Status"
        else:
            status_url = f"/etl/api/v1/file/status/{encoded_string}"
            status_name = "File Status"

        max_wait_time = timeout
        start_time = time.time()
        poll_count = 0
        monitoring_start = time.time()

        # Словарь для хранения block_run_id по block_id
        block_run_ids = {}

        while time.time() - start_time < max_wait_time:
            if stop_manager.is_stop_called():
                self.log("Stop called during status monitoring", logging.ERROR)
                return False

            poll_count += 1
            status_response = self._retry_request(
                self.client.get, url=status_url, name=status_name, timeout=30
            )

            if status_response and status_response.ok:
                status_data = status_response.json()

                # Обрабатываем разные форматы ответов
                if is_pm_flow:
                    # Для PM потоков: извлекаем статус из result.status
                    result_data = status_data.get("result", {})
                    current_status = result_data.get("status")
                    flow_id_from_response = result_data.get("flow_id")

                    # Извлекаем информацию о блоках включая block_run_id
                    blocks_status = []
                    for block in result_data.get("blocks", []):
                        block_id = block.get("block_id")
                        block_status = block.get("status")
                        block_run_id = block.get("block_run_id")

                        blocks_status.append(f"{block_id}: {block_status}")

                        # Сохраняем block_run_id
                        if block_id and block_run_id:
                            block_run_ids[block_id] = block_run_id

                            # Логируем block_run_id при первом обнаружении или изменении статуса
                            if poll_count == 1:
                                self.log(f"Block '{block_id}' run_id: {block_run_id}")

                    # Логируем информацию о блоках каждые 10 опросов
                    if blocks_status and poll_count % 10 == 1:
                        block_status_str = " | ".join(blocks_status)
                        elapsed = int(time.time() - monitoring_start)
                        self.log(f"PM flow {flow_id_from_response} - Blocks: {block_status_str} - Elapsed: {elapsed}s")

                else:
                    # Для файловых потоков: старая структура
                    current_status = status_data.get("status")
                    error_message = status_data.get("error", "No error details")

                if current_status == "success":
                    processing_time = time.time() - monitoring_start
                    minutes = int(processing_time // 60)
                    seconds = processing_time % 60

                    self.log(f"Status 'success' received! {'PM' if is_pm_flow else 'File'} processing completed.")
                    self.log(f"{'PM' if is_pm_flow else 'File'} processing time: {minutes}m {seconds:.1f}s")

                    # Логируем все собранные block_run_id при успешном завершении
                    if is_pm_flow and block_run_ids:
                        self.log(f"Completed block_run_ids: {block_run_ids}")

                    if not is_pm_flow and db_id and target_schema and total_lines is not None:
                        validation_result = self._validate_row_count(
                            db_id, target_schema, flow_id, total_lines
                        )

                        if flow_processing_start:
                            total_processing_time = time.time() - flow_processing_start
                            FLOW_PROCESSING_DURATION.labels(flow_id=str(flow_id)).observe(total_processing_time)
                            self.log(f"Validation: {'PASS' if validation_result else 'FAIL'}")

                    # Возвращаем block_run_ids для PM потоков
                    if is_pm_flow:
                        return {"success": True, "block_run_ids": block_run_ids}
                    return True

                elif current_status == "failed":
                    error_msg = status_data.get("error",
                                                "No error details") if not is_pm_flow else "Check PM blocks for details"
                    self.log(f"The task ended with an error: {error_msg}", logging.ERROR)

                    # Для PM потоков логируем детали блоков при ошибке
                    if is_pm_flow and blocks_status:
                        self.log(f"PM blocks status at failure: {' | '.join(blocks_status)}")
                        # Логируем block_run_id для неуспешных блоков
                        for block in result_data.get("blocks", []):
                            if block.get("status") == "failed":
                                self.log(f"Failed block_run_id: {block.get('block_run_id')}")

                    return False

                elif current_status in ["running", "pending", "scheduled"]:
                    if poll_count % 5 == 0:
                        elapsed = int(time.time() - monitoring_start)
                        status_info = f"{'PM' if is_pm_flow else 'File'} status: {current_status}"

                        if is_pm_flow and blocks_status:
                            # Для PM показываем прогресс по блокам
                            running_blocks = [b for b in blocks_status if "running" in b.lower()]
                            success_blocks = [b for b in blocks_status if "success" in b.lower()]
                            status_info += f" - Blocks: {len(running_blocks)} running, {len(success_blocks)} success"

                        self.log(f"{status_info} - Elapsed: {elapsed}s, Poll: {poll_count}")

                else:
                    # Неизвестные или другие статусы
                    if poll_count % 10 == 0:
                        elapsed = int(time.time() - monitoring_start)
                        self.log(f"Current status: {current_status} - Elapsed: {elapsed}s, Poll: {poll_count}")

            else:
                if poll_count % 5 == 0:  # Реже логируем ошибки опросов
                    self.log(f"Status check failed (attempt {poll_count})", logging.WARNING)

            time.sleep(CONFIG["upload_control"]["pool_interval"])

        self.log(f"Status wait timeout ({max_wait_time}s) expired for {'PM' if is_pm_flow else 'File'} flow",
                 logging.ERROR)
        return False

    def _validate_row_count(self, db_id, target_schema, flow_id, expected_rows):
        """Validate row count in database table"""
        try:
            self.log(f"Start validating data for the table Tube_{flow_id}")

            payload = {
                "client_id": "",
                "database_id": str(db_id),
                "json": True,
                "runAsync": False,
                "schema": target_schema,
                "sql": f'SELECT COUNT(*) FROM "{target_schema}"."Tube_{flow_id}"',
                "sql_editor_id": "4",
                "tab": "Locust Validation",
                "tmp_table_name": "",
                "select_as_cta": False,
                "ctas_method": "TABLE",
                "queryLimit": 1000,
                "expand_data": True,
            }

            self.log(f"Sending a validation request for the table Tube_{flow_id}")

            resp = self._retry_request(
                self.client.post,
                url="/api/v1/sqllab/execute/",
                name="Validate row count",
                json=payload,
                headers={"Content-Type": "application/json"},
            )

            if resp and resp.status_code == 200:
                data = resp.json()
                if data.get("data") and data["data"]:
                    db_count = data["data"][0].get("count()", 0)

                    # Записываем метрики валидации
                    DB_ROW_COUNT.labels(flow_id=str(flow_id)).set(db_count)

                    validation_success = db_count == expected_rows
                    COUNT_VALIDATION_RESULT.labels(flow_id=str(flow_id)).set(
                        1 if validation_success else 0
                    )

                    self.log(
                        f"Rows in DB: {db_count}, expected: {expected_rows}"
                    )
                    return validation_success

            return False

        except Exception as e:
            self.log(f"Validation error: {str(e)}", logging.ERROR)
            # Записываем метрику неудачной валидации
            COUNT_VALIDATION_RESULT.labels(flow_id=str(flow_id)).set(0)
            return False

    def _get_dag_pm_params(self, flow_id):
        """Get DAG parameters for Process Mining block"""
        url = (
            f"/etl/api/v1/flow/dag_params/v2/spm_dashboard_creation_v_0_2"
            f"?q=(active:!f,block_id:1,enum_limit:20,flow_id:{flow_id})"
        )
        resp = self._retry_request(
            self.client.get, url, name="Get PM DAG parameters", timeout=30
        )
        if not resp or not resp.ok:
            self.log(f"Failed to get PM DAG params. Status: {resp.status_code if resp else 'No response'}")
            return None, None, None, None

        source_connection = source_schema = None
        result_data = resp.json().get("result", [])

        for item in result_data:
            if item[0] == "source_connection":
                source_connection = item[1]["value"]
            elif item[0] == "source_schema":
                source_schema = item[1]["value"]

        return source_connection, source_schema

    def _create_pm_flow(self, worker_id=0, source_connection=None, source_schema=None, table_name=None,
                             base_flow_name=None):
        """Create a new flow with only Process Mining block"""
        try:
            flow_name = f"{base_flow_name}_PM"

            flow_data = copy.deepcopy(CONFIG["flow_template"])
            flow_data["label"] = flow_name

            pm_block = {
                "id": "spm_dashboard_creation_v_0_2[0]",
                "parent_ids": [],
                "label": "Расчет метрик Process Mining",
                "status": "deferred",
                "type": "spm_dashboard_creation_v_0_2",
                "config": {
                    "source_connection": source_connection,
                    "source_schema": source_schema,
                    "source_table": table_name,
                    "dashboard_title": table_name,
                    "threshold": 30,
                    "validation_types": CONFIG["validation_types"],
                    "duplicate_reaction": "DROP_BY_KEY",
                    "marking": CONFIG["marking_config"],
                    "packet_size": 0,
                    "run_auto_insights": False,
                    "autoinsights_timeout_sec": 36000,
                    "is_config_valid": True
                },
                "number": 1,
                "x": 152,
                "y": 0,
                "block_id": "spm_dashboard_creation_v_0_2[0]",
                "dag_id": "spm_dashboard_creation_v_0_2"
            }

            flow_data["config_inactive"]["blocks"] = [pm_block]

            resp = self._retry_request(
                self.client.post,
                CONFIG["api"]["flow_endpoint"],
                name="Create PM-only flow",
                json=flow_data,
                timeout=20,
            )

            if not resp or not resp.ok:
                self.log(f"Failed to create PM-only flow. Status: {resp.status_code if resp else 'No response'}",
                         logging.ERROR)
                FLOW_CREATIONS.labels(status="failed").inc()
                return None, None

            new_flow_id = resp.json().get("id")
            FLOW_CREATIONS.labels(status="success").inc()
            self.log(f"Created PM-only flow: {flow_name} (ID: {new_flow_id})")
            return flow_name, new_flow_id

        except Exception as e:
            self.log(f"Error creating PM-only flow: {str(e)}", logging.ERROR)
            return None, None

    def _start_pm_flow(self, pm_flow_id, source_connection, source_schema, table_name):
        """Start Process Mining flow processing with configuration body"""
        try:
            self.log(f"Starting Process Mining flow {pm_flow_id}")

            # Формируем тело запроса с конфигурацией
            request_body = {
                "config": {
                    "blocks": [
                        {
                            "block_id": "spm_dashboard_creation_v_0_2[0]",
                            "config": {
                                "activity_end_col": "timestamp_end",
                                "activity_name_col": "activity",
                                "activity_start_col": "timestamp_start",
                                "autoinsights_timeout_sec": 36000,
                                "case_col_name": "case_id",
                                "dashboard_title": table_name,
                                "duplicate_reaction": "DROP_BY_KEY",
                                "is_config_valid": True,
                                "marking": CONFIG["marking_config"],
                                "packet_size": 0,
                                "run_auto_insights": False,
                                "source_connection": source_connection,
                                "source_schema": source_schema,
                                "source_table": table_name,
                                "threshold": 30,
                                "validation_types": CONFIG["validation_types"]
                            },
                            "dag_id": "spm_dashboard_creation_v_0_2",
                            "id": "spm_dashboard_creation_v_0_2[0]",
                            "is_deprecated": False,
                            "label": "Расчет метрик Process Mining",
                            "number": 1,
                            "parent_ids": [],
                            "type": "spm_dashboard_creation_v_0_2",
                            "x": 152,
                            "y": 0
                        }
                    ]
                }
            }

            # Запускаем PM поток с телом конфигурации
            start_resp = self._retry_request(
                self.client.post,
                url=f"/etl/api/v1/flow/{pm_flow_id}/trigger",
                name="Start PM flow",
                json=request_body,
                timeout=30,
            )

            if not start_resp or not start_resp.ok:
                self.log(
                    f"Failed to start PM flow {pm_flow_id}. Status: {start_resp.status_code if start_resp else 'No response'}",
                    logging.ERROR)
                return None

            # Проверяем статус код 202 ACCEPTED
            if start_resp.status_code != 202:
                self.log(f"Unexpected status code for PM flow start: {start_resp.status_code}", logging.ERROR)
                return None

            response_data = start_resp.json()

            # Извлекаем run_id из структуры ответа
            run_id = response_data.get("result", {}).get("run_id")

            if not run_id:
                self.log(f"No run_id in PM start response for flow {pm_flow_id}. Response: {response_data}",
                         logging.ERROR)
                return None

            self.log(f"PM flow {pm_flow_id} started successfully with run_id: {run_id}")
            return run_id

        except Exception as e:
            self.log(f"Error starting PM flow: {str(e)}", logging.ERROR)
            return None

    def _get_dashboard_url_from_artefacts(self, pm_flow_id, block_id, block_run_id, run_id):
        """Extracting the dashboard URL from PM flow artifacts."""

        # Формируем параметры фильтра
        filter_params = (
            f"(filters:!("
            f"(col:flow_id,opr:eq,value:'{pm_flow_id}'),"
            f"(col:block_id,opr:eq,value:'{block_id}'),"
            f"(col:block_dag_run_id,opr:eq,value:'{block_run_id}'),"
            f"(col:flow_dag_run_id,opr:eq,value:'{run_id}')"
            f"),order_column:timestamp,order_direction:desc,page:0,page_size:12)"
        )

        # URL-кодируем параметры
        encoded_params = quote(filter_params, safe='')
        artefact_url = f"/etl/api/v1/flowartefact/?q={encoded_params}"

        self.log(f"Fetching artefacts for flow_id={pm_flow_id}, block_id={block_id}")

        response = self._retry_request(
            self.client.get,
            url=artefact_url,
            name="Flow Artefacts",
            timeout=30
        )

        if not response or not response.ok:
            self.log(f"Failed to fetch flow artefacts: {response.status_code if response else 'No response'}",
                     logging.ERROR)
            return None

        try:
            data = response.json()
            artefacts = data.get("result", [])

            if not artefacts:
                self.log("No artefacts found in response", logging.WARNING)
                return None

            # Ищем артефакт с event_type = "DASHBOARD_CREATED"
            for artefact in artefacts:
                event_type = artefact.get("event_type")

                if event_type == "DASHBOARD_CREATED":
                    dashboard_url = artefact.get("object_url")
                    object_id = artefact.get("object_id")

                    if dashboard_url:
                        self.log(f"Found dashboard URL: {dashboard_url} (object_id: {object_id})")
                        return dashboard_url
                    else:
                        self.log("DASHBOARD_CREATED found but object_url is missing", logging.WARNING)

            # Если не нашли нужный event_type, логируем доступные
            available_events = [a.get("event_type") for a in artefacts]
            self.log(f"DASHBOARD_CREATED not found. Available event_types: {available_events}", logging.WARNING)
            return None

        except Exception as e:
            self.log(f"Error parsing artefacts response: {e}", logging.ERROR)
            return None

    def _open_dashboard(self, dashboard_url):
        """Открывает дашборд по URL и проверяет успешность загрузки."""

        if not dashboard_url:
            self.log("Dashboard URL is empty", logging.ERROR)
            return False

        self.log(f"Opening dashboard: {dashboard_url}")

        response = self._retry_request(
            self.client.get,
            url=dashboard_url,
            name="Dashboard Load",
            timeout=60
        )

        if response and response.ok:
            self.log(f"Dashboard loaded successfully: {dashboard_url}")
            return True
        else:
            status = response.status_code if response else 'No response'
            self.log(f"Failed to load dashboard: {status}", logging.ERROR)
            return False
