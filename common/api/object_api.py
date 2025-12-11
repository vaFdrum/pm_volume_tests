"""
Object API module for Superset objects (charts, dashboards, filters)

Содержит классы для работы с объектами Superset:
- ChartApi: создание и сохранение виджетов (charts)
"""

import json
import logging
import os
import random
import re
from typing import Optional, Dict, Any, Tuple


class ChartApi:
    """
    API для работы с чартами (виджетами) Superset

    Endpoints:
    - POST /api/v1/chart/data - создать виджет (получить данные)
    - POST /api/v1/chart/ - сохранить виджет

    Использование:
        chart_api = ChartApi(client, log_function)

        # Создать и сохранить чарт
        success = chart_api.create_and_save_chart(
            datasource_id="577",
            chart_type="table"  # или "histogramChart", "supersetGraph"
        )
    """

    # Доступные типы чартов
    CHART_TYPES = ["table", "histogramChart", "supersetGraph"]

    # Базовый путь к JSON шаблонам
    PAYLOAD_BASE_PATH = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "payload", "charts"
    )

    def __init__(self, client, log_function=None):
        """
        Args:
            client: HTTP клиент (Locust client или requests session)
            log_function: Функция для логирования (опционально)
        """
        self.client = client
        self.log = log_function or (lambda msg, level=logging.INFO: None)

    def _load_json_template(self, chart_type: str, template_name: str) -> Optional[Dict]:
        """
        Загружает JSON шаблон из файла

        Args:
            chart_type: Тип чарта (table, histogramChart, supersetGraph)
            template_name: Имя шаблона (create, save)

        Returns:
            Dict с данными шаблона или None при ошибке
        """
        file_path = os.path.join(
            self.PAYLOAD_BASE_PATH,
            chart_type,
            f"{chart_type}_{template_name}.json"
        )

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            self.log(f"Template not found: {file_path}", logging.ERROR)
            return None
        except json.JSONDecodeError as e:
            self.log(f"Invalid JSON in {file_path}: {e}", logging.ERROR)
            return None

    def _prepare_create_request(self, datasource_id: str, chart_type: str) -> Optional[Dict]:
        """
        Подготавливает запрос для создания чарта (POST /api/v1/chart/data)

        Args:
            datasource_id: ID датасета Superset
            chart_type: Тип чарта

        Returns:
            Dict с телом запроса или None при ошибке
        """
        template = self._load_json_template(chart_type, "create")
        if not template:
            return None

        # Заменяем datasource_id в шаблоне
        template["datasource"]["id"] = int(datasource_id)

        # Обновляем queries
        if "queries" in template and template["queries"]:
            template["queries"][0]["url_params"]["datasource_id"] = datasource_id

        # Обновляем form_data
        if "form_data" in template:
            template["form_data"]["datasource"] = f"{datasource_id}__table"
            if "url_params" in template["form_data"]:
                template["form_data"]["url_params"]["datasource_id"] = datasource_id

        return template

    def _prepare_save_request(
        self,
        datasource_id: str,
        chart_type: str,
        chart_name: str
    ) -> Optional[Dict]:
        """
        Подготавливает запрос для сохранения чарта (POST /api/v1/chart/)

        Args:
            datasource_id: ID датасета Superset
            chart_type: Тип чарта
            chart_name: Имя чарта для сохранения

        Returns:
            Dict с телом запроса или None при ошибке
        """
        template = self._load_json_template(chart_type, "save")
        if not template:
            return None

        # Обновляем основные поля
        template["slice_name"] = chart_name
        template["datasource_id"] = int(datasource_id)

        # Обновляем params (это JSON строка)
        if "params" in template:
            params = json.loads(template["params"]) if isinstance(template["params"], str) else template["params"]
            params["datasource"] = f"{datasource_id}__table"
            template["params"] = json.dumps(params)

        # Обновляем query_context (это тоже JSON строка)
        if "query_context" in template:
            qc = json.loads(template["query_context"]) if isinstance(template["query_context"], str) else template["query_context"]
            qc["datasource"]["id"] = int(datasource_id)
            qc["form_data"]["datasource"] = f"{datasource_id}__table"
            template["query_context"] = json.dumps(qc)

        return template

    def create_chart(self, datasource_id: str, chart_type: str) -> Tuple[bool, Optional[Dict]]:
        """
        Создаёт чарт (получает данные) - POST /api/v1/chart/data

        Args:
            datasource_id: ID датасета Superset
            chart_type: Тип чарта (table, histogramChart, supersetGraph)

        Returns:
            Tuple[success: bool, response_data: Optional[Dict]]
        """
        if chart_type not in self.CHART_TYPES:
            self.log(f"Unknown chart type: {chart_type}. Available: {self.CHART_TYPES}", logging.ERROR)
            return False, None

        request_body = self._prepare_create_request(datasource_id, chart_type)
        if not request_body:
            return False, None

        self.log(f"Creating chart type={chart_type}, datasource_id={datasource_id}")

        try:
            response = self.client.post(
                "/api/v1/chart/data",
                json=request_body,
                name=f"[ChartApi] Create {chart_type}",
                headers={"Content-Type": "application/json"}
            )

            if response.status_code == 200:
                self.log(f"Chart created successfully: {chart_type}")
                return True, response.json() if response.text else None
            else:
                self.log(f"Failed to create chart: {response.status_code} - {response.text[:200]}", logging.ERROR)
                return False, None

        except Exception as e:
            self.log(f"Error creating chart: {e}", logging.ERROR)
            return False, None

    def save_chart(
        self,
        datasource_id: str,
        chart_type: str,
        chart_name: Optional[str] = None
    ) -> Tuple[bool, Optional[int]]:
        """
        Сохраняет чарт - POST /api/v1/chart/

        Args:
            datasource_id: ID датасета Superset
            chart_type: Тип чарта
            chart_name: Имя чарта (если None - генерируется автоматически)

        Returns:
            Tuple[success: bool, chart_id: Optional[int]]
        """
        if chart_type not in self.CHART_TYPES:
            self.log(f"Unknown chart type: {chart_type}", logging.ERROR)
            return False, None

        # Генерируем имя если не указано
        if not chart_name:
            chart_name = f"{chart_type}_{random.randint(1000, 9999)}"

        request_body = self._prepare_save_request(datasource_id, chart_type, chart_name)
        if not request_body:
            return False, None

        self.log(f"Saving chart: {chart_name} (type={chart_type})")

        try:
            response = self.client.post(
                "/api/v1/chart/",
                json=request_body,
                name=f"[ChartApi] Save {chart_type}",
                headers={"Content-Type": "application/json"}
            )

            if response.status_code == 201:
                data = response.json() if response.text else {}
                chart_id = data.get("id")
                self.log(f"Chart saved: {chart_name} (id={chart_id})")
                return True, chart_id
            else:
                self.log(f"Failed to save chart: {response.status_code} - {response.text[:200]}", logging.ERROR)
                return False, None

        except Exception as e:
            self.log(f"Error saving chart: {e}", logging.ERROR)
            return False, None

    def create_and_save_chart(
        self,
        datasource_id: str,
        chart_type: Optional[str] = None,
        chart_name: Optional[str] = None
    ) -> Tuple[bool, Optional[int]]:
        """
        Создаёт и сохраняет чарт (полный цикл)

        Args:
            datasource_id: ID датасета Superset
            chart_type: Тип чарта (если None - выбирается случайно)
            chart_name: Имя чарта (если None - генерируется автоматически)

        Returns:
            Tuple[success: bool, chart_id: Optional[int]]
        """
        # Выбираем случайный тип если не указан
        if not chart_type:
            chart_type = random.choice(self.CHART_TYPES)

        # Шаг 1: Создаём чарт (получаем данные)
        create_success, _ = self.create_chart(datasource_id, chart_type)
        if not create_success:
            return False, None

        # Шаг 2: Сохраняем чарт
        save_success, chart_id = self.save_chart(datasource_id, chart_type, chart_name)

        return save_success, chart_id

    @staticmethod
    def extract_datasource_id_from_title(dashboard_title: str) -> Optional[str]:
        """
        Извлекает datasource_id из заголовка дашборда

        Формат: "[577] Tube_503" -> "577"

        Args:
            dashboard_title: Заголовок дашборда

        Returns:
            datasource_id как строка или None
        """
        match = re.search(r'\[(\d+)\]', dashboard_title)
        if match:
            return match.group(1)
        return None
