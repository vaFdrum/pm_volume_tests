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

    # Маппинг имён файлов шаблонов
    # Формат: chart_type -> (create_file, save_file)
    TEMPLATE_FILES = {
        "table": ("tableCreate.json", "tableSave.json"),
        "histogramChart": ("histogramChartCreate.json", "histogramChartSave.json"),
        "supersetGraph": ("T1566supersetGraphCreate.json", "T1566supersetGraphSave.json"),
    }

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
        if chart_type not in self.TEMPLATE_FILES:
            self.log(f"Unknown chart type: {chart_type}", logging.ERROR)
            return None

        # Получаем имя файла из маппинга
        create_file, save_file = self.TEMPLATE_FILES[chart_type]
        filename = create_file if template_name == "create" else save_file

        file_path = os.path.join(
            self.PAYLOAD_BASE_PATH,
            chart_type,
            filename
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

    @staticmethod
    def extract_dashboard_id_from_url(dashboard_url: str) -> Optional[str]:
        """
        Извлекает dashboard_id из URL дашборда

        Форматы URL:
        - /superset/dashboard/123/
        - /dashboard/123/
        - /superset/dashboard/123

        Args:
            dashboard_url: URL дашборда

        Returns:
            dashboard_id как строка или None
        """
        # Паттерн для извлечения ID из URL
        match = re.search(r'/dashboard/(\d+)/?', dashboard_url)
        if match:
            return match.group(1)
        return None

    def get_dashboard_info(self, dashboard_url: str) -> Optional[Dict]:
        """
        Получает информацию о дашборде через Superset API

        Args:
            dashboard_url: URL дашборда

        Returns:
            Dict с информацией о дашборде:
            {
                'id': int,
                'title': str,
                'datasource_id': str или None
            }
            или None при ошибке
        """
        dashboard_id = self.extract_dashboard_id_from_url(dashboard_url)
        if not dashboard_id:
            self.log(f"Could not extract dashboard_id from URL: {dashboard_url}", logging.ERROR)
            return None

        api_url = f"/api/v1/dashboard/{dashboard_id}"
        self.log(f"Fetching dashboard info: {api_url}")

        try:
            response = self.client.get(
                api_url,
                name="[ChartApi] Get Dashboard Info",
                headers={"Content-Type": "application/json"}
            )

            if response.status_code == 200:
                data = response.json()
                result = data.get("result", {})
                title = result.get("dashboard_title", "")
                datasource_id = self.extract_datasource_id_from_title(title)

                self.log(f"Dashboard info: id={dashboard_id}, title='{title}', datasource_id={datasource_id}")

                return {
                    'id': int(dashboard_id),
                    'title': title,
                    'datasource_id': datasource_id
                }
            else:
                self.log(f"Failed to get dashboard info: {response.status_code}", logging.ERROR)
                return None

        except Exception as e:
            self.log(f"Error getting dashboard info: {e}", logging.ERROR)
            return None
