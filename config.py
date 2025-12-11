"""Configuration module for the Locust load test."""

import os
import yaml
from typing import Dict, Any, List, Type
from dotenv import load_dotenv

load_dotenv()

# Глобальный реестр задач
TASK_REGISTRY = {}


def register_tasks(registry: Dict[str, Type]) -> None:
    """Регистрирует задачи для использования в сценариях"""
    global TASK_REGISTRY
    TASK_REGISTRY.update(registry)


def load_multiple_tasks_config(scenario_names: str = None) -> List[Type]:
    """
    Загружает задачи для нескольких сценариев из конфига

    Args:
        scenario_names: Имена сценариев через запятую (если None, берётся из LOCUST_SCENARIO)

    Returns:
        List[Type]: Список классов задач из всех сценариев
    """
    if scenario_names is None:
        scenario_names = os.getenv('LOCUST_SCENARIO', 'default')

    scenario_list = [s.strip() for s in scenario_names.split(',')]

    # Берём сценарии из CONFIG
    scenario_tasks = CONFIG.get('scenarios', {})

    tasks = []
    used_task_names = set()

    for scenario_name in scenario_list:
        task_names = scenario_tasks.get(scenario_name, [])

        for task_name in task_names:
            if task_name in TASK_REGISTRY and task_name not in used_task_names:
                tasks.append(TASK_REGISTRY[task_name])
                used_task_names.add(task_name)
                print(f"Added task '{task_name}' from scenario '{scenario_name}'")

    if not tasks:
        if TASK_REGISTRY:
            tasks = [list(TASK_REGISTRY.values())[0]]
            print(f"Warning: No tasks found for scenarios {scenario_list}, using fallback")
        else:
            raise ValueError("No tasks registered in TASK_REGISTRY")

    print(f"Loaded scenarios {scenario_list} with {len(tasks)} unique tasks: {[t.__name__ for t in tasks]}")
    return tasks


def load_config(config_path: str = None) -> Dict[str, Any]:
    """
    Loads configuration from YAML file and substitutes secrets from .env
    """
    if config_path is None:
        config_path = os.getenv("CONFIG_PATH")
        if config_path is None:
            if os.path.exists("config_multi.yaml"):
                config_path = "config_multi.yaml"
                print("Auto-detected config: config_multi.yaml")
            elif os.path.exists("config_ift.yaml"):
                config_path = "config_ift.yaml"
                print("Auto-detected config: config_ift.yaml")
            else:
                print("No config file found, using fallback configuration")
                return get_fallback_config()

    if not os.path.exists(config_path):
        print(f"Config file not found: {config_path}, using fallback configuration")
        return get_fallback_config()

    with open(config_path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file)

    # Обработка base_url из .env
    if config.get("api", {}).get("base_url") == "FROM_ENV":
        env_base_url = os.getenv("BASE_URL")
        if env_base_url:
            config["api"]["base_url"] = env_base_url
            print("Successfully substituted BASE_URL from environment")
        else:
            print("WARNING: BASE_URL is set to FROM_ENV but environment variable is not set")

    # Обработка max_iterations из .env
    if config.get("max_iterations") == "FROM_ENV":
        env_max_iterations = os.getenv("MAX_ITERATIONS")
        if env_max_iterations:
            try:
                config["max_iterations"] = int(env_max_iterations)
                print(f"Successfully substituted MAX_ITERATIONS from environment: {config['max_iterations']}")
            except ValueError:
                config["max_iterations"] = 1
                print("WARNING: MAX_ITERATIONS must be integer, using default: 1")
        else:
            config["max_iterations"] = 1
            print("WARNING: max_iterations is set to FROM_ENV but MAX_ITERATIONS environment variable is not set, using default: 1")

    # Обработка csv_file_path
    if config.get("csv_file_path") == "FROM_ENV":
        env_csv_path = os.getenv("CSV_FILE_PATH")
        if env_csv_path:
            config["csv_file_path"] = env_csv_path
            print("Successfully substituted CSV_FILE_PATH from environment")
        else:
            print("WARNING: csv_file_path is set to FROM_ENV but CSV_FILE_PATH environment variable is not set")

    # Проверяем существование CSV файла
    if config.get("csv_file_path") and isinstance(config["csv_file_path"], str):
        if not os.path.exists(config["csv_file_path"]):
            print(f"WARNING: CSV file not found at {config['csv_file_path']}")

    # Обработка паролей пользователей
    if "users" in config:
        for user in config["users"]:
            if user.get("password") == "FROM_ENV":
                env_password = os.getenv("PASSWORD")
                if env_password:
                    user["password"] = env_password
                    print(f"Successfully substituted password for user: {user.get('username')}")
                else:
                    print(f"WARNING: Password for user {user.get('username')} is FROM_ENV but PASSWORD environment variable is not set")

    # Обработка ClickHouse конфигурации
    if "clickhouse" in config:
        ch_config = config["clickhouse"]

        # Host
        if ch_config.get("host") == "FROM_ENV":
            env_ch_host = os.getenv("CLICKHOUSE_HOST")
            if env_ch_host:
                ch_config["host"] = env_ch_host
                print("Successfully substituted CLICKHOUSE_HOST from environment")
            else:
                print("WARNING: ClickHouse host is FROM_ENV but CLICKHOUSE_HOST environment variable is not set")

        # Port
        if ch_config.get("port") == "FROM_ENV":
            env_ch_port = os.getenv("CLICKHOUSE_PORT")
            if env_ch_port:
                try:
                    ch_config["port"] = int(env_ch_port)
                    print("Successfully substituted CLICKHOUSE_PORT from environment")
                except ValueError:
                    print("WARNING: CLICKHOUSE_PORT must be integer, using default: 8123")
                    ch_config["port"] = 8123
            else:
                print("WARNING: ClickHouse port is FROM_ENV but CLICKHOUSE_PORT environment variable is not set")

        # User
        if ch_config.get("user") == "FROM_ENV":
            env_ch_user = os.getenv("CLICKHOUSE_USER")
            if env_ch_user:
                ch_config["user"] = env_ch_user
                print("Successfully substituted CLICKHOUSE_USER from environment")
            else:
                print("WARNING: ClickHouse user is FROM_ENV but CLICKHOUSE_USER environment variable is not set")

        # Password
        if ch_config.get("password") == "FROM_ENV":
            env_ch_password = os.getenv("CLICKHOUSE_PASSWORD")
            if env_ch_password:
                ch_config["password"] = env_ch_password
                print("Successfully substituted CLICKHOUSE_PASSWORD from environment")
            else:
                print("WARNING: ClickHouse password is FROM_ENV but CLICKHOUSE_PASSWORD environment variable is not set")

    print(f"Successfully loaded configuration from: {config_path}")
    return config


def get_fallback_config() -> Dict[str, Any]:
    """Return fallback configuration when no config files are found"""
    try:
        max_iterations = int(os.getenv("MAX_ITERATIONS", "1"))
    except ValueError:
        max_iterations = 1
        print("WARNING: MAX_ITERATIONS must be integer, using default: 1")

    return {
        "scenarios": {
            "process_metrics": ["process_metrics"],
            "load_test": ["load_test"],
            "mixed": ["load_test", "process_metrics"],
            "default": ["process_metrics"],
            "tc_load_001": ["tc_load_001"],
            "tc_load_002": ["tc_load_002"],
            "tc_load_003": ["tc_load_003_heavy", "tc_load_003_light"],
            "tc_load_003_heavy": ["tc_load_003_heavy"],
            "tc_load_003_light": ["tc_load_003_light"]
        },
        "users": [
            {
                "username": os.getenv("USER1_USERNAME"),
                "password": os.getenv("USER1_PASSWORD"),
            },
            {
                "username": os.getenv("USER2_USERNAME"),
                "password": os.getenv("USER2_PASSWORD"),
            },
            {
                "username": os.getenv("USER3_USERNAME"),
                "password": os.getenv("USER3_PASSWORD"),
            },
            {
                "username": os.getenv("USER4_USERNAME"),
                "password": os.getenv("USER4_PASSWORD"),
            },
            {
                "username": os.getenv("USER5_USERNAME"),
                "password": os.getenv("USER5_PASSWORD"),
            },
        ],
        "api": {
            "base_url": os.getenv("BASE_URL", ""),
            "flow_endpoint": "/etl/api/v1/flow/",
        },
        "upload_control": {
            "timeout_small": 300,
            "timeout_large": 3600,
            "chunk_threshold": 200,
            "pool_interval": 5,
        },
        "max_iterations": max_iterations,
        "log_verbose": True,
        "log_debug": False,
        "log_level": "INFO",
        "csv_file_path": os.getenv("CSV_FILE_PATH", ""),
        "chunk_size": 4 * 1024 * 1024,
        "max_retries": 3,
        "retry_delay": 2,
        "request_timeout": 30,
        "clickhouse": {
            "enabled": os.getenv("CLICKHOUSE_ENABLED", "true").lower() == "true",
            "host": os.getenv("CLICKHOUSE_HOST", "localhost"),
            "port": int(os.getenv("CLICKHOUSE_PORT", "8123")),
            "user": os.getenv("CLICKHOUSE_USER", "default"),
            "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
            "monitoring_interval": int(os.getenv("CLICKHOUSE_MONITORING_INTERVAL", "10")),
        },
    }


# Загружаем конфигурацию при импорте модуля
try:
    CONFIG = load_config()
except Exception as e:
    print(f"Error loading configuration: {e}")
    print("Falling back to default config...")
    CONFIG = get_fallback_config()
