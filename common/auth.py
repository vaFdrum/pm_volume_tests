"""Authentication helpers"""

from urllib.parse import urljoin
from bs4 import BeautifulSoup
import time
import logging
from config import CONFIG
from common.metrics import AUTH_ATTEMPTS, AUTH_DURATION, SESSION_STATUS, ACTIVE_USERS


def extract_login_form(html, username, password):
    """Extract login form data from HTML"""
    soup = BeautifulSoup(html, features="html.parser")
    form = soup.find("form")
    if not form or not form.get("action"):
        return None
    action_url = urljoin(CONFIG["api"]["base_url"], form["action"])
    return {
        "action": action_url,
        "payload": {
            "flowType": "byLogin",
            "username": username,
            "formattedUsername": username,
            "password": password,
        },
    }


def establish_session(client, username, password, session_id, log_function=None):
    """Establish user session with authentication"""
    auth_start_time = time.time()

    for attempt in range(CONFIG["max_retries"]):
        try:
            client.cookies.clear()

            # 1) GET login page
            resp = _retry_request(
                client, client.get, "/", "Get login page", timeout=10
            )
            if not resp or resp.status_code != 200:
                AUTH_ATTEMPTS.labels(username=username, success="false").inc()
                continue

            form = extract_login_form(resp.text, username, password)
            if not form:
                AUTH_ATTEMPTS.labels(username=username, success="false").inc()
                continue

            # 2) POST credentials
            resp = _retry_request(
                client,
                client.post,
                form["action"],
                "Submit credentials",
                data=form["payload"],
                allow_redirects=False,
                timeout=15,
            )
            if not resp or resp.status_code != 302:
                AUTH_ATTEMPTS.labels(username=username, success="false").inc()
                continue

            location = resp.headers.get("Location")
            if not location:
                AUTH_ATTEMPTS.labels(username=username, success="false").inc()
                continue

            # 3) Complete redirect
            resp = _retry_request(
                client,
                client.get,
                urljoin(form["action"], location),
                "Complete auth redirect",
                timeout=10,
            )
            if resp and resp.status_code == 200:

                # Записываем метрики успешной аутентификации
                auth_duration = time.time() - auth_start_time
                AUTH_DURATION.observe(auth_duration)
                AUTH_ATTEMPTS.labels(username=username, success="true").inc()
                SESSION_STATUS.labels(username=username).set(1)
                ACTIVE_USERS.inc()

                return True

        except Exception as e:
            if log_function:
                log_function(f"Auth attempt {attempt + 1} failed: {str(e)}", logging.WARNING)
            AUTH_ATTEMPTS.labels(username=username, success="false").inc()
            time.sleep(CONFIG["retry_delay"])

    SESSION_STATUS.labels(username=username).set(0)
    return False


def _retry_request(client, method, url, name, **kwargs):
    """Retry mechanism with timeouts (вспомогательная функция)"""
    timeout = kwargs.pop("timeout", CONFIG["request_timeout"])
    
    for attempt in range(CONFIG["max_retries"]):
        try:
            kwargs["timeout"] = timeout
            with method(url, name=name, catch_response=True, **kwargs) as response:
                if response.status_code < 400:
                    return response
                elif 400 <= response.status_code < 500:
                    return response
        except Exception as e:
            if attempt < CONFIG["max_retries"] - 1:
                delay = CONFIG["retry_delay"] * (2 ** attempt)
                time.sleep(min(delay, 10))
    
    return None
