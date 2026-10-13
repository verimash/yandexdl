"""
mitmproxy-аддон для Яндекс Диска.

1. Перехватывает ответы
       https://disk.yandex.ru/models-v2?m=intapi/photo-get-clusters-with-resources
   и пересылает JSON на локальный сервер (POST /api/ingest).
2. Запоминает авторизацию, с которой браузер ходит в models-v2: заголовок Cookie, токен sk,
   connection_id и User-Agent, а также обновления кук из Set-Cookie. Всё это уходит на POST /api/auth
   и лежит в отдельной базе auth.db. По этим данным сервер сам запрашивает ссылки на скачивание
   видео (models-v2?m=mpfs/url).

Запуск:
    mitmdump -s capture_addon.py --listen-host 127.0.0.1 --listen-port 8000

При запуске в логе должна появиться строка «[ydisk] аддон загружен (с авторизацией)».
Если её нет, запущена старая версия файла.

Ответ клиенту не меняется — аддон только читает. Значения кук в логи не пишутся.
"""
import hashlib
import json
import os
import re
import threading
import time
import urllib.request
from email.utils import parsedate_to_datetime
from urllib.parse import parse_qs

from mitmproxy import ctx, http

SERVER_URL = os.environ.get("YDISK_SERVER_URL", "http://127.0.0.1:8001/api/ingest")
AUTH_URL = os.environ.get("YDISK_AUTH_URL", SERVER_URL.rsplit("/api/", 1)[0] + "/api/auth")
TARGET_PATH_PREFIX = "/models-v2"
TARGET_MODEL = "photo-get-clusters-with-resources"
YANDEX_BASES = ("yandex.ru", "yandex.com", "ya.ru", "yandex.net")
SK_RE = re.compile(r'"sk"\s*:\s*"([^"]+)"')
CID_RE = re.compile(r'"connection_id"\s*:\s*"([^"]+)"')


def is_yandex_host(host: str) -> bool:
    """Проверяет, относится ли host к Яндексу: *.yandex.ru, *.ya.ru, *.yandex.com, *.yandex.net"""
    if not host:
        return False
    host_lower = host.lower()
    for base in YANDEX_BASES:
        if host_lower == base or host_lower.endswith("." + base):
            return True
    return False


def find_payload(obj):
    """Ищет объект с ключами clusters + resources.

    Поддерживает как «голый» JSON, так и обёртку models-v2:
    {"models": [{"model": "...", "data": {...}}]}
    """
    if isinstance(obj, dict):
        if "clusters" in obj and "resources" in obj:
            return obj
        for value in obj.values():
            found = find_payload(value)
            if found is not None:
                return found
    elif isinstance(obj, list):
        for value in obj:
            found = find_payload(value)
            if found is not None:
                return found
    return None


# ---------------------------------------------------------------- куки и токен

def parse_cookie_header(values):
    """Заголовки Cookie (их может быть несколько, у HTTP/2 так бывает) -> [[имя, значение], ...] по порядку."""
    out = []
    for h in values or []:
        for part in h.split(";"):
            part = part.strip()
            if "=" not in part:
                continue
            name, value = part.split("=", 1)
            if name.strip():
                out.append([name.strip(), value])
    return out


def parse_set_cookie(raw, req_host):
    """Строка Set-Cookie -> словарь (или None). Срок жизни: Max-Age главнее Expires."""
    first, *attrs = [p.strip() for p in raw.split(";")]
    if "=" not in first:
        return None
    name, value = first.split("=", 1)
    ck = {"name": name.strip(), "value": value, "domain": req_host.lower(), "host_only": True,
          "path": "/", "expires": None, "delete": False}
    expires = max_age = None
    for a in attrs:
        k, _, v = a.partition("=")
        k, v = k.strip().lower(), v.strip()
        if k == "domain" and v:
            ck["domain"], ck["host_only"] = v.lstrip(".").lower(), False
        elif k == "path" and v:
            ck["path"] = v
        elif k == "max-age":
            try:
                max_age = int(v)
            except ValueError:
                pass
        elif k == "expires" and v:
            try:
                expires = parsedate_to_datetime(v).timestamp()
            except (TypeError, ValueError):
                pass
    now = time.time()
    if max_age is not None:
        expires = now + max_age
    if expires is not None:
        ck["expires"] = expires
        ck["delete"] = expires <= now
    return ck


def cookie_applies(ck):
    """Относится ли кука к yandex-домену."""
    d = ck["domain"]
    if ck["host_only"]:
        return is_yandex_host(d)
    return any(is_yandex_host(h) or h.endswith("." + d) for h in [d] + list(YANDEX_BASES))


def extract_sk(req):
    """Токен sk и connection_id из тела (JSON или form) либо из query запроса к models-v2."""
    sk = conn = None
    if req.method == "POST":
        text = req.get_text(strict=False) or ""
        try:
            obj = json.loads(text)
        except ValueError:
            obj = None
            form = parse_qs(text)
            sk = (form.get("sk") or [None])[0]
            conn = (form.get("connection_id") or [None])[0]
        if isinstance(obj, dict):
            sk, conn = obj.get("sk"), obj.get("connection_id")
        if not sk:       # запасной путь: sk лежит глубже или тело нестандартное
            m = SK_RE.search(text)
            sk = m.group(1) if m else None
        if not conn:
            m = CID_RE.search(text)
            conn = m.group(1) if m else None
    if not sk:
        sk = req.query.get("sk")
    return (sk if isinstance(sk, str) else None), (conn if isinstance(conn, str) else None)


class YDiskCapture:
    def __init__(self):
        # Запросы к локальному серверу не должны идти через системный прокси (и через сам mitmproxy).
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self._sent = []            # отпечатки уже отправленных данных авторизации (чтобы не слать одно и то же)
        self._lock = threading.Lock()

    def running(self):
        ctx.log.info(f"[ydisk] аддон загружен (с авторизацией): ingest -> {SERVER_URL}, auth -> {AUTH_URL}")

    def _send(self, body: bytes):
        req = urllib.request.Request(
            SERVER_URL,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with self.opener.open(req, timeout=60) as resp:
                result = resp.read().decode("utf-8", "replace")
            ctx.log.info(f"[ydisk] сервер ответил: {result}")
        except Exception as e:  # noqa: BLE001
            ctx.log.error(f"[ydisk] не удалось отправить на {SERVER_URL}: {e}")

    # ---------- авторизация

    def _send_auth(self, payload: dict):
        # connection_id у каждого запроса свой, в отпечаток его не берём, иначе слали бы всё подряд
        fp_src = {k: v for k, v in payload.items() if k != "connection_id"}
        fp = hashlib.sha1(json.dumps(fp_src, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()
        with self._lock:
            if fp in self._sent:
                return
            self._sent.append(fp)
            del self._sent[:-50]
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        threading.Thread(target=self._post_auth, args=(body, fp, payload), daemon=True).start()

    def _post_auth(self, body: bytes, fp: str, payload: dict):
        req = urllib.request.Request(AUTH_URL, data=body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with self.opener.open(req, timeout=20) as resp:
                resp.read()
            what = []
            if payload.get("cookies"):
                what.append(f"кук: {len(payload['cookies'])}")
            if payload.get("set_cookies"):
                what.append(f"обновлений Set-Cookie: {len(payload['set_cookies'])}")
            if payload.get("sk"):
                what.append("sk")
            ctx.log.info("[ydisk] авторизация отправлена на сервер (" + ", ".join(what) + ")")
        except Exception as e:  # noqa: BLE001
            with self._lock:      # не получилось: пусть при следующем запросе попробуем снова
                if fp in self._sent:
                    self._sent.remove(fp)
            ctx.log.error(f"[ydisk] не удалось отправить авторизацию на {AUTH_URL}: {e}")

    def request(self, flow: http.HTTPFlow):
        req = flow.request
        if not is_yandex_host(req.pretty_host) or not req.path.startswith(TARGET_PATH_PREFIX):
            return
        cookies = parse_cookie_header(req.headers.get_all("Cookie"))
        if not cookies:
            return
        sk, conn = extract_sk(req)
        self._send_auth({
            "host": req.pretty_host,
            "cookies": cookies,
            "sk": sk,
            "connection_id": conn,
            "user_agent": req.headers.get("User-Agent"),
        })

    def _capture_set_cookies(self, flow: http.HTTPFlow):
        host = flow.request.pretty_host.lower()
        if not is_yandex_host(host):
            return
        items = []
        for raw in flow.response.headers.get_all("Set-Cookie"):
            ck = parse_set_cookie(raw, host)
            if ck and cookie_applies(ck):
                items.append(ck)
        if items:
            self._send_auth({"host": host, "set_cookies": items})

    # ---------- ответы

    def response(self, flow: http.HTTPFlow):
        if flow.response is None:
            return
        
        req = flow.request
        if not is_yandex_host(req.pretty_host):
            return
        if not req.path.startswith(TARGET_PATH_PREFIX):
            return

        # Обработка HTTP 403 — сессия или sk протухли
        if flow.response.status_code == 403:
            ctx.log.error(
                "[ydisk] ⚠️  Яндекс ответил HTTP 403: сессия или sk протухли — "
                "обнови страницу Диска в браузере"
            )
            return

        self._capture_set_cookies(flow)

        # req.path включает query-string, модель может быть url-encoded (intapi%2Fphoto-...)
        if TARGET_MODEL not in req.path:
            return
        if flow.response.status_code != 200:
            return

        try:
            data = json.loads(flow.response.text)
        except Exception as e:  # noqa: BLE001
            ctx.log.warn(f"[ydisk] ответ не JSON: {e}")
            return

        payload = find_payload(data)
        if payload is None:
            ctx.log.warn("[ydisk] в ответе нет clusters/resources — пропускаю")
            return

        n_clusters = len(payload.get("clusters", {}).get("fetched", []))
        n_resources = len(payload.get("resources", {}).get("fetched", []))
        ctx.log.info(f"[ydisk] перехвачено: кластеров={n_clusters}, ресурсов={n_resources}")

        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        # В отдельном потоке, чтобы не задерживать трафик
        threading.Thread(target=self._send, args=(body,), daemon=True).start()


addons = [YDiskCapture()]