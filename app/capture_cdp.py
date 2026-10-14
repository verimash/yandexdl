"""
Замена capture/addon.py: вместо mitmproxy слушаем трафик встроенного в приложение
браузера (QWebEngineView / Chromium) напрямую через Chrome DevTools Protocol (CDP).

Теперь окно — обычный браузер с вкладками, и пользователь может открыть Диск в
любой из них (или в нескольких сразу). Поэтому CDPCapture не подключается к одной
заранее найденной вкладке, а раз в poll_interval секунд опрашивает список вкладок
(http://127.0.0.1:<devtools_port>/json) и на каждую новую вкладку с disk.yandex в URL
заводит отдельный _TabWatcher — тот уже слушает её трафик один в один как раньше.

Логика извлечения данных (что искать, что считать "целевым" ответом) повторяет
capture/addon.py — меняется только источник событий. Сервер (server/server.py,
эндпоинты /api/ingest и /api/auth) не меняется вообще.

ВАЖНО (ограничения прототипа, не проверено на реальном трафике):
  - Нужны события Network.requestWillBeSentExtraInfo / Network.responseReceivedExtraInfo —
    они дают "сырые" заголовки (Cookie, Set-Cookie), которые CDP по умолчанию прячет
    из обычных Network.requestWillBeSent/responseReceived.
  - requestWillBeSent и requestWillBeSentExtraInfo для одного requestId могут прийти
    в любом порядке — поэтому всё собирается в буфер и обрабатывается по готовности.
  - Опрос списка вкладок через /json раз в пару секунд — просто и надёжно для
    прототипа; при желании можно заменить на push-уведомления через Target-домен
    (Target.setDiscoverTargets + Target.targetCreated) на отдельном browser-level
    websocket-соединении — но это заметно больше кода ради экономии 1-2 секунд.
"""
import json
import os
import re
import threading
import time
import urllib.request
from urllib.parse import parse_qs, urlparse

import websocket  # pip install websocket-client

SERVER_URL = os.environ.get("YDISK_SERVER_URL", "http://127.0.0.1:8001/api/ingest")
AUTH_URL = os.environ.get("YDISK_AUTH_URL", SERVER_URL.rsplit("/api/", 1)[0] + "/api/auth")

TARGET_MODEL = "photo-get-clusters-with-resources"
TARGET_HOST_HINT = "disk.yandex"
YANDEX_BASES = ("yandex.ru", "yandex.com", "ya.ru", "yandex.net")

SK_RE = re.compile(r'"sk"\s*:\s*"([^"]+)"')
CID_RE = re.compile(r'"connection_id"\s*:\s*"([^"]+)"')

MAX_BUFFERED_REQUESTS = 800  # на одну вкладку


def is_yandex_host(host: str) -> bool:
    if not host:
        return False
    h = host.lower()
    return any(h == b or h.endswith("." + b) for b in YANDEX_BASES)


def find_payload(obj):
    if isinstance(obj, dict):
        if "clusters" in obj and "resources" in obj:
            return obj
        for v in obj.values():
            r = find_payload(v)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = find_payload(v)
            if r is not None:
                return r
    return None


def _headers_get(headers: dict, name: str):
    name_lower = name.lower()
    for k, v in (headers or {}).items():
        if k.lower() == name_lower:
            return v
    return None


def parse_cookie_header(raw: str):
    out = []
    for part in (raw or "").split(";"):
        part = part.strip()
        if "=" not in part:
            continue
        name, value = part.split("=", 1)
        if name.strip():
            out.append([name.strip(), value])
    return out


def parse_set_cookie_block(raw: str):
    out = []
    for line in (raw or "").split("\n"):
        line = line.strip()
        if not line:
            continue
        ck = _parse_one_set_cookie(line)
        if ck:
            out.append(ck)
    return out


def _parse_one_set_cookie(raw: str):
    first, *attrs = [p.strip() for p in raw.split(";")]
    if "=" not in first:
        return None
    name, value = first.split("=", 1)
    ck = {"name": name.strip(), "value": value, "expires": None, "delete": False}
    expires = max_age = None
    for a in attrs:
        k, _, v = a.partition("=")
        k, v = k.strip().lower(), v.strip()
        if k == "max-age":
            try:
                max_age = int(v)
            except ValueError:
                pass
        elif k == "expires" and v:
            try:
                from email.utils import parsedate_to_datetime
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


def _extract_sk_conn(url: str, post_data: str):
    sk = conn = None
    text = post_data or ""
    if text:
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                sk, conn = obj.get("sk"), obj.get("connection_id")
        except ValueError:
            form = parse_qs(text)
            sk = (form.get("sk") or [None])[0]
            conn = (form.get("connection_id") or [None])[0]
        if not sk:
            m = SK_RE.search(text)
            sk = m.group(1) if m else None
        if not conn:
            m = CID_RE.search(text)
            conn = m.group(1) if m else None
    if not sk:
        qs = parse_qs(urlparse(url).query)
        sk = (qs.get("sk") or [None])[0]
    return (sk if isinstance(sk, str) else None), (conn if isinstance(conn, str) else None)


def _post_json(url: str, payload: dict, timeout=20):
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


class _TabWatcher(threading.Thread):
    """Слушает CDP-события одной конкретной вкладки (одно websocket-соединение)."""

    def __init__(self, target_id, ws_url, on_log, on_count):
        super().__init__(daemon=True)
        self.target_id = target_id
        self.ws_url = ws_url
        self.on_log = on_log
        self.on_count = on_count  # on_count(n_clusters, n_resources)
        self._ws = None
        self._send_lock = threading.Lock()
        self._next_id = 1
        self._events = {}
        self._results = {}
        self._stop = threading.Event()
        self._req = {}
        self._sent_auth_fp = []

    def stop(self):
        self._stop.set()
        try:
            if self._ws:
                self._ws.close()
        except Exception:
            pass

    def run(self):
        try:
            self._ws = websocket.create_connection(self.ws_url, timeout=5)
        except Exception as e:  # noqa: BLE001
            self.on_log(f"[cdp] не смог подключиться к вкладке {self.target_id}: {e}")
            return
        self._ws.settimeout(1.0)
        self._send("Network.enable", {})
        self.on_log(f"[cdp] слушаю вкладку {self.target_id}")
        try:
            while not self._stop.is_set():
                try:
                    raw = self._ws.recv()
                except websocket.WebSocketTimeoutException:
                    continue
                except Exception:
                    break
                if not raw:
                    break
                try:
                    msg = json.loads(raw)
                except ValueError:
                    continue
                self._dispatch(msg)
        finally:
            self.on_log(f"[cdp] вкладка {self.target_id} закрыта/отключена")
            try:
                self._ws.close()
            except Exception:
                pass

    # ---------------------------------------------------------- CDP команды

    def _send(self, method, params, wait=False, timeout=10):
        msg_id = self._next_id
        self._next_id += 1
        ev = None
        if wait:
            ev = threading.Event()
            self._events[msg_id] = ev
        with self._send_lock:
            self._ws.send(json.dumps({"id": msg_id, "method": method, "params": params}))
        if wait:
            ev.wait(timeout)
            return self._results.pop(msg_id, None)
        return None

    # ---------------------------------------------------------- диспетчер

    def _dispatch(self, msg):
        if "id" in msg:
            msg_id = msg["id"]
            ev = self._events.pop(msg_id, None)
            if ev:
                self._results[msg_id] = msg.get("result")
                ev.set()
            return
        method = msg.get("method")
        params = msg.get("params") or {}
        if method == "Network.requestWillBeSent":
            self._on_request(params)
        elif method == "Network.requestWillBeSentExtraInfo":
            self._on_request_extra(params)
        elif method == "Network.responseReceived":
            self._on_response(params)
        elif method == "Network.responseReceivedExtraInfo":
            self._on_response_extra(params)
        elif method == "Network.loadingFinished":
            self._on_loading_finished(params)

    def _req_entry(self, request_id):
        e = self._req.get(request_id)
        if e is None:
            e = {}
            self._req[request_id] = e
            if len(self._req) > MAX_BUFFERED_REQUESTS:
                for old_id in list(self._req.keys())[: MAX_BUFFERED_REQUESTS // 4]:
                    self._req.pop(old_id, None)
        return e

    # ---------- запрос

    def _on_request(self, params):
        request_id = params.get("requestId")
        req = params.get("request", {})
        url = req.get("url", "")
        if "/models-v2" not in url:
            return
        host = urlparse(url).hostname
        if not is_yandex_host(host):
            return
        e = self._req_entry(request_id)
        e["url"] = url
        e["host"] = host
        e["is_target_req"] = True
        sk, conn = _extract_sk_conn(url, req.get("postData"))
        e["sk"], e["connection_id"] = sk, conn
        self._maybe_send_auth(request_id)

    def _on_request_extra(self, params):
        request_id = params.get("requestId")
        headers = params.get("headers") or {}
        cookie_header = _headers_get(headers, "Cookie")
        if cookie_header is None:
            return
        e = self._req_entry(request_id)
        e["cookie_header"] = cookie_header
        e["user_agent"] = _headers_get(headers, "User-Agent")
        self._maybe_send_auth(request_id)

    def _maybe_send_auth(self, request_id):
        e = self._req.get(request_id)
        if not e or not e.get("is_target_req"):
            return
        if "cookie_header" not in e:
            return
        if e.get("auth_sent"):
            return
        cookies = parse_cookie_header(e["cookie_header"])
        if not cookies:
            return
        payload = {
            "host": e.get("host"),
            "cookies": cookies,
            "sk": e.get("sk"),
            "connection_id": e.get("connection_id"),
            "user_agent": e.get("user_agent"),
        }
        fp = f"{e.get('sk')}|{len(cookies)}|{e.get('user_agent')}"
        if fp in self._sent_auth_fp:
            e["auth_sent"] = True
            return
        self._sent_auth_fp.append(fp)
        del self._sent_auth_fp[:-50]
        e["auth_sent"] = True
        threading.Thread(target=self._post_auth, args=(payload,), daemon=True).start()

    def _post_auth(self, payload):
        try:
            _post_json(AUTH_URL, payload)
            n = len(payload.get("cookies") or [])
            self.on_log(f"[cdp] авторизация отправлена (кук: {n}, sk: {'да' if payload.get('sk') else 'нет'})")
        except Exception as e:  # noqa: BLE001
            self.on_log(f"[cdp] не удалось отправить авторизацию: {e}")

    # ---------- ответ

    def _on_response(self, params):
        request_id = params.get("requestId")
        resp = params.get("response", {})
        url = resp.get("url", "")
        host = urlparse(url).hostname
        if not is_yandex_host(host):
            return
        e = self._req_entry(request_id)
        e["resp_host"] = host
        status = resp.get("status")
        if status == 403:
            self.on_log("[cdp] ⚠️ HTTP 403 — сессия/sk протухли, обнови страницу Диска")
        if TARGET_MODEL in url and status == 200:
            e["fetch_body"] = True

    def _on_response_extra(self, params):
        request_id = params.get("requestId")
        e = self._req.get(request_id)
        if not e or not e.get("resp_host"):
            return
        headers = params.get("headers") or {}
        set_cookie_raw = _headers_get(headers, "set-cookie")
        if not set_cookie_raw:
            return
        items = parse_set_cookie_block(set_cookie_raw)
        if not items:
            return
        threading.Thread(
            target=self._post_auth_setcookies, args=(e["resp_host"], items), daemon=True
        ).start()

    def _post_auth_setcookies(self, host, items):
        try:
            _post_json(AUTH_URL, {"host": host, "set_cookies": items})
            self.on_log(f"[cdp] обновления Set-Cookie отправлены ({len(items)})")
        except Exception as e:  # noqa: BLE001
            self.on_log(f"[cdp] не удалось отправить Set-Cookie: {e}")

    # ---------- тело целевого ответа -> /api/ingest

    def _on_loading_finished(self, params):
        request_id = params.get("requestId")
        e = self._req.get(request_id)
        if not e or not e.get("fetch_body"):
            self._req.pop(request_id, None)
            return
        e["fetch_body"] = False
        threading.Thread(target=self._fetch_and_ingest, args=(request_id,), daemon=True).start()

    def _fetch_and_ingest(self, request_id):
        result = self._send("Network.getResponseBody", {"requestId": request_id}, wait=True)
        self._req.pop(request_id, None)
        if not result or "body" not in result:
            self.on_log(f"[cdp] не удалось получить тело ответа ({request_id})")
            return
        text = result["body"]
        try:
            data = json.loads(text)
        except ValueError as e:
            self.on_log(f"[cdp] ответ не JSON: {e}")
            return
        payload = find_payload(data)
        if payload is None:
            self.on_log("[cdp] в ответе нет clusters/resources — пропускаю")
            return
        n_clusters = len(payload.get("clusters", {}).get("fetched", []))
        n_resources = len(payload.get("resources", {}).get("fetched", []))
        self.on_log(f"[cdp] перехвачено: кластеров={n_clusters}, ресурсов={n_resources}")
        self.on_count(n_clusters, n_resources)
        try:
            _post_json(SERVER_URL, payload)
        except Exception as e:  # noqa: BLE001
            self.on_log(f"[cdp] не удалось отправить на сервер: {e}")


class CDPCapture:
    """
    Менеджер: раз в poll_interval секунд опрашивает список открытых вкладок и
    заводит/убирает по одному _TabWatcher на каждую вкладку с disk.yandex в URL.
    Работает с любым числом открытых вкладок Диска одновременно.
    """

    def __init__(self, devtools_port: int, on_log=print, on_stats=None, poll_interval: float = 2.0):
        self.devtools_port = devtools_port
        self.on_log = on_log
        self.on_stats = on_stats or (lambda **kw: None)
        self.poll_interval = poll_interval
        self._stop = threading.Event()
        self._watchers = {}  # target_id -> _TabWatcher
        self._lock = threading.Lock()
        self._totals = {"clusters": 0, "resources": 0}

    def start(self):
        threading.Thread(target=self._poll_loop, daemon=True).start()

    def stop(self):
        self._stop.set()
        with self._lock:
            for w in self._watchers.values():
                w.stop()

    def _poll_loop(self):
        while not self._stop.is_set():
            self._scan_targets()
            time.sleep(self.poll_interval)

    def _scan_targets(self):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{self.devtools_port}/json", timeout=3) as resp:
                targets = json.loads(resp.read())
        except Exception as e:  # noqa: BLE001
            self.on_log(f"[cdp] не смог получить список вкладок (порт {self.devtools_port}): {e}")
            return

        live_ids = set()
        for t in targets:
            if t.get("type") != "page" or TARGET_HOST_HINT not in (t.get("url") or ""):
                continue
            tid = t.get("id")
            live_ids.add(tid)
            with self._lock:
                existing = self._watchers.get(tid)
                if existing and existing.is_alive():
                    continue
                w = _TabWatcher(tid, t["webSocketDebuggerUrl"], on_log=self.on_log, on_count=self._on_count)
                self._watchers[tid] = w
            w.start()
            self.on_log(f"[cdp] новая вкладка с Диском: {t.get('url')}")

        with self._lock:
            # вкладки, которые закрылись сами (их watcher уже умер) — убираем из учёта
            dead = [tid for tid, w in self._watchers.items() if not w.is_alive()]
            for tid in dead:
                self._watchers.pop(tid, None)
            active = len(self._watchers)
        self.on_stats(active_tabs=active)

    def _on_count(self, n_clusters, n_resources):
        self._totals["clusters"] += n_clusters
        self._totals["resources"] += n_resources
        self.on_stats(total_clusters=self._totals["clusters"], total_resources=self._totals["resources"])
