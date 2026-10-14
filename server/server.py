"""
Локальный сервер:
  POST /api/ingest   — принимает JSON из photo-get-clusters-with-resources, пишет в SQLite
  POST /api/auth     — куки, sk и User-Agent из mitmproxy-аддона (лежат в отдельной базе auth.db)
  GET  /api/auth/status — есть ли авторизация (значения кук наружу не отдаются)
  GET  /             — галерея: сетка по дням, одним длинным списком
  GET  /chunk        — следующая порция дней для подгрузки при прокрутке
  GET  /status       — статус загрузки: вкладки «Фото» и «Видео» (#video)
  POST /api/dl/start|stop|threads|retry-failed, GET /api/dl/status — загрузка фото
  POST /api/vd/start|stop|settings|retry-failed, GET /api/vd/status — загрузка видео через recover.py
  GET  /api/items    — те же данные в JSON
  GET  /api/stats    — статистика

Запуск:
    pip install flask pillow pillow-heif   # pillow-heif — чтобы делать превью из HEIC
    python server.py            # http://127.0.0.1:8001

Для видео рядом с server.py должны лежать recover.py и recover_runner.py, а ffmpeg — в PATH
(ffmpeg нужен для сборки повреждённых видео и для превью видео).
"""
import bisect
import codecs
import hashlib
import io
import json
import os
import random
import re
import shutil
import signal
import socket
import sqlite3
import ssl
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from contextlib import contextmanager
from datetime import datetime, timezone

from flask import Flask, Response, jsonify, render_template_string, request, send_file

DB_PATH = os.environ.get("YDISK_DB", os.path.join(os.path.dirname(os.path.abspath(__file__)), "gallery.db"))
HOST = "127.0.0.1"
PORT = int(os.environ.get("YDISK_PORT", "8001"))
DAYS_PER_CHUNK = 10  # сколько дней отдаём за одну подгрузку

# --- скачивание файлов ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILES_DIR = os.environ.get("YDISK_FILES", os.path.join(BASE_DIR, "files"))
THUMBS_DIR = os.environ.get("YDISK_THUMBS", os.path.join(BASE_DIR, "thumbs"))
THUMB_MAX_BYTES = 30 * 1024  # целевой размер превью
THUMB_SIDE = 360             # длинная сторона превью, px (если не влезает в 30 КБ — уменьшаем)
QUEUE_SIZE = 100            # сколько файлов всегда ждут в очереди
DEFAULT_THREADS = 8
MAX_THREADS = 32
# порядок попыток: сначала оригинал, потом превью от большего к меньшему
SIZE_ORDER = ["ORIGINAL", "XXXL", "XXL", "XL", "L", "M", "S", "XS", "XXS", "XXXS", "DEFAULT"]
ATTEMPT_TIMEOUT = 10        # сек: на соединение и на ожидание каждого блока данных
ATTEMPT_DEADLINE = 180      # сек: потолок на одну попытку целиком (против «капающих» соединений)
FAIL_STREAK_STOP = 15       # столько файлов подряд без успеха — автоматически останавливаемся
USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
DL_COLUMNS = {
    "dl_status": "TEXT",     # NULL / queued / downloading / done / failed
    "dl_size": "TEXT",       # ORIGINAL / XXXL / ... — что реально скачали
    "dl_file": "TEXT",       # путь относительно files/
    "dl_bytes": "INTEGER",
    "dl_error": "TEXT",
    "dl_attempts": "TEXT",
    "dl_at": "TEXT",
}
# --- видео: свои колонки, чтобы не пересекаться с загрузкой фото ---
VD_COLUMNS = {
    "vd_status": "TEXT",     # NULL / queued / downloading / done / failed
    "vd_phase": "TEXT",
    "vd_file": "TEXT",       # путь относительно files/
    "vd_bytes": "INTEGER",   # размер файла на Диске
    "vd_lost": "INTEGER",    # сколько байт потеряно (0 — оригинал без потерь)
    "vd_error": "TEXT",
    "vd_note": "TEXT",
    "vd_at": "TEXT",
}
EXT_BY_CTYPE = {"image/jpeg": ".jpg", "image/jpg": ".jpg", "image/png": ".png", "image/webp": ".webp",
                "image/heic": ".heic", "image/heif": ".heif", "image/gif": ".gif"}

# --- видео и авторизация ---
MB = 1024 * 1024
AUTH_DB_PATH = os.environ.get("YDISK_AUTH_DB", os.path.join(BASE_DIR, "auth.db"))   # куки из mitmproxy: отдельная база
API_BASE = os.environ.get("YDISK_API_BASE", "https://disk.yandex.ru")
API_TIMEOUT = 20            # сек: запрос ссылки на скачивание (mpfs/url)
RUNNER = os.path.join(BASE_DIR, "recover_runner.py")
VD_QUEUE_SIZE = 20          # сколько видео ждут в очереди
VD_LINK_RETRIES = 5         # сколько раз на фазу брать свежую ссылку (просрочка, 403)
VD_FAIL_STREAK_STOP = 5     # столько видео подряд не скачались — останавливаемся
VD_DEFAULTS = {
    "videos": 2,            # сколько видео качается одновременно
    "jobs": 12,             # потоки проверки блоков внутри одного видео (recover.py --jobs)
    "block_kb": 256,        # --block
    "sub_kb": 16,           # --sub
    "timeout": 20,          # --timeout
    "sub_timeout": 6,       # --sub-timeout
    "retries": 2,           # --retries
    "probe_size": 4096,     # --probe-size
    "probe_timeout": 10,    # --probe-timeout
    "refine": True,         # после скачивания — прогон --refine
    "refine_jobs": 12,      # потоки refine (--jobs на этапе уточнения)
    "stride_kb": 64,        # --stride
    "reencode": False,      # повреждённые видео сразу перекодировать в H.264
}
VD_LIMITS = {
    "videos": (1, 8), "jobs": (1, 64), "block_kb": (16, 8192), "sub_kb": (1, 1024), "timeout": (3, 600),
    "sub_timeout": (1, 120), "retries": (1, 10), "probe_size": (512, 1048576), "probe_timeout": (1, 300),
    "refine_jobs": (1, 64), "stride_kb": (4, 4096),
}

app = Flask(__name__)
app.config["MAX_CONTENT_LENGTH"] = 256 * 1024 * 1024

VIDEO_EXT = {"mov", "mp4", "m4v", "avi", "mkv", "3gp", "mts", "webm"}
NAME_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2}) (\d{2})-(\d{2})-(\d{2})")


# ---------------------------------------------------------------- DB

def db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    with db() as c:
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS clusters (
                id          TEXT PRIMARY KEY,
                size        INTEGER,
                updated_at  TEXT
            );
            CREATE TABLE IF NOT EXISTS items (
                path        TEXT PRIMARY KEY,   -- например /photounlim/2026-10-09 12-08-58.MOV
                name        TEXT,
                taken       TEXT,               -- ISO-время из имени файла (для сортировки)
                mediatype   TEXT,               -- image / video
                mimetype    TEXT,
                size        INTEGER,            -- байты (есть только если пришёл resource)
                width       INTEGER,
                height      INTEGER,
                duration_ms INTEGER,
                etime       INTEGER,
                ctime       INTEGER,
                mtime       INTEGER,
                resource_id TEXT,
                file_id     TEXT,
                item_id     TEXT,
                cluster_id  TEXT,
                albums      TEXT,               -- JSON-список
                raw         TEXT,               -- полный JSON ресурса (с подписанными ссылками, они протухают)
                first_seen  TEXT,
                updated_at  TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_items_taken ON items(taken);
            CREATE INDEX IF NOT EXISTS idx_items_type  ON items(mediatype);
            """
        )
        cols = {r[1] for r in c.execute("PRAGMA table_info(items)")}
        for col, typ in list(DL_COLUMNS.items()) + list(VD_COLUMNS.items()):
            if col not in cols:
                c.execute(f"ALTER TABLE items ADD COLUMN {col} {typ}")
        c.execute("CREATE INDEX IF NOT EXISTS idx_items_dl ON items(dl_status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_items_vd ON items(vd_status)")
        c.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)")
        # после перезапуска «зависшие» элементы очереди возвращаем в исходное состояние
        c.execute("UPDATE items SET dl_status=NULL WHERE dl_status IN ('queued','downloading')")
        c.execute("UPDATE items SET vd_status=NULL WHERE vd_status IN ('queued','downloading')")


# ---------------------------------------------------------------- ingest

def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def name_from_path(path: str) -> str:
    return path.rsplit("/", 1)[-1]


def taken_from_name(name: str):
    m = NAME_DATE_RE.match(name)
    if not m:
        return None
    y, mo, d, h, mi, s = m.groups()
    return f"{y}-{mo}-{d}T{h}:{mi}:{s}"


def guess_mediatype(name: str, albums=None):
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    if ext in VIDEO_EXT or (albums and "videos" in albums):
        return "video"
    return "image"


def upsert_cluster_item(c, cluster_id, it, ts):
    path = it.get("id")
    if not path:
        return
    name = name_from_path(path)
    albums = it.get("albums") or []
    c.execute(
        """
        INSERT INTO items(path, name, taken, mediatype, width, height, item_id, cluster_id, albums, first_seen, updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(path) DO UPDATE SET
            width      = COALESCE(excluded.width,  items.width),
            height     = COALESCE(excluded.height, items.height),
            item_id    = COALESCE(excluded.item_id, items.item_id),
            cluster_id = COALESCE(excluded.cluster_id, items.cluster_id),
            albums     = COALESCE(excluded.albums, items.albums),
            mediatype  = COALESCE(items.mediatype, excluded.mediatype),
            updated_at = excluded.updated_at
        """,
        (
            path, name, taken_from_name(name), guess_mediatype(name, albums),
            it.get("width"), it.get("height"), it.get("itemId"), cluster_id,
            json.dumps(albums, ensure_ascii=False), ts, ts,
        ),
    )


def upsert_resource(c, r, ts):
    path = r.get("path") or r.get("id")
    if not path:
        return
    name = r.get("name") or name_from_path(path)
    meta = r.get("meta") or {}
    video_info = meta.get("video_info") or {}
    c.execute(
        """
        INSERT INTO items(path, name, taken, mediatype, mimetype, size, duration_ms,
                          etime, ctime, mtime, resource_id, file_id, cluster_id, raw, first_seen, updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(path) DO UPDATE SET
            name        = excluded.name,
            mediatype   = COALESCE(excluded.mediatype, items.mediatype),
            mimetype    = COALESCE(excluded.mimetype, items.mimetype),
            size        = COALESCE(excluded.size, items.size),
            duration_ms = COALESCE(excluded.duration_ms, items.duration_ms),
            etime       = COALESCE(excluded.etime, items.etime),
            ctime       = COALESCE(excluded.ctime, items.ctime),
            mtime       = COALESCE(excluded.mtime, items.mtime),
            resource_id = COALESCE(excluded.resource_id, items.resource_id),
            file_id     = COALESCE(excluded.file_id, items.file_id),
            cluster_id  = COALESCE(items.cluster_id, excluded.cluster_id),
            raw         = excluded.raw,
            updated_at  = excluded.updated_at
        """,
        (
            path, name, taken_from_name(name), meta.get("mediatype"), meta.get("mimetype"),
            meta.get("size"), video_info.get("duration"),
            r.get("etime"), r.get("ctime"), r.get("mtime"),
            meta.get("resource_id"), meta.get("file_id"), r.get("clusterId"),
            json.dumps(r, ensure_ascii=False), ts, ts,
        ),
    )


def ingest(payload: dict):
    ts = now_iso()
    clusters = (payload.get("clusters") or {}).get("fetched") or []
    resources = (payload.get("resources") or {}).get("fetched") or []

    with db() as c:
        before = c.execute("SELECT COUNT(*) FROM items").fetchone()[0]

        for cl in clusters:
            cid = cl.get("id")
            c.execute(
                "INSERT INTO clusters(id,size,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET size=excluded.size, updated_at=excluded.updated_at",
                (cid, cl.get("size"), ts),
            )
            for it in cl.get("items") or []:
                upsert_cluster_item(c, cid, it, ts)

        for r in resources:
            upsert_resource(c, r, ts)

        after = c.execute("SELECT COUNT(*) FROM items").fetchone()[0]

    return {
        "clusters": len(clusters),
        "resources": len(resources),
        "new_items": after - before,
        "total_items": after,
    }


@app.post("/api/ingest")
def api_ingest():
    payload = request.get_json(force=True, silent=True)
    if not isinstance(payload, dict):
        return jsonify(error="expected JSON object"), 400
    result = ingest(payload)
    print(f"[ingest] {result}")
    return jsonify(result)


# ---------------------------------------------------------------- авторизация (auth.db)

@contextmanager
def authc():
    c = sqlite3.connect(AUTH_DB_PATH, timeout=30)
    try:
        yield c
        c.commit()
    finally:
        c.close()


def init_auth_db():
    new = not os.path.exists(AUTH_DB_PATH)
    with authc() as c:
        c.execute("PRAGMA journal_mode=WAL")
        c.execute("CREATE TABLE IF NOT EXISTS cookies ("
                  "name TEXT PRIMARY KEY, value TEXT, expires REAL, pos INTEGER, updated_at TEXT)")
        c.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
    if new:
        try:
            os.chmod(AUTH_DB_PATH, 0o600)    # в базе сессионные куки: читать должен только владелец
        except OSError:
            pass


def ingest_auth(p: dict):
    """
    cookies      — полный снимок заголовка Cookie из запроса браузера: заменяет всю банку целиком;
    set_cookies  — обновления из Set-Cookie ответов Яндекса: применяются поверх (с учётом удаления);
    sk, connection_id, user_agent, host — последние виденные значения.
    """
    ts = now_iso()
    n_snap = n_set = 0
    with authc() as c:
        snap = p.get("cookies")
        if isinstance(snap, list) and snap:
            rows = [(str(x[0]), str(x[1])) for x in snap if isinstance(x, (list, tuple)) and len(x) == 2 and x[0]]
            if rows:
                c.execute("DELETE FROM cookies")
                for i, (name, value) in enumerate(rows):
                    c.execute("INSERT OR REPLACE INTO cookies(name,value,expires,pos,updated_at) VALUES(?,?,NULL,?,?)",
                              (name, value, i, ts))
                n_snap = len(rows)
        for ck in p.get("set_cookies") or []:
            if not isinstance(ck, dict) or not ck.get("name"):
                continue
            if ck.get("delete"):
                c.execute("DELETE FROM cookies WHERE name=?", (ck["name"],))
            else:
                c.execute(
                    "INSERT INTO cookies(name,value,expires,pos,updated_at) "
                    "VALUES(?,?,?,(SELECT COALESCE(MAX(pos),0)+1 FROM cookies),?) "
                    "ON CONFLICT(name) DO UPDATE SET value=excluded.value, expires=excluded.expires, "
                    "updated_at=excluded.updated_at",
                    (ck["name"], ck.get("value", ""), ck.get("expires"), ts))
            n_set += 1
        have_sk = False
        for key in ("sk", "connection_id", "user_agent", "host"):
            v = p.get(key)
            if isinstance(v, str) and v:
                c.execute("INSERT INTO meta(key,value) VALUES(?,?) "
                          "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, v))
                have_sk = have_sk or key == "sk"
        if n_snap or n_set or have_sk:
            c.execute("INSERT INTO meta(key,value) VALUES('updated_at',?) "
                      "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (ts,))
    return {"cookies": n_snap, "set_cookies": n_set, "sk": have_sk}


def load_auth():
    with authc() as c:
        rows = c.execute("SELECT name,value,expires FROM cookies ORDER BY pos").fetchall()
        meta = {k: v for k, v in c.execute("SELECT key,value FROM meta")}
    now = time.time()
    return {
        "cookies": [(n, v) for n, v, e in rows if e is None or e > now],
        "sk": meta.get("sk"),
        "connection_id": meta.get("connection_id"),
        "user_agent": meta.get("user_agent"),
        "updated_at": meta.get("updated_at"),
    }


def auth_summary():
    """Для интерфейса: без значений кук."""
    a = load_auth()
    age = None
    if a["updated_at"]:
        try:
            age = max(0, int((datetime.now(timezone.utc) - datetime.fromisoformat(a["updated_at"])).total_seconds()))
        except ValueError:
            pass
    names = {n for n, _ in a["cookies"]}
    return {"cookies": len(a["cookies"]), "has_sk": bool(a["sk"]), "has_session": "Session_id" in names,
            "age_sec": age}


@app.post("/api/auth")
def api_auth():
    payload = request.get_json(force=True, silent=True)
    if not isinstance(payload, dict):
        return jsonify(error="expected JSON object"), 400
    res = ingest_auth(payload)
    print(f"[auth] кук в снимке: {res['cookies']}, обновлений: {res['set_cookies']}, sk: {res['sk']}")
    return jsonify(res)


@app.get("/api/auth/status")
def api_auth_status():
    return jsonify(auth_summary())


class AuthError(Exception):
    """Нет авторизации или она протухла: нужно заново открыть Диск через mitmproxy."""


class VideoError(Exception):
    """Это видео не удалось скачать/собрать (остальные продолжаются)."""


def find_key(obj, key):
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = find_key(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = find_key(v, key)
            if r is not None:
                return r
    return None


def get_download_url(path: str) -> str:
    """POST models-v2?m=mpfs/url с сохранёнными куками и sk -> прямая ссылка на скачивание."""
    a = load_auth()
    if not a["cookies"] or not a["sk"]:
        raise AuthError("нет данных авторизации: открой Яндекс Диск в браузере через mitmproxy "
                        "(обнови страницу), аддон передаст куки и sk")
    cid = a["connection_id"] or ""
    cid = (cid[:-3] + f"{random.randint(0, 999):03d}") if len(cid) > 6 else str(int(time.time() * 1000))
    body = {"sk": a["sk"], "connection_id": cid, "apiMethod": "mpfs/url", "requestParams": {"path": path}}
    req = urllib.request.Request(
        API_BASE + "/models-v2?m=mpfs/url",
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers={
            "Accept": "*/*",
            "Content-Type": "application/json",
            "Cookie": "; ".join(f"{n}={v}" for n, v in a["cookies"]),
            "User-Agent": a["user_agent"] or USER_AGENT,
            "Origin": API_BASE,
            "Referer": API_BASE + "/client/photo",
            "X-Requested-With": "XMLHttpRequest",
        },
    )
    try:
        with VD.opener.open(req, timeout=API_TIMEOUT) as resp:
            text = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            raise AuthError(f"Яндекс ответил HTTP {e.code}: сессия или sk протухли — обнови страницу Диска "
                            "в браузере через mitmproxy")
        raise VideoError(f"запрос ссылки: HTTP {e.code}")
    except (urllib.error.URLError, socket.timeout, TimeoutError, OSError) as e:
        raise VideoError(f"запрос ссылки: {short_err(e)}")
    try:
        data = json.loads(text)
    except ValueError:
        raise AuthError("Яндекс вернул не JSON (скорее всего, страницу входа): сессия протухла")
    url = find_key(data, "file")
    if not isinstance(url, str) or not url:
        err = find_key(data, "error") or find_key(data, "code") or data
        raise VideoError("Яндекс не вернул ссылку: " + json.dumps(err, ensure_ascii=False)[:200])
    return "https:" + url if url.startswith("//") else url


# ---------------------------------------------------------------- queries

def build_filters(args):
    where, params = [], []
    q = (args.get("q") or "").strip()
    if q:
        where.append("name LIKE ?")
        params.append(f"%{q}%")
    mt = args.get("type")
    if mt in ("image", "video"):
        where.append("mediatype = ?")
        params.append(mt)
    album = (args.get("album") or "").strip()
    if album:
        where.append("albums LIKE ?")
        params.append(f'%"{album}"%')
    sql = (" WHERE " + " AND ".join(where)) if where else ""
    return sql, params


def stats():
    with db() as c:
        row = c.execute(
            """
            SELECT COUNT(*) AS total,
                   SUM(mediatype='image') AS images,
                   SUM(mediatype='video') AS videos,
                   SUM(resource_id IS NOT NULL) AS with_resource,
                   COALESCE(SUM(size),0) AS bytes
            FROM items
            """
        ).fetchone()
        clusters = c.execute("SELECT COUNT(*) FROM clusters").fetchone()[0]
    d = dict(row)
    d["clusters"] = clusters
    return {k: (v or 0) for k, v in d.items()}


def all_albums():
    found = set()
    with db() as c:
        for (a,) in c.execute("SELECT DISTINCT albums FROM items WHERE albums IS NOT NULL"):
            try:
                found.update(json.loads(a))
            except Exception:  # noqa: BLE001
                pass
    return sorted(found)


def human_size(n):
    if not n:
        return ""
    n = float(n)
    for unit in ("Б", "КБ", "МБ", "ГБ"):
        if n < 1024 or unit == "ГБ":
            return f"{n:.0f} {unit}" if unit == "Б" else f"{n:.1f} {unit}"
        n /= 1024


def human_duration(ms):
    if not ms:
        return ""
    s = int(ms // 1000)
    return f"{s // 60}:{s % 60:02d}"


@app.get("/api/stats")
def api_stats():
    return jsonify(stats())


@app.get("/api/items")
def api_items():
    sql, params = build_filters(request.args)
    limit = min(int(request.args.get("limit", 1000)), 10000)
    offset = int(request.args.get("offset", 0))
    with db() as c:
        rows = c.execute(
            "SELECT path,name,taken,mediatype,mimetype,size,width,height,duration_ms,albums,cluster_id,resource_id "
            f"FROM items{sql} ORDER BY COALESCE(taken,'') DESC, name DESC LIMIT ? OFFSET ?",
            params + [limit, offset],
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["albums"] = json.loads(d["albums"]) if d["albums"] else []
        out.append(d)
    return jsonify(out)


# ---------------------------------------------------------------- gallery

MONTHS = ["января", "февраля", "марта", "апреля", "мая", "июня",
          "июля", "августа", "сентября", "октября", "ноября", "декабря"]


def day_label(day):
    if not day:
        return "Без даты"
    try:
        d = datetime.strptime(day, "%Y-%m-%d")
    except ValueError:
        return day
    s = f"{d.day} {MONTHS[d.month - 1]}"
    if d.year != datetime.now().year:
        s += f" {d.year}"
    return s


PAGE = """<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Фото — локальная галерея</title>
<style>
  :root { color-scheme: dark light; --tile:130px; --bg:#202024; --fg:#ececf1; --mut:#8e8e99; --line:#33333a; --cell:#44444c; --badge:rgba(0,0,0,.55); }
  @media (prefers-color-scheme: light) { :root { --bg:#fff; --fg:#1b1b1f; --mut:#6b6b76; --line:#e3e3ea; --cell:#e4e4ea; --badge:rgba(0,0,0,.55); } }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--fg); font:13px/1.4 system-ui, sans-serif; }
  main { padding:14px 24px 60px; }
  header { display:flex; flex-wrap:wrap; gap:10px 16px; align-items:center; margin-bottom:6px; }
  h1 { font-size:17px; margin:0; font-weight:600; }
  .stats { color:var(--mut); font-size:12px; flex:1 1 auto; }
  form { display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin:8px 0 4px; }
  input, select, button { font:inherit; padding:5px 10px; border:1px solid var(--line); border-radius:8px; background:transparent; color:var(--fg); }
  input[type=range] { padding:0; width:110px; }
  h2 { font-size:13px; font-weight:600; margin:22px 0 8px; }
  h2 .cnt { color:var(--mut); font-weight:400; margin-left:6px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fill, minmax(var(--tile), 1fr)); gap:4px; }
  .tile { position:relative; aspect-ratio:1; background:var(--cell); border-radius:2px; overflow:hidden; }
  .tile a.open { position:absolute; inset:0; z-index:2; cursor:zoom-in; }
  .tile .badge, .tile .dl, .tile .tname { pointer-events:none; z-index:3; }
  .tile img { position:absolute; inset:0; width:100%; height:100%; object-fit:cover; display:block; }
  .tile.has-img .tname { opacity:0; background:linear-gradient(transparent, rgba(0,0,0,.7)); color:#fff;
                         left:0; right:0; bottom:0; padding:14px 6px 4px; transition:opacity .15s; }
  .tile.has-img:hover .tname { opacity:1; }
  .tname { position:absolute; left:6px; right:6px; bottom:5px; font:10px/1.25 ui-monospace, Menlo, monospace;
           color:var(--fg); opacity:.75; word-break:break-all; display:-webkit-box; -webkit-line-clamp:3; -webkit-box-orient:vertical; overflow:hidden; }
  .badge { position:absolute; left:6px; top:6px; background:var(--badge); color:#fff; border-radius:10px; padding:1px 7px; font-size:11px; }
  .dl { position:absolute; right:6px; top:6px; background:#2e7d32; color:#fff; border-radius:10px; padding:1px 7px; font-size:10px; letter-spacing:.02em; }
  .dl.low { background:#b26a00; }
  nav { display:flex; gap:14px; }
  nav a { color:var(--mut); text-decoration:none; }
  nav a.on { color:var(--fg); font-weight:600; }
  #status { color:var(--mut); text-align:center; padding:28px 0; min-height:20px; }
  .empty { color:var(--mut); margin-top:30px; }

  #viewer { position:fixed; inset:0; z-index:100; display:flex; align-items:center; justify-content:center;
            background:rgba(0,0,0,.82); backdrop-filter:blur(2px); padding:40px; }
  #viewer[hidden] { display:none; }
  #viewer .vwrap { position:relative; max-width:100%; max-height:100%; display:flex; }
  #viewer img, #viewer video { display:block; max-width:100%; max-height:calc(100vh - 80px); border-radius:4px;
                                box-shadow:0 10px 40px rgba(0,0,0,.5); }
  #viewer .vclose { position:absolute; top:-18px; right:-18px; width:36px; height:36px; border-radius:50%;
                     background:#1c1c20; color:#fff; border:1px solid rgba(255,255,255,.2); font-size:18px;
                     line-height:1; cursor:pointer; display:flex; align-items:center; justify-content:center; }
  #viewer .vclose:hover { background:#2a2a30; }
  #viewer .vcaption { position:absolute; left:0; right:0; bottom:-30px; text-align:center; color:#cfcfd6;
                       font-size:12px; word-break:break-all; }
</style>
</head>
<body>
<main>
  <header>
    <h1>Фото</h1>
    <nav><a href="/" class="on">Галерея</a><a href="/status">Статус загрузки</a></nav>
    <div class="stats">
      всего {{ s.total }} · фото {{ s.images }} · видео {{ s.videos }} ·
      с данными ресурса {{ s.with_resource }} · кластеров {{ s.clusters }} · {{ human(s.bytes) }}
    </div>
  </header>

  <form method="get">
    <input name="q" value="{{ base.q }}" placeholder="поиск по имени">
    <select name="type">
      <option value="">фото + видео</option>
      <option value="image" {{ 'selected' if base.type=='image' }}>только фото</option>
      <option value="video" {{ 'selected' if base.type=='video' }}>только видео</option>
    </select>
    <select name="album">
      <option value="">все альбомы</option>
      {% for a in albums %}<option value="{{ a }}" {{ 'selected' if a==base.album }}>{{ a }}</option>{% endfor %}
    </select>
    <button>Показать</button>
    <label class="stats">размер <input type="range" id="size" min="70" max="260" value="130"></label>
  </form>

  {% if not fragment.strip() %}<p class="empty">Пока пусто. Открой фото в Яндекс Диске через mitmproxy — данные появятся здесь.</p>{% endif %}

  <div id="list">{{ fragment|safe }}</div>
  <div id="sentinel"></div>
  <div id="status"></div>
</main>

<div id="viewer" hidden>
  <div class="vwrap">
    <button class="vclose" type="button" aria-label="Закрыть" title="Закрыть (Esc)">✕</button>
    <span class="vcaption"></span>
  </div>
</div>

<script>
  const FILTERS = {{ base|tojson }};
  let cursor = {{ cursor|tojson }};          // последний показанный день (null — ничего нет)
  let hasMore = {{ has_more|tojson }};
  let loading = false;

  const list = document.getElementById('list');
  const statusEl = document.getElementById('status');
  const sentinel = document.getElementById('sentinel');

  // просмотр фото/видео поверх галереи (вместо target=_blank — во встроенной
  // вкладке приложения новые окна/вкладки не открываются)
  const viewer = document.getElementById('viewer');
  const vwrap = viewer.querySelector('.vwrap');
  const vcaption = viewer.querySelector('.vcaption');
  let vmedia = null;

  function closeViewer() {
    viewer.hidden = true;
    document.body.style.overflow = '';
    if (vmedia) { vmedia.pause && vmedia.pause(); vmedia.remove(); vmedia = null; }
  }

  function openViewer(rid, type, name) {
    if (vmedia) vmedia.remove();
    vmedia = document.createElement(type === 'video' ? 'video' : 'img');
    if (type === 'video') { vmedia.controls = true; vmedia.autoplay = true; }
    else { vmedia.alt = name || ''; }
    vmedia.src = '/file/' + rid;
    vwrap.insertBefore(vmedia, vcaption);
    vcaption.textContent = name || '';
    viewer.hidden = false;
    document.body.style.overflow = 'hidden';
  }

  list.addEventListener('click', (e) => {
    const a = e.target.closest('a.open');
    if (!a) return;
    e.preventDefault();
    openViewer(a.dataset.rid, a.dataset.type, a.dataset.name);
  });

  viewer.addEventListener('click', (e) => {
    if (e.target === viewer) closeViewer();   // клик по затемнённому фону
  });
  viewer.querySelector('.vclose').onclick = closeViewer;
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && !viewer.hidden) closeViewer();
  });

  // размер плиток
  const sizeEl = document.getElementById('size');
  try { const v = localStorage.getItem('tile'); if (v) sizeEl.value = v; } catch (e) {}
  function applySize() {
    document.documentElement.style.setProperty('--tile', sizeEl.value + 'px');
    try { localStorage.setItem('tile', sizeEl.value); } catch (e) {}
  }
  sizeEl.addEventListener('input', applySize); applySize();

  // подгрузка по мере прокрутки
  async function loadMore() {
    if (loading || !hasMore) return;
    loading = true;
    statusEl.textContent = 'загрузка…';
    try {
      const p = new URLSearchParams({ ...FILTERS, before: cursor });
      const r = await fetch('/chunk?' + p);
      if (!r.ok) throw new Error('HTTP ' + r.status);
      const j = await r.json();
      list.insertAdjacentHTML('beforeend', j.html);
      hasMore = j.has_more;
      if (j.cursor !== null) cursor = j.cursor;
      statusEl.textContent = hasMore ? '' : 'это всё';
    } catch (e) {
      statusEl.textContent = 'ошибка загрузки, пробую ещё раз…';
      setTimeout(() => { loading = false; loadMore(); }, 2000);
      return;
    }
    loading = false;
    // если после догрузки низ всё ещё на экране — проверим заново
    io.unobserve(sentinel); io.observe(sentinel);
  }

  const io = new IntersectionObserver((entries) => {
    if (entries.some(e => e.isIntersecting)) loadMore();
  }, { rootMargin: '1500px 0px' });
  if (hasMore) io.observe(sentinel); else if (list.children.length) statusEl.textContent = 'это всё';
</script>
</body>
</html>
"""

FRAGMENT = """{% for day, items in groups %}
<section>
  <h2>{{ day_label(day) }}<span class="cnt">{{ items|length }}</span></h2>
  <div class="grid">
    {% for it in items %}
    {% set got = it.dl_status=='done' or (it.mediatype=='video' and it.vd_status=='done') %}
    <div class="tile{{ ' has-img' if got }}" title="{{ it.name }}{% if it.size %} · {{ human(it.size) }}{% endif %}{% if it.width and it.height %} · {{ it.width }}×{{ it.height }}{% endif %}">
      {% if got %}<a class="open" href="/file/{{ it.rid }}" data-rid="{{ it.rid }}" data-type="{{ it.mediatype }}" data-name="{{ it.name }}" rel="noopener"></a>{% endif %}
      {% if got %}<img src="/thumb/{{ it.rid }}" loading="lazy" decoding="async" alt="" onerror="this.parentNode.classList.remove('has-img');this.remove()">{% endif %}
      {% if it.mediatype=='video' %}<span class="badge">▶{% if it.duration_ms %} {{ dur(it.duration_ms) }}{% endif %}</span>{% endif %}
      {% if it.dl_status=='done' and it.dl_size %}<span class="dl{{ ' low' if it.dl_size!='ORIGINAL' }}">{{ it.dl_size }}</span>{% endif %}
      {% if it.mediatype=='video' and it.vd_status=='done' %}<span class="dl{{ ' low' if it.vd_lost }}">{% if it.vd_lost %}потеряно {{ it.lostpct }}%{% else %}оригинал{% endif %}</span>{% endif %}
      <span class="tname">{{ it.name }}</span>
    </div>
    {% endfor %}
  </div>
</section>
{% endfor %}"""

DAY_EXPR = "COALESCE(substr(taken,1,10),'')"


def load_chunk(args, before=None):
    """Следующая порция дней (новые -> старые). before — день-курсор: берём дни строго старше."""
    sql, params = build_filters(args)
    with db() as c:
        if before is None:
            q = f"SELECT {DAY_EXPR} AS day FROM items{sql} GROUP BY day ORDER BY day DESC LIMIT ?"
            qp = params + [DAYS_PER_CHUNK + 1]
        else:
            q = (f"SELECT day FROM (SELECT {DAY_EXPR} AS day FROM items{sql}) "
                 "WHERE day < ? GROUP BY day ORDER BY day DESC LIMIT ?")
            qp = params + [before, DAYS_PER_CHUNK + 1]
        days = [r[0] for r in c.execute(q, qp)]
        has_more = len(days) > DAYS_PER_CHUNK
        days = days[:DAYS_PER_CHUNK]

        rows = []
        if days:
            marks = ",".join("?" * len(days))
            cond = f"{DAY_EXPR} IN ({marks})"
            where = f"{sql} AND {cond}" if sql else f" WHERE {cond}"
            rows = c.execute(
                "SELECT rowid AS rid,name,taken,mediatype,size,width,height,duration_ms,dl_status,dl_size,"
                "vd_status,vd_lost "
                f"FROM items{where} ORDER BY COALESCE(taken,'') DESC, name DESC",
                params + days,
            ).fetchall()

    by_day = {d: [] for d in days}
    for r in rows:
        d = dict(r)
        d["lostpct"] = round(100.0 * d["vd_lost"] / d["size"], 1) if d.get("vd_lost") and d.get("size") else 0
        by_day[(r["taken"] or "")[:10]].append(d)
    groups = [(d, by_day[d]) for d in days]
    cursor = days[-1] if days else None
    return groups, has_more, cursor


def render_fragment(groups):
    return render_template_string(
        FRAGMENT, groups=groups, human=human_size, dur=human_duration, day_label=day_label
    )


@app.get("/chunk")
def chunk():
    groups, has_more, cursor = load_chunk(request.args, request.args.get("before"))
    return jsonify(html=render_fragment(groups), has_more=has_more, cursor=cursor)


@app.get("/")
def gallery():
    groups, has_more, cursor = load_chunk(request.args)
    base = {k: request.args.get(k, "") for k in ("q", "type", "album")}
    return render_template_string(
        PAGE,
        s=stats(), albums=all_albums(), base=base, human=human_size,
        fragment=render_fragment(groups), cursor=cursor, has_more=has_more,
    )


# ---------------------------------------------------------------- общие помощники загрузки

@contextmanager
def dbc():
    """Соединение, которое гарантированно коммитится и закрывается."""
    c = db()
    try:
        yield c
        c.commit()
    finally:
        c.close()


def get_setting(key, default=None):
    with dbc() as c:
        r = c.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return r[0] if r else default


def set_setting(key, value):
    with dbc() as c:
        c.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, str(value)),
        )


_BAD_CHARS = re.compile(r'[\\/:*?"<>|\x00-\x1f]')


def safe_name(s):
    return _BAD_CHARS.sub("_", s or "").strip() or "file"


def short_err(e):
    if isinstance(e, DeadlineError):
        return "превышен общий лимит времени"
    if isinstance(e, urllib.error.HTTPError):
        return f"HTTP {e.code}"
    if isinstance(e, urllib.error.URLError):
        r = e.reason
        return "timeout" if isinstance(r, (socket.timeout, TimeoutError)) else f"сеть: {r}"
    if isinstance(e, (socket.timeout, TimeoutError)):
        return "timeout"
    return f"{type(e).__name__}: {e}"


class Aborted(Exception):
    """Загрузка остановлена пользователем."""


class DeadlineError(Exception):
    """Попытка длится дольше ATTEMPT_DEADLINE."""


def build_opener():
    # Скачиваем напрямую: системные/переменные прокси (в т.ч. сам mitmproxy) игнорируем.
    handlers = [urllib.request.ProxyHandler({})]
    if os.environ.get("YDISK_INSECURE_SSL"):
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        handlers.append(urllib.request.HTTPSHandler(context=ctx))
    return urllib.request.build_opener(*handlers)


ELIGIBLE = "mediatype='image' AND raw IS NOT NULL AND dl_status IS NULL"
ELIGIBLE_VD = "mediatype='video' AND raw IS NOT NULL AND vd_status IS NULL"
GALLERY_ORDER = "ORDER BY COALESCE(taken,'') DESC, name DESC"   # как в галерее: сверху вниз


class Downloader:
    def __init__(self):
        self.threads = DEFAULT_THREADS
        self.stop_event = threading.Event()
        self.stop_event.set()              # изначально «не запущено»
        self.finishing = threading.Event()  # менеджер закончил: воркеры выходят, когда очередь пуста
        self.manager = None
        self.workers = {}
        self.active = {}                   # path -> прогресс текущей загрузки
        self.alock = threading.Lock()
        self.slock = threading.Lock()
        self.state_lock = threading.Lock()
        self.claim_lock = threading.Lock()
        self.refill_lock = threading.Lock()
        self.window = deque()              # (monotonic, bytes) для скорости
        self.session = {"done": 0, "failed": 0, "bytes": 0, "started": None}
        self.fail_streak = 0
        self.message = ""
        self.opener = build_opener()

    # ---------- управление

    def load_settings(self):
        try:
            self.threads = max(1, min(MAX_THREADS, int(get_setting("threads", DEFAULT_THREADS))))
        except (TypeError, ValueError):
            self.threads = DEFAULT_THREADS

    def set_threads(self, n):
        self.threads = max(1, min(MAX_THREADS, int(n)))
        set_setting("threads", self.threads)
        return self.threads

    def _alive(self):
        return bool(self.manager and self.manager.is_alive()) or any(t.is_alive() for t in self.workers.values())

    def state(self):
        if not self._alive():
            return "idle"
        return "stopping" if self.stop_event.is_set() else "running"

    def start(self):
        with self.state_lock:
            if self._alive():
                return {"ok": False, "error": "уже запущено" if not self.stop_event.is_set() else "ещё останавливается, подожди секунду"}
            self.stop_event.clear()
            self.finishing.clear()
            self.message = ""
            self.fail_streak = 0
            self.workers = {}
            with self.alock:
                self.active = {}
            self.session = {"done": 0, "failed": 0, "bytes": 0, "started": time.time()}
            self.manager = threading.Thread(target=self._manager_loop, name="dl-manager", daemon=True)
            self.manager.start()
        return {"ok": True}

    def stop(self, message=""):
        if message:
            self.message = message
        self.stop_event.set()

    def retry_failed(self):
        with dbc() as c:
            cur = c.execute(
                "UPDATE items SET dl_status=NULL, dl_error=NULL, dl_attempts=NULL WHERE dl_status='failed'"
            )
            return cur.rowcount

    # ---------- менеджер

    def refill(self):
        """Держит в очереди ровно QUEUE_SIZE ожидающих файлов (самые верхние в галерее — первыми)."""
        with self.refill_lock:
            with dbc() as c:
                have = c.execute("SELECT COUNT(*) FROM items WHERE dl_status='queued'").fetchone()[0]
                need = QUEUE_SIZE - have
                if need <= 0:
                    return
                rows = c.execute(
                    f"SELECT path FROM items WHERE {ELIGIBLE} {GALLERY_ORDER} LIMIT ?", (need,)
                ).fetchall()
                if rows:
                    ts = now_iso()
                    c.executemany(
                        "UPDATE items SET dl_status='queued', dl_at=? WHERE path=?",
                        [(ts, r[0]) for r in rows],
                    )

    def _finished(self):
        with dbc() as c:
            r = c.execute(
                "SELECT COALESCE(SUM(dl_status='queued'),0), COALESCE(SUM(dl_status='downloading'),0), "
                f"COALESCE(SUM({ELIGIBLE}),0) FROM items"
            ).fetchone()
        return not any(r)

    def _ensure_workers(self):
        for idx in range(self.threads):
            t = self.workers.get(idx)
            if t is None or not t.is_alive():
                t = threading.Thread(target=self._worker_loop, args=(idx,), name=f"dl-{idx}", daemon=True)
                self.workers[idx] = t
                t.start()

    def _manager_loop(self):
        natural = False
        try:
            while not self.stop_event.is_set():
                self._ensure_workers()
                self.refill()
                if self._finished():
                    natural = True
                    break
                time.sleep(0.5)
        except Exception as e:  # noqa: BLE001
            self.message = f"ошибка менеджера: {e}"
            self.stop_event.set()
        finally:
            if natural and not self.message:
                self.message = "Готово: очередь пуста."
            self.finishing.set()
            for t in list(self.workers.values()):
                t.join(timeout=30)
            with dbc() as c:
                c.execute("UPDATE items SET dl_status=NULL WHERE dl_status IN ('queued','downloading')")

    # ---------- воркер

    def _claim(self):
        with self.claim_lock:
            with dbc() as c:
                row = c.execute(
                    f"SELECT path,name,taken,raw FROM items WHERE dl_status='queued' {GALLERY_ORDER} LIMIT 1"
                ).fetchone()
                if not row:
                    return None
                c.execute("UPDATE items SET dl_status='downloading', dl_at=? WHERE path=?", (now_iso(), row["path"]))
                return dict(row)

    def _worker_loop(self, idx):
        while not self.stop_event.is_set() and idx < self.threads:
            row = self._claim()
            if row is None:
                if self.finishing.is_set():
                    break
                time.sleep(0.3)
                continue
            try:
                self._process(row)
            except Exception as e:  # noqa: BLE001
                self._record_fail(row["path"], f"внутренняя ошибка: {e}", [])
            self.refill()

    def _set_item(self, path, status, size=None, file=None, nbytes=None, error=None, attempts=None):
        with dbc() as c:
            c.execute(
                "UPDATE items SET dl_status=?, dl_size=?, dl_file=?, dl_bytes=?, dl_error=?, dl_attempts=?, dl_at=? WHERE path=?",
                (status, size, file, nbytes, (error or None) and error[:500],
                 json.dumps(attempts, ensure_ascii=False) if attempts else None, now_iso(), path),
            )

    def _on_result(self, ok, nbytes=0):
        with self.slock:
            if ok:
                self.session["done"] += 1
                self.session["bytes"] += nbytes
                self.fail_streak = 0
            else:
                self.session["failed"] += 1
                self.fail_streak += 1
                if self.fail_streak >= FAIL_STREAK_STOP and not self.stop_event.is_set():
                    self.message = (f"{self.fail_streak} файлов подряд не скачались — остановился. "
                                    "Возможно, ссылки протухли (HTTP 403): прокрути ленту на Диске заново "
                                    "через mitmproxy, чтобы обновить ссылки, и нажми «Повторить ошибки». "
                                    "Либо облако сейчас недоступно.")
                    self.stop_event.set()

    def _record_fail(self, path, error, attempts):
        self._set_item(path, "failed", error=error, attempts=attempts)
        self._on_result(False)

    def _process(self, row):
        path, name = row["path"], row["name"]
        day = (row["taken"] or "")[:10] or "no-date"
        try:
            meta = (json.loads(row["raw"] or "{}").get("meta")) or {}
            sizes = {s["name"]: s["url"] for s in (meta.get("sizes") or []) if s.get("name") and s.get("url")}
        except Exception:  # noqa: BLE001
            sizes = {}
        order = [s for s in SIZE_ORDER if s in sizes]
        if not order:
            self._record_fail(path, "нет ссылок на скачивание", [])
            return

        prog = {"name": name, "size": order[0], "bytes": 0, "total": None,
                "attempt": 1, "of": len(order), "since": time.time()}
        with self.alock:
            self.active[path] = prog
        attempts = []
        try:
            for i, size in enumerate(order, 1):
                if self.stop_event.is_set():
                    raise Aborted()
                prog.update(size=size, bytes=0, total=None, attempt=i)
                url = sizes[size]
                if url.startswith("//"):
                    url = "https:" + url
                try:
                    rel, nbytes = self._fetch(path, name, day, size, url, prog)
                except Aborted:
                    raise
                except Exception as e:  # noqa: BLE001
                    attempts.append(f"{size}: {short_err(e)}")
                    continue
                self._set_item(path, "done", size=size, file=rel, nbytes=nbytes, attempts=attempts)
                self._on_result(True, nbytes)
                return
            self._record_fail(path, "; ".join(attempts), attempts)
        except Aborted:
            self._set_item(path, None)
        finally:
            with self.alock:
                self.active.pop(path, None)

    def _fetch(self, path, name, day, size, url, prog):
        day_dir = os.path.join(FILES_DIR, day)
        os.makedirs(day_dir, exist_ok=True)
        tag = hashlib.md5(path.encode("utf-8")).hexdigest()[:8]
        tmp = os.path.join(day_dir, f".{tag}.{size}.part")
        req = urllib.request.Request(url, headers={
            "User-Agent": USER_AGENT, "Accept": "*/*", "Referer": "https://disk.yandex.ru/",
        })
        started = time.monotonic()
        got = 0
        try:
            with self.opener.open(req, timeout=ATTEMPT_TIMEOUT) as resp:
                ctype = resp.headers.get_content_type()
                if ctype in ("text/html", "application/json", "text/plain", "application/xml"):
                    raise ValueError(f"в ответе {ctype} вместо файла")
                clen = resp.headers.get("Content-Length")
                total = int(clen) if clen and clen.isdigit() else None
                prog["total"] = total
                with open(tmp, "wb") as f:
                    while True:
                        if self.stop_event.is_set():
                            raise Aborted()
                        if time.monotonic() - started > ATTEMPT_DEADLINE:
                            raise DeadlineError()
                        chunk = resp.read(65536)
                        if not chunk:
                            break
                        f.write(chunk)
                        got += len(chunk)
                        prog["bytes"] = got
                        self.window.append((time.monotonic(), len(chunk)))
            if got == 0:
                raise IOError("пустой ответ")
            if total is not None and got != total:
                raise IOError(f"неполный файл {got}/{total}")

            stem, ext0 = os.path.splitext(safe_name(name))
            if size == "ORIGINAL":
                final = safe_name(name)
            else:
                final = f"{stem}.{size}{EXT_BY_CTYPE.get(ctype, '.jpg')}"
            rel = f"{day}/{final}"
            with dbc() as c:
                clash = c.execute("SELECT 1 FROM items WHERE dl_file=? AND path<>? LIMIT 1", (rel, path)).fetchone()
            if clash:
                s2, e2 = os.path.splitext(final)
                final = f"{s2}_{tag}{e2}"
                rel = f"{day}/{final}"
            os.replace(tmp, os.path.join(day_dir, final))
            return rel, got
        finally:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass

    # ---------- статус для страницы

    def snapshot(self):
        with dbc() as c:
            r = c.execute(
                f"""SELECT
                    COALESCE(SUM(mediatype='image'),0),
                    COALESCE(SUM(mediatype='image' AND raw IS NOT NULL),0),
                    COALESCE(SUM(mediatype='image' AND raw IS NULL),0),
                    COALESCE(SUM(dl_status='done'),0),
                    COALESCE(SUM(dl_status='failed'),0),
                    COALESCE(SUM(dl_status='queued'),0),
                    COALESCE(SUM(dl_status='downloading'),0),
                    COALESCE(SUM({ELIGIBLE}),0),
                    COALESCE(SUM(CASE WHEN dl_status='done' THEN dl_bytes END),0)
                FROM items"""
            ).fetchone()
            counts = dict(zip(
                ["images", "with_links", "no_links", "done", "failed", "queued", "downloading", "remaining", "done_bytes"],
                list(r),
            ))
            by_size = {s: n for s, n in c.execute(
                "SELECT dl_size, COUNT(*) FROM items WHERE dl_status='done' GROUP BY dl_size")}
            queue = [x[0] for x in c.execute(
                f"SELECT name FROM items WHERE dl_status='queued' {GALLERY_ORDER} LIMIT ?", (QUEUE_SIZE,))]
            recent_done = [
                {"name": x["name"], "size": x["dl_size"], "bytes": x["dl_bytes"], "file": x["dl_file"], "at": x["dl_at"]}
                for x in c.execute(
                    "SELECT name,dl_size,dl_bytes,dl_file,dl_at FROM items WHERE dl_status='done' "
                    "ORDER BY dl_at DESC, name DESC LIMIT 25")
            ]
            recent_failed = [
                {"name": x["name"], "error": x["dl_error"], "at": x["dl_at"]}
                for x in c.execute(
                    "SELECT name,dl_error,dl_at FROM items WHERE dl_status='failed' "
                    "ORDER BY dl_at DESC, name DESC LIMIT 25")
            ]

        now = time.monotonic()
        try:
            while self.window and now - self.window[0][0] > 10:
                self.window.popleft()
            items = list(self.window)
        except IndexError:
            items = []
        speed = 0
        if items:
            span = max(1.0, now - items[0][0])
            speed = sum(n for _, n in items) / span

        with self.alock:
            active = sorted(self.active.values(), key=lambda p: p["since"])
            active = [dict(p, elapsed=round(time.time() - p["since"], 1)) for p in active]

        return {
            "state": self.state(),
            "message": self.message,
            "threads": self.threads,
            "queue_size": QUEUE_SIZE,
            "files_dir": os.path.abspath(FILES_DIR),
            "speed": speed,
            "session": dict(self.session),
            "counts": counts,
            "by_size": by_size,
            "active": active,
            "queue": queue,
            "recent_done": recent_done,
            "recent_failed": recent_failed,
        }


DL = Downloader()


# ---------------------------------------------------------------- видео: recover.py

def read_state(path):
    try:
        with open(path, encoding="utf-8") as f:
            st = json.load(f)
        return st if isinstance(st, dict) and st.get("size") else None
    except (OSError, ValueError):
        return None


def ranges_sum(rs):
    return sum(max(0, int(e) - int(s)) for s, e in rs or [])


def _cover(ranges, starts, a, b):
    """Сколько байт отрезка [a,b) покрыто непересекающимися отсортированными диапазонами."""
    i = max(0, bisect.bisect_right(starts, a) - 1)
    tot = 0
    while i < len(ranges) and ranges[i][0] < b:
        tot += max(0, min(ranges[i][1], b) - max(ranges[i][0], a))
        i += 1
    return tot


def make_bar(size, ok, bad, n=120):
    """
    Полоска из n сегментов, по символу на сегмент:
      g — прочитан целиком, p — читается прямо сейчас (частично), u — ещё не проверялся,
      y — часть сегмента потеряна, r — сегмент потерян больше чем наполовину.
    """
    if not size:
        return "u" * n
    ok = sorted([int(s), int(e)] for s, e in ok or [])
    bad = sorted([int(s), int(e)] for s, e in bad or [])
    so, sb = [r[0] for r in ok], [r[0] for r in bad]
    out = []
    for i in range(n):
        a, b = size * i // n, size * (i + 1) // n
        if b <= a:
            out.append("u")
            continue
        length = b - a
        x = _cover(bad, sb, a, b)
        o = _cover(ok, so, a, b)
        if x > 0:
            out.append("r" if x * 2 >= length else "y")
        elif o >= length:
            out.append("g")
        elif o > 0:
            out.append("p")
        else:
            out.append("u")
    return "".join(out)


def move_over(src, dst):
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    if os.path.exists(dst):
        os.remove(dst)
    try:
        os.replace(src, dst)
    except OSError:
        shutil.move(src, dst)


class VideoDownloader:
    """
    Для каждого видео:
      1. свежая ссылка (mpfs/url с куками из auth.db) -> recover_runner.py (скачивание блоками, битые места -> нули);
      2. если есть потери и включено «уточнять»: снова свежая ссылка -> recover.py --refine (+ докачка найденных островов);
      3. нет потерь: сырой файл = оригинал, кладём как есть; есть потери: recover.py --skip-download
         (ffmpeg: переупаковка или перекодирование) -> <имя>.recovered.mp4.
    Ссылку recover.py получает через stdin (в списке процессов её не видно).
    Прогресс между запусками хранится в files/<дата>/.vd/<метка>/v.state.json — после остановки докачка продолжается.
    """

    def __init__(self):
        self.settings = dict(VD_DEFAULTS)
        self.stop_event = threading.Event()
        self.stop_event.set()
        self.finishing = threading.Event()
        self.manager = None
        self.workers = {}
        self.active = {}
        self.alock = threading.Lock()
        self.slock = threading.Lock()
        self.state_lock = threading.Lock()
        self.claim_lock = threading.Lock()
        self.refill_lock = threading.Lock()
        self.session = {"done": 0, "failed": 0, "bytes": 0, "started": None}
        self.fail_streak = 0
        self.message = ""
        self.opener = build_opener()

    # ---------- настройки

    @staticmethod
    def _clean(key, value):
        default = VD_DEFAULTS[key]
        if isinstance(default, bool):
            if isinstance(value, bool):
                return value
            return str(value).strip().lower() in ("1", "true", "yes", "on")
        lo, hi = VD_LIMITS[key]
        return max(lo, min(hi, int(float(value))))

    def load_settings(self):
        for key in VD_DEFAULTS:
            raw = get_setting("vd_" + key)
            if raw is None:
                continue
            try:
                self.settings[key] = self._clean(key, json.loads(raw))
            except (TypeError, ValueError):
                pass

    def update_settings(self, body):
        errors = []
        for key, value in (body or {}).items():
            if key not in VD_DEFAULTS:
                continue
            try:
                self.settings[key] = self._clean(key, value)
                set_setting("vd_" + key, json.dumps(self.settings[key]))
            except (TypeError, ValueError):
                errors.append(key)
        return dict(self.settings), errors

    # ---------- управление

    def _alive(self):
        return bool(self.manager and self.manager.is_alive()) or any(t.is_alive() for t in self.workers.values())

    def state(self):
        if not self._alive():
            return "idle"
        return "stopping" if self.stop_event.is_set() else "running"

    def start(self):
        with self.state_lock:
            if self._alive():
                return {"ok": False, "error": "уже запущено" if not self.stop_event.is_set() else "ещё останавливается, подожди"}
            a = auth_summary()
            if not a["cookies"] or not a["has_sk"]:
                return {"ok": False, "error": "нет данных авторизации: открой Диск в браузере через mitmproxy "
                                              "(аддон передаст куки и sk), потом повтори"}
            if not os.path.isfile(os.path.join(BASE_DIR, "recover.py")) or not os.path.isfile(RUNNER):
                return {"ok": False, "error": "рядом с server.py должны лежать recover.py и recover_runner.py"}
            self.stop_event.clear()
            self.finishing.clear()
            self.message = ""
            self.fail_streak = 0
            self.workers = {}
            with self.alock:
                self.active = {}
            self.session = {"done": 0, "failed": 0, "bytes": 0, "started": time.time()}
            self.manager = threading.Thread(target=self._manager_loop, name="vd-manager", daemon=True)
            self.manager.start()
        return {"ok": True}

    def stop(self, message=""):
        if message:
            self.message = message
        self.stop_event.set()

    def retry_failed(self):
        with dbc() as c:
            cur = c.execute("UPDATE items SET vd_status=NULL, vd_error=NULL WHERE vd_status='failed'")
            return cur.rowcount

    # ---------- менеджер

    def refill(self):
        with self.refill_lock:
            with dbc() as c:
                have = c.execute("SELECT COUNT(*) FROM items WHERE vd_status='queued'").fetchone()[0]
                need = VD_QUEUE_SIZE - have
                if need <= 0:
                    return
                rows = c.execute(
                    f"SELECT path FROM items WHERE {ELIGIBLE_VD} {GALLERY_ORDER} LIMIT ?", (need,)).fetchall()
                if rows:
                    ts = now_iso()
                    c.executemany("UPDATE items SET vd_status='queued', vd_at=? WHERE path=?",
                                  [(ts, r[0]) for r in rows])

    def _finished(self):
        with dbc() as c:
            r = c.execute(
                "SELECT COALESCE(SUM(vd_status='queued'),0), COALESCE(SUM(vd_status='downloading'),0), "
                f"COALESCE(SUM({ELIGIBLE_VD}),0) FROM items").fetchone()
        return not any(r)

    def _ensure_workers(self):
        for idx in range(self.settings["videos"]):
            t = self.workers.get(idx)
            if t is None or not t.is_alive():
                t = threading.Thread(target=self._worker_loop, args=(idx,), name=f"vd-{idx}", daemon=True)
                self.workers[idx] = t
                t.start()

    def _manager_loop(self):
        natural = False
        try:
            while not self.stop_event.is_set():
                self._ensure_workers()
                self.refill()
                if self._finished():
                    natural = True
                    break
                time.sleep(0.5)
        except Exception as e:  # noqa: BLE001
            self.message = f"ошибка менеджера: {e}"
            self.stop_event.set()
        finally:
            if natural and not self.message:
                self.message = "Готово: очередь видео пуста."
            self.finishing.set()
            for t in list(self.workers.values()):
                t.join(timeout=60)
            with dbc() as c:
                c.execute("UPDATE items SET vd_status=NULL WHERE vd_status IN ('queued','downloading')")

    # ---------- воркер

    def _claim(self):
        with self.claim_lock:
            with dbc() as c:
                row = c.execute(
                    f"SELECT path,name,taken,size FROM items WHERE vd_status='queued' {GALLERY_ORDER} LIMIT 1"
                ).fetchone()
                if not row:
                    return None
                c.execute("UPDATE items SET vd_status='downloading', vd_at=? WHERE path=?", (now_iso(), row["path"]))
                return dict(row)

    def _worker_loop(self, idx):
        while not self.stop_event.is_set() and idx < self.settings["videos"]:
            row = self._claim()
            if row is None:
                if self.finishing.is_set():
                    break
                time.sleep(0.3)
                continue
            try:
                self._process(row)
            except Exception as e:  # noqa: BLE001
                self._record_fail(row["path"], f"внутренняя ошибка: {type(e).__name__}: {e}")
            self.refill()

    def _set_video(self, path, status, phase=None, file=None, nbytes=None, lost=None, error=None, note=None):
        with dbc() as c:
            c.execute(
                "UPDATE items SET vd_status=?, vd_phase=?, vd_file=?, vd_bytes=?, vd_lost=?, vd_error=?, vd_note=?, vd_at=? "
                "WHERE path=?",
                (status, phase, file, nbytes, lost, (error or None) and error[:500], note, now_iso(), path))

    def _on_result(self, ok, nbytes=0):
        with self.slock:
            if ok:
                self.session["done"] += 1
                self.session["bytes"] += nbytes
                self.fail_streak = 0
            else:
                self.session["failed"] += 1
                self.fail_streak += 1
                if self.fail_streak >= VD_FAIL_STREAK_STOP and not self.stop_event.is_set():
                    self.message = (f"{self.fail_streak} видео подряд не скачались — остановился. "
                                    "Проверь ссылки/авторизацию (обнови страницу Диска через mitmproxy), "
                                    "потом «Повторить ошибки».")
                    self.stop_event.set()

    def _record_fail(self, path, error):
        self._set_video(path, "failed", error=error)
        self._on_result(False)

    # ---------- запуск recover.py

    @staticmethod
    def _terminate(proc):
        if proc.poll() is not None:
            return
        try:
            if os.name == "posix":
                proc.send_signal(signal.SIGINT)      # recover.py сохранит прогресс и выйдет с кодом 130
            else:
                proc.terminate()
        except Exception:  # noqa: BLE001
            pass
        try:
            proc.wait(timeout=20)
            return
        except subprocess.TimeoutExpired:
            pass
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:  # noqa: BLE001
            try:
                proc.kill()
            except Exception:  # noqa: BLE001
                pass

    def _run_recover(self, prog, args, url, idle_limit):
        """Запускает recover_runner.py; ссылку отдаёт через stdin. Возвращает (код, важные строки «! …»)."""
        cmd = [sys.executable, "-u", RUNNER] + args
        env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE if url else subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env, cwd=BASE_DIR)
        prog["proc"] = proc
        if url:
            try:
                proc.stdin.write((url + "\n").encode("utf-8"))
                proc.stdin.close()
            except OSError:
                pass
        important = deque(maxlen=6)
        last = [time.monotonic()]

        def reader():
            dec = codecs.getincrementaldecoder("utf-8")("replace")
            buf = ""
            fd = proc.stdout.fileno()
            while True:
                try:
                    data = os.read(fd, 4096)
                except OSError:
                    break
                if not data:
                    break
                last[0] = time.monotonic()
                buf += dec.decode(data)
                parts = re.split(r"[\r\n]", buf)
                buf = parts[-1]
                for p in parts[:-1]:
                    p = p.strip()
                    if not p:
                        continue
                    prog["line"] = p[:220]
                    if p.startswith("!") or p.startswith("ffmpeg") or "не вышло" in p:
                        important.append(p[:220])
                if buf.strip():
                    prog["line"] = buf.strip()[:220]

        rt = threading.Thread(target=reader, daemon=True)
        rt.start()
        try:
            while proc.poll() is None:
                if self.stop_event.is_set():
                    self._terminate(proc)
                    break
                if time.monotonic() - last[0] > idle_limit:
                    self._terminate(proc)
                    rt.join(5)
                    raise VideoError(f"recover.py молчит больше {int(idle_limit)} с — прервал")
                time.sleep(0.4)
            proc.wait()
            rt.join(5)
        finally:
            prog["proc"] = None
            if proc.poll() is None:
                self._terminate(proc)
        if self.stop_event.is_set():
            raise Aborted()
        return proc.returncode, list(important)

    # ---------- обработка одного видео

    def _unique_rel(self, day, final, path, tag):
        rel = f"{day}/{final}"
        with dbc() as c:
            clash = c.execute("SELECT 1 FROM items WHERE (dl_file=? OR vd_file=?) AND path<>? LIMIT 1",
                              (rel, rel, path)).fetchone()
        if clash:
            stem, ext = os.path.splitext(final)
            final = f"{stem}_{tag}{ext}"
            rel = f"{day}/{final}"
        return rel

    def _process(self, row):
        path, name = row["path"], row["name"]
        day = (row["taken"] or "")[:10] or "no-date"
        tag = hashlib.md5(path.encode("utf-8")).hexdigest()[:8]
        workdir = os.path.join(FILES_DIR, day, ".vd", tag)
        os.makedirs(workdir, exist_ok=True)
        out = os.path.join(workdir, "v.mp4")
        base = os.path.join(workdir, "v")
        state_path = base + ".state.json"
        cfg = dict(self.settings)
        prog = {"name": name, "phase": "link", "line": "", "since": time.time(), "state_path": state_path,
                "size": row.get("size") or 0, "attempt": 0, "proc": None, "speed": 0.0,
                "_pg": 0, "_pt": time.monotonic()}
        with self.alock:
            self.active[path] = prog
        try:
            st = read_state(state_path)
            complete = bool(st) and ranges_sum(st.get("ok")) + ranges_sum(st.get("bad")) >= st["size"]
            if not complete:
                self._download_phase(prog, path, out, state_path, cfg, row.get("size"), refine=False)
            st = read_state(state_path)
            if not st:
                raise VideoError("recover.py не создал файл состояния")
            marker = os.path.join(workdir, ".refined")
            if cfg["refine"] and ranges_sum(st.get("bad")) > 0 and not os.path.exists(marker):
                self._download_phase(prog, path, out, state_path, cfg, row.get("size"), refine=True)
                open(marker, "w").close()
            self._finalize(prog, row, day, tag, workdir, out, base, cfg)
        except Aborted:
            self._set_video(path, None)
        except AuthError as e:
            self._set_video(path, None)
            self.stop(f"Остановился: {e}")
        except VideoError as e:
            self._record_fail(path, str(e))
        finally:
            with self.alock:
                self.active.pop(path, None)

    def _download_phase(self, prog, path, out, state_path, cfg, size_hint, refine):
        prog["phase"] = "refine" if refine else "download"
        idle = max(900, 10 * max(cfg["timeout"] * cfg["retries"], cfg["probe_timeout"]))
        last_err = ""
        for attempt in range(1, VD_LINK_RETRIES + 1):
            if self.stop_event.is_set():
                raise Aborted()
            prog.update(attempt=attempt, line="запрашиваю ссылку на скачивание…")
            url = get_download_url(path)
            args = ["-o", out, "--no-ffmpeg",
                    "--block", str(cfg["block_kb"] * 1024), "--sub", str(cfg["sub_kb"] * 1024),
                    "--timeout", str(cfg["timeout"]), "--sub-timeout", str(cfg["sub_timeout"]),
                    "--retries", str(cfg["retries"]),
                    "--probe-size", str(cfg["probe_size"]), "--probe-timeout", str(cfg["probe_timeout"])]
            if size_hint:
                args += ["--size", str(int(size_hint))]
            if refine:
                args += ["--refine", "--jobs", str(cfg["refine_jobs"]), "--stride", str(cfg["stride_kb"] * 1024)]
            else:
                args += ["--jobs", str(cfg["jobs"])]
            prog["line"] = "запускаю recover.py…"
            rc, important = self._run_recover(prog, args, url, idle)
            if rc == 0:
                return
            if rc == 3:       # ссылка просрочена / 403 / нет Range: берём новую, прогресс сохранён
                last_err = "ссылка недоступна (HTTP 403/404/410 или нет Range)"
                if important:
                    last_err += ": " + " | ".join(important[-2:])
                time.sleep(1)
                continue
            raise VideoError(f"recover.py завершился с кодом {rc}" + (": " + " | ".join(important[-3:]) if important else ""))
        raise VideoError(f"{VD_LINK_RETRIES} раза подряд ссылка не сработала — {last_err}")

    def _finalize(self, prog, row, day, tag, workdir, out, base, cfg):
        path, name = row["path"], row["name"]
        st = read_state(base + ".state.json")
        if not st:
            raise VideoError("нет файла состояния recover.py")
        size = int(st["size"])
        lost = ranges_sum(st.get("bad"))
        covered = ranges_sum(st.get("ok")) + lost
        if covered < size:
            raise VideoError(f"обработано {covered} из {size} байт — запусти ещё раз")
        raw = base + ".raw.bin"

        if lost == 0:
            rel = self._unique_rel(day, safe_name(name), path, tag)
            move_over(raw, os.path.join(FILES_DIR, rel))
            shutil.rmtree(workdir, ignore_errors=True)
            self._set_video(path, "done", phase="done", file=rel, nbytes=size, lost=0, note="оригинал без потерь")
            self._on_result(True, size)
            return

        prog.update(phase="assemble", line="собираю файл через ffmpeg…")
        if os.path.exists(out):
            os.remove(out)
        args = ["--skip-download", "-o", out] + (["--reencode"] if cfg["reencode"] else [])
        rc, important = self._run_recover(prog, args, None, 3600)
        if os.path.isfile(out) and os.path.getsize(out) > 2048:
            stem = os.path.splitext(safe_name(name))[0]
            rel = self._unique_rel(day, f"{stem}.recovered.mp4", path, tag)
            move_over(out, os.path.join(FILES_DIR, rel))
            try:
                os.remove(base + ".patched.mp4")
            except OSError:
                pass
            note = (f"потеряно {human_size(lost)} из {human_size(size)} ({100.0 * lost / size:.2f}%); "
                    f"сырые данные: files/{day}/.vd/{tag}/")
            self._set_video(path, "done", phase="done", file=rel, nbytes=size, lost=lost, note=note)
            self._on_result(True, size)
            return
        raise VideoError("не удалось собрать файл (нет ffmpeg или он не справился)"
                         + (": " + " | ".join(important[-2:]) if important else "")
                         + f". Сырые данные сохранены: files/{day}/.vd/{tag}/")

    # ---------- статус для страницы

    def snapshot(self):
        with dbc() as c:
            r = c.execute(
                f"""SELECT
                    COALESCE(SUM(mediatype='video'),0),
                    COALESCE(SUM(mediatype='video' AND raw IS NOT NULL),0),
                    COALESCE(SUM(mediatype='video' AND raw IS NULL),0),
                    COALESCE(SUM(vd_status='done'),0),
                    COALESCE(SUM(vd_status='done' AND vd_lost>0),0),
                    COALESCE(SUM(vd_status='failed'),0),
                    COALESCE(SUM(vd_status='queued'),0),
                    COALESCE(SUM(vd_status='downloading'),0),
                    COALESCE(SUM({ELIGIBLE_VD}),0),
                    COALESCE(SUM(CASE WHEN vd_status='done' THEN vd_bytes END),0),
                    COALESCE(SUM(CASE WHEN vd_status='done' THEN vd_lost END),0)
                FROM items"""
            ).fetchone()
            counts = dict(zip(
                ["videos", "with_links", "no_links", "done", "partial", "failed", "queued", "downloading",
                 "remaining", "done_bytes", "lost_bytes"], list(r)))
            queue = [x[0] for x in c.execute(
                f"SELECT name FROM items WHERE vd_status='queued' {GALLERY_ORDER} LIMIT ?", (VD_QUEUE_SIZE,))]
            recent_done = [
                {"name": x["name"], "bytes": x["vd_bytes"], "lost": x["vd_lost"], "note": x["vd_note"],
                 "file": x["vd_file"], "at": x["vd_at"]}
                for x in c.execute("SELECT name,vd_bytes,vd_lost,vd_note,vd_file,vd_at FROM items "
                                   "WHERE vd_status='done' ORDER BY vd_at DESC, name DESC LIMIT 25")]
            recent_failed = [
                {"name": x["name"], "error": x["vd_error"], "at": x["vd_at"]}
                for x in c.execute("SELECT name,vd_error,vd_at FROM items WHERE vd_status='failed' "
                                   "ORDER BY vd_at DESC, name DESC LIMIT 25")]

        active = []
        now = time.monotonic()
        with self.alock:
            progs = sorted(self.active.values(), key=lambda p: p["since"])
            for p in progs:
                st = read_state(p["state_path"])
                size = int(st["size"]) if st else int(p.get("size") or 0)
                ok = st.get("ok", []) if st else []
                bad = st.get("bad", []) if st else []
                got, lost = ranges_sum(ok), ranges_sum(bad)
                dt = now - p["_pt"]
                if dt >= 1.0:
                    inst = max(0.0, got - p["_pg"]) / dt
                    p["speed"] = inst if not p["speed"] else 0.5 * p["speed"] + 0.5 * inst
                    p["_pg"], p["_pt"] = got, now
                active.append({
                    "name": p["name"], "phase": p["phase"], "line": p.get("line", ""), "attempt": p["attempt"],
                    "elapsed": round(time.time() - p["since"], 1), "size": size, "got": got, "lost": lost,
                    "speed": p["speed"], "bar": make_bar(size, ok, bad, 120),
                })

        return {
            "state": self.state(),
            "message": self.message,
            "settings": dict(self.settings),
            "limits": VD_LIMITS,
            "queue_size": VD_QUEUE_SIZE,
            "files_dir": os.path.abspath(FILES_DIR),
            "have_ffmpeg": bool(shutil.which("ffmpeg")),
            "auth": auth_summary(),
            "session": dict(self.session),
            "counts": counts,
            "active": active,
            "queue": queue,
            "recent_done": recent_done,
            "recent_failed": recent_failed,
        }


VD = VideoDownloader()


def cleanup_part_files():
    for root, _dirs, files in os.walk(FILES_DIR):
        for f in files:
            if f.endswith(".part"):
                try:
                    os.remove(os.path.join(root, f))
                except OSError:
                    pass


@app.before_request
def _require_json_for_post():
    # POST только с Content-Type: application/json — чужая веб-страница не сможет дёрнуть
    # локальный сервер «простым» запросом (браузер потребует CORS-preflight, которого у нас нет).
    if request.method == "POST" and not request.is_json:
        return jsonify(error="expected application/json"), 415


@app.get("/status")
def status_page():
    return Response(STATUS_HTML, mimetype="text/html")


@app.get("/api/dl/status")
def api_dl_status():
    return jsonify(DL.snapshot())


@app.post("/api/dl/start")
def api_dl_start():
    res = DL.start()
    return jsonify(res), (200 if res.get("ok") else 409)


@app.post("/api/dl/stop")
def api_dl_stop():
    DL.stop()
    return jsonify(ok=True, state=DL.state())


@app.post("/api/dl/threads")
def api_dl_threads():
    body = request.get_json(silent=True) or {}
    try:
        n = DL.set_threads(int(body.get("threads")))
    except (TypeError, ValueError):
        return jsonify(ok=False, error="threads должен быть числом"), 400
    return jsonify(ok=True, threads=n)


@app.post("/api/dl/retry-failed")
def api_dl_retry_failed():
    return jsonify(ok=True, reset=DL.retry_failed())


@app.get("/api/vd/status")
def api_vd_status():
    return jsonify(VD.snapshot())


@app.post("/api/vd/start")
def api_vd_start():
    res = VD.start()
    return jsonify(res), (200 if res.get("ok") else 409)


@app.post("/api/vd/stop")
def api_vd_stop():
    VD.stop()
    return jsonify(ok=True, state=VD.state())


@app.post("/api/vd/settings")
def api_vd_settings():
    settings, errors = VD.update_settings(request.get_json(silent=True) or {})
    return jsonify(ok=not errors, settings=settings, invalid=errors)


@app.post("/api/vd/retry-failed")
def api_vd_retry_failed():
    return jsonify(ok=True, reset=VD.retry_failed())


STATUS_HTML = r"""<!doctype html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Статус загрузки</title>
<style>
  :root { color-scheme: dark light; --bg:#202024; --fg:#ececf1; --mut:#8e8e99; --line:#33333a; --cell:#2c2c33; --ok:#2e7d32; --low:#b26a00; --bad:#c62828; }
  @media (prefers-color-scheme: light) { :root { --bg:#fff; --fg:#1b1b1f; --mut:#6b6b76; --line:#e3e3ea; --cell:#f1f1f5; } }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--fg); font:13px/1.4 system-ui, sans-serif; }
  main { padding:14px 24px 60px; max-width:1300px; }
  header { display:flex; flex-wrap:wrap; gap:10px 16px; align-items:center; margin-bottom:8px; }
  h1 { font-size:17px; margin:0; font-weight:600; }
  nav { display:flex; gap:14px; }
  nav a { color:var(--mut); text-decoration:none; }
  nav a.on { color:var(--fg); font-weight:600; }
  .tabs { display:flex; gap:6px; margin:6px 0 4px; }
  .tab.on { background:var(--cell); font-weight:600; }
  h3 { font-size:13px; margin:22px 0 8px; font-weight:600; }
  h3 small { color:var(--mut); font-weight:400; margin-left:6px; }
  .controls { display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin:10px 0; }
  button, input { font:inherit; padding:6px 12px; border:1px solid var(--line); border-radius:8px; background:transparent; color:var(--fg); }
  button { cursor:pointer; }
  button.primary { background:var(--ok); border-color:var(--ok); color:#fff; }
  button:disabled { opacity:.4; cursor:default; }
  input[type=number] { width:90px; }
  input[type=checkbox] { padding:0; width:auto; }
  .pill { padding:2px 10px; border-radius:10px; font-size:12px; background:var(--cell); }
  .pill.running { background:var(--ok); color:#fff; }
  .pill.stopping { background:var(--low); color:#fff; }
  .msg { background:var(--low); color:#fff; padding:8px 12px; border-radius:8px; margin:8px 0; }
  .cards { display:grid; grid-template-columns:repeat(auto-fit, minmax(130px, 1fr)); gap:8px; margin:12px 0; }
  .card { background:var(--cell); border-radius:8px; padding:10px 12px; }
  .card b { display:block; font-size:19px; }
  .card span { color:var(--mut); font-size:12px; }
  .bar { height:8px; background:var(--line); border-radius:4px; overflow:hidden; }
  .bar i { display:block; height:100%; background:var(--ok); width:0; transition:width .4s; }
  .chip { display:inline-block; padding:1px 8px; border-radius:10px; font-size:11px; color:#fff; background:var(--low); margin-right:6px; }
  .chip.ORIGINAL { background:var(--ok); }
  .chip.phase { background:#3a6ea5; }
  .cols { display:grid; grid-template-columns:repeat(auto-fit, minmax(420px, 1fr)); gap:0 28px; }
  table { width:100%; border-collapse:collapse; }
  td { padding:4px 10px 4px 0; border-bottom:1px solid var(--line); vertical-align:middle; }
  .mono { font-family:ui-monospace, Menlo, monospace; word-break:break-all; }
  .mut { color:var(--mut); }
  .box { max-height:280px; overflow:auto; border:1px solid var(--line); border-radius:8px; padding:6px 10px; }
  .box div { padding:1px 0; }
  .err { color:var(--bad); font-size:12px; }
  .warn { color:var(--low); }
  details.settings { margin:10px 0; border:1px solid var(--line); border-radius:8px; padding:8px 12px; }
  details.settings summary { cursor:pointer; font-weight:600; }
  .sgrid { display:grid; grid-template-columns:repeat(auto-fill, minmax(280px, 1fr)); gap:8px 20px; margin:10px 0; }
  .sgrid label { display:flex; flex-direction:column; gap:2px; font-size:12px; }
  .sgrid label small { color:var(--mut); }
  .sgrid label.chk { flex-direction:row; align-items:center; gap:8px; }
  .vcard { background:var(--cell); border-radius:8px; padding:10px 12px; margin:8px 0; }
  .vcard .top { display:flex; flex-wrap:wrap; gap:6px 12px; align-items:center; }
  .vcard canvas { display:block; width:100%; height:16px; margin:8px 0 4px; border-radius:3px; background:#444; }
  .legend { font-size:11px; color:var(--mut); display:flex; flex-wrap:wrap; gap:4px 14px; margin-top:4px; }
  .legend i { display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:4px; vertical-align:-1px; }
</style>
</head>
<body>
<main>
  <header>
    <h1>Статус загрузки</h1>
    <nav><a href="/">Галерея</a><a href="/status" class="on">Статус загрузки</a></nav>
  </header>
  <div class="tabs"><button id="tabPhoto" class="tab on">Фото</button><button id="tabVideo" class="tab">Видео</button></div>

<!-- ============================== ФОТО ============================== -->
<div id="photo">
  <div class="controls">
    <span id="state" class="pill">…</span>
    <button id="btnStart" class="primary">Скачать только фото</button>
    <button id="btnStop">Остановить</button>
    <span class="mut">потоков:</span>
    <input id="threads" type="number" min="1" max="32" value="8">
    <button id="btnThreads">Применить</button>
    <button id="btnRetry">Повторить ошибки</button>
  </div>
  <div class="mut" id="dir"></div>
  <div class="msg" id="msg" hidden></div>

  <div class="cards" id="cards"></div>
  <div class="bar"><i id="bar"></i></div>
  <div class="mut" id="barText" style="margin-top:4px"></div>
  <div id="bysize" style="margin-top:10px"></div>

  <h3>Качается сейчас <small id="activeCount"></small></h3>
  <table id="active"></table>

  <div class="cols">
    <div>
      <h3>В ожидании <small id="queueCount"></small></h3>
      <div class="box mono" id="queue"></div>
    </div>
    <div>
      <h3>Недавно скачано</h3>
      <table id="done"></table>
    </div>
  </div>

  <h3>Ошибки <small id="failCount"></small></h3>
  <table id="failed"></table>
</div>

<!-- ============================== ВИДЕО ============================== -->
<div id="video" hidden>
  <div class="controls">
    <span id="vstate" class="pill">…</span>
    <button id="vStart" class="primary">Скачать только видео</button>
    <button id="vStop">Остановить</button>
    <button id="vRetry">Повторить ошибки</button>
  </div>
  <div class="mut" id="vAuth"></div>
  <div class="mut" id="vDir"></div>
  <div class="msg" id="vMsg" hidden></div>

  <details class="settings" open>
    <summary>Настройки загрузки видео</summary>
    <div class="sgrid" id="vSettings"></div>
    <div class="controls">
      <button id="vApply">Применить</button>
      <span class="mut" id="vApplyNote">Размеры, таймауты и потоки действуют на видео, которые начнут качаться после «Применить». «Видео одновременно» — сразу.</span>
    </div>
  </details>

  <div class="cards" id="vCards"></div>
  <div class="bar"><i id="vBar"></i></div>
  <div class="mut" id="vBarText" style="margin-top:4px"></div>

  <h3>Качается сейчас <small id="vActiveCount"></small></h3>
  <div id="vActive"></div>
  <div class="legend">
    <span><i style="background:#2e7d32"></i>прочитано</span>
    <span><i style="background:#8bc34a"></i>читается сейчас</span>
    <span><i style="background:#666"></i>ещё не проверено</span>
    <span><i style="background:#ef6c00"></i>частично потеряно</span>
    <span><i style="background:#c62828"></i>потеряно (предположительно)</span>
  </div>

  <div class="cols">
    <div>
      <h3>В ожидании <small id="vQueueCount"></small></h3>
      <div class="box mono" id="vQueue"></div>
    </div>
    <div>
      <h3>Недавно скачано</h3>
      <table id="vDone"></table>
    </div>
  </div>

  <h3>Ошибки <small id="vFailCount"></small></h3>
  <table id="vFailed"></table>
</div>
</main>

<script>
  const SIZES = ["ORIGINAL","XXXL","XXL","XL","L","M","S","XS","XXS","XXXS","DEFAULT"];
  const $ = (id) => document.getElementById(id);

  function fmtBytes(n) {
    n = Number(n) || 0;
    const u = ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ']; let i = 0;
    while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
    return (i ? n.toFixed(1) : Math.round(n)) + ' ' + u[i];
  }
  function fmtTime(iso) { try { return new Date(iso).toLocaleTimeString(); } catch (e) { return ''; } }
  function el(tag, cls, text) {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text !== undefined && text !== null) e.textContent = text;
    return e;
  }
  function row(...cells) {
    const tr = document.createElement('tr');
    for (const c of cells) { const td = document.createElement('td'); if (c instanceof Node) td.appendChild(c); else td.textContent = c; tr.appendChild(td); }
    return tr;
  }
  async function api(path, body) {
    const opt = body === undefined ? {} : { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) };
    const r = await fetch(path, opt);
    return r.json();
  }

  // ---------------- вкладки
  let tab = location.hash === '#video' ? 'video' : 'photo';
  function showTab(t) {
    tab = t;
    $('photo').hidden = t !== 'photo'; $('video').hidden = t !== 'video';
    $('tabPhoto').classList.toggle('on', t === 'photo'); $('tabVideo').classList.toggle('on', t === 'video');
    history.replaceState(null, '', t === 'video' ? '#video' : '#photo');
    refresh();
  }
  $('tabPhoto').onclick = () => showTab('photo');
  $('tabVideo').onclick = () => showTab('video');

  // ---------------- фото
  function render(s) {
    const c = s.counts;
    $('state').textContent = { idle: 'остановлено', running: 'идёт загрузка', stopping: 'останавливаюсь…' }[s.state] || s.state;
    $('state').className = 'pill ' + s.state;
    $('btnStart').disabled = s.state !== 'idle';
    $('btnStop').disabled = s.state !== 'running';
    $('btnRetry').disabled = c.failed === 0 || s.state !== 'idle';
    $('btnRetry').textContent = 'Повторить ошибки' + (c.failed ? ` (${c.failed})` : '');
    if (document.activeElement !== $('threads')) $('threads').value = s.threads;
    $('dir').textContent = 'Папка: ' + s.files_dir + '/<дата>/';
    $('msg').hidden = !s.message; $('msg').textContent = s.message || '';

    const cards = [
      [fmtBytes(s.speed) + '/с', 'скорость'],
      [c.done, 'скачано (' + fmtBytes(c.done_bytes) + ')'],
      [c.remaining, 'ещё не в очереди'],
      [c.queued + ' / ' + s.queue_size, 'в ожидании'],
      [c.downloading + ' / ' + s.threads, 'качается'],
      [c.failed, 'ошибок'],
      [c.no_links, 'фото без ссылок'],
    ];
    $('cards').replaceChildren(...cards.map(([v, l]) => { const d = el('div', 'card'); d.append(el('b', null, v), el('span', null, l)); return d; }));

    const total = c.with_links;
    $('bar').style.width = total ? (100 * c.done / total).toFixed(1) + '%' : '0';
    $('barText').textContent = total ? `${c.done} из ${total} фото со ссылками (ошибок: ${c.failed})` : 'Пока нет фото со ссылками на скачивание — прокрути ленту на Диске через mitmproxy.';

    const chips = SIZES.filter(k => s.by_size[k]).map(k => el('span', 'chip ' + k, `${k} ${s.by_size[k]}`));
    $('bysize').replaceChildren(...chips);

    $('activeCount').textContent = s.active.length ? `(${s.active.length})` : '';
    $('active').replaceChildren(...s.active.map(p => {
      const pct = p.total ? Math.min(100, 100 * p.bytes / p.total) : 0;
      const bar = el('div', 'bar'); const i = el('i'); i.style.width = pct + '%'; bar.appendChild(i);
      return row(el('span', 'mono', p.name), el('span', 'chip ' + p.size, p.size), `${p.attempt}/${p.of}`,
                 bar, fmtBytes(p.bytes) + (p.total ? ' / ' + fmtBytes(p.total) : ''), p.elapsed + ' с');
    }));

    $('queueCount').textContent = `(${s.queue.length} из ${s.queue_size})`;
    $('queue').replaceChildren(...s.queue.map(n => el('div', null, n)));

    $('done').replaceChildren(...s.recent_done.map(d => row(el('span', 'mono', d.name), el('span', 'chip ' + d.size, d.size), fmtBytes(d.bytes), fmtTime(d.at))));
    $('failCount').textContent = c.failed ? `(${c.failed})` : '';
    $('failed').replaceChildren(...s.recent_failed.map(f => row(el('span', 'mono', f.name), el('span', 'err', f.error || ''), fmtTime(f.at))));
  }

  // ---------------- видео
  const VFIELDS = [
    ["videos", "Видео одновременно", "сколько видео качается параллельно"],
    ["jobs", "Потоки проверки блоков (на каждое видео)", "recover.py --jobs"],
    ["block_kb", "Размер блока, КБ", "--block"],
    ["sub_kb", "Мелкий кусок, КБ", "--sub: на что режем нечитаемый блок"],
    ["timeout", "Таймаут блока, с", "--timeout"],
    ["sub_timeout", "Таймаут куска, с", "--sub-timeout"],
    ["retries", "Попыток на блок", "--retries"],
    ["probe_size", "Размер пробы, байт", "--probe-size (refine)"],
    ["probe_timeout", "Таймаут пробы, с", "--probe-timeout (refine)"],
    ["refine_jobs", "Потоки refine", "--jobs на этапе уточнения"],
    ["stride_kb", "Шаг сетки refine, КБ", "--stride"],
  ];
  const VBOOLS = [
    ["refine", "После скачивания уточнять битые зоны (--refine)"],
    ["reencode", "Повреждённые видео перекодировать в H.264 (иначе: сначала переупаковка)"],
  ];
  const BAR_COLORS = { g: '#2e7d32', p: '#8bc34a', u: '#666', y: '#ef6c00', r: '#c62828' };
  const PHASES = { link: 'ссылка', download: 'скачивание', refine: 'уточнение битых зон', assemble: 'сборка ffmpeg' };
  let vDirty = false, vBuilt = false;

  function buildVSettings(limits) {
    const box = $('vSettings'); box.replaceChildren();
    for (const [k, label, hint] of VFIELDS) {
      const l = el('label'); const inp = el('input'); inp.type = 'number'; inp.id = 'vs_' + k;
      if (limits[k]) { inp.min = limits[k][0]; inp.max = limits[k][1]; }
      inp.addEventListener('input', () => { vDirty = true; });
      l.append(el('span', null, label), inp, el('small', null, hint));
      box.appendChild(l);
    }
    for (const [k, label] of VBOOLS) {
      const l = el('label', 'chk'); const inp = el('input'); inp.type = 'checkbox'; inp.id = 'vs_' + k;
      inp.addEventListener('change', () => { vDirty = true; });
      l.append(inp, el('span', null, label));
      box.appendChild(l);
    }
    vBuilt = true;
  }
  function fillVSettings(s) {
    if (vDirty) return;
    for (const [k] of VFIELDS) $('vs_' + k).value = s[k];
    for (const [k] of VBOOLS) $('vs_' + k).checked = !!s[k];
  }
  function drawBar(canvas, bar) {
    const W = canvas.width = canvas.clientWidth || 600, H = canvas.height = 16;
    const g = canvas.getContext('2d'), w = W / bar.length;
    for (let i = 0; i < bar.length; i++) {
      g.fillStyle = BAR_COLORS[bar[i]] || '#666';
      g.fillRect(Math.floor(i * w), 0, Math.ceil(w), H);
    }
  }
  function pct(a, b) { return b ? (100 * a / b).toFixed(b && a / b < 0.1 ? 2 : 1) : '0'; }

  function renderV(s) {
    const c = s.counts;
    if (!vBuilt) buildVSettings(s.limits);
    fillVSettings(s.settings);
    $('vstate').textContent = { idle: 'остановлено', running: 'идёт загрузка', stopping: 'останавливаюсь…' }[s.state] || s.state;
    $('vstate').className = 'pill ' + s.state;
    $('vStart').disabled = s.state !== 'idle';
    $('vStop').disabled = s.state !== 'running';
    $('vRetry').disabled = c.failed === 0 || s.state !== 'idle';
    $('vRetry').textContent = 'Повторить ошибки' + (c.failed ? ` (${c.failed})` : '');
    $('vDir').textContent = 'Папка: ' + s.files_dir + '/<дата>/  ·  ffmpeg: ' + (s.have_ffmpeg ? 'найден' : 'НЕ НАЙДЕН (повреждённые видео не соберутся)');
    $('vDir').className = s.have_ffmpeg ? 'mut' : 'warn';
    const a = s.auth;
    if (a.cookies && a.has_sk) {
      $('vAuth').textContent = `Авторизация: кук ${a.cookies}, sk есть` + (a.has_session ? '' : ', но нет Session_id') +
        (a.age_sec !== null ? `, обновлена ${a.age_sec < 90 ? a.age_sec + ' с' : Math.round(a.age_sec / 60) + ' мин'} назад` : '');
      $('vAuth').className = 'mut';
    } else {
      $('vAuth').textContent = 'Авторизации нет: открой Яндекс Диск в браузере через mitmproxy — аддон передаст куки и sk.';
      $('vAuth').className = 'warn';
    }
    $('vMsg').hidden = !s.message; $('vMsg').textContent = s.message || '';

    let speed = 0; for (const p of s.active) speed += p.speed || 0;
    const cards = [
      [fmtBytes(speed) + '/с', 'скорость'],
      [c.done, 'скачано (' + fmtBytes(c.done_bytes) + ')'],
      [c.partial, 'из них с потерями (' + fmtBytes(c.lost_bytes) + ')'],
      [c.remaining, 'ещё не в очереди'],
      [c.queued + ' / ' + s.queue_size, 'в ожидании'],
      [c.downloading + ' / ' + s.settings.videos, 'качается'],
      [c.failed, 'ошибок'],
      [c.no_links, 'видео без данных ресурса'],
    ];
    $('vCards').replaceChildren(...cards.map(([v, l]) => { const d = el('div', 'card'); d.append(el('b', null, v), el('span', null, l)); return d; }));
    const total = c.with_links;
    $('vBar').style.width = total ? (100 * c.done / total).toFixed(1) + '%' : '0';
    $('vBarText').textContent = total ? `${c.done} из ${total} видео (ошибок: ${c.failed})` : 'Пока нет видео с данными ресурса — прокрути ленту на Диске через mitmproxy.';

    $('vActiveCount').textContent = s.active.length ? `(${s.active.length})` : '';
    const cards2 = [], bars = [];
    for (const p of s.active) {
      const d = el('div', 'vcard'); const top = el('div', 'top');
      top.append(el('span', 'mono', p.name), el('span', 'chip phase', PHASES[p.phase] || p.phase),
                 el('span', 'mut', p.attempt > 1 ? `попытка ссылки ${p.attempt}` : ''), el('span', 'mut', p.elapsed + ' с'));
      const cv = document.createElement('canvas'); bars.push([cv, p.bar]);
      const info = el('div', 'mut', `${fmtBytes(p.got)} из ${fmtBytes(p.size)} (${pct(p.got, p.size)}%)  ·  предположительно потеряно ${fmtBytes(p.lost)} (${pct(p.lost, p.size)}%)  ·  ${fmtBytes(p.speed)}/с`);
      d.append(top, cv, info, el('div', 'mut mono', p.line || ''));
      cards2.push(d);
    }
    $('vActive').replaceChildren(...cards2);
    for (const [cv, b] of bars) drawBar(cv, b);

    $('vQueueCount').textContent = `(${s.queue.length} из ${s.queue_size})`;
    $('vQueue').replaceChildren(...s.queue.map(n => el('div', null, n)));
    $('vDone').replaceChildren(...s.recent_done.map(d => row(el('span', 'mono', d.name), fmtBytes(d.bytes),
      d.lost ? el('span', 'warn', 'потеряно ' + fmtBytes(d.lost)) : el('span', 'mut', 'без потерь'), fmtTime(d.at))));
    $('vFailCount').textContent = c.failed ? `(${c.failed})` : '';
    $('vFailed').replaceChildren(...s.recent_failed.map(f => row(el('span', 'mono', f.name), el('span', 'err', f.error || ''), fmtTime(f.at))));
  }

  let busy = false;
  async function refresh() {
    if (busy) return; busy = true;
    try {
      if (tab === 'photo') render(await api('/api/dl/status'));
      else renderV(await api('/api/vd/status'));
    } catch (e) { /* сервер перезапускается */ }
    busy = false;
  }
  $('btnStart').onclick = async () => { const r = await api('/api/dl/start', {}); if (r.error) alert(r.error); refresh(); };
  $('btnStop').onclick = async () => { await api('/api/dl/stop', {}); refresh(); };
  $('btnThreads').onclick = async () => { await api('/api/dl/threads', { threads: parseInt($('threads').value, 10) }); $('threads').blur(); refresh(); };
  $('btnRetry').onclick = async () => { await api('/api/dl/retry-failed', {}); refresh(); };

  $('vStart').onclick = async () => { if (vDirty) await applyV(); const r = await api('/api/vd/start', {}); if (r.error) alert(r.error); refresh(); };
  $('vStop').onclick = async () => { await api('/api/vd/stop', {}); refresh(); };
  $('vRetry').onclick = async () => { await api('/api/vd/retry-failed', {}); refresh(); };
  async function applyV() {
    const body = {};
    for (const [k] of VFIELDS) body[k] = parseInt($('vs_' + k).value, 10);
    for (const [k] of VBOOLS) body[k] = $('vs_' + k).checked;
    const r = await api('/api/vd/settings', body);
    vDirty = false;
    $('vApplyNote').textContent = r.invalid && r.invalid.length ? 'Не принято: ' + r.invalid.join(', ') : 'Применено.';
    refresh();
  }
  $('vApply').onclick = applyV;

  showTab(tab); setInterval(refresh, 1000);
</script>
</body>
</html>
"""


# ---------------------------------------------------------------- превью

try:
    from PIL import Image, ImageOps
    try:
        from pillow_heif import register_heif_opener
        register_heif_opener()
        HAVE_HEIF = True
    except ImportError:
        HAVE_HEIF = False
    HAVE_PIL = True
except ImportError:  # без Pillow галерея работает, но без превью
    HAVE_PIL = False
    HAVE_HEIF = False

VIEW_MAX_SIDE = 4096          # длинная сторона JPEG, в который конвертируем HEIC для просмотра
_thumb_sem = threading.Semaphore(4)   # не больше 4 одновременных сжатий


def _encode_thumb(src, dst):
    """Сжимает картинку в JPEG ≤ THUMB_MAX_BYTES: сначала роняем качество, потом уменьшаем сторону."""
    with Image.open(src) as im:
        try:
            im.draft("RGB", (THUMB_SIDE * 2, THUMB_SIDE * 2))   # быстрый декод больших JPEG
        except Exception:  # noqa: BLE001
            pass
        im = ImageOps.exif_transpose(im)
        if im.mode != "RGB":
            im = im.convert("RGB")
        side = THUMB_SIDE
        data = b""
        while True:
            t = im.copy()
            t.thumbnail((side, side), Image.LANCZOS)
            for q in (78, 68, 58, 48, 40, 32):
                buf = io.BytesIO()
                t.save(buf, "JPEG", quality=q, optimize=True, progressive=True)
                data = buf.getvalue()
                if len(data) <= THUMB_MAX_BYTES:
                    break
            if len(data) <= THUMB_MAX_BYTES or side <= 96:
                break
            side = int(side * 0.8)
    tmp = dst + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, dst)


def _video_poster(src, dst_jpg):
    """Один кадр из видео через ffmpeg (для плитки в галерее)."""
    ff = shutil.which("ffmpeg")
    if not ff:
        raise RuntimeError("ffmpeg не найден")
    for ss in ("1", "0"):
        r = subprocess.run([ff, "-v", "error", "-y", "-ss", ss, "-i", src, "-frames:v", "1", "-q:v", "3", dst_jpg],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=60)
        if r.returncode == 0 and os.path.isfile(dst_jpg) and os.path.getsize(dst_jpg) > 0:
            return
    raise RuntimeError("не удалось достать кадр")


def _done_file(rid):
    """Абсолютный путь скачанного файла по номеру строки (фото или видео), либо None."""
    with dbc() as c:
        row = c.execute("SELECT dl_status, dl_file, vd_status, vd_file FROM items WHERE rowid=?", (rid,)).fetchone()
    if not row:
        return None
    rel = row["vd_file"] if row["vd_status"] == "done" and row["vd_file"] else (
        row["dl_file"] if row["dl_status"] == "done" else None)
    if not rel:
        return None
    src = os.path.realpath(os.path.join(FILES_DIR, rel))
    root = os.path.realpath(FILES_DIR)
    if not src.startswith(root + os.sep) or not os.path.isfile(src):
        return None
    return src


@app.get("/thumb/<int:rid>")
def thumb(rid):
    if not HAVE_PIL:
        return Response("Pillow не установлен: pip install pillow", status=501)
    src = _done_file(rid)
    if not src:
        return Response(status=404)

    key = hashlib.md5(f"{src}|{os.path.getmtime(src)}|{THUMB_SIDE}".encode("utf-8")).hexdigest()
    dst = os.path.join(THUMBS_DIR, key[:2], key + ".jpg")
    if not os.path.isfile(dst):
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with _thumb_sem:
            if not os.path.isfile(dst):      # вдруг другой поток уже сделал
                poster = None
                try:
                    if os.path.splitext(src)[1].lstrip(".").lower() in VIDEO_EXT:
                        poster = dst + ".poster.jpg"
                        _video_poster(src, poster)
                        _encode_thumb(poster, dst)
                    else:
                        _encode_thumb(src, dst)
                except Exception as e:  # noqa: BLE001
                    print(f"[thumb] {src}: {type(e).__name__}: {e}")
                    return Response(status=415)
                finally:
                    if poster and os.path.exists(poster):
                        try:
                            os.remove(poster)
                        except OSError:
                            pass
    resp = send_file(dst, mimetype="image/jpeg", max_age=31536000, conditional=True)
    resp.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    return resp


@app.get("/file/<int:rid>")
def open_file(rid):
    """Открывает скачанный файл (оригинал, либо лучшее из скачанного). HEIC/HEIF браузеры не показывают —
    их отдаём как большой JPEG (если установлен pillow-heif). Видео отдаётся с поддержкой Range (перемотка)."""
    src = _done_file(rid)
    if not src:
        return Response("файл не найден", status=404)
    if src.lower().endswith((".heic", ".heif")) and HAVE_PIL and HAVE_HEIF:
        key = hashlib.md5(f"view|{src}|{os.path.getmtime(src)}|{VIEW_MAX_SIDE}".encode("utf-8")).hexdigest()
        dst = os.path.join(THUMBS_DIR, "view", key + ".jpg")
        if not os.path.isfile(dst):
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            try:
                with Image.open(src) as im:
                    im = ImageOps.exif_transpose(im)
                    if im.mode != "RGB":
                        im = im.convert("RGB")
                    im.thumbnail((VIEW_MAX_SIDE, VIEW_MAX_SIDE), Image.LANCZOS)
                    tmp = dst + ".tmp"
                    im.save(tmp, "JPEG", quality=90)
                os.replace(tmp, dst)
            except Exception as e:  # noqa: BLE001
                print(f"[view] {src}: {type(e).__name__}: {e}")
                return send_file(src, conditional=True)   # хотя бы отдадим как есть
        return send_file(dst, mimetype="image/jpeg", conditional=True, max_age=3600)
    return send_file(src, conditional=True, max_age=3600)


if __name__ == "__main__":
    init_db()
    init_auth_db()
    os.makedirs(FILES_DIR, exist_ok=True)
    os.makedirs(THUMBS_DIR, exist_ok=True)
    cleanup_part_files()
    DL.load_settings()
    VD.load_settings()
    print(f"DB: {DB_PATH}\nАвторизация: {AUTH_DB_PATH}")
    print(f"Файлы: {FILES_DIR}\nПревью: {THUMBS_DIR}" + ("" if HAVE_PIL else "  (!) нет Pillow: pip install pillow"))
    if not shutil.which("ffmpeg"):
        print("(!) ffmpeg не найден в PATH: повреждённые видео не соберутся, у видео не будет превью")
    print(f"Статус загрузки: http://{HOST}:{PORT}/status")
    print(f"Галерея: http://{HOST}:{PORT}/")
    app.run(host=HOST, port=PORT, threaded=True)