"""
Запускает server/server.py (Flask) в отдельном потоке того же процесса,
вместо отдельного `python3 server.py` во втором терминале.

server.py сам НЕ меняется: мы используем его как обычный модуль — вызываем те же
функции инициализации, что и его `if __name__ == "__main__":`, и поднимаем
приложение через werkzeug.serving.make_server (а не app.run()), потому что
app.run() ставит обработчики сигналов и не рассчитан на запуск не в главном потоке.

Пути к БД/файлам/превью server.py берёт из переменных окружения YDISK_DB /
YDISK_FILES / YDISK_THUMBS / YDISK_AUTH_DB (с запасным вариантом — папка рядом
с server.py). В dev-режиме запасной вариант и так ок (репозиторий = рабочая
папка). В собранном PyInstaller-бинарнике server.py лежит в read-only бандле
(_MEIPASS/папка рядом с exe) — писать туда либо нельзя, либо бессмысленно
(для --onefile это временная папка, которая исчезает при выходе). Поэтому
перед импортом server.py мы явно выставляем эти переменные на
paths.user_data_dir() — домашнюю директорию пользователя, которая одинаково
работает и в dev, и в собранном виде.
"""
import os
import shutil
import sys
import threading

from werkzeug.serving import make_server

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from paths import resource_dir, user_data_dir  # noqa: E402


def _set_data_dir_env():
    data = user_data_dir()
    os.environ.setdefault("YDISK_DB", os.path.join(data, "gallery.db"))
    os.environ.setdefault("YDISK_FILES", os.path.join(data, "files"))
    os.environ.setdefault("YDISK_THUMBS", os.path.join(data, "thumbs"))
    os.environ.setdefault("YDISK_AUTH_DB", os.path.join(data, "auth.db"))


def _import_server_module():
    _set_data_dir_env()
    server_dir = os.path.join(resource_dir(), "server")
    if server_dir not in sys.path:
        sys.path.insert(0, server_dir)
    import server as server_module  # noqa: WPS433  (намеренный поздний импорт)
    return server_module


class ServerThread(threading.Thread):
    def __init__(self, on_log=print):
        super().__init__(daemon=True)
        self.on_log = on_log
        self._srv = None
        self._mod = None
        self.base_url = None

    def run(self):
        mod = _import_server_module()
        self._mod = mod

        mod.init_db()
        mod.init_auth_db()
        # Apply a previously selected output directory before scanning files.
        mod.load_files_dir_setting()
        os.makedirs(mod.files_dir(), exist_ok=True)
        os.makedirs(mod.THUMBS_DIR, exist_ok=True)
        self.on_log(f"[server] проверка существующих файлов: {mod.reconcile_existing_downloads()}")
        mod.cleanup_part_files()
        mod.DL.load_settings()
        mod.VD.load_settings()

        if not shutil.which("ffmpeg"):
            self.on_log("(!) ffmpeg не найден в PATH — видео собрать не выйдет")

        self.base_url = f"http://{mod.HOST}:{mod.PORT}"
        self.on_log(f"[server] галерея: {self.base_url}/   статус: {self.base_url}/status")

        self._srv = make_server(mod.HOST, mod.PORT, mod.app, threaded=True)
        self._srv.serve_forever()

    def stop(self):
        if self._srv:
            self._srv.shutdown()
