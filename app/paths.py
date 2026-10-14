"""
Пути, общие для dev-режима и собранного PyInstaller-бинарника.

Два разных понятия:

- resource_dir()  — где лежат БАНДЛЕННЫЕ файлы только для чтения (наш же код:
  server/server.py, server/recover.py, server/recover_runner.py, бандленный ffmpeg).
  В dev-режиме это корень репозитория (yandexdl-desktop/). В собранном виде —
  sys._MEIPASS (PyInstaller распаковывает туда data-файлы на время запуска;
  для --onedir это просто папка рядом с exe).

- user_data_dir()  — куда можно и нужно ПИСАТЬ (gallery.db, auth.db, скачанные
  файлы, превью, профиль браузера). Никогда не внутри resource_dir(): там может
  не быть прав на запись (Program Files на Windows, /Applications на macOS,
  /usr/... или AppImage-монтирование на Linux), и для --onefile это вообще
  временная папка, которая удаляется при выходе.

ffmpeg_dir() возвращает папку с бандленным ffmpeg, если он был добавлен в сборку
(см. yandexdl.spec) — её нужно подставить в PATH раньше всего остального, до
импорта Qt. Если бандла нет (взяли system ffmpeg), возвращает None, и просто
используется то, что уже есть в PATH.
"""
import os
import sys
from typing import Optional


def is_frozen() -> bool:
    return bool(getattr(sys, "frozen", False))


def resource_dir() -> str:
    """Корень только-для-чтения: исходники в dev-режиме, распакованный бандл в exe."""
    if is_frozen():
        # PyInstaller: --onefile -> sys._MEIPASS (временная распаковка);
        # --onedir -> папка рядом с exe (sys._MEIPASS тоже выставлен и указывает туда же).
        return getattr(sys, "_MEIPASS", os.path.dirname(sys.executable))
    # dev: app/paths.py -> .. -> корень репозитория (yandexdl-desktop/)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def app_dir() -> str:
    """Папка, где лежит сам exe/бинарник (для файлов, которые пользователь мог
    положить рядом с ним вручную — сейчас не используется, но может пригодиться)."""
    if is_frozen():
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


def user_data_dir() -> str:
    """Папка для БД/скачанных файлов/профиля браузера — всегда в домашней
    директории пользователя, не рядом с программой."""
    if sys.platform == "win32":
        base = os.environ.get("LOCALAPPDATA") or os.path.join(os.path.expanduser("~"), "AppData", "Local")
        d = os.path.join(base, "YandexDLDesktop")
    elif sys.platform == "darwin":
        d = os.path.join(os.path.expanduser("~"), "Library", "Application Support", "YandexDLDesktop")
    else:
        base = os.environ.get("XDG_DATA_HOME") or os.path.join(os.path.expanduser("~"), ".local", "share")
        d = os.path.join(base, "yandexdl-desktop")
    os.makedirs(d, exist_ok=True)
    return d


def ffmpeg_dir() -> Optional[str]:
    """Папка с бандленным ffmpeg (если yandexdl.spec его добавил), иначе None —
    тогда используется системный ffmpeg из PATH, как и раньше."""
    candidate = os.path.join(resource_dir(), "ffmpeg_bin")
    exe = "ffmpeg.exe" if sys.platform == "win32" else "ffmpeg"
    return candidate if os.path.isfile(os.path.join(candidate, exe)) else None
