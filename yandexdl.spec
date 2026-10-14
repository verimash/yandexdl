# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller-спек. Собирает ТОЛЬКО под ту ОС, на которой запущен `pyinstaller` —
PyInstaller не кросс-компилирует: Windows-exe можно собрать только на Windows,
.app — только на macOS, Linux-бинарник — только на Linux. Чтобы получить все
три сразу без трёх своих машин — см. .github/workflows/build.yml (матрица
windows-latest / macos-latest / ubuntu-latest) и build.md в корне репо.

Запуск (на каждой целевой ОС свой):
    pyinstaller yandexdl.spec

Результат — в dist/YandexDLDesktop/ (папка со всем необходимым, --onedir).
Нарочно НЕ --onefile: онефайл распаковывает себя во временную папку при каждом
запуске (медленнее, и там быстро теряются куки/БД между запусками, если их туда
же по ошибке положить) — тут это не нужно, раз все пользовательские данные и
так идут в user_data_dir() (см. app/paths.py), а не рядом с бинарником.
"""
import os
import sys

block_cipher = None

ROOT = os.path.abspath(os.path.dirname(SPEC))
APP_DIR = os.path.join(ROOT, "app")
SERVER_DIR = os.path.join(ROOT, "server")

# --- server/ — только данные (server.py и т.д. НЕ компилируются, main.py читает
# и запускает их как обычные .py-файлы; recover_runner.py подпроцессом/диспетчером
# — см. _dispatch_recover_runner_if_needed в app/main.py) ---
datas = [
    (os.path.join(SERVER_DIR, "server.py"), "server"),
    (os.path.join(SERVER_DIR, "recover.py"), "server"),
    (os.path.join(SERVER_DIR, "recover_runner.py"), "server"),
]

# --- опциональный бандленный ffmpeg: положи бинарник сюда перед сборкой, если
# не хочешь полагаться на системный ffmpeg в PATH пользователя. Если папки нет —
# просто пропускается, приложение использует ffmpeg из PATH как раньше. ---
if sys.platform == "win32":
    _ff_name = "ffmpeg.exe"
elif sys.platform == "darwin":
    _ff_name = "ffmpeg"
else:
    _ff_name = "ffmpeg"
_ff_vendor = os.path.join(ROOT, "vendor", "ffmpeg", sys.platform, _ff_name)
if os.path.isfile(_ff_vendor):
    datas.append((_ff_vendor, "ffmpeg_bin"))

hiddenimports = [
    "werkzeug.serving",
    "PIL.Image",
    "PIL.ImageOps",
    "pillow_heif",
    "websocket",
    "engineio.async_drivers.threading",
]

a = Analysis(
    [os.path.join(APP_DIR, "main.py")],
    pathex=[APP_DIR, SERVER_DIR],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    cipher=block_cipher,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="YandexDLDesktop",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,  # GUI-приложение — без консольного окна
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    upx_exclude=[],
    name="YandexDLDesktop",
)

if sys.platform == "darwin":
    app = BUNDLE(
        coll,
        name="YandexDLDesktop.app",
        icon=None,
        bundle_identifier="ru.yandexdl.desktop",
        info_plist={
            "NSHighResolutionCapable": "True",
            "LSMinimumSystemVersion": "11.0",
        },
    )
