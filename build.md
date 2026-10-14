# Сборка бинарника (Windows / macOS / Linux)

## Главное ограничение

PyInstaller **не кросс-компилирует**. `pyinstaller yandexdl.spec`, запущенный на
Linux, даёт Linux-бинарник — и только его. Чтобы получить Windows-exe, macOS-`.app`
и Linux-бинарник одновременно, нужно либо запустить сборку на трёх разных машинах
(или ВМ), либо — проще — воспользоваться готовым workflow
`.github/workflows/build.yml`: он гоняет сборку на матрице из трёх раннеров
GitHub Actions (`windows-latest`/`macos-latest`/`ubuntu-latest`) и кладёт три
готовых zip-архива в Artifacts.

## Вариант A — через GitHub Actions (рекомендуется)

1. Запуши репозиторий (с новыми `app/paths.py`, `yandexdl.spec`,
   `requirements-build.txt`, `.github/workflows/build.yml`) на GitHub.
2. Вкладка **Actions** → workflow **build** → **Run workflow** (или просто
   запушь тег `v1.0.0` — соберётся автоматически).
3. Через несколько минут в этом же run, внизу, появится **Artifacts**:
   `YandexDLDesktop-windows.zip`, `YandexDLDesktop-macos.zip`,
   `YandexDLDesktop-linux.zip`. Скачиваешь — готово, без своих трёх машин.

Код не подписан (нет Apple Developer ID / Windows code signing certificate) —
Windows Defender SmartScreen и macOS Gatekeeper будут ругаться на первый запуск
("Неизвестный разработчик"/"Windows protected your PC"). Это ожидаемо для
dev-сборки; обход на macOS — ПКМ по приложению → "Открыть" → подтвердить. Для
реального распространения понадобится подпись (отдельная большая тема, не
в этом workflow).

## Вариант B — локально, на каждой ОС вручную

Одинаково для всех трёх:

```bash
cd yandexdl-desktop
python3 -m venv .venv
source .venv/bin/activate        # Windows (cmd):  .venv\Scripts\activate.bat
                                  # Windows (PowerShell): .venv\Scripts\Activate.ps1
pip install -r requirements.txt -r requirements-build.txt
pyinstaller yandexdl.spec
```

Результат:

- **Windows** — `dist\YandexDLDesktop\YandexDLDesktop.exe` (папка целиком нужна
  для запуска — там DLL и Qt-ресурсы, переносить/раздавать нужно всю папку
  `dist\YandexDLDesktop\`, не один exe).
- **macOS** — `dist/YandexDLDesktop.app` (двойной клик или `open dist/YandexDLDesktop.app`).
- **Linux** — `dist/YandexDLDesktop/YandexDLDesktop` (запускать из этой же папки
  — рядом лежат нужные `.so`).

### ffmpeg

По умолчанию приложение рассчитывает на системный `ffmpeg` в `PATH` пользователя
(как и раньше, в dev-режиме) — его в сборку `.spec` не кладёт. Если хочешь
бандлить свой `ffmpeg` (чтобы пользователю не пришлось ставить его отдельно),
положи бинарник **перед сборкой** сюда:

```
vendor/ffmpeg/win32/ffmpeg.exe
vendor/ffmpeg/darwin/ffmpeg
vendor/ffmpeg/linux/ffmpeg
```

(`sys.platform` на той машине, где собираешь — т.е. на каждой ОС свой файл, по
одному при каждой сборке). `yandexdl.spec` сам подхватит файл, если он есть —
ничего в спеке менять не нужно. Статические сборки ffmpeg под каждую ОС можно
взять с официального https://ffmpeg.org/download.html (например,
gyan.dev — Windows, evermeet.cx — macOS, johnvansickle.com — Linux static
builds). Лицензия ffmpeg (LGPL/GPL в зависимости от сборки) — если
распространяешь бинарник публично, свериться с условиями конкретной сборки.

### Системные зависимости для сборки на Linux

Голый Ubuntu/Debian — нужны библиотеки для Qt WebEngine, иначе PyInstaller
соберёт, но бинарник не запустится:

```bash
sudo apt-get install -y libegl1 libopengl0 libxkbcommon0 libxcb-cursor0 \
  libnss3 libxcomposite1 libxdamage1 libxrandr2 libxtst6 libgbm1
```

(то же самое уже прописано в `.github/workflows/build.yml` для Варианта A).

## Что изменилось в коде ради сборки

- **`app/paths.py`** (новый файл) — отличает dev-режим от собранного
  (`sys.frozen`), даёт `resource_dir()` (только для чтения: `server.py` и т.п.
  бандлятся как данные, см. `yandexdl.spec`) и `user_data_dir()` (куда реально
  можно писать — домашняя директория пользователя, не папка программы).
- **`app/server_runner.py`** — перед импортом `server.py` выставляет
  `YDISK_DB`/`YDISK_FILES`/`YDISK_THUMBS`/`YDISK_AUTH_DB` на `user_data_dir()`,
  чтобы БД и скачанные файлы не пытались писаться внутрь read-only бандла
  (`Program Files`, `/Applications`, смонтированный read-only образ и т. п.).
- **`app/main.py`** — `_dispatch_recover_runner_if_needed()`: `server.py`
  качает видео через `[sys.executable, "-u", "recover_runner.py", ...]`; в
  dev-режиме `sys.executable` — это `python3`, всё само работает, но в
  собранном виде `sys.executable` — это наш же exe (другого Python рядом нет).
  Этот перехватчик ловит именно такой вызов аргументов до импорта Qt и
  выполняет `recover_runner.py` напрямую через `runpy`, не поднимая окно.
- **`server/server.py` не менялся вообще** — все изменения снаружи, через env
  и диспетчер в `main.py`, как и в оригинальной архитектуре проекта.
