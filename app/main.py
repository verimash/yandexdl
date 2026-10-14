"""
YandexDL Desktop — прототип, v2: единое окно-браузер.

Вкладка 0 ("YandexDL") закреплена за нашим локальным сайтом — галереей/статусом
server.py, её нельзя закрыть. Остальные вкладки — обычный браузер: адресная строка,
назад/вперёд/обновить, "+" открывает новую вкладку, вкладки закрываются крестиком.
Пользователь просто открывает disk.yandex.ru (вкладка 2 открыта на нём по умолчанию)
и работает как в обычном браузере — можно хоть в нескольких вкладках сразу.

Все обычные вкладки используют один и тот же персистентный QWebEngineProfile
(общие куки/сессия), поэтому логин не слетает между вкладками и перезапусками.

Лог перехвата и статистика — в выезжающей снизу панели (QDockWidget), чтобы не
мешать "браузерности" основного окна; открывается кнопкой "Лог" на тулбаре.

ВАЖНО: QTWEBENGINE_REMOTE_DEBUGGING нужно выставить ДО импорта QtWebEngine*.

Это прототип: не запускался и не тестировался в песочнице (нет сети и PySide6).

--- Про сборку в exe/app/бинарник (PyInstaller) ---

server.py качает видео через ОТДЕЛЬНЫЙ подпроцесс recover_runner.py: вызывает
`[sys.executable, "-u", <путь к recover_runner.py>] + args`. В dev-режиме
sys.executable — это настоящий python3, всё работает само. В СОБРАННОМ приложении
sys.executable — это НАШ ЖЕ exe (другого питона рядом нет), так что такой вызов
на самом деле означает "запусти себя опять с этими аргументами". Шаг 0 ниже ловит
именно этот паттерн — ПЕРЕД импортом Qt, чтобы не поднимать GUI ради одного
скачивания видео — и выполняет recover_runner.py напрямую. Это сделано в main.py,
а не в server.py, именно чтобы server.py не пришлось трогать вообще.
"""
import os
import socket
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from paths import resource_dir, ffmpeg_dir  # noqa: E402

# --- 0. бандлed ffmpeg (если есть, см. yandexdl.spec) — в PATH до всего остального ---

_ff_dir = ffmpeg_dir()
if _ff_dir:
    os.environ["PATH"] = _ff_dir + os.pathsep + os.environ.get("PATH", "")


def _dispatch_recover_runner_if_needed():
    """См. пояснение в докстринге модуля. Если argv — это тот самый вызов от
    server.py, выполняем recover_runner.py и выходим, не трогая Qt вообще."""
    if len(sys.argv) >= 3 and sys.argv[1] == "-u" and os.path.basename(sys.argv[2]) == "recover_runner.py":
        import runpy
        real_runner = os.path.join(resource_dir(), "server", "recover_runner.py")
        sys.argv = [real_runner] + sys.argv[3:]
        runpy.run_path(real_runner, run_name="__main__")
        sys.exit(0)


_dispatch_recover_runner_if_needed()

# --- 1. свободный порт под remote-debugging, выставляем env ДО импорта Qt ---


def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


DEVTOOLS_PORT = int(os.environ.get("YDISK_DEVTOOLS_PORT") or _free_port())
os.environ["QTWEBENGINE_REMOTE_DEBUGGING"] = str(DEVTOOLS_PORT)

# --- 2. теперь можно импортировать Qt ---

from PySide6.QtCore import Qt, Signal, QObject, QUrl, QTimer
from PySide6.QtGui import QIcon, QAction
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPlainTextEdit, QTabWidget, QToolBar, QLineEdit, QToolButton, QDockWidget,
)
from PySide6.QtWebEngineWidgets import QWebEngineView
from PySide6.QtWebEngineCore import QWebEngineProfile, QWebEnginePage

from server_runner import ServerThread  # noqa: E402
from capture_cdp import CDPCapture  # noqa: E402
from webauthn_dialog import attach_webauthn_handler  # noqa: E402
from paths import user_data_dir  # noqa: E402

DEFAULT_NEW_TAB_URL = "https://disk.yandex.ru"


class LogBridge(QObject):
    """CDPCapture и ServerThread живут в фоновых потоках — сигналы Qt безопасно
    перекидывают их сообщения в GUI-поток (Qt сам ставит slot-вызов в очередь)."""
    log = Signal(str)
    stats = Signal(dict)


class LogDock(QDockWidget):
    """Выезжающая снизу панель с логом перехвата и короткой строкой статуса."""

    def __init__(self, parent=None):
        super().__init__("Лог перехвата", parent)
        self.setAllowedAreas(Qt.BottomDockWidgetArea | Qt.RightDockWidgetArea)
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setContentsMargins(4, 4, 4, 4)

        self.status_label = QLabel("Запускаюсь…")
        self.status_label.setStyleSheet("font-weight: bold;")
        layout.addWidget(self.status_label)

        self.log_view = QPlainTextEdit(readOnly=True)
        self.log_view.setMaximumBlockCount(2000)
        layout.addWidget(self.log_view)

        self.setWidget(w)

    def append_log(self, text: str):
        self.log_view.appendPlainText(text)

    def update_stats(self, d: dict):
        parts = []
        if "active_tabs" in d:
            parts.append(f"вкладок с Диском под наблюдением: {d['active_tabs']}")
        if "total_clusters" in d:
            parts.append(f"кластеров: {d['total_clusters']}")
        if "total_resources" in d:
            parts.append(f"ресурсов: {d['total_resources']}")
        if parts:
            self.status_label.setText(" · ".join(parts))


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("YandexDL Desktop (прототип)")
        self.resize(1400, 900)

        # --- Flask-сервер в потоке этого же процесса ---
        self.bridge = LogBridge()
        self.bridge.log.connect(self._on_log)
        self.bridge.stats.connect(self._on_stats)
        self.server_thread = ServerThread(on_log=self._log_from_thread)
        self.server_thread.start()

        # --- общий профиль для всех обычных вкладок (одна "банка" кук) ---
        profile_dir = os.path.join(user_data_dir(), "browser-profile")
        os.makedirs(profile_dir, exist_ok=True)
        self.browser_profile = QWebEngineProfile("yandexdl-browser", self)
        self.browser_profile.setPersistentStoragePath(profile_dir)
        self.browser_profile.setPersistentCookiesPolicy(
            QWebEngineProfile.PersistentCookiesPolicy.ForcePersistentCookies
        )

        # --- тулбар: назад/вперёд/обновить/адресная строка/новая вкладка/лог ---
        toolbar = QToolBar("Навигация")
        toolbar.setMovable(False)
        self.addToolBar(toolbar)

        self.back_btn = QToolButton(text="◀")
        self.fwd_btn = QToolButton(text="▶")
        self.reload_btn = QToolButton(text="⟳")
        self.back_btn.clicked.connect(lambda: self._current_view() and self._current_view().back())
        self.fwd_btn.clicked.connect(lambda: self._current_view() and self._current_view().forward())
        self.reload_btn.clicked.connect(lambda: self._current_view() and self._current_view().reload())
        toolbar.addWidget(self.back_btn)
        toolbar.addWidget(self.fwd_btn)
        toolbar.addWidget(self.reload_btn)

        self.address_bar = QLineEdit()
        self.address_bar.returnPressed.connect(self._navigate_from_address_bar)
        toolbar.addWidget(self.address_bar)

        new_tab_btn = QToolButton(text="+ вкладка")
        new_tab_btn.clicked.connect(lambda: self._open_tab(DEFAULT_NEW_TAB_URL))
        toolbar.addWidget(new_tab_btn)

        log_btn = QToolButton(text="Лог")
        toolbar.addWidget(log_btn)

        # --- вкладки ---
        self.tabs = QTabWidget()
        self.tabs.setTabsClosable(True)
        self.tabs.setMovable(True)
        self.tabs.tabCloseRequested.connect(self._close_tab)
        self.tabs.currentChanged.connect(self._on_current_tab_changed)
        self.setCentralWidget(self.tabs)

        # вкладка 0: наш сайт, закреплена (без крестика)
        self.site_view = self._make_view(profile=None)  # дефолтный off-the-record профиль — ок для своего же localhost
        self.site_view.load(QUrl("about:blank"))
        self.tabs.addTab(self.site_view, "YandexDL")
        self.tabs.tabBar().setTabButton(0, self.tabs.tabBar().ButtonPosition.RightSide, None)

        # вкладка 1: обычный браузер, сразу на Диске
        self._open_tab(DEFAULT_NEW_TAB_URL)
        self.tabs.setCurrentIndex(1)

        # --- лог-панель снизу, по умолчанию скрыта ---
        self.log_dock = LogDock(self)
        self.addDockWidget(Qt.BottomDockWidgetArea, self.log_dock)
        self.log_dock.hide()
        log_btn.clicked.connect(lambda: self.log_dock.setVisible(not self.log_dock.isVisible()))

        # --- когда сервер поднялся, подставляем его адрес в закреплённую вкладку ---
        QTimer.singleShot(500, self._load_site_tab)

        # --- захват CDP: следит сразу за всеми вкладками с disk.yandex ---
        self.capture = CDPCapture(
            devtools_port=DEVTOOLS_PORT,
            on_log=self._log_from_thread,
            on_stats=self._stats_from_thread,
        )
        QTimer.singleShot(1500, self.capture.start)

    # ---------------------------------------------------------- вкладки

    def _make_view(self, profile):
        view = QWebEngineView()
        page = QWebEnginePage(profile, view) if profile is not None else QWebEnginePage(view)
        view.setPage(page)
        view.urlChanged.connect(lambda url, v=view: self._on_url_changed(v, url))
        view.titleChanged.connect(lambda title, v=view: self._on_title_changed(v, title))
        if profile is not None:
            # вход по Face ID/Touch ID/Windows Hello/ключу нужен только на "настоящих"
            # вкладках браузера (Диск), не на нашей служебной вкладке с localhost
            attach_webauthn_handler(page, parent_widget=self)
        return view

    def _open_tab(self, url: str):
        view = self._make_view(profile=self.browser_profile)
        view.load(QUrl(url))
        idx = self.tabs.addTab(view, "Новая вкладка")
        self.tabs.setCurrentIndex(idx)
        return view

    def _close_tab(self, index: int):
        if index == 0:
            return  # вкладку с нашим сайтом не закрываем
        widget = self.tabs.widget(index)
        self.tabs.removeTab(index)
        if widget:
            widget.deleteLater()

    def _current_view(self):
        return self.tabs.currentWidget()

    def _on_current_tab_changed(self, index):
        view = self.tabs.widget(index)
        if view is not None:
            self.address_bar.setText(view.url().toString())

    def _on_url_changed(self, view, url):
        if view is self._current_view():
            self.address_bar.setText(url.toString())

    def _on_title_changed(self, view, title):
        idx = self.tabs.indexOf(view)
        if idx > 0 and title:  # вкладку 0 не переименовываем
            self.tabs.setTabText(idx, title[:24])

    def _navigate_from_address_bar(self):
        text = self.address_bar.text().strip()
        if not text:
            return
        if "://" not in text:
            text = "https://" + text
        view = self._current_view()
        if view is not None:
            view.load(QUrl(text))

    def _load_site_tab(self):
        if self.server_thread.base_url:
            self.site_view.load(QUrl(self.server_thread.base_url + "/status"))
        else:
            QTimer.singleShot(500, self._load_site_tab)  # сервер ещё не поднялся — ждём

    # ---- мосты из фоновых потоков в GUI-поток (через Qt-сигналы — thread-safe) ----

    def _log_from_thread(self, text: str):
        self.bridge.log.emit(text)

    def _stats_from_thread(self, **kw):
        self.bridge.stats.emit(kw)

    def _on_log(self, text: str):
        self.log_dock.append_log(text)

    def _on_stats(self, d: dict):
        self.log_dock.update_stats(d)

    def closeEvent(self, event):
        self.capture.stop()
        self.server_thread.stop()
        super().closeEvent(event)


def main():
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
