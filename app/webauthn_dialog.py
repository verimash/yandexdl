"""
Обработчик QWebEnginePage.webAuthUxRequested — появился в Qt/PySide6 6.7.

До этого сигнала QtWebEngine вообще не давал хост-приложению возможность
отреагировать на запрос WebAuthn (вход по ключу/отпечатку/лицу): страница просила
показать диалог, а ловить было некому — для пользователя это выглядело как
"окно должно появиться, но ничего не происходит".

Важно понимать, что именно показывает ЭТОТ диалог, а что — нет:
  - "Выбери аккаунт" (SelectAccount) и "введи PIN от ключа" (CollectPin) —
    это и есть тот самый "UX", который Chromium просит показать ХОСТ-приложение
    (то есть нас). Эти состояния мы рисуем сами, ниже.
  - Сам нативный системный диалог Touch ID / Windows Hello ("приложи палец") —
    показывает ОС, не мы. Он должен появиться поверх всего автоматически, когда
    Chromium внутри Qt дойдёт до вызова платформенного аутентификатора. Если он
    НЕ появляется — это уже не про webAuthUxRequested, а про то, что сборка
    Chromium внутри PySide6 на этой ОС не может достучаться до Secure Enclave /
    Windows Hello (на macOS это частый источник проблем у встроенных Chromium-движков
    без нужных entitlements/подписи — см. README, раздел про WebAuthn).

Используется так: на каждой QWebEnginePage, где может понадобиться вход по Диску,
  page.webAuthUxRequested.connect(lambda req: WebAuthnDialogController(req, parent).show())
"""
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QListWidget, QListWidgetItem,
    QLineEdit, QPushButton, QDialogButtonBox,
)

try:
    from PySide6.QtWebEngineCore import QWebEngineWebAuthUxRequest
    State = QWebEngineWebAuthUxRequest.WebAuthUxState
except ImportError:  # более старый PySide6 без поддержки WebAuthn
    QWebEngineWebAuthUxRequest = None
    State = None


class WebAuthnDialogController:
    """
    Не сам QDialog, а контроллер поверх одного QWebEngineWebAuthUxRequest: диалог
    пересоздаёт содержимое при каждом stateChanged (сам request живёт, пока идёт
    аутентификация, и может пройти через несколько состояний подряд).
    """

    def __init__(self, request, parent_widget=None):
        self.request = request
        self.parent_widget = parent_widget
        self.dialog = QDialog(parent_widget)
        self.dialog.setWindowTitle("Вход (WebAuthn)")
        self.dialog.setModal(False)  # не блокируем окно — вкладка должна оставаться живой
        self.dialog.setMinimumWidth(360)
        self._layout = QVBoxLayout(self.dialog)

        request.stateChanged.connect(self._render)
        self.dialog.finished.connect(lambda _: self.request.cancel())
        self._render()
        self.dialog.show()

    # ---------------------------------------------------------- отрисовка состояний

    def _clear(self):
        while self._layout.count():
            item = self._layout.takeAt(0)
            w = item.widget()
            if w:
                w.deleteLater()

    def _render(self):
        self._clear()
        state = self.request.state()
        party = self.request.relyingPartyId() or "сайт"

        if state == State.SelectAccount:
            self._layout.addWidget(QLabel(f"Выбери, каким способом войти на {party}:"))
            lst = QListWidget()
            for name in self.request.userNames():
                QListWidgetItem(name, lst)
            self._layout.addWidget(lst)
            btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
            btns.accepted.connect(lambda: self._select_account(lst))
            btns.rejected.connect(self.request.cancel)
            self._layout.addWidget(btns)

        elif state == State.CollectPin:
            pin_req = self.request.pinRequest()
            msg = "Введи PIN от ключа безопасности"
            if pin_req is not None and getattr(pin_req, "error", None):
                msg += f"\n(ошибка предыдущей попытки: {pin_req.error})"
            self._layout.addWidget(QLabel(msg))
            edit = QLineEdit()
            edit.setEchoMode(QLineEdit.Password)
            self._layout.addWidget(edit)
            btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
            btns.accepted.connect(lambda: self._submit_pin(edit))
            btns.rejected.connect(self.request.cancel)
            self._layout.addWidget(btns)

        elif state == State.FinishTokenCollection:
            self._layout.addWidget(QLabel(
                f"Подтверди вход на {party}: приложи палец / посмотри в камеру / "
                f"тронь ключ безопасности — системный диалог должен появиться сам."
            ))
            btns = QDialogButtonBox(QDialogButtonBox.Cancel)
            btns.rejected.connect(self.request.cancel)
            self._layout.addWidget(btns)

        elif state == State.RequestFailed:
            reason = self.request.requestFailureReason()
            self._layout.addWidget(QLabel(f"Не получилось войти: {reason}"))
            row = QHBoxLayout()
            retry_btn = QPushButton("Повторить")
            cancel_btn = QPushButton("Отмена")
            retry_btn.clicked.connect(self.request.retry)
            cancel_btn.clicked.connect(self._close)
            row.addWidget(retry_btn)
            row.addWidget(cancel_btn)
            self._layout.addLayout(row)

        elif state == State.Completed:
            self._close()

        else:  # NotStarted и прочее — ничего не показываем
            self._layout.addWidget(QLabel("Подключаюсь к аутентификатору…"))

    def _select_account(self, lst: QListWidget):
        item = lst.currentItem()
        if item:
            self.request.setSelectedAccount(item.text())

    def _submit_pin(self, edit: QLineEdit):
        self.request.setPin(edit.text())
        edit.clear()

    def _close(self):
        self.dialog.finished.disconnect()  # не дёргать cancel() второй раз при закрытии
        self.request.cancel()
        self.dialog.close()


def attach_webauthn_handler(page, parent_widget=None):
    """Подключает обработчик WebAuthn-запросов к одной QWebEnginePage.

    Безопасно вызывать на любом PySide6 — если сигнала нет (версия < 6.7),
    просто ничего не делает (вместо падения).
    """
    if QWebEngineWebAuthUxRequest is None:
        return False
    signal = getattr(page, "webAuthUxRequested", None)
    if signal is None:
        return False
    signal.connect(lambda req: WebAuthnDialogController(req, parent_widget))
    return True
