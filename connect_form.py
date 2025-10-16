from typing import Dict
from PySide6.QtWidgets import (
    QDialog, QLineEdit, QFormLayout, QPushButton, QHBoxLayout, QVBoxLayout,
    QLabel, QMessageBox
)
from PySide6.QtCore import Signal, Qt
from config import settings
from db import Database

styles = ""

class ConnectionDialog(QDialog):
    connected = Signal(dict)
    _ever_connected = False
    _last_connection_info = {}

    def __init__(self, parent=None, *, connect_callback=None, recreate_callback=None):
        super().__init__(parent)
        self.setWindowTitle("Подключение к БД")
        self.setModal(True)
        self._connect_callback = connect_callback
        self._recreate_callback = recreate_callback
        self._connected = bool(self.__class__._ever_connected)
        self._connection_info = dict(self.__class__._last_connection_info)
        self.init_ui()
        self.connect_signals()
        self.update_ui_state()
        self.setStyleSheet(styles)

    def init_ui(self):
        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.host_edit = QLineEdit()
        self.port_edit = QLineEdit()
        self.name_edit = QLineEdit()
        self.user_edit = QLineEdit()

        form = QFormLayout()
        form.addRow("DB_PASSWORD:", self.password_edit)
        form.addRow("DB_HOST:", self.host_edit)
        form.addRow("DB_PORT:", self.port_edit)
        form.addRow("DB_NAME:", self.name_edit)
        form.addRow("DB_USER:", self.user_edit)

        self.connect_btn = QPushButton("Подключиться")
        self.recreate_btn = QPushButton("Пересоздать таблицы")
        self.load_env_btn = QPushButton("Взять из окружения")

        btn_layout = QHBoxLayout()
        btn_layout.addWidget(self.connect_btn)
        btn_layout.addWidget(self.recreate_btn)
        btn_layout.addWidget(self.load_env_btn)

        self.status_label = QLabel("")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignLeft)

        main_layout = QVBoxLayout()
        main_layout.addLayout(form)
        main_layout.addLayout(btn_layout)
        main_layout.addWidget(self.status_label)

        self.setLayout(main_layout)
        self.setFixedWidth(480)

    def connect_signals(self):
        self.connect_btn.clicked.connect(self.on_connect_clicked)
        self.recreate_btn.clicked.connect(self.on_recreate_clicked)
        self.load_env_btn.clicked.connect(self.on_load_env_clicked)

    def on_load_env_clicked(self):
        env_values = {
            'DB_PASSWORD': getattr(settings, 'DB_PASSWORD', None),
            'DB_HOST': getattr(settings, 'DB_HOST', None),
            'DB_PORT': getattr(settings, 'DB_PORT', None),
            'DB_NAME': getattr(settings, 'DB_NAME', None),
            'DB_USER': getattr(settings, 'DB_USER', None),
        }
        mapping = {
            'DB_PASSWORD': self.password_edit,
            'DB_HOST': self.host_edit,
            'DB_PORT': self.port_edit,
            'DB_NAME': self.name_edit,
            'DB_USER': self.user_edit
        }
        missing = []
        for key, widget in mapping.items():
            val = env_values.get(key)
            if val is not None and val != "":
                widget.setText(str(val))
            else:
                missing.append(key)
        if missing:
            QMessageBox.warning(
                self,
                "Переменные окружения",
                "Некоторые значения не найдены в переменных окружения: {}\nПоля, которые были найдены, заполнены.".format(
                    ", ".join(missing)
                )
            )

    def on_connect_clicked(self):
        if self._connected:
            return
        params = self.get_connection_params()
        if '' in params.values():
            QMessageBox.critical(self, "ошибка", 'не все поля заполнены')
            return
        self.set_actions_enabled(False)
        self.status_label.setText("Подключение...")
        try:
            if self._connect_callback is not None:
                result = self._connect_callback(params)
            else:
                try:
                    tmp = Database(params)
                    tmp.close()
                    result = True
                except Exception:
                    result = False
        except Exception:
            result = False
        if result:
            self._connected = True
            self._connection_info = params.copy()
            self.__class__._ever_connected = True
            self.__class__._last_connection_info = params.copy()
            self.status_label.setText("Подключено успешно.")
            QMessageBox.information(self, "Подключение", "Подключение прошло успешно.")
            self.connected.emit(self._connection_info)
        else:
            self._connected = False
            self._connection_info = {}
            self.status_label.setText("Не удалось подключиться.")
            QMessageBox.critical(self, "Подключение", "Подключение не удалось.")
        self.set_actions_enabled(True)

    def on_recreate_clicked(self):
        if not self._connected:
            QMessageBox.warning(self, "Пересоздание таблиц", "Сначала подключитесь к базе данных.")
            return
        self.set_actions_enabled(False)
        self.status_label.setText("Пересоздание таблиц...")
        try:
            if self._recreate_callback is not None:
                result = self._recreate_callback(self._connection_info)
            else:
                tmp = Database(self._connection_info)
                result = tmp.recreate_tables()
                tmp.close()
            if result:
                self.status_label.setText("Таблицы пересозданы успешно.")
                msg_box = QMessageBox(self)
                msg_box.setWindowTitle("Пересоздание таблиц")
                msg_box.setText("Таблицы успешно пересозданы.")
                msg_box.setIcon(QMessageBox.Information)
                msg_box.addButton(QMessageBox.Ok)
                msg_box.exec()
            else:
                self.status_label.setText("Не удалось пересоздать таблицы.")
                QMessageBox.critical(self, "Пересоздание таблиц", "Пересоздание не выполнено.")
        except Exception as exc:
            self.status_label.setText("Ошибка при пересоздании таблиц.")
            QMessageBox.critical(self, "Ошибка при пересоздании таблиц", f"{type(exc).__name__}: {exc}")
        finally:
            self.set_actions_enabled(True)

    def set_actions_enabled(self, enabled: bool):
        edits_enabled = enabled and (not self._connected)
        self.password_edit.setEnabled(edits_enabled)
        self.host_edit.setEnabled(edits_enabled)
        self.port_edit.setEnabled(edits_enabled)
        self.name_edit.setEnabled(edits_enabled)
        self.user_edit.setEnabled(edits_enabled)
        self.connect_btn.setEnabled(enabled and (not self._connected))
        self.recreate_btn.setEnabled(enabled and self._connected)
        self.load_env_btn.setEnabled(edits_enabled)

    def update_ui_state(self):
        if self._connected:
            self.status_label.setText("Подключено.")
        else:
            if self.__class__._ever_connected:
                self._connected = True
                self._connection_info = dict(self.__class__._last_connection_info)
                self.status_label.setText("Подключено.")
            else:
                self.status_label.setText("Не подключено.")
        self.set_actions_enabled(True)

    def get_connection_params(self) -> Dict[str, str]:
        return {
            'DB_PASSWORD': self.password_edit.text(),
            'DB_HOST': self.host_edit.text(),
            'DB_PORT': self.port_edit.text(),
            'DB_NAME': self.name_edit.text(),
            'DB_USER': self.user_edit.text()
        }