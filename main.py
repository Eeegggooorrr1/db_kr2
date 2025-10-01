# src/app/ui_main.py
from typing import Optional, Dict, Callable
import sys

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QHBoxLayout, QVBoxLayout, QPushButton, QLabel,
    QComboBox, QStackedWidget, QFrame, QSplitter, QSizePolicy, QScrollArea, QMessageBox
)
from PySide6.QtCore import Qt, QSize

from db import Database
from add_form import AddDialog
from alter_form import AlterTableDialog
from refresh_manager import TableManager



class AppMainWindow(QMainWindow):
    BUTTONS = [
        ("connect", "Подключиться"),
        ("migrate", "Изменить структуру"),
        ("add", "Добавить данные"),
        ("view", "Посмотреть данные"),
    ]

    def __init__(self, db: Database, button_callbacks = None):
        super().__init__()
        self.db = db
        self.button_callbacks = button_callbacks or {}
        self.active_insert_widget = None
        self.active_migrate_widget = None
        self.setWindowTitle("ыыыыыыыыыыыыыыыыыыы")

        self.table_manager = TableManager(self.db, parent=self)

        self._init_ui()
        self.apply_styles()

    def _init_ui(self):
        root = QWidget()
        root_layout = QVBoxLayout(root)
        root_layout.setContentsMargins(6, 6, 6, 6)
        root_layout.setSpacing(8)

        self.top_bar = QFrame()
        top_layout = QHBoxLayout(self.top_bar)
        top_layout.setContentsMargins(8, 6, 8, 6)
        top_layout.setSpacing(6)

        title = QLabel("DB Forms")
        title.setObjectName("appTitle")
        title.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Preferred)
        top_layout.addWidget(title)
        top_layout.addStretch(1)

        self.top_buttons = {}
        for key, label in self.BUTTONS:
            btn = QPushButton(label)
            btn.setObjectName(f"topBtn_{key}")
            btn.setFixedHeight(30)
            btn.setMinimumWidth(120)
            btn.clicked.connect(self._make_top_button_handler(key))
            top_layout.addWidget(btn)
            self.top_buttons[key] = btn

        screen = QApplication.primaryScreen()
        if screen:
            h = screen.size().height()
            top_height = max(48, int(h * 0.07))
            self.top_bar.setFixedHeight(top_height)

        root_layout.addWidget(self.top_bar)

        self.splitter = QSplitter(Qt.Horizontal)
        self.splitter.setChildrenCollapsible(False)

        left_frame = QFrame()
        left_layout = QVBoxLayout(left_frame)
        left_layout.setContentsMargins(10, 10, 10, 10)
        left_layout.setSpacing(8)

        self.left_stack = QStackedWidget()
        self.left_stack.addWidget(self._empty_panel("Подключение"))
        self.left_stack.addWidget(self._build_left_migrate_panel())
        self.left_stack.addWidget(self._build_left_add_panel())
        self.left_stack.addWidget(self._empty_panel("Просмотр"))
        left_layout.addWidget(self.left_stack)
        left_layout.addStretch(1)

        right_frame = QFrame()
        right_layout = QVBoxLayout(right_frame)
        right_layout.setContentsMargins(10, 10, 10, 10)
        right_layout.setSpacing(8)

        self.right_heading = QLabel("Рабочая область")
        right_layout.addWidget(self.right_heading)

        self.right_stack = QStackedWidget()
        self.right_stack.addWidget(self._welcome_page())
        self.right_stack.addWidget(self._placeholder_page("Подключение"))

        self.migrate_container_page = QWidget()
        self.migrate_container_layout = QVBoxLayout(self.migrate_container_page)
        self.migrate_container_layout.setContentsMargins(0, 0, 0, 0)
        self.migrate_container_layout.setSpacing(6)
        self.right_stack.addWidget(self.migrate_container_page)

        self.add_container_page = QWidget()
        self.add_container_layout = QVBoxLayout(self.add_container_page)
        self.add_container_layout.setContentsMargins(0, 0, 0, 0)
        self.add_container_layout.setSpacing(6)
        self.right_stack.addWidget(self.add_container_page)

        self.right_stack.addWidget(self._placeholder_page("Просмотр"))

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(self.right_stack)
        right_layout.addWidget(scroll)

        self.splitter.addWidget(left_frame)
        self.splitter.addWidget(right_frame)
        self.splitter.setStretchFactor(0, 0)
        self.splitter.setStretchFactor(1, 1)
        self.splitter.setSizes([360, 1200])

        root_layout.addWidget(self.splitter)
        self.setCentralWidget(root)

        self.show_context("connect")

    def _build_left_migrate_panel(self):
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        self.migrate_buttons_layout = QVBoxLayout()
        for i in self.table_manager.tables():
            btn = QPushButton(i)
            btn.clicked.connect(lambda _, t=i: self.on_migrate_table_selected(t))
            self.migrate_buttons_layout.addWidget(btn)

        layout.addLayout(self.migrate_buttons_layout)

        self.table_manager.tablesChanged.connect(self._rebuild_migrate_buttons)

        layout.addStretch(1)
        return w

    def _rebuild_migrate_buttons(self, tables):
        while self.migrate_buttons_layout.count():
            item = self.migrate_buttons_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        for i in tables:
            btn = QPushButton(i)
            btn.clicked.connect(lambda _, t=i: self.on_migrate_table_selected(t))
            self.migrate_buttons_layout.addWidget(btn)

    def on_migrate_table_selected(self, table_name: str):
        print('qqqq', table_name)
        if not table_name:
            return

        if self.active_migrate_widget is not None:
            try:
                self.active_migrate_widget.setParent(None)
                self.active_migrate_widget.deleteLater()
            except Exception:
                pass
            self.active_migrate_widget = None

        dialog = AlterTableDialog(table_name=table_name, db=self.db, table_manager=self.table_manager, parent=self.migrate_container_page)
        dialog.setWindowFlags(Qt.Widget)
        dialog.setParent(self.migrate_container_page)

        dialog.tablesChanged.connect(self.table_manager.handle_external_change)

        for i in reversed(range(self.migrate_container_layout.count())):
            item = self.migrate_container_layout.itemAt(i)
            w = item.widget()
            if w:
                w.setParent(None)

        self.migrate_container_layout.addWidget(dialog)
        dialog.show()

        self.active_migrate_widget = dialog

        self.left_stack.setCurrentIndex(1)
        self.right_stack.setCurrentIndex(2)

    def _empty_panel(self, title: str):
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.addWidget(QLabel(title))
        layout.addStretch(1)
        return w

    def _placeholder_page(self, title: str):
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.addWidget(QLabel(f"{title} — ещё не реализовано"))
        layout.addStretch(1)
        return w

    def _welcome_page(self):
        w = QWidget()
        layout = QVBoxLayout(w)
        hi = QLabel("<h2>Добро пожаловать</h2>")
        hi.setTextFormat(Qt.RichText)
        layout.addWidget(hi)
        layout.addWidget(QLabel("Выберите действие сверху."))
        layout.addStretch(1)
        return w

    def _build_left_add_panel(self):
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        self.add_buttons_layout = QVBoxLayout()
        for i in self.table_manager.tables():
            btn = QPushButton(i)
            btn.clicked.connect(lambda _, t=i: self.on_add_table_selected(t))
            self.add_buttons_layout.addWidget(btn)

        layout.addLayout(self.add_buttons_layout)

        self.table_manager.tablesChanged.connect(self._rebuild_add_buttons)

        layout.addStretch(1)
        return w

    def _rebuild_add_buttons(self, tables):
        while self.add_buttons_layout.count():
            item = self.add_buttons_layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()
        for i in tables:
            btn = QPushButton(i)
            btn.clicked.connect(lambda _, t=i: self.on_add_table_selected(t))
            self.add_buttons_layout.addWidget(btn)

    def on_add_table_selected(self, table_name: str):
        if not table_name:
            return

        if self.active_insert_widget is not None:
            try:
                self.active_insert_widget.setParent(None)
                self.active_insert_widget.deleteLater()
            except Exception:
                pass
            self.active_insert_widget = None

        dialog = AddDialog(table_name=table_name, db=self.db, table_manager=self.table_manager, parent=self.add_container_page)
        dialog.setWindowFlags(Qt.Widget)
        dialog.setParent(self.add_container_page)

        dialog.tablesChanged.connect(self.table_manager.handle_external_change)

        for i in reversed(range(self.add_container_layout.count())):
            item = self.add_container_layout.itemAt(i)
            w = item.widget()
            if w:
                w.setParent(None)

        self.add_container_layout.addWidget(dialog)
        dialog.show()

        self.active_insert_widget = dialog

        self.left_stack.setCurrentIndex(2)
        self.right_stack.setCurrentIndex(3)
        self.right_heading.setText(f"Добавление — {table_name}")

    def _make_top_button_handler(self, key: str):
        def handler():
            idx_map = {
                "connect": (0, 1),
                "migrate": (1, 2),
                "add": (2, 3),
                "view": (3, 4),
            }
            left_idx, right_idx = idx_map.get(key, (0, 0))
            self.left_stack.setCurrentIndex(left_idx)
            self.right_stack.setCurrentIndex(right_idx)
            self.right_heading.setText(self.top_buttons[key].text())
            cb = self.button_callbacks.get(key)
            if cb:
                try:
                    cb()
                except Exception:
                    pass
        return handler

    def show_context(self, key: str):
        if key in [k for k, _ in self.BUTTONS]:
            self._make_top_button_handler(key)()

    def apply_styles(self):
        base = """
        QMainWindow, QWidget { font-family: 'Segoe UI', Roboto, Arial, sans-serif; font-size: 13px; color: #222; background: #f5f7fa; }

        QFrame#topBar { background: qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #ffffff, stop:1 #f3f6fb); border-bottom: 1px solid #e6eef8; }

        QLabel#appTitle { font-weight:600; font-size:16px; color: #1f2d3d; }

        QPushButton {
            min-height: 28px;
            border-radius: 8px;
            padding: 6px 12px;
            border: 1px solid rgba(34,45,61,0.07);
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1, stop:0 #ffffff, stop:1 #f7fbff);
            transition: all 100ms ease;
        }
        QPushButton:hover {
            transform: translateY(-1px);
            border: 1px solid rgba(34,45,61,0.12);
            background: qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #ffffff, stop:1 #eef6ff);
        }
        QPushButton:pressed {
            transform: translateY(0);
            padding-top:7px;
            padding-bottom:5px;
            background: qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #eaf2ff, stop:1 #dfeeff);
            border: 1px solid rgba(34,45,61,0.14);
        }

        QPushButton:focus {
            outline: none;
            border: 1px solid #7aa7ff;
            box-shadow: none; /* Qt CSS не поддерживает настоящие box-shadow, используем эффект через GraphicsEffect в коде */
        }

        QPushButton#topBtn_connect, QPushButton#topBtn_migrate, QPushButton#topBtn_add, QPushButton#topBtn_view {
            border-radius: 6px;
            padding: 6px 10px;
            background: transparent;
        }
        QPushButton#topBtn_connect:hover, QPushButton#topBtn_migrate:hover, QPushButton#topBtn_add:hover, QPushButton#topBtn_view:hover {
            background: rgba(30,58,120,0.04);
        }

        QWidget#leftPanel { background: transparent; }
        QWidget#buttonsContainer QPushButton {
            text-align: left;
            padding-left: 12px;
            border: 1px solid rgba(34,45,61,0.04);
            background: #ffffff;
        }
        QWidget#buttonsContainer QPushButton:hover {
            background: qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #fbfdff, stop:1 #f3f9ff);
        }

        QLineEdit, QComboBox, QTextEdit { border: 1px solid #e0e6ec; border-radius: 6px; padding: 6px; background: #fff; }

        QLabel.small { font-size: 11px; color: #7a8794; }
        """
        self.setStyleSheet(base)


def main():
    app = QApplication(sys.argv)
    db = Database()
    w = AppMainWindow(db=db)
    w.resize(QSize(1200, 550))
    w.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
