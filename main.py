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

    def __init__(self, db: Database, button_callbacks: Optional[Dict[str, Callable]] = None):
        super().__init__()
        self.db = db
        self.button_callbacks = button_callbacks or {}
        self.active_insert_widget = None
        self.active_migrate_widget = None
        self.setWindowTitle("DB Form Designer — Integrated")

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

        self.top_buttons: Dict[str, QPushButton] = {}
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
        left_layout.addWidget(QLabel("Контекст"))

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
        layout.addWidget(QLabel("Изменение структуры — выбор таблицы"))

        self.migrate_table_combo = QComboBox()
        self.migrate_table_combo.setModel(self.table_manager.model)
        self.migrate_table_combo.currentTextChanged.connect(self.on_migrate_table_selected)
        layout.addWidget(self.migrate_table_combo)

        open_btn = QPushButton("Открыть форму изменения")
        open_btn.clicked.connect(lambda: self._make_top_button_handler("migrate")())
        layout.addWidget(open_btn)
        layout.addStretch(1)
        return w

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
        self.right_heading.setText(f"Изменение структуры — {table_name}")

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
        layout.addWidget(QLabel("Добавить данные — выбор таблицы"))

        self.table_combo = QComboBox()
        self.table_combo.setModel(self.table_manager.model)
        self.table_combo.currentTextChanged.connect(self.on_table_selected)
        layout.addWidget(self.table_combo)

        open_btn = QPushButton("Открыть форму добавления")
        open_btn.clicked.connect(lambda: self._make_top_button_handler("add")())
        layout.addWidget(open_btn)
        layout.addStretch(1)
        return w

    def on_table_selected(self, table_name: str):
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
        QWidget { font-family: 'Segoe UI', Roboto, Arial, sans-serif; font-size: 13px; color: #222; }
        QFrame { background: #fff; }
        QLineEdit, QComboBox, QTextEdit { border: 1px solid #d8dde3; border-radius: 6px; padding: 6px; background: #fff; }
        QPushButton { min-height: 28px; border-radius:6px; padding:6px 10px; }
        QPushButton#topBtn_add { background: qlineargradient(x1:0,y1:0,x2:0,y2:1, stop:0 #e6f0ff, stop:1 #d6e9ff); font-weight:600; }
        QLabel#appTitle { font-weight:600; font-size:16px; }
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
