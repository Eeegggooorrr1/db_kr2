import logging
from typing import Optional, Dict, Callable
import sys

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QHBoxLayout, QVBoxLayout, QPushButton, QLabel,
    QComboBox, QStackedWidget, QFrame, QSplitter, QSizePolicy, QScrollArea, QMessageBox, QTextEdit
)
from PySide6.QtCore import Qt, QSize, Signal, QObject

from connect_form import ConnectionDialog
from db import Database
from add_form import AddDialog
from alter_form import AlterTableDialog
from logger import LogsWindow
from refresh_manager import TableManager
from view_form import SQLStubWindow
from view_results_form import TableResultWidget

class AppMainWindow(QMainWindow):
    BUTTONS = [
        ("logs", "Логи"),
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
        self.active_view_widget = None
        self.view_container_page = None
        self.view_container_layout = None
        self.active_view_result_widget = None
        self.setWindowTitle("ыыыыыыыыыыыыыыыыыыыы")

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
        self.left_stack.addWidget(self._empty_panel(" "))
        self.left_stack.addWidget(self._build_left_migrate_panel())
        self.left_stack.addWidget(self._build_left_add_panel())
        self.left_stack.addWidget(self._build_left_view_panel())
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
        self.logs_page = LogsWindow(parent=self)
        self.right_stack.addWidget(self.logs_page)

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

        self.view_container_page = QWidget()
        self.view_container_layout = QVBoxLayout(self.view_container_page)
        self.view_container_layout.setContentsMargins(0, 0, 0, 0)
        self.view_container_layout.setSpacing(6)
        self.right_stack.addWidget(self.view_container_page)

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

        self.show_context("logs")

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
        layout.addWidget(QLabel(f"{title}  ещё не реализовано"))
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
        self.right_heading.setText(f"Добавление в {table_name}")

    def _build_left_view_panel(self):
        w = QWidget()
        layout = QVBoxLayout(w)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)

        self.view_left_container_page = QWidget()
        self.view_left_container_layout = QVBoxLayout(self.view_left_container_page)
        self.view_left_container_layout.setContentsMargins(0, 0, 0, 0)
        self.view_left_container_layout.setSpacing(6)

        layout.addWidget(self.view_left_container_page)
        layout.addStretch(1)
        return w

    def _clear_layout(self, layout):
        while layout and layout.count():
            item = layout.takeAt(0)
            if item is None:
                continue
            widget = item.widget()
            if widget:
                try:
                    layout.removeWidget(widget)
                except Exception:
                    pass
                widget.setParent(None)
                try:
                    widget.deleteLater()
                except Exception:
                    pass
            else:
                child_layout = item.layout()
                if child_layout:
                    self._clear_layout(child_layout)

    def _attach_view_left_widget(self):
        if self.active_view_widget is not None:
            return

        try:
            stub = SQLStubWindow(db = self.db)
            stub.apply_sql.connect(self._on_view_apply_sql)
        except Exception as e:
            print("Не удалось создать SQLStubWindow:", e)
            return

        try:
            stub.setWindowFlags(Qt.Widget)
            stub.setParent(self.view_left_container_page)
            stub.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
            self.view_left_container_layout.addWidget(stub)
            stub.show()
            self.active_view_widget = stub
        except Exception as e:
            print("Ошибка при встраивании SQLStubWindow:", e)
            try:
                stub.setParent(None)
                stub.deleteLater()
            except Exception:
                pass

    def _detach_view_left_widget(self):
        if self.active_view_widget is not None:
            try:
                self.active_view_widget.setParent(None)
                self.active_view_widget.deleteLater()
            except Exception:
                pass
            self.active_view_widget = None

        if self.view_left_container_layout is not None:
            self._clear_layout(self.view_left_container_layout)
    def _make_top_button_handler(self, key: str):
        def handler():
            idx_map = {
                "logs": (0, 1),
                "migrate": (1, 2),
                "add": (2, 3),
                "view": (3, 4),
            }
            if key != "view":
                try:
                    self._detach_view_left_widget()
                except Exception:
                    pass

            left_idx, right_idx = idx_map.get(key, (0, 0))
            self.left_stack.setCurrentIndex(left_idx)
            self.right_stack.setCurrentIndex(right_idx)
            self.right_heading.setText(self.top_buttons[key].text())

            if key == "view":
                try:
                    self._attach_view_left_widget()
                except Exception:
                    pass

            cb = self.button_callbacks.get(key)
            if cb:
                try:
                    cb()
                except Exception:
                    pass

        return handler

    def _on_view_apply_sql(self, sql: str):
        try:
            if self.active_view_result_widget is not None:
                self.active_view_result_widget.setParent(None)
                self.active_view_result_widget.deleteLater()
                self.active_view_result_widget = None
        except Exception:
            pass

        if self.view_container_layout is not None:
            self._clear_layout(self.view_container_layout)

        try:
            tv = TableResultWidget(self.db, sql, parent=self.view_container_page)
            self.view_container_layout.addWidget(tv)
            tv.show()
            self.active_view_result_widget = tv
        except Exception as e:
            print("Error creating TableResultWidget:", e)
            QMessageBox.critical(self, "Error", f"Cannot show table result: {e}")
            return

        try:
            idx = self.right_stack.indexOf(self.view_container_page)
            if idx != -1:
                self.right_stack.setCurrentIndex(idx)
        except Exception:
            pass

        try:
            self.left_stack.setCurrentIndex(3)
        except Exception:
            pass

    def show_context(self, key: str):
        if key in [k for k, _ in self.BUTTONS]:
            self._make_top_button_handler(key)()

    def apply_styles(self):
        base = """
        QMainWindow, QWidget { 
            font-family: 'Segoe UI', Roboto, Arial, sans-serif; 
            font-size: 13px; 
            color: #2d3748; 
            background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:1, stop:0 #fbfdff, stop:1 #f6f9fb);
        }

        QFrame#topBar { 
            background: rgba(255,255,255,0.95);
            border-bottom: 1px solid rgba(226,232,240,0.9);
            padding: 6px 0px;
        }

        QLabel#appTitle { 
            font-weight: 700; 
            font-size: 17px; 
            color: #153e75; 
        }

        QScrollBar:vertical {
            background: rgba(250,252,255,0.5);
            width: 10px;
            margin: 2px;
            border-radius: 8px;
        }

        QScrollBar::handle:vertical {
            background: qlineargradient(spread:pad, x1:0, y1:0, x2:0, y2:1, stop:0 #cce7ff, stop:1 #9fd3ff);
            border-radius: 8px;
            min-height: 22px;
            border: 1px solid rgba(120,170,255,0.3);
        }

        QScrollBar::handle:vertical:hover {
            background: qlineargradient(spread:pad, x1:0, y1:0, x2:0, y2:1, stop:0 #a8d9ff, stop:1 #6fbaff);
        }

        QScrollBar::handle:vertical:pressed {
            background: #4a90e2;
        }

        QScrollBar:horizontal {
            background: rgba(250,252,255,0.5);
            height: 10px;
            margin: 2px;
            border-radius: 8px;
        }

        QScrollBar::handle:horizontal {
            background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0, stop:0 #cce7ff, stop:1 #9fd3ff);
            border-radius: 8px;
            min-width: 22px;
            border: 1px solid rgba(120,170,255,0.3);
        }

        QScrollBar::handle:horizontal:hover {
            background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0, stop:0 #a8d9ff, stop:1 #6fbaff);
        }

        QLineEdit, QComboBox, QTextEdit { 
            border: 1px solid rgba(226,232,240,0.95);
            border-radius: 8px; 
            padding: 8px 10px; 
            background: #ffffff; 
            font-size: 13px;
            selection-background-color: #06b6d4; 
            selection-color: #ffffff;
        }

        QLineEdit:focus, QComboBox:focus, QTextEdit:focus {
            border: 1px solid rgba(6,182,212,0.95);
            background: #f0fdff;
        }

        QLineEdit:hover, QComboBox:hover, QTextEdit:hover {
            border: 1px solid rgba(79,148,255,0.7);
            background: #f8fcff;
        }

        QComboBox::drop-down { border: none; width: 26px; }
        QComboBox::down-arrow { border: none; image: none; }

        QComboBox QAbstractItemView {
            background: #ffffff;
            border: 1px solid rgba(226,232,240,0.95);
            border-radius: 6px;
            selection-background-color: #06b6d4;
            selection-color: white;
        }

        QCheckBox {
            spacing: 8px;
            color: #3a4752;
        }

        QCheckBox::indicator {
            width: 18px;
            height: 18px;
            border: 1px solid rgba(203,213,224,0.95);
            border-radius: 5px;
            background: #ffffff;
        }

        QCheckBox::indicator:hover {
            border: 1px solid rgba(79,148,255,0.6);
            background: #eef6ff;
        }

        QCheckBox::indicator:checked {
            background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:1, stop:0 #06b6d4, stop:1 #2b6cb0);
            border: 1px solid rgba(30,105,160,0.9);
        }

        QPushButton {
            min-height: 36px;
            border-radius: 10px;
            padding: 8px 14px;
            border: 1px solid rgba(226,232,240,0.9);
            background: qlineargradient(spread:pad, x1:0, y1:0, x2:0, y2:1, stop:0 #ffffff, stop:1 #f4faff);
            color: #2d3748;
            font-weight: 600;
        }

        QPushButton:hover {
            background: qlineargradient(spread:pad, x1:0, y1:0, x2:0, y2:1, stop:0 #e7f5ff, stop:1 #d9eeff);
            border: 1px solid rgba(79,148,255,0.45);
            color: #1d4ed8;
        }

        QPushButton:pressed {
            background: qlineargradient(spread:pad, x1:0, y1:0, x2:0, y2:1, stop:0 #cde8ff, stop:1 #a8d9ff);
            border: 1px solid rgba(79,148,255,0.6);
            color: #153e75;
        }

        QPushButton:focus {
            border: 2px solid rgba(6,182,212,0.25);
            background: #f0fdff;
        }

        QPushButton#topBtn_connect, QPushButton#topBtn_migrate, QPushButton#topBtn_add, QPushButton#topBtn_view, QPushButton#topBtn_logs {
            border-radius: 8px;
            padding: 6px 10px;
            background: transparent;
            border: none;
            color: #374151;
        }

        QPushButton#topBtn_connect:hover, QPushButton#topBtn_migrate:hover, QPushButton#topBtn_add:hover, QPushButton#topBtn_view:hover, QPushButton#topBtn_logs:hover  {
            background: rgba(6,182,212,0.12);
            color: #0891b2;
        }

        QWidget#buttonsContainer QPushButton {
            text-align: left;
            padding: 10px 14px;
            border: none;
            border-radius: 12px;
            margin: 4px 6px;
            background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0, stop:0 #ffffff, stop:1 #f3faff);
            color: #22303a;
            font-weight: 500;
        }

        QWidget#buttonsContainer QPushButton:hover {
            background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0, stop:0 #e0f6ff, stop:1 #caecff);
            color: #1d4ed8;
        }

        QWidget#buttonsContainer QPushButton:pressed {
            background: #b7e1fb;
            color: #153e75;
        }

        QLabel.small { 
            font-size: 11px; 
            color: #66757a; 
        }

        QProgressBar {
            border: 1px solid rgba(226,232,240,0.95);
            border-radius: 8px;
            background: #f7fbfd;
            text-align: center;
            min-height: 18px;
        }

        QProgressBar::chunk {
            background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0, stop:0 #06b6d4, stop:1 #2b6cb0);
            border-radius: 8px;
        }

        QListView, QTreeView, QTableView {
            background: #ffffff;
            border: 1px solid rgba(226,232,240,0.95);
            border-radius: 8px;
            outline: none;
        }

        QHeaderView::section {
            background: linear-gradient(#fbfdff, #f3f8ff);
            padding: 8px 10px;
            border: none;
            border-right: 1px solid rgba(226,232,240,0.9);
            border-bottom: 1px solid rgba(226,232,240,0.9);
            font-weight: 700;
            color: #344e63;
        }

        QTreeView::item, QListView::item, QTableView::item {
            padding: 8px 10px;
            border-bottom: 1px solid transparent;
            color: #2d3748;
        }

        QTreeView::item:selected, QListView::item:selected, QTableView::item:selected {
            background: qlineargradient(spread:pad, x1:0, y1:0, x2:1, y2:0, stop:0 #06b6d4, stop:1 #2b6cb0);
            color: #ffffff;
            border-radius: 6px;
        }

        QTreeView::item:hover, QListView::item:hover, QTableView::item:hover {
            background: #eaf8fb;
            border-bottom: 1px solid #cfeff7;
        }

        QTreeView::item:selected:hover, QListView::item:selected:hover, QTableView::item:selected:hover {
            background: #145a80;
            color: #ffffff;
        }

        QTreeView::branch:has-siblings:!adjoins-item { border-image: url(vline.png) 0; }
        QTreeView::branch:has-siblings:adjoins-item { border-image: url(branch-more.png) 0; }
        QTreeView::branch:!has-children:!has-siblings:adjoins-item { border-image: url(branch-end.png) 0; }
        QTreeView::branch:has-children:!has-siblings:closed, QTreeView::branch:closed:has-children:has-siblings { border-image: none; image: url(branch-closed.png); }
        QTreeView::branch:open:has-children:!has-siblings, QTreeView::branch:open:has-children:has-siblings  { border-image: none; image: url(branch-open.png); }
        """

        self.setStyleSheet(base)


def main():
    app = QApplication(sys.argv)

    try:
        db = Database()
        w = AppMainWindow(db=db)
        w.resize(1200, 550)
        w.show()
        sys.exit(app.exec())
    except Exception:
        print('вв')

    connected_params: Dict[str, str] = {}

    def connect_callback(params: Dict[str, str]) -> bool:
        try:
            temp_db = Database(params)
            temp_db.close()
            connected_params.update(params)
            return True
        except Exception:
            return False

    def recreate_callback(connection_info: Dict[str, str]) -> bool:
        try:
            tmp = Database(connection_info)
            result = tmp.recreate_tables()
            tmp.close()
            return bool(result)
        except Exception:
            return False
    dlg = ConnectionDialog(
        None,
        connect_callback=connect_callback,
        recreate_callback=recreate_callback
    )
    def _on_connected(info: Dict[str, str]):
        connected_params.update(info)
        dlg.accept()
    dlg.connected.connect(_on_connected)
    dlg.exec()
    if not connected_params:
        QMessageBox.critical(None, "Ошибка", "Подключение не выполнено. Приложение будет закрыто.")
        sys.exit(1)
    try:
        db = Database(connected_params)
        w = AppMainWindow(db=db)
        w.resize(1200, 550)
        w.show()
        sys.exit(app.exec())
    except Exception:
        QMessageBox.critical(None, "Ошибка", "Не удалось создать главное окно после подключения к БД.")
        sys.exit(1)
if __name__ == "__main__":
    main()
