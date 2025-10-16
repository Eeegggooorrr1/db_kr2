from typing import Optional, Dict, Callable
import sys

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QHBoxLayout, QVBoxLayout, QPushButton, QLabel,
    QComboBox, QStackedWidget, QFrame, QSplitter, QSizePolicy, QScrollArea, QMessageBox
)
from PySide6.QtCore import Qt, QSize

from connect_form import ConnectionDialog
from db import Database
from add_form import AddDialog
from alter_form import AlterTableDialog
from refresh_manager import TableManager
from view_form import SQLStubWindow
from view_results_form import TableResultWidget


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
        self.active_view_widget = None
        self.view_container_page = None
        self.view_container_layout = None
        self.active_view_result_widget = None
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
                "connect": (0, 1),
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
            background: #f7fafc; 
        }

        QFrame#topBar { 
            background: #ffffff; 
            border-bottom: 1px solid #e2e8f0; 
            padding: 4px 0px;
        }

        QLabel#appTitle { 
            font-weight: 600; 
            font-size: 16px; 
            color: #1a202c; 
        }

        QScrollBar:vertical {
            background: #f8f9fa;
            width: 10px;
            margin: 0px;
            border-radius: 5px;
        }

        QScrollBar::handle:vertical {
            background: #cbd5e0;
            border-radius: 5px;
            min-height: 20px;
        }

        QScrollBar::handle:vertical:hover {
            background: #a0aec0;
        }

        QScrollBar::handle:vertical:pressed {
            background: #718096;
        }

        QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
            height: 0px;
        }

        QScrollBar:horizontal {
            background: #f8f9fa;
            height: 10px;
            margin: 0px;
            border-radius: 5px;
        }

        QScrollBar::handle:horizontal {
            background: #cbd5e0;
            border-radius: 5px;
            min-width: 20px;
        }

        QScrollBar::handle:horizontal:hover {
            background: #a0aec0;
        }

        QCheckBox {
            spacing: 6px;
            color: #4a5568;
        }

        QCheckBox::indicator {
            width: 16px;
            height: 16px;
            border: 1px solid #cbd5e0;
            border-radius: 3px;
            background: #ffffff;
        }

        QCheckBox::indicator:hover {
            border: 1px solid #a0aec0;
        }

        QCheckBox::indicator:checked {
            background: #4299e1;
            border: 1px solid #4299e1;
        }

        QCheckBox::indicator:checked:hover {
            background: #3182ce;
            border: 1px solid #3182ce;
        }

        QCheckBox::indicator:checked:disabled {
            background: #a0aec0;
            border: 1px solid #a0aec0;
        }

        QCheckBox::indicator:unchecked:disabled {
            background: #edf2f7;
            border: 1px solid #e2e8f0;
        }

        QPushButton {
            min-height: 30px;
            border-radius: 6px;
            padding: 6px 12px;
            border: 1px solid #e2e8f0;
            background: #ffffff;
            color: #4a5568;
            font-weight: 500;
        }

        QPushButton:hover {
            background: #edf2f7;
            border: 1px solid #cbd5e0;
        }

        QPushButton:pressed {
            background: #e2e8f0;
            border: 1px solid #a0aec0;
        }

        QPushButton:focus {
            border: 1px solid #4299e1;
        }

        QPushButton#topBtn_connect, QPushButton#topBtn_migrate, QPushButton#topBtn_add, QPushButton#topBtn_view {
            border-radius: 4px;
            padding: 6px 10px;
            background: transparent;
            border: none;
            color: #4a5568;
        }

        QPushButton#topBtn_connect:hover, QPushButton#topBtn_migrate:hover, QPushButton#topBtn_add:hover, QPushButton#topBtn_view:hover {
            background: #ebf8ff;
            color: #3182ce;
        }

        QWidget#leftPanel { 
            background: transparent; 
        }

        QWidget#buttonsContainer QPushButton {
            text-align: left;
            padding: 10px 12px;
            border: none;
            border-radius: 4px;
            margin: 1px 2px;
            background: #ffffff;
        }

        QWidget#buttonsContainer QPushButton:hover {
            background: #ebf8ff;
            color: #3182ce;
        }

        QWidget#buttonsContainer QPushButton:pressed {
            background: #bee3f8;
        }

        QLineEdit, QComboBox, QTextEdit { 
            border: 1px solid #e2e8f0; 
            border-radius: 4px; 
            padding: 6px 8px; 
            background: #ffffff; 
            font-size: 13px;
            selection-background-color: #4299e1;
        }

        QLineEdit:focus, QComboBox:focus, QTextEdit:focus {
            border: 1px solid #4299e1;
        }

        QLineEdit:hover, QComboBox:hover, QTextEdit:hover {
            border: 1px solid #cbd5e0;
        }

        QComboBox::drop-down {
            border: none;
            width: 20px;
        }

        QComboBox::down-arrow {
            border-left: 4px solid transparent;
            border-right: 4px solid transparent;
            border-top: 4px solid #718096;
            width: 0px;
            height: 0px;
        }

        QComboBox QAbstractItemView {
            background: white;
            border: 1px solid #e2e8f0;
            border-radius: 4px;
            selection-background-color: #4299e1;
            selection-color: white;
        }

        QLabel.small { 
            font-size: 11px; 
            color: #718096; 
        }

        QProgressBar {
            border: 1px solid #e2e8f0;
            border-radius: 3px;
            background: #f7fafc;
            text-align: center;
        }

        QProgressBar::chunk {
            background: #4299e1;
            border-radius: 2px;
        }

        QListView, QTreeView, QTableView {
            background: #ffffff;
            border: 1px solid #e2e8f0;
            border-radius: 4px;
            outline: none;
        }

        QHeaderView::section {
            background: #f7fafc;
            padding: 6px 8px;
            border: none;
            border-right: 1px solid #e2e8f0;
            border-bottom: 1px solid #e2e8f0;
            font-weight: 600;
            color: #4a5568;
        }

        QHeaderView::section:last {
            border-right: none;
        }

        QTreeView::item, QListView::item, QTableView::item {
            padding: 6px 8px;
            border-bottom: 1px solid transparent;
            color: #4a5568;
        }

        QTreeView::item:selected, QListView::item:selected, QTableView::item:selected {
            background: #4299e1;
            color: #ffffff;
            border-radius: 2px;
        }

        QTreeView::item:hover, QListView::item:hover, QTableView::item:hover {
            background: #ebf8ff;
            border-bottom: 1px solid #bee3f8;
        }

        QTreeView::item:selected:hover, QListView::item:selected:hover, QTableView::item:selected:hover {
            background: #3182ce;
            color: #ffffff;
        }

        QTreeView::branch:has-siblings:!adjoins-item {
            border-image: url(vline.png) 0;
        }

        QTreeView::branch:has-siblings:adjoins-item {
            border-image: url(branch-more.png) 0;
        }

        QTreeView::branch:!has-children:!has-siblings:adjoins-item {
            border-image: url(branch-end.png) 0;
        }

        QTreeView::branch:has-children:!has-siblings:closed,
        QTreeView::branch:closed:has-children:has-siblings {
            border-image: none;
            image: url(branch-closed.png);
        }

        QTreeView::branch:open:has-children:!has-siblings,
        QTreeView::branch:open:has-children:has-siblings  {
            border-image: none;
            image: url(branch-open.png);
        }
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
