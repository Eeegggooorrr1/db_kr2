from PySide6.QtCore import Signal, Qt
from PySide6.QtWidgets import (
    QDialog, QFormLayout, QLineEdit, QPushButton, QVBoxLayout, QMessageBox, QLabel, QWidget, QHBoxLayout, QComboBox,
    QCheckBox, QSizePolicy, QDateEdit, QDateTimeEdit, QTextEdit, QScrollArea, QFrame
)
from PySide6.QtGui import QIntValidator, QDoubleValidator
from typing import Dict, Any
from sqlalchemy import Integer, inspect
from sqlalchemy import Boolean, Enum as SAEnum, Float, Date, DateTime, String, JSON, ARRAY
from datetime import datetime, date

from db import Database
from validators import validate_table_data
from sqlalchemy import Table

class FieldLine:
    def __init__(self, container_widget, editor, get_value_fn, error_label: QLabel):
        self.container = container_widget
        self.editor = editor
        self._get_value = get_value_fn
        self.error_label = error_label

    def set_error(self, text: str):
        if text:
            self.error_label.setText(text)
            self.error_label.setVisible(True)
        else:
            self.error_label.clear()
            self.error_label.setVisible(False)

    def get_value(self):
        try:
            return self._get_value()
        except Exception:
            return None

class InputBuilder:
    def build_field(self, col) -> FieldLine:
        placeholder_parts = [str(col.type)]
        if col.primary_key:
            placeholder_parts.append("primary key")
        if getattr(col, "_inspector_unique", False) or getattr(col, "unique", False):
            placeholder_parts.append("unique")
        placeholder_parts.append("nullable" if col.nullable else "not null")
        if getattr(col, "server_default", None) is not None:
            try:
                placeholder_parts.append(f"default = {col.server_default.arg}")
            except Exception:
                pass
        checks = [f"check ({c.sqltext})" for c in col.table.constraints if hasattr(c, 'sqltext') and col.name in str(c.sqltext)]
        placeholder_parts.extend(checks)
        placeholder = ", ".join(filter(None, placeholder_parts))

        error_label = QLabel()
        error_label.setStyleSheet("color: red;")
        error_label.setVisible(False)

        if isinstance(col.type, SAEnum):
            editor = QComboBox()
            enums = getattr(col.type, "enums", []) or []
            editor.addItem("")
            for e in enums:
                editor.addItem(str(e))
            def getter():
                t = editor.currentText()
                return t if t != "" else None
            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)
            return FieldLine(container, editor, getter, error_label)

        if isinstance(col.type, Boolean):
            editor = QCheckBox()
            if col.nullable:
                editor.setTristate(True)
            def getter():
                st = editor.checkState()
                if editor.isTristate():
                    if st == Qt.PartiallyChecked:
                        return None
                    return True if st == Qt.Checked else False
                return True if st == Qt.Checked else False
            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)
            return FieldLine(container, editor, getter, error_label)

        if isinstance(col.type, Integer):
            editor = QLineEdit()
            editor.setValidator(QIntValidator())
            def getter():
                txt = editor.text().strip()
                return int(txt) if txt != "" else None
            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)
            try:
                editor.setPlaceholderText(placeholder)
            except Exception:
                pass
            return FieldLine(container, editor, getter, error_label)

        if isinstance(col.type, (Float, )):
            editor = QLineEdit()
            editor.setValidator(QDoubleValidator())
            def getter():
                txt = editor.text().strip()
                if txt == "":
                    return None
                try:
                    return float(txt)
                except Exception:
                    return None
            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)
            try:
                editor.setPlaceholderText(placeholder)
            except Exception:
                pass
            return FieldLine(container, editor, getter, error_label)

        if isinstance(col.type, Date):
            editor = QDateEdit()
            editor.setCalendarPopup(True)
            editor.setDisplayFormat("yyyy-MM-dd")
            def getter():
                qd = editor.date()
                if not qd.isValid():
                    return None
                return date(qd.year(), qd.month(), qd.day())
            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)
            return FieldLine(container, editor, getter, error_label)

        if isinstance(col.type, DateTime):
            editor = QDateTimeEdit()
            editor.setCalendarPopup(True)
            editor.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
            def getter():
                qdt = editor.dateTime()
                if not qdt.isValid():
                    return None
                try:
                    return qdt.toPython()
                except Exception:
                    dt = qdt.date()
                    t = qdt.time()
                    return datetime(dt.year(), dt.month(), dt.day(), t.hour(), t.minute(), t.second())
            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)
            return FieldLine(container, editor, getter, error_label)

        if isinstance(col.type, ARRAY):
            editor = QTextEdit()
            editor.setAcceptRichText(False)
            def getter():
                txt = editor.toPlainText()
                lines = [ln.strip() for ln in txt.splitlines() if ln.strip() != ""]
                if not lines:
                    return None
                return lines
            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)
            try:
                editor.setPlaceholderText(placeholder)
            except Exception:
                pass
            return FieldLine(container, editor, getter, error_label)

        if isinstance(col.type, JSON):
            editor = QTextEdit()
            def getter():
                txt = editor.toPlainText().strip()
                return txt if txt != "" else None
            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)
            return FieldLine(container, editor, getter, error_label)

        editor = QLineEdit()
        def getter():
            txt = editor.text().strip()
            return txt if txt != "" else None
        container = QWidget()
        vbox = QVBoxLayout(container)
        vbox.setContentsMargins(0, 0, 0, 0)
        vbox.addWidget(editor)
        vbox.addWidget(error_label)
        try:
            editor.setPlaceholderText(placeholder)
        except Exception:
            pass
        return FieldLine(container, editor, getter, error_label)

class AddDialog(QDialog):
    tablesChanged = Signal(str)
    def __init__(self, table_name: str, db: Database, table_manager = None,  parent=None):
        super().__init__(parent)
        self.db = db
        self.table: Table = db.get_table(table_name)
        self.setWindowTitle(f"Add row  {table_name}")
        self.table_manager = table_manager
        self.layout = QVBoxLayout(self)
        self.form_layout = QFormLayout()
        self.field_map = {}
        constrs = [i['column_names'][0] for i in self.db.insp.get_unique_constraints(self.table.name)]
        for c in self.table.columns:
            setattr(c, "_inspector_unique", c.name in constrs)
        builder = InputBuilder()
        for col in self.table.columns:
            if col.autoincrement and col.primary_key:
                continue

            fl = builder.build_field(col)

            label_text = col.name
            if not col.nullable:
                label_text += " *"

            self.form_layout.addRow(label_text, fl.container)
            self.field_map[col.name] = fl

        self.layout.addLayout(self.form_layout)

        btn_row = QHBoxLayout()
        self.btn_submit = QPushButton("Добавить")
        self.btn_cancel = QPushButton("Отмена")
        btn_row.addStretch(1)
        btn_row.addWidget(self.btn_cancel)
        btn_row.addWidget(self.btn_submit)
        self.layout.addLayout(btn_row)

        self.btn_submit.clicked.connect(self.on_submit)
        self.btn_cancel.clicked.connect(self.reject)

    def clear_errors(self):
        for fl in self.field_map.values():
            fl.set_error("")

    def set_errors(self, errors):
        for k, v in errors.items():
            fl = self.field_map.get(k)
            if fl:
                fl.set_error(v)

    def gather_raw(self):
        data = {}
        for name, fl in self.field_map.items():
            val = fl.get_value()
            data[name] = val
        return data

    def on_submit(self):
        self.clear_errors()
        raw = self.gather_raw()
        validated, errors = validate_table_data(self.table, raw, db=self.db)
        if errors:
            self.set_errors(errors)
            return

        try:
            self.db.insert_row(self.table, validated)
            QMessageBox.information(self, "Ок", "Запись добавлена")
            self.accept()

        except Exception as e:
            QMessageBox.critical(self, "Ошибка вставки", str(e))
