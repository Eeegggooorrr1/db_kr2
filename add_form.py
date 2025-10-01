from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QDialog, QFormLayout, QLineEdit, QPushButton, QVBoxLayout, QMessageBox, QLabel, QWidget, QHBoxLayout, QComboBox,
    QCheckBox, QSizePolicy
)
from PySide6.QtGui import QIntValidator
from typing import Dict, Any
from sqlalchemy import Integer, inspect

from db import Database
from validators import validate_table_data
from sqlalchemy import Table

class FieldLine:
    def __init__(self, editor: QLineEdit, error_label: QLabel):
        self.editor = editor
        self.error_label = error_label

    def set_error(self, text: str):
        if text:
            self.error_label.setText(text)
            self.error_label.setVisible(True)
        else:
            self.error_label.clear()
            self.error_label.setVisible(False)



class AddDialog(QDialog):
    tablesChanged = Signal(str)
    def __init__(self, table_name: str, db: Database, table_manager = None,  parent=None):
        super().__init__(parent)
        self.db = db
        self.table: Table = db.get_table(table_name)
        self.setWindowTitle(f"Add row — {table_name}")
        self.table_manager = table_manager
        self.layout = QVBoxLayout(self)
        self.form_layout = QFormLayout()
        self.field_map = {}
        constrs = [i['column_names'][0] for i in self.db.insp.get_unique_constraints(self.table.name)]
        print(constrs)
        for col in self.table.columns:
            print(col.__dict__)
            is_unique = col.name in constrs
            data = ', '.join(filter(None, [str(col.type), 'primary key' if col.primary_key else None,
                                    'unique' if is_unique else None, 'nullable' if col.nullable else 'not null',
                                    f'default = {col.server_default.arg}' if col.server_default is not None else None]
                                    + [f"check ({check.sqltext})" for check in col.table.constraints
                                       if hasattr(check, 'sqltext') and col.name in str(check.sqltext)]))

            if col.autoincrement and col.primary_key:
                continue

            editor = QLineEdit()
            if isinstance(col.type, Integer):
                editor.setValidator(QIntValidator())
            editor.setPlaceholderText(data)

            error_label = QLabel()
            error_label.setStyleSheet("color: red;")
            error_label.setVisible(False)

            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)

            label_text = col.name
            if not col.nullable:
                label_text += " *"
            if col.unique:
                label_text += " (unique)"

            self.form_layout.addRow(label_text, container)
            self.field_map[col.name] = FieldLine(editor, error_label)

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

    def set_errors(self, errors: Dict[str, str]):
        for k, v in errors.items():
            fl = self.field_map.get(k)
            if fl:
                fl.set_error(v)

    def gather_raw(self) -> Dict[str, Any]:
        data: Dict[str, Any] = {}
        for name, fl in self.field_map.items():
            txt = fl.editor.text()
            data[name] = txt if txt != "" else None
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
