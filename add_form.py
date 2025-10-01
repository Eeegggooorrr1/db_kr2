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


class AlterTableDialog(QDialog):
    tablesChanged = Signal()

    def __init__(self, table_name: str, db, parent=None):
        super().__init__(parent)
        self.db = db
        self.table = db.get_table(table_name)
        print(self.table)
        self.setWindowTitle(f"Alter table — {table_name}")

        self.layout = QVBoxLayout(self)
        self.form_layout = QFormLayout()

        self.table_name_edit = QLineEdit(table_name)
        self.form_layout.addRow("Table name:", self.table_name_edit)

        self.columns_widget = QWidget()
        self.columns_layout = QVBoxLayout(self.columns_widget)
        self.columns_layout.setContentsMargins(0, 0, 0, 0)

        self.column_widgets = {}
        self.next_column_id = 1
        self.old_table_name = self.table_name_edit.text()
        self.old_data = {}

        for col in self.table.columns:
            column_id, column_widget = self.add_column_widget(col)
            self.old_data[column_id] = self.get_column_data(column_widget)

        self.form_layout.addRow("Columns:", self.columns_widget)

        btn_add_column = QPushButton("Add column")
        btn_add_column.clicked.connect(lambda: self.add_column_widget())

        self.layout.addLayout(self.form_layout)
        self.layout.addWidget(btn_add_column)

        btn_row = QHBoxLayout()
        self.btn_apply = QPushButton("Apply")
        self.btn_cancel = QPushButton("Cancel")
        btn_row.addStretch(1)
        btn_row.addWidget(self.btn_cancel)
        btn_row.addWidget(self.btn_apply)
        self.layout.addLayout(btn_row)

        self.btn_apply.clicked.connect(self.on_apply)
        self.btn_cancel.clicked.connect(self.reject)

    def add_column_widget(self, col=None):
        widget = QWidget()
        layout = QHBoxLayout(widget)
        layout.setContentsMargins(0, 0, 0, 0)

        column_id = self.next_column_id
        self.next_column_id += 1

        widget.column_id = column_id

        name_edit = QLineEdit(col.name if col is not None else "")
        type_combo = QComboBox()
        type_combo.addItems(["INTEGER", "TEXT", "REAL", "BLOB", "DATE"])

        not_null_check = QCheckBox("NOT NULL")
        unique_check = QCheckBox("UNIQUE")
        pk_check = QCheckBox("PK")

        default_edit = QLineEdit()
        default_edit.setPlaceholderText("DEFAULT value")

        check_edit = QLineEdit()
        check_edit.setPlaceholderText("CHECK condition")

        fk_edit = QLineEdit()
        fk_edit.setPlaceholderText("FK table")

        fk_column_edit = QLineEdit()
        fk_column_edit.setPlaceholderText("FK column")

        btn_remove = QPushButton("X", styleSheet="color: red;")
        btn_remove.clicked.connect(lambda: self.remove_column_widget(widget))
        text_edits = [name_edit, default_edit, check_edit, fk_edit, fk_column_edit]

        for edit in text_edits:
            edit.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
            edit.setMinimumWidth(95)

        if col is not None:
            type_combo.setCurrentText(str(col.type))
            not_null_check.setChecked(not col.nullable)
            unique_check.setChecked(
                bool(col.name in [i['column_names'][0] for i in self.db.insp.get_unique_constraints(self.table.name)]))
            pk_check.setChecked(bool(col.primary_key))

            try:
                type_text = str(col.type)
                type_combo.setCurrentText(type_text)

                if col.server_default is not None:
                    default_edit.setText(str(col.server_default.arg))

                check_constraints = [f"({check.sqltext})" for check in col.table.constraints
                                     if hasattr(check, 'sqltext') and col.name in str(check.sqltext)]
                if check_constraints:
                    check_edit.setText(check_constraints[0])

                if hasattr(col, 'foreign_keys') and col.foreign_keys:
                    for fk in col.foreign_keys:
                        fk_edit.setText(fk.column.table.name)
                        fk_column_edit.setText(fk.column.name)

            except:
                pass

        layout.addWidget(QLabel("Name:"))
        layout.addWidget(name_edit, 1)
        layout.addWidget(QLabel("Type:"))
        layout.addWidget(type_combo, 1)
        layout.addWidget(not_null_check, 1)
        layout.addWidget(unique_check)
        layout.addWidget(pk_check, 1)
        layout.addWidget(QLabel("Default:"))
        layout.addWidget(default_edit, 1)
        layout.addWidget(QLabel("Check:"))
        layout.addWidget(check_edit, 1)
        layout.addWidget(QLabel("FK Table:"))
        layout.addWidget(fk_edit, 1)
        layout.addWidget(QLabel("FK Column:"))
        layout.addWidget(fk_column_edit, 1)
        layout.addWidget(btn_remove)

        self.columns_layout.addWidget(widget)
        self.column_widgets[column_id] = widget

        return column_id, widget

    def get_column_data(self, widget):
        children = widget.findChildren(QLineEdit) + widget.findChildren(QComboBox) + widget.findChildren(QCheckBox)

        name_edit = None
        type_combo = None
        not_null_check = None
        unique_check = None
        pk_check = None
        default_edit = None
        check_edit = None
        fk_edit = None
        fk_column_edit = None

        for child in children:
            if isinstance(child, QLineEdit):
                if child.placeholderText() == "DEFAULT value":
                    default_edit = child
                elif child.placeholderText() == "CHECK condition":
                    check_edit = child
                elif child.placeholderText() == "FK table":
                    fk_edit = child
                elif child.placeholderText() == "FK column":
                    fk_column_edit = child
                else:
                    name_edit = child
            elif isinstance(child, QComboBox):
                type_combo = child
            elif isinstance(child, QCheckBox):
                text = child.text()
                if text == "NOT NULL":
                    not_null_check = child
                elif text == "UNIQUE":
                    unique_check = child
                elif text == "PK":
                    pk_check = child

        if name_edit and type_combo:
            return {
                'name': name_edit.text(),
                'type': type_combo.currentText(),
                'not_null': not_null_check.isChecked() if not_null_check else False,
                'unique': unique_check.isChecked() if unique_check else False,
                'primary_key': pk_check.isChecked() if pk_check else False,
                'default': default_edit.text() if default_edit else None,
                'check': check_edit.text() if check_edit else None,
                'fk_table': fk_edit.text() if fk_edit else None,
                'fk_column': fk_column_edit.text() if fk_column_edit else None
            }
        return {}

    def remove_column_widget(self, widget):
        column_id = widget.column_id
        self.columns_layout.removeWidget(widget)
        widget.deleteLater()
        del self.column_widgets[column_id]

    def gather_changes(self):
        new_data = {}
        self.new_table_name = self.table_name_edit.text()
        for column_id, widget in self.column_widgets.items():
            new_data[column_id] = self.get_column_data(widget)

        return new_data

    def on_apply(self):
        new_data = self.gather_changes()
        try:
            self.db.alter_table(self.old_table_name, self.new_table_name, self.old_data, new_data)
            QMessageBox.information(self, "Успешно", "Структура изменена")
            self.tablesChanged.emit()
            try:
                self.accept()
            except Exception:
                self.setParent(None)
                self.deleteLater()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))

class AddDialog(QDialog):
    def __init__(self, table_name: str, db: Database, parent=None):
        super().__init__(parent)
        self.db = db
        self.table: Table = db.get_table(table_name)
        self.setWindowTitle(f"Add row — {table_name}")

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
