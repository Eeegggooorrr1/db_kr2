from typing import Dict, Any, Optional
import re
from datetime import date, datetime

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QTableView, QMessageBox,
    QDialog, QTextEdit, QLineEdit, QComboBox, QCheckBox, QDateEdit, QDateTimeEdit, QTextEdit,
    QFormLayout
)
from PySide6.QtGui import QStandardItemModel, QStandardItem, QIntValidator, QDoubleValidator
from PySide6.QtCore import Qt, Signal

from sqlalchemy import text, select, update, and_
from sqlalchemy import Table as SATable
from sqlalchemy.types import Enum as SAEnum, Boolean, Integer, Float, Date, DateTime, ARRAY, JSON

from validators import validate_table_data


class FieldLine:
    def __init__(self, container_widget: QWidget, editor: QWidget, get_value_fn, error_label: QLabel, set_value_fn=None):
        self.container = container_widget
        self.editor = editor
        self._get_value = get_value_fn
        self.error_label = error_label
        self._set_value = set_value_fn

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

    def set_value(self, val):
        if not self._set_value:
            return
        try:
            self._set_value(val)
        except Exception:
            pass

# InputBuilder
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
            def setter(v):
                if v is None:
                    editor.setCurrentIndex(0)
                else:
                    s = str(v)
                    idx = editor.findText(s)
                    if idx >= 0:
                        editor.setCurrentIndex(idx)
                    else:
                        editor.addItem(s)
                        editor.setCurrentText(s)
            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)
            return FieldLine(container, editor, getter, error_label, setter)

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
            def setter(v):
                if editor.isTristate():
                    if v is None:
                        editor.setCheckState(Qt.PartiallyChecked)
                    else:
                        editor.setCheckState(Qt.Checked if v else Qt.Unchecked)
                else:
                    editor.setChecked(bool(v))
            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)
            return FieldLine(container, editor, getter, error_label, setter)

        if isinstance(col.type, Integer):
            editor = QLineEdit()
            editor.setValidator(QIntValidator())
            def getter():
                txt = editor.text().strip()
                return int(txt) if txt != "" else None
            def setter(v):
                editor.setText("" if v is None else str(v))
            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)
            try:
                editor.setPlaceholderText(placeholder)
            except Exception:
                pass
            return FieldLine(container, editor, getter, error_label, setter)

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
            def setter(v):
                editor.setText("" if v is None else str(v))
            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)
            try:
                editor.setPlaceholderText(placeholder)
            except Exception:
                pass
            return FieldLine(container, editor, getter, error_label, setter)

        if isinstance(col.type, Date):
            editor = QDateEdit()
            editor.setCalendarPopup(True)
            editor.setDisplayFormat("yyyy-MM-dd")
            def getter():
                qd = editor.date()
                if not qd.isValid():
                    return None
                return date(qd.year(), qd.month(), qd.day())
            def setter(v):
                if v is None:
                    try:
                        editor.clear()
                    except Exception:
                        pass
                    return
                if isinstance(v, date):
                    editor.setDate(v)
                else:
                    try:
                        editor.setDate(date.fromisoformat(str(v)))
                    except Exception:
                        pass
            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)
            return FieldLine(container, editor, getter, error_label, setter)

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
            def setter(v):
                if v is None:
                    try:
                        editor.clear()
                    except Exception:
                        pass
                    return
                if isinstance(v, datetime):
                    try:
                        editor.setDateTime(v)
                    except Exception:
                        pass
                else:
                    try:
                        editor.setDateTime(datetime.fromisoformat(str(v)))
                    except Exception:
                        pass
            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)
            return FieldLine(container, editor, getter, error_label, setter)

        if isinstance(col.type, ARRAY):
            editor = QTextEdit()
            editor.setAcceptRichText(False)
            def getter():
                txt = editor.toPlainText()
                lines = [ln.strip() for ln in txt.splitlines() if ln.strip() != ""]
                if not lines:
                    return None
                return lines
            def setter(v):
                if v is None:
                    editor.setPlainText("")
                elif isinstance(v, (list, tuple)):
                    editor.setPlainText("\n".join(str(x) for x in v))
                else:
                    editor.setPlainText(str(v))
            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)
            try:
                editor.setPlaceholderText(placeholder)
            except Exception:
                pass
            return FieldLine(container, editor, getter, error_label, setter)

        if isinstance(col.type, JSON):
            editor = QTextEdit()
            def getter():
                txt = editor.toPlainText().strip()
                return txt if txt != "" else None
            def setter(v):
                if v is None:
                    editor.setPlainText("")
                else:
                    editor.setPlainText(str(v))
            container = QWidget()
            vbox = QVBoxLayout(container)
            vbox.setContentsMargins(0, 0, 0, 0)
            vbox.addWidget(editor)
            vbox.addWidget(error_label)
            return FieldLine(container, editor, getter, error_label, setter)

        editor = QLineEdit()
        def getter():
            txt = editor.text().strip()
            return txt if txt != "" else None
        def setter(v):
            editor.setText("" if v is None else str(v))
        container = QWidget()
        vbox = QVBoxLayout(container)
        vbox.setContentsMargins(0, 0, 0, 0)
        vbox.addWidget(editor)
        vbox.addWidget(error_label)
        try:
            editor.setPlaceholderText(placeholder)
        except Exception:
            pass
        return FieldLine(container, editor, getter, error_label, setter)

class AddDialog(QDialog):
    tablesChanged = Signal(str)
    def __init__(self, table_name: str, db, table_manager = None,  parent=None):
        super().__init__(parent)
        self.db = db
        self.table: SATable = db.get_table(table_name)
        self.setWindowTitle(f"Add row — {table_name}")
        self.table_manager = table_manager
        self.layout = QVBoxLayout(self)
        self.form_layout = QFormLayout()
        self.field_map: Dict[str, FieldLine] = {}
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
            try:
                self.tablesChanged.emit(self.table.name)
            except Exception:
                pass
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка вставки", str(e))

class EditDialog(QDialog):
    tablesChanged = Signal(str)
    def __init__(self, table_name: str, db, pk_dict: Dict[str, Any], table_manager=None, parent=None):
        super().__init__(parent)
        self.db = db
        self.table: SATable = db.get_table(table_name)
        self.pk_dict = pk_dict or {}
        self.setWindowTitle(f"Edit row — {table_name}")
        self.table_manager = table_manager
        self.layout = QVBoxLayout(self)
        self.form_layout = QFormLayout()
        self.field_map: Dict[str, FieldLine] = {}
        constrs = [i['column_names'][0] for i in self.db.insp.get_unique_constraints(self.table.name)]
        for c in self.table.columns:
            setattr(c, "_inspector_unique", c.name in constrs)
        builder = InputBuilder()
        for col in self.table.columns:
            fl = builder.build_field(col)
            label_text = col.name
            if not col.nullable:
                label_text += " *"
            self.form_layout.addRow(label_text, fl.container)
            self.field_map[col.name] = fl
            if col.primary_key:
                editor = fl.editor
                try:
                    if hasattr(editor, "setReadOnly"):
                        editor.setReadOnly(True)
                    if hasattr(editor, "setEnabled"):
                        editor.setEnabled(False)
                except Exception:
                    pass
        self.layout.addLayout(self.form_layout)
        btn_row = QHBoxLayout()
        self.btn_submit = QPushButton("Save")
        self.btn_cancel = QPushButton("Cancel")
        btn_row.addStretch(1)
        btn_row.addWidget(self.btn_cancel)
        btn_row.addWidget(self.btn_submit)
        self.layout.addLayout(btn_row)
        self.btn_submit.clicked.connect(self.on_submit)
        self.btn_cancel.clicked.connect(self.reject)
        self._load_row_and_prefill()

    def _load_row_and_prefill(self):
        conds = []
        for pk_col in self.table.primary_key.columns:
            name = pk_col.name
            if name not in self.pk_dict:
                QMessageBox.warning(self, "PK missing", f"Primary key value for '{name}' not provided.")
                return
            val = self.pk_dict[name]
            conds.append(self.table.c[name] == val)
        session = self.db.SessionLocal()
        try:
            stmt = select(self.table).where(and_(*conds)).limit(1)
            r = session.execute(stmt).mappings().first()
            if not r:
                QMessageBox.warning(self, "Not found", "Row not found in database.")
                return
            for col in self.table.columns:
                fl = self.field_map.get(col.name)
                if not fl:
                    continue
                val = r.get(col.name)
                fl.set_value(val)
        finally:
            session.close()

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
        update_data = {k: v for k, v in validated.items() if k not in [c.name for c in self.table.primary_key.columns]}
        if not update_data:
            QMessageBox.information(self, "No changes", "No updatable fields provided.")
            return
        conds = []
        for pk_col in self.table.primary_key.columns:
            name = pk_col.name
            if name not in self.pk_dict:
                QMessageBox.critical(self, "Missing PK", f"Primary key '{name}' value missing.")
                return
            conds.append(self.table.c[name] == self.pk_dict[name])
        session = self.db.SessionLocal()
        try:
            stmt = update(self.table).where(and_(*conds)).values(**update_data)
            session.execute(stmt)
            session.commit()
            QMessageBox.information(self, "Ok", "Row updated")
            try:
                self.tablesChanged.emit(self.table.name)
            except Exception:
                pass
            self.accept()
        except Exception as e:
            session.rollback()
            QMessageBox.critical(self, "Update error", str(e))
        finally:
            session.close()

# TableResultWidget
class TableResultWidget(QWidget):
    editRequested = Signal(object, dict)
    def __init__(self, db, sql: str, parent=None, max_rows=1000):
        super().__init__(parent)
        self.db = db
        self.sql = sql
        self.max_rows = max_rows
        self._columns = []
        self._rows = []
        self._model: Optional[QStandardItemModel] = None
        self._primary_table: Optional[str] = None
        self._primary_table_pk_cols = []
        self.setup_ui()
        self.load_and_build()

    def setup_ui(self):
        self.layout = QVBoxLayout(self)
        self.info_row = QWidget()
        info_l = QHBoxLayout(self.info_row)
        info_l.setContentsMargins(0,0,0,0)
        self.info_label = QLabel("Executing...")
        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.load_and_build)
        info_l.addWidget(self.info_label)
        info_l.addStretch()
        info_l.addWidget(self.refresh_btn)
        self.layout.addWidget(self.info_row)
        self.table_view = QTableView()
        self.table_view.clicked.connect(self._on_table_clicked)
        self.layout.addWidget(self.table_view)

    def load_and_build(self):
        try:
            with self.db.engine.connect() as conn:
                result = conn.execute(text(self.sql))
                try:
                    columns = result.keys()
                except Exception:
                    columns = []
                rows = result.fetchall()
        except Exception as e:
            err = str(e)
            QMessageBox.critical(self, "Query error", f"Error executing SQL:\n{err}")
            self.info_label.setText("Error executing query")
            self.table_view.setModel(QStandardItemModel(0, 0))
            return

        total_rows = len(rows)
        displayed_rows = rows[: self.max_rows]
        model = QStandardItemModel()
        model.setColumnCount(len(columns) + 1)
        header_labels = [str(c) for c in columns] + ["Edit"]
        model.setHorizontalHeaderLabels(header_labels)
        for row in displayed_rows:
            items = []
            for val in row:
                item = QStandardItem("" if val is None else str(val))
                item.setEditable(False)
                items.append(item)
            edit_item = QStandardItem("Edit")
            edit_item.setEditable(False)
            edit_item.setTextAlignment(Qt.AlignCenter)
            items.append(edit_item)
            model.appendRow(items)
        self.table_view.setModel(model)
        self.info_label.setText(f"Rows: {total_rows} (showing {len(displayed_rows)})")
        self._columns = list(columns)
        self._rows = displayed_rows
        self._model = model
        self._primary_table = self._extract_table_from_sql(self.sql)
        self._primary_table_pk_cols = []
        if self._primary_table:
            try:
                tbl = self.db.get_table(self._primary_table)
                self._primary_table_pk_cols = [c.name for c in tbl.primary_key.columns]
            except Exception:
                try:
                    pk_info = self.db.insp.get_pk_constraint(self._primary_table)
                    self._primary_table_pk_cols = pk_info.get("constrained_columns", []) or []
                except Exception:
                    self._primary_table_pk_cols = []

    def _on_table_clicked(self, index):
        if not index.isValid():
            return
        col = index.column()
        model = self._model
        last_col = model.columnCount() - 1
        if col != last_col:
            return
        row = index.row()
        row_values = [model.item(row, c).text() for c in range(len(self._columns))]
        pk_dict = {}
        if self._primary_table_pk_cols:
            for pk_col in self._primary_table_pk_cols:
                try:
                    idx = self._columns.index(pk_col)
                except ValueError:
                    found_idx = None
                    for i, colname in enumerate(self._columns):
                        if colname.endswith(f".{pk_col}") or colname == f"{self._primary_table}.{pk_col}":
                            found_idx = i
                            break
                    idx = found_idx
                if idx is None:
                    continue
                val = model.item(row, idx).text()
                pk_dict[pk_col] = None if val == "" else val
        else:
            if len(self._columns) >= 1:
                pk_dict[self._columns[0]] = None if row_values[0] == "" else row_values[0]

        try:
            dlg = EditDialog(self._primary_table, self.db, pk_dict, parent=self)
            dlg.tablesChanged.connect(lambda t: self.load_and_build())
            res = dlg.exec()
            if res == QDialog.Accepted:
                self.load_and_build()
        except NameError:
            QMessageBox.critical(self, "Missing EditDialog", "EditDialog is not available/imported.")
        except Exception as e:
            QMessageBox.critical(self, "Edit error", str(e))

    def _extract_table_from_sql(self, sql: str) -> Optional[str]:
        if not sql:
            return None
        s = sql.strip()
        m = re.search(r'\bFROM\b\s*(\()?\s*([A-Za-z0-9_."]+)', s, flags=re.IGNORECASE)
        if not m:
            return None
        if m.group(1) == '(':
            return None
        tbl = m.group(2)
        return self._cleanup_table_name(tbl)

    def _cleanup_table_name(self, raw: str) -> Optional[str]:
        if not raw:
            return None
        raw = raw.strip()
        parts = re.split(r'\.', raw)
        last = parts[-1]
        last = last.strip()
        if last.startswith('"') and last.endswith('"'):
            last = last[1:-1]
        return last