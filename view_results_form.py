from typing import Dict, Any, Optional, List
import re
from datetime import date, datetime

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QTableView, QMessageBox,
    QDialog, QTextEdit, QLineEdit, QComboBox, QCheckBox, QDateEdit, QDateTimeEdit, QTextEdit,
    QFormLayout, QDialogButtonBox, QSpinBox, QSizePolicy
)
from PySide6.QtGui import QStandardItemModel, QStandardItem, QIntValidator, QDoubleValidator
from PySide6.QtCore import Qt, Signal

from sqlalchemy import text, select, update, and_
from sqlalchemy import Table as SATable
from sqlalchemy.types import Enum as SAEnum, Boolean, Integer, Float, Date, DateTime, ARRAY, JSON

from edit_form import EditDialog
from validators import validate_table_data

class OperationDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Операция над строкой")
        self.op = None
        self.params = {}
        self.setup_ui()

    def setup_ui(self):
        self.setMinimumWidth(360)
        layout = QVBoxLayout(self)
        top = QHBoxLayout()
        lbl = QLabel("Операция")
        self.op_combo = QComboBox()
        self.op_combo.addItems(["UPPER", "LOWER", "SUBSTRING", "TRIM", "LPAD", "RPAD"])
        self.op_combo.currentTextChanged.connect(self._on_op_changed)
        top.addWidget(lbl)
        top.addWidget(self.op_combo)
        layout.addLayout(top)

        self.param_form = QFormLayout()
        self.substr_start_edit = QLineEdit()
        self.substr_start_edit.setPlaceholderText("начало")
        self.substr_len_edit = QLineEdit()
        self.substr_len_edit.setPlaceholderText("длина")
        self.pad_length_edit = QLineEdit()
        self.pad_length_edit.setPlaceholderText("итоговая длина")
        self.pad_text_edit = QLineEdit()
        self.pad_text_edit.setPlaceholderText("текст заполнителя")

        self.param_form.addRow("SUBSTRING: start", self.substr_start_edit)
        self.param_form.addRow("SUBSTRING: length", self.substr_len_edit)
        self.param_form.addRow("LPAD/RPAD: total length", self.pad_length_edit)
        self.param_form.addRow("LPAD/RPAD: pad text", self.pad_text_edit)
        layout.addLayout(self.param_form)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)
        self._on_op_changed(self.op_combo.currentText())

    def _on_op_changed(self, op):
        is_sub = op == "SUBSTRING"
        is_pad = op in ("LPAD", "RPAD")
        self.substr_start_edit.setVisible(is_sub)
        self.substr_len_edit.setVisible(is_sub)
        self.param_form.labelForField(self.substr_start_edit).setVisible(is_sub)
        self.param_form.labelForField(self.substr_len_edit).setVisible(is_sub)
        self.pad_length_edit.setVisible(is_pad)
        self.pad_text_edit.setVisible(is_pad)
        self.param_form.labelForField(self.pad_length_edit).setVisible(is_pad)
        self.param_form.labelForField(self.pad_text_edit).setVisible(is_pad)

    def accept(self):
        self.op = self.op_combo.currentText()
        self.params = {
            "start": self.substr_start_edit.text(),
            "length": self.substr_len_edit.text(),
            "pad_length": self.pad_length_edit.text(),
            "pad_text": self.pad_text_edit.text(),
        }
        super().accept()


class TableResultWidget(QWidget):
    editRequested = Signal(object, dict)

    def __init__(self, db, sql: str, parent=None, max_rows=1000):
        super().__init__(parent)
        self.db = db
        self.sql = sql
        self.max_rows = max_rows
        self._columns: List[str] = []
        self._rows = []
        self._model: Optional[QStandardItemModel] = None
        self._primary_table: Optional[str] = None
        self._primary_table_pk_cols: List[str] = []
        self._current_index = None
        self._sel_connected = False
        self.setup_ui()
        self.load_and_build()

    def setup_ui(self):
        self.layout = QVBoxLayout(self)
        self.info_row = QWidget()
        info_l = QHBoxLayout(self.info_row)
        info_l.setContentsMargins(0, 0, 0, 0)
        #self.info_label = QLabel("Executing")
        self.refresh_btn = QPushButton("Обновить")
        self.refresh_btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.refresh_btn.clicked.connect(self.load_and_build)
        self.edit_small_btn = QPushButton("ред.")
        #self.edit_small_btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.edit_small_btn.setFixedHeight(100)
        self.edit_small_btn.setFixedWidth(60)
        self.edit_small_btn.setVisible(False)
        self.edit_small_btn.clicked.connect(self._on_action_button_clicked)
        self.reset_test_data_btn = QPushButton("Перезаписать тестовые данные")
        self.reset_test_data_btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.reset_test_data_btn.clicked.connect(lambda: self.db.reset())
        info_l.addStretch()
        info_l.addWidget(self.edit_small_btn)
        info_l.addWidget(self.reset_test_data_btn)
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
            QMessageBox.critical(self, "Query error", f"Error executing SQL:\n{str(e)}")
            self.info_label.setText("Error executing query")
            self.table_view.setModel(QStandardItemModel(0, 0))
            return

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
        if self.table_view.selectionModel() and not self._sel_connected:
            try:
                self.table_view.selectionModel().selectionChanged.connect(self._on_selection_changed)
                self._sel_connected = True
            except Exception:
                pass
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
        if col == last_col:
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
            except Exception as e:
                QMessageBox.critical(self, "Edit error", str(e))
        else:
            self._current_index = index
            sel = self.table_view.selectionModel().selectedIndexes()
            self.edit_small_btn.setVisible(bool(sel))

    def _on_selection_changed(self, selected, deselected):
        indexes = selected.indexes()
        if not indexes:
            self._current_index = None
            self.edit_small_btn.setVisible(False)
            return
        index = indexes[0]
        self._current_index = index
        self.edit_small_btn.setVisible(True)

    def _on_action_button_clicked(self):
        sel_indexes = [i for i in self.table_view.selectionModel().selectedIndexes() if i.isValid()]
        if not sel_indexes:
            QMessageBox.information(self, "Нет выделения", "Нет выделенных ячеек")
            return
        sel_indexes = [i for i in sel_indexes if i.column() < len(self._columns)]
        if not sel_indexes:
            QMessageBox.information(self, "Нет выделения", "Выделены только служебные колонки")
            return
        dlg = OperationDialog(self)
        if dlg.exec() != QDialog.Accepted:
            return
        op = dlg.op
        params = dlg.params
        updated = 0
        skipped = 0
        grouped_by_row: Dict[int, List] = {}
        for idx in sel_indexes:
            grouped_by_row.setdefault(idx.row(), []).append(idx.column())
        with self.db.engine.begin() as conn:
            for row_idx, cols in grouped_by_row.items():
                row_values = [self._model.item(row_idx, c).text() for c in range(len(self._columns))]
                pk_where, bind_params = self._build_pk(row_idx, row_values)
                for col_idx in cols:
                    col_name = self._columns[col_idx]
                    target_expr, extra_params = self.build_expr(col_name, op, params)
                    stmt = f'UPDATE {self._primary_table or ""} SET {col_name} = {target_expr} WHERE {pk_where}'
                    params_all = {}
                    params_all.update(bind_params)
                    params_all.update(extra_params)
                    try:
                        conn.execute(text(stmt), params_all)
                        updated += 1
                    except Exception:
                        try:
                            conn.execute(text(stmt.replace("SUBSTRING(", "SUBSTR(")), params_all)
                            updated += 1
                        except Exception:
                            skipped += 1
                            continue
        self.load_and_build()
        QMessageBox.information(self, "Готово", f"Обновлено: {updated}\nПропущено: {skipped}")

    def _build_pk(self, row_idx, row_values):
        binds = {}
        conditions = []
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
                val = row_values[idx]
                param_name = f"pk_{pk_col}_{row_idx}"
                binds[param_name] = None if val == "" else val
                conditions.append(f"{pk_col} = :{param_name}")
        else:
            for i, colname in enumerate(self._columns):
                val = row_values[i]
                param_name = f"col_{i}_{row_idx}"
                binds[param_name] = None if val == "" else val
                conditions.append(f"{colname} IS NULL" if binds[param_name] is None else f"{colname} = :{param_name}")
        where_clause = " AND ".join(conditions) if conditions else "1=0"
        return where_clause, binds

    def build_expr(self, col_name, op, params):
        extra = {}
        if op == "UPPER":
            expr = f"UPPER({col_name})"
        elif op == "LOWER":
            expr = f"LOWER({col_name})"
        elif op == "TRIM":
            expr = f"TRIM({col_name})"
        elif op == "SUBSTRING":
            start = params.get("start") or ""
            length = params.get("length") or ""
            extra["start"] = int(start) if start.isdigit() else 1
            extra["length"] = int(length) if length.isdigit() else 1
            expr = f"SUBSTRING({col_name}, :start, :length)"
        elif op == "LPAD":
            pad_len = params.get("pad_length") or ""
            pad_text = params.get("pad_text") or " "
            extra["pad_len"] = int(pad_len) if pad_len.isdigit() else 1
            extra["pad_text"] = pad_text
            expr = f"LPAD({col_name}, :pad_len, :pad_text)"
        elif op == "RPAD":
            pad_len = params.get("pad_length") or ""
            pad_text = params.get("pad_text") or " "
            extra["pad_len"] = int(pad_len) if pad_len.isdigit() else 1
            extra["pad_text"] = pad_text
            expr = f"RPAD({col_name}, :pad_len, :pad_text)"
        else:
            expr = col_name
        return expr, extra

    def _extract_table_from_sql(self, sql):
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

    def _cleanup_table_name(self, raw):
        if not raw:
            return None
        raw = raw.strip()
        parts = re.split(r'\.', raw)
        last = parts[-1]
        last = last.strip()
        if last.startswith('"') and last.endswith('"'):
            last = last[1:-1]
        return last