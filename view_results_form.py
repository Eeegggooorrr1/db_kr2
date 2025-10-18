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

from edit_form import EditDialog
from validators import validate_table_data

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
        self.info_label = QLabel("Executing")
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
        #self.info_label.setText(f"Rows: {total_rows} (showing {len(displayed_rows)})")
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