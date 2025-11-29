from typing import Dict, Any, Optional, List
import re
from datetime import date, datetime

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QTableView, QMessageBox,
    QDialog, QTextEdit, QLineEdit, QComboBox, QCheckBox, QDateEdit, QDateTimeEdit, QTextEdit,
    QFormLayout, QDialogButtonBox, QSpinBox, QSizePolicy, QFrame, QSplitter
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


class ViewsDialog(QDialog):

    def __init__(self, db, parent=None):
        super().__init__(parent)
        self.db = db
        self.setWindowTitle("представления")
        self.setMinimumSize(800, 400)
        self._current_view_sql = None
        self._viewer: Optional[TableResultWidget] = None
        self.setup_ui()
        self._load_views()

    def setup_ui(self):
        layout = QVBoxLayout(self)
        top_row = QHBoxLayout()
        self.normal_btn = QPushButton("Представления")
        self.mat_btn = QPushButton("Материализованные представления")
        self.normal_btn.setCheckable(True)
        self.mat_btn.setCheckable(True)
        self.normal_btn.setChecked(True)
        self.normal_btn.clicked.connect(self._on_type_changed)
        self.mat_btn.clicked.connect(self._on_type_changed)
        top_row.addWidget(self.normal_btn)
        top_row.addWidget(self.mat_btn)
        top_row.addStretch()
        layout.addLayout(top_row)

        control_row = QHBoxLayout()
        self.views_combo = QComboBox()
        self.show_btn = QPushButton("Показать")
        self.show_btn.clicked.connect(self._on_show_view)
        control_row.addWidget(QLabel("Выбрать:"))
        control_row.addWidget(self.views_combo)
        control_row.addWidget(self.show_btn)
        layout.addLayout(control_row)

        self.viewer_frame = QFrame()
        self.viewer_layout = QVBoxLayout(self.viewer_frame)
        self.viewer_layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.viewer_frame)

    def _on_type_changed(self):
        sender = self.sender()
        if sender == self.normal_btn:
            self.mat_btn.setChecked(False)
            self.normal_btn.setChecked(True)
        else:
            self.normal_btn.setChecked(False)
            self.mat_btn.setChecked(True)
        self._load_views()

    def _load_views(self):
        try:
            with self.db.engine.connect() as conn:
                if self.normal_btn.isChecked():
                    q = text(
                        "SELECT table_schema, table_name "
                        "FROM information_schema.views "
                        "WHERE table_schema NOT IN ('pg_catalog','information_schema') "
                        "ORDER BY table_schema, table_name"
                    )
                    res = conn.execute(q).fetchall()
                    items = [f"{r.table_schema}.{r.table_name}" for r in res]
                else:
                    q = text(
                        "SELECT schemaname, matviewname "
                        "FROM pg_matviews "
                        "WHERE schemaname NOT IN ('pg_catalog','information_schema') "
                        "ORDER BY schemaname, matviewname"
                    )
                    res = conn.execute(q).fetchall()
                    items = [f"{r.schemaname}.{r.matviewname}" for r in res]
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось получить список представлений:\n{str(e)}")
            items = []
        self.views_combo.clear()
        self.views_combo.addItems(items)

    def _on_show_view(self):
        sel = self.views_combo.currentText().strip()
        if not sel:
            QMessageBox.information(self, "Нет выбора", "Вы не выбрали представление")
            return
        if '.' in sel:
            schema, name = sel.split('.', 1)
        else:
            schema, name = None, sel
        if schema:
            sql = f'SELECT * FROM "{schema}"."{name}"'
        else:
            sql = f'SELECT * FROM "{name}"'
        for i in reversed(range(self.viewer_layout.count())):
            w = self.viewer_layout.itemAt(i).widget()
            if w:
                w.setParent(None)
        try:
            viewer = TableResultWidget(self.db, sql, parent=self)
            self.viewer_layout.addWidget(viewer)
            self._viewer = viewer
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось показать представление:\n{e}")


class SaveViewDialog(QDialog):
    def __init__(self, db, sql_to_save: str, materialized: bool = False, parent=None):
        super().__init__(parent)
        self.db = db
        self.sql_to_save = sql_to_save
        self.materialized = materialized
        self.setWindowTitle("Сохранить материализованное представление" if materialized else "Сохранить представление")
        self.setMinimumWidth(400)
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)
        form = QFormLayout()
        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText('view_name')
        self.replace_chk = QCheckBox("OR REPLACE")
        form.addRow("Имя:", self.name_edit)
        form.addRow(self.replace_chk)
        layout.addLayout(form)
        self.preview = QTextEdit()
        self.preview.setReadOnly(True)
        self.preview.setPlainText(self.sql_to_save or "")
        layout.addWidget(QLabel("SQL:"))
        layout.addWidget(self.preview)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self._on_accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def _on_accept(self):
        name = self.name_edit.text().strip()
        if not name:
            QMessageBox.information(self, "Введите имя", "Введите имя представления")
            return
        if '.' in name:
            schema, vname = name.split('.', 1)
            full_name = f'"{schema}"."{vname}"'
        else:
            full_name = f'"{name}"'
        is_mat = "MATERIALIZED " if self.materialized else ""
        if self.replace_chk.isChecked():
            if is_mat:
                stmt = f'DROP MATERIALIZED VIEW IF EXISTS {name} CASCADE; CREATE MATERIALIZED VIEW {full_name} AS {self.sql_to_save}'
            else:
                replace_clause = "OR REPLACE "
                stmt = f'CREATE {replace_clause}VIEW {full_name} AS {self.sql_to_save}'
        else:
            stmt = f'CREATE {is_mat}VIEW {full_name} AS {self.sql_to_save}'
        try:
            with self.db.engine.begin() as conn:
                conn.execute(text(stmt))
            QMessageBox.information(self, "Успех", "Представление создано успешно")
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка создания", f"Ошибка при создании представления:\n{e}")


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
        self.refresh_btn = QPushButton("обн.")
        self.refresh_btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.refresh_btn.clicked.connect(self.load_and_build)
        self.edit_small_btn = QPushButton("ред.")
        self.edit_small_btn.setFixedHeight(100)
        self.edit_small_btn.setFixedWidth(60)
        self.edit_small_btn.setVisible(False)
        self.edit_small_btn.clicked.connect(self._on_action_button_clicked)
        self.reset_test_data_btn = QPushButton("сброс до тестовых")
        self.reset_test_data_btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.reset_test_data_btn.clicked.connect(lambda: self.db.reset())

        self.view_views_btn = QPushButton("Просм. представлений")
        self.view_views_btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.view_views_btn.clicked.connect(self._on_view_views_clicked)

        self.save_view_btn = QPushButton("Сохр. представление")
        self.save_view_btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.save_view_btn.clicked.connect(self._on_save_view_clicked)

        self.save_matview_btn = QPushButton("Сохр. мат. представление")
        self.save_matview_btn.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.Fixed)
        self.save_matview_btn.clicked.connect(self._on_save_matview_clicked)

        info_l.addStretch()
        info_l.addWidget(self.edit_small_btn)
        info_l.addWidget(self.reset_test_data_btn)
        info_l.addWidget(self.refresh_btn)

        info_l.addWidget(self.view_views_btn)
        info_l.addWidget(self.save_view_btn)
        info_l.addWidget(self.save_matview_btn)

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
            try:
                self.info_label.setText("Error executing query")
            except Exception:
                pass
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

    def _on_view_views_clicked(self):
        try:
            dlg = ViewsDialog(self.db, parent=self)
            dlg.exec()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось открыть просмотр представлений:\n{e}")

    def _on_save_view_clicked(self):
        if not self.sql or not self.sql.strip():
            QMessageBox.information(self, "Нет SQL", "Нечего сохранять")
            return
        try:
            dlg = SaveViewDialog(self.db, self.sql, materialized=False, parent=self)
            dlg.exec()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось сохранить представление:\n{e}")

    def _on_save_matview_clicked(self):
        if not self.sql or not self.sql.strip():
            QMessageBox.information(self, "Нет SQL", "Нечего сохранять ")
            return
        try:
            dlg = SaveViewDialog(self.db, self.sql, materialized=True, parent=self)
            dlg.exec()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось сохранить материализованное представление:\n{e}")
