from typing import Dict, List, Tuple

from PySide6.QtGui import QStandardItemModel, QStandardItem
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QGroupBox, QDialog, QComboBox, QLineEdit,
    QFormLayout, QLabel, QListWidget, QTextEdit,
    QScrollArea, QCheckBox, QFrame, QMessageBox, QSpinBox, QTableView
)
from PySide6.QtCore import Qt, Signal
import sys

from sqlalchemy import text

AGG_FUNCS = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']
TEXT_OPS = ['LIKE', '~', '~*', '!~', '!~*']


def qualified_from_tuple(col):
    return f"{col[0]}.{col[1]}"


class ConditionDialog(QDialog):
    def __init__(self, columns, parent=None, title='Добавить условие'):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.columns = columns or []
        self.setup_ui()

    def setup_ui(self):
        layout = QFormLayout(self)
        self.col_cb = QComboBox()
        self.col_cb.addItems([f"{t}.{c}" for t, c in self.columns])
        self.op_cb = QComboBox()
        self.op_cb.addItems(['=', '!=', '<', '<=', '>', '>='] + TEXT_OPS)
        self.val_le = QLineEdit()
        add_btn = QPushButton('Добавить')
        add_btn.clicked.connect(self.accept)
        layout.addRow('Столбец', self.col_cb)
        layout.addRow('Оператор', self.op_cb)
        layout.addRow('Значение', self.val_le)
        layout.addRow(add_btn)

    def get_condition(self):
        val = self.val_le.text().replace("'", "''")
        col = self.col_cb.currentText() if self.col_cb.currentIndex() >= 0 else ''
        op = self.op_cb.currentText() if self.op_cb.currentIndex() >= 0 else '='
        if col == '':
            return "1=1"
        return f"{col} {op} '{val}'"


class JoinDialog(QDialog):
    def __init__(self, schema, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Добавить соединение")
        self.schema = schema or {}
        self.setup_ui()

    def setup_ui(self):
        layout = QFormLayout(self)
        self.left_table_cb = QComboBox()
        self.left_table_cb.addItems(sorted(self.schema.keys()))
        self.right_table_cb = QComboBox()
        self.right_table_cb.addItems(sorted(self.schema.keys()))
        self.left_field_cb = QComboBox()
        self.right_field_cb = QComboBox()
        self.update_fields()
        self.left_table_cb.currentIndexChanged.connect(self.update_fields)
        self.right_table_cb.currentIndexChanged.connect(self.update_fields)
        self.join_type_cb = QComboBox()
        self.join_type_cb.addItems(['INNER', 'LEFT', 'RIGHT', 'FULL'])
        add_btn = QPushButton('Добавить')
        add_btn.clicked.connect(self.accept)
        layout.addRow('Левая таблица', self.left_table_cb)
        layout.addRow('Правая таблица', self.right_table_cb)
        layout.addRow('Поле (слева)', self.left_field_cb)
        layout.addRow('Поле (справа)', self.right_field_cb)
        layout.addRow('Тип', self.join_type_cb)
        layout.addRow(add_btn)

    def update_fields(self):
        lt = self.left_table_cb.currentText()
        rt = self.right_table_cb.currentText()
        self.left_field_cb.clear()
        self.right_field_cb.clear()
        if lt and lt in self.schema:
            self.left_field_cb.addItems(self.schema.get(lt, []))
        if rt and rt in self.schema:
            self.right_field_cb.addItems(self.schema.get(rt, []))

    def get_join(self):
        left = self.left_table_cb.currentText()
        right = self.right_table_cb.currentText()
        lf = self.left_field_cb.currentText()
        rf = self.right_field_cb.currentText()
        jtype = self.join_type_cb.currentText()
        desc = f"{jtype} JOIN {right} ON {left}.{lf} = {right}.{rf}"
        return {
            'type': jtype,
            'left': left,
            'right': right,
            'lf': lf,
            'rf': rf,
            'desc': desc
        }


class SQLStubWindow(QWidget):
    apply_sql = Signal(str)

    def __init__(self, db=None, parent=None):
        super().__init__(parent)
        self.db = db
        self.selected_columns = []
        self.where_conditions = []
        self.having_conditions = []
        self.group_by = []
        self.order_by = []
        self.joins = []
        self.aggregates = []
        self.table_to_col_cbs = {}
        self.all_col_cbs = []
        self.schema = {}
        self._load_schema_from_db()
        self.setup_ui()
        #self.apply_styles()
        self.update_sql_preview()

    def _load_schema_from_db(self):
        if self.db is None:
            self.schema = {}
            return
        try:
            tables = self.db.list_tables() or []
        except Exception:
            tables = []
        schema = {}
        for t in tables:
            try:
                table_obj = self.db.get_table(t)
                cols = [c.name for c in table_obj.columns]
                schema[t] = cols
            except Exception:
                continue
        self.schema = schema

    def setup_ui(self):
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        container = QWidget()
        scroll.setWidget(container)
        layout_outer = QVBoxLayout(self)
        layout_outer.setContentsMargins(0, 0, 0, 0)
        layout_outer.addWidget(scroll)
        main_layout = QVBoxLayout(container)
        main_layout.setContentsMargins(12, 12, 12, 12)
        main_layout.setSpacing(12)

        sel_group = QGroupBox('Выбор столбцов')
        sel_layout = QVBoxLayout()

        for table in sorted(self.schema.keys()):
            cols = self.schema.get(table, [])
            header_widget = QWidget()
            header_l = QHBoxLayout(header_widget)
            header_l.setContentsMargins(0, 0, 0, 0)
            table_cb = QCheckBox()
            table_label = QLabel(f"<b>{table}</b>")
            header_l.addWidget(table_cb)
            header_l.addWidget(table_label)
            header_l.addStretch()
            sel_layout.addWidget(header_widget)
            cols_widget = QWidget()
            cols_layout = QVBoxLayout(cols_widget)
            cols_layout.setContentsMargins(24, 0, 0, 6)
            cols_layout.setSpacing(4)
            col_cbs = []
            for c in cols:
                cb = QCheckBox(f"{table}.{c}")
                cb.table = table
                cb.col = c
                cb.stateChanged.connect(self.on_checkbox_changed)
                cols_layout.addWidget(cb)
                col_cbs.append(cb)
                self.all_col_cbs.append(cb)
            self.table_to_col_cbs[table] = col_cbs

            def make_table_toggle(t):
                def toggle(state):
                    for cb in self.table_to_col_cbs.get(t, []):
                        cb.setChecked(bool(state))
                    self.on_checkbox_changed(None)
                return toggle

            table_cb.toggled.connect(make_table_toggle(table))
            sel_layout.addWidget(cols_widget)

        sel_group.setLayout(sel_layout)
        main_layout.addWidget(sel_group)

        join_group = QGroupBox('Соединения')
        jlayout = QVBoxLayout()
        self.join_list = QListWidget()
        jlayout.addWidget(self.join_list)
        jbtn_row = QHBoxLayout()
        add_join_btn = QPushButton('Добавить')
        add_join_btn.clicked.connect(self.open_add_join_dialog)
        clear_join_btn = QPushButton('Очистить')
        clear_join_btn.clicked.connect(self.clear_join)
        jbtn_row.addWidget(add_join_btn)
        jbtn_row.addWidget(clear_join_btn)
        jbtn_row.addStretch()
        jlayout.addLayout(jbtn_row)
        join_group.setLayout(jlayout)
        main_layout.addWidget(join_group)

        where_group = QGroupBox('WHERE')
        wlayout = QVBoxLayout()
        self.where_list = QListWidget()
        wlayout.addWidget(self.where_list)
        wbtn_row = QHBoxLayout()
        add_where_btn = QPushButton('Добавить')
        add_where_btn.clicked.connect(self.add_where_condition)
        clear_where_btn = QPushButton('Очистить')
        clear_where_btn.clicked.connect(self.clear_where)
        wbtn_row.addWidget(add_where_btn)
        wbtn_row.addWidget(clear_where_btn)
        wbtn_row.addStretch()
        wlayout.addLayout(wbtn_row)
        where_group.setLayout(wlayout)
        main_layout.addWidget(where_group)

        group_group = QGroupBox('GROUP BY')
        glayout = QVBoxLayout()
        self.group_list = QListWidget()
        glayout.addWidget(self.group_list)
        gbtn_row = QHBoxLayout()
        add_group_btn = QPushButton('group')
        add_group_btn.clicked.connect(self.add_group_by)
        add_agg_btn = QPushButton('агрегат')
        add_agg_btn.clicked.connect(self.add_aggregate)
        clear_group_btn = QPushButton('Очистить')
        clear_group_btn.clicked.connect(self.clear_group_by)
        gbtn_row.addWidget(add_group_btn)
        gbtn_row.addWidget(add_agg_btn)
        gbtn_row.addWidget(clear_group_btn)
        gbtn_row.addStretch()
        glayout.addLayout(gbtn_row)
        group_group.setLayout(glayout)
        main_layout.addWidget(group_group)

        having_group = QGroupBox('HAVING')
        hlay = QVBoxLayout()
        self.having_list = QListWidget()
        hlay.addWidget(self.having_list)
        hbtn_row = QHBoxLayout()
        add_having_btn = QPushButton('Добавить')
        add_having_btn.clicked.connect(self.add_having)
        clear_having_btn = QPushButton('Очистить')
        clear_having_btn.clicked.connect(self.clear_having)
        hbtn_row.addWidget(add_having_btn)
        hbtn_row.addWidget(clear_having_btn)
        hbtn_row.addStretch()
        hlay.addLayout(hbtn_row)
        having_group.setLayout(hlay)
        main_layout.addWidget(having_group)

        order_group = QGroupBox('ORDER BY')
        ol = QVBoxLayout()
        self.order_list = QListWidget()
        ol.addWidget(self.order_list)
        obtn_row = QHBoxLayout()
        add_order_btn = QPushButton('Добавить')
        add_order_btn.clicked.connect(self.add_order_by)
        clear_order_btn = QPushButton('Очистить')
        clear_order_btn.clicked.connect(self.clear_order_by)
        obtn_row.addWidget(add_order_btn)
        obtn_row.addWidget(clear_order_btn)
        obtn_row.addStretch()
        ol.addLayout(obtn_row)
        order_group.setLayout(ol)
        main_layout.addWidget(order_group)

        preview_group = QGroupBox('запрос')
        pl = QVBoxLayout()
        self.sql_preview = QTextEdit()
        self.sql_preview.setReadOnly(True)
        pl.addWidget(self.sql_preview)
        btns = QHBoxLayout()
        apply_btn = QPushButton('Применить')
        apply_btn.clicked.connect(self.on_apply_clicked)
        btns.addWidget(apply_btn)
        btns.addStretch()
        pl.addLayout(btns)
        preview_group.setLayout(pl)
        main_layout.addWidget(preview_group)

        main_layout.addStretch()

    def apply_styles(self):
        self.setStyleSheet('''
            QWidget { background: #f7f9fb; }
            QGroupBox { background: white; border: 1px solid #d0d7de; border-radius: 8px; padding: 8px; }
            QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 4px; }
            QPushButton { padding: 6px 10px; border-radius: 6px; }
            QPushButton:hover { background: #e6f0ff; }
            QCheckBox { padding: 4px; }
            QLabel { color: #2b2f33; }
            QListWidget { background: #ffffff; border: 1px solid #e6e9ee; border-radius:6px; }
            QTextEdit { background: #0f1720; color: #e6eef8; font-family: monospace; border-radius:6px; }
        ''')

    def on_checkbox_changed(self, state):
        sel = []
        for cb in self.all_col_cbs:
            try:
                if cb.isChecked():
                    sel.append((cb.table, cb.col))
            except Exception:
                continue
        self.selected_columns = sel
        self.update_sql_preview()

    def open_add_join_dialog(self):
        dlg = JoinDialog(self.schema, self)
        if dlg.exec():
            j = dlg.get_join()
            if j['left'] and j['right'] and j['lf'] and j['rf']:
                self.joins.append(j)
                self.join_list.addItem(j['desc'])
                QMessageBox.information(self, 'Соединение добавлено', f"Добавлено: {j['desc']}")
                self.update_sql_preview()
            else:
                QMessageBox.warning(self, 'Соединение не добавлено', 'Выберите таблицы и поля для соединения.')

    def remove_selected_join(self):
        row = self.join_list.currentRow()
        if row >= 0:
            self.join_list.takeItem(row)
            try:
                del self.joins[row]
            except Exception:
                pass
            self.update_sql_preview()

    def add_where_condition(self):
        cols = [(t, c) for t in self.schema for c in self.schema[t]]
        dlg = ConditionDialog(cols, self, title='Добавить WHERE-условие')
        if dlg.exec():
            cond = dlg.get_condition()
            self.where_conditions.append(cond)
            self.where_list.addItem(cond)
            self.update_sql_preview()

    def remove_selected_where(self):
        row = self.where_list.currentRow()
        if row >= 0:
            self.where_list.takeItem(row)
            try:
                del self.where_conditions[row]
            except Exception:
                pass
            self.update_sql_preview()

    def clear_where(self):
        self.where_list.clear()
        self.where_conditions.clear()
        self.update_sql_preview()

    def add_group_by(self):
        dlg = QDialog(self)
        dlg.setWindowTitle('Добавить столбец в GROUP BY')
        layout = QFormLayout(dlg)
        cb = QComboBox()
        cb.addItems([f"{t}.{c}" for t in self.schema for c in self.schema[t]])
        add_btn = QPushButton('Добавить')
        add_btn.clicked.connect(dlg.accept)
        layout.addRow(cb)
        layout.addRow(add_btn)
        if dlg.exec():
            val = cb.currentText()
            if val:
                self.group_by.append(val)
                self.group_list.addItem(val)
                self.update_sql_preview()

    def remove_selected_group(self):
        row = self.group_list.currentRow()
        if row >= 0:
            self.group_list.takeItem(row)
            try:
                del self.group_by[row]
            except Exception:
                pass
            self.update_sql_preview()

    def clear_group_by(self):
        self.group_list.clear()
        self.group_by.clear()
        self.aggregates.clear()
        self.update_sql_preview()

    def add_aggregate(self):
        dlg = QDialog(self)
        dlg.setWindowTitle('Добавить агрегат')
        layout = QFormLayout(dlg)
        col_cb = QComboBox()
        col_cb.addItems([f"{t}.{c}" for t in self.schema for c in self.schema[t]])
        agg_cb = QComboBox()
        agg_cb.addItems(AGG_FUNCS)
        alias_le = QLineEdit()
        add_btn = QPushButton('Добавить')
        add_btn.clicked.connect(dlg.accept)
        layout.addRow('Столбец', col_cb)
        layout.addRow('Функция', agg_cb)
        layout.addRow('Псевдоним', alias_le)
        layout.addRow(add_btn)
        if dlg.exec():
            fn = agg_cb.currentText()
            col = col_cb.currentText()
            alias = alias_le.text() or f"{fn.lower()}_{col.replace('.', '_')}"
            if col:
                self.aggregates.append((fn, col, alias))
                self.group_list.addItem(f"{fn}({col}) AS {alias}")
                self.update_sql_preview()

    def add_having(self):
        if not self.group_by and not self.aggregates:
            QMessageBox.warning(self, 'HAVING недоступно', 'Нужно добавить GROUP BY или агрегат перед использованием HAVING.')
            return
        cols = [(t, c) for t in self.schema for c in self.schema[t]]
        dlg = ConditionDialog(cols, self, title='Добавить HAVING-условие')
        if dlg.exec():
            cond = dlg.get_condition()
            self.having_conditions.append(cond)
            self.having_list.addItem(cond)
            self.update_sql_preview()

    def remove_selected_having(self):
        row = self.having_list.currentRow()
        if row >= 0:
            self.having_list.takeItem(row)
            try:
                del self.having_conditions[row]
            except Exception:
                pass
            self.update_sql_preview()

    def clear_having(self):
        self.having_list.clear()
        self.having_conditions.clear()
        self.update_sql_preview()

    def add_order_by(self):
        dlg = QDialog(self)
        dlg.setWindowTitle('Добавить ORDER BY')
        layout = QFormLayout(dlg)
        col_cb = QComboBox()
        col_cb.addItems([f"{t}.{c}" for t in self.schema for c in self.schema[t]])
        dir_cb = QComboBox()
        dir_cb.addItems(['ASC', 'DESC'])
        add_btn = QPushButton('Добавить')
        add_btn.clicked.connect(dlg.accept)
        layout.addRow('Столбец', col_cb)
        layout.addRow('Направление', dir_cb)
        layout.addRow(add_btn)
        if dlg.exec():
            entry = f"{col_cb.currentText()} {dir_cb.currentText()}"
            if col_cb.currentText():
                self.order_by.append(entry)
                self.order_list.addItem(entry)
                self.update_sql_preview()

    def remove_selected_order(self):
        row = self.order_list.currentRow()
        if row >= 0:
            self.order_list.takeItem(row)
            try:
                del self.order_by[row]
            except Exception:
                pass
            self.update_sql_preview()

    def clear_order_by(self):
        self.order_list.clear()
        self.order_by.clear()
        self.update_sql_preview()

    def wrap_with_clear(self, widget):
        w = QWidget()
        l = QHBoxLayout(w)
        l.setContentsMargins(0, 0, 0, 0)
        l.addWidget(widget)
        clr = QPushButton('Очистить')

        def do_clear():
            if isinstance(widget, QComboBox):
                if widget.count():
                    widget.setCurrentIndex(0)
            elif isinstance(widget, QLineEdit):
                widget.clear()
            elif isinstance(widget, QSpinBox):
                widget.setValue(0)

        clr.clicked.connect(do_clear)
        l.addWidget(clr)
        return w

    def build_sql(self) -> str:
        parts = []
        if self.selected_columns:
            parts.extend([f"{t}.{c}" for t, c in self.selected_columns])
        if self.aggregates:
            for fn, col, alias in self.aggregates:
                parts.append(f"{fn}({col}) AS {alias}")
        select_clause = ', '.join(parts) if parts else '*'
        sql_lines = [f"SELECT {select_clause}"]
        from_clause = ""
        join_clauses = []
        if self.joins:
            used_tables = set()
            base_table = self.joins[0].get('left') or ''
            if base_table:
                used_tables.add(base_table)
                from_clause = base_table
            for j in self.joins:
                left = j.get('left', '')
                right = j.get('right', '')
                lf = j.get('lf', '')
                rf = j.get('rf', '')
                jtype = j.get('type', 'INNER')
                if right:
                    join_clauses.append(f"{jtype} JOIN {right} ON {left}.{lf} = {right}.{rf}")
                    used_tables.add(right)
                    used_tables.add(left)
        else:
            tables = set(t for t, _ in self.selected_columns)
            if len(tables) == 1:
                from_clause = next(iter(tables))
            elif len(tables) > 1:
                from_clause = ', '.join(sorted(tables))
            else:
                all_tables = sorted(self.schema.keys())
                from_clause = all_tables[0] if all_tables else ''
        if from_clause:
            sql_lines.append(f"FROM {from_clause}")
        else:
            sql_lines.append("FROM /* no table selected */")
        sql_lines.extend(join_clauses)
        if self.where_conditions:
            sql_lines.append("WHERE " + " AND ".join(self.where_conditions))
        if self.group_by:
            sql_lines.append("GROUP BY " + ", ".join(self.group_by))
        if self.having_conditions:
            sql_lines.append("HAVING " + " AND ".join(self.having_conditions))
        if self.order_by:
            sql_lines.append("ORDER BY " + ", ".join(self.order_by))
        return "\n".join(sql_lines)

    def on_apply_clicked(self):
        try:
            self.update_sql_preview()
        except Exception:
            pass
        sql = self.sql_preview.toPlainText().strip()
        if not sql:
            QMessageBox.warning(self, "Пустой SQL", "SQL пустой — нечего применять.")
            return
        self.apply_sql.emit(sql)

    def update_sql_preview(self):
        try:
            s = self.build_sql()
        except Exception as e:
            s = f"Error: {e}"
        self.sql_preview.setPlainText(s)

    def clear_join(self):
        self.join_list.clear()
        self.joins.clear()
        self.update_sql_preview()