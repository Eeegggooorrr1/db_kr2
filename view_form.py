import re
from typing import Dict, List, Tuple

from PySide6.QtGui import QStandardItemModel, QStandardItem
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QGroupBox, QDialog, QComboBox, QLineEdit,
    QFormLayout, QLabel, QListWidget, QTextEdit,
    QScrollArea, QCheckBox, QFrame, QMessageBox, QSpinBox, QTableView, QListWidgetItem
)
from PySide6.QtCore import Qt, Signal
import sys

from sqlalchemy import text

AGG_FUNCS = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX']
TEXT_OPS = ['LIKE', '~', '~*', '!~', '!~*', 'SIMILAR TO', 'NOT SIMILAR TO']

class WindowDialog(QDialog):
    def __init__(self, columns, parent=None, title='Добавить оконную функцию'):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.columns = columns or []
        self.setup_ui()


    def setup_ui(self):
        layout = QFormLayout(self)

        self.op_cb = QComboBox()
        self.op_cb.addItems(['LAG', 'LEAD', 'RANK', 'ROW_NUMBER'] + AGG_FUNCS)
        self.op_cb.currentIndexChanged.connect(self.on_op_changed)
        layout.addRow('Функция', self.op_cb)
        self.col_cb = QComboBox()
        self.col_cb.addItems([f"{t}.{c}" for t, c in self.columns])
        layout.addRow('Столбец', self.col_cb)
        self.lag_lead_container = QWidget()
        lag_lead_layout = QHBoxLayout(self.lag_lead_container)
        lag_lead_layout.setContentsMargins(0, 0, 0, 0)
        self.offset_spin = QSpinBox()
        self.offset_spin.setMinimum(1)
        self.offset_spin.setMaximum(1000)
        self.offset_spin.setValue(1)
        lag_lead_layout.addWidget(QLabel("Offset:"))
        lag_lead_layout.addWidget(self.offset_spin)
        self.default_le = QLineEdit()
        self.default_le.setPlaceholderText("Значение по умолчанию")
        lag_lead_layout.addWidget(QLabel("Default:"))
        lag_lead_layout.addWidget(self.default_le)
        lag_lead_layout.addStretch()
        self.lag_lead_container.setVisible(False)
        self.params_label = QLabel("Параметры")
        layout.addRow(self.params_label, self.lag_lead_container)
        self.partition_cb = QComboBox()
        self.partition_cb.addItems([''] + [f"{t}.{c}" for t, c in self.columns])
        layout.addRow('Партиция', self.partition_cb)
        self.sort_container = QWidget()
        sort_layout = QHBoxLayout(self.sort_container)
        sort_layout.setContentsMargins(0, 0, 0, 0)
        self.sort_col_cb = QComboBox()
        self.sort_col_cb.addItems([''] + [f"{t}.{c}" for t, c in self.columns])
        sort_layout.addWidget(self.sort_col_cb)
        self.sort_dir_cb = QComboBox()
        self.sort_dir_cb.addItems(['ASC', 'DESC'])
        sort_layout.addWidget(self.sort_dir_cb)
        sort_layout.addStretch()
        layout.addRow('Сортировка', self.sort_container)
        self.frame_type_cb = QComboBox()
        self.frame_type_cb.addItems(['', 'ROWS', 'RANGE'])
        layout.addRow('Тип фрейма', self.frame_type_cb)
        frame_part_container = QWidget()
        frame_part_layout = QHBoxLayout(frame_part_container)
        frame_part_layout.setContentsMargins(0, 0, 0, 0)
        self.frame_start_cb = QComboBox()
        self.frame_start_cb.addItems(
            ['UNBOUNDED PRECEDING', 'PRECEDING', 'CURRENT ROW', 'FOLLOWING', 'UNBOUNDED FOLLOWING'])
        frame_part_layout.addWidget(self.frame_start_cb)
        self.frame_start_spin = QSpinBox()
        self.frame_start_spin.setMinimum(1)
        self.frame_start_spin.setMaximum(1000000)
        self.frame_start_spin.setVisible(False)
        frame_part_layout.addWidget(self.frame_start_spin)
        frame_part_layout.addWidget(QLabel("AND"))
        self.frame_end_cb = QComboBox()
        self.frame_end_cb.addItems(
            ['CURRENT ROW', 'FOLLOWING', 'UNBOUNDED FOLLOWING', 'PRECEDING', 'UNBOUNDED PRECEDING'])
        frame_part_layout.addWidget(self.frame_end_cb)
        self.frame_end_spin = QSpinBox()
        self.frame_end_spin.setMinimum(1)
        self.frame_end_spin.setMaximum(1000000)
        self.frame_end_spin.setVisible(False)
        frame_part_layout.addWidget(self.frame_end_spin)
        frame_part_layout.addStretch()
        layout.addRow('Фрейм', frame_part_container)
        self.frame_type_cb.currentIndexChanged.connect(
            lambda _: frame_part_container.setVisible(bool(self.frame_type_cb.currentText())))
        self.frame_start_cb.currentIndexChanged.connect(self.on_frame_changed)
        self.frame_end_cb.currentIndexChanged.connect(self.on_frame_changed)
        frame_part_container.setVisible(False)
        self.alias_le = QLineEdit()
        layout.addRow('Имя', self.alias_le)
        add_btn = QPushButton('Добавить')
        add_btn.clicked.connect(self.accept)
        layout.addRow(add_btn)
        self.on_op_changed()

    def quot(self, ident):
        if not ident:
            return ident
        parts = ident.split('.')
        esc = []
        for p in parts:
            if p.isidentifier():
                esc.append(p)
            else:
                esc.append('"' + p.replace('"', '""') + '"')
        return '.'.join(esc)

    def on_frame_changed(self):
        start = self.frame_start_cb.currentText()
        if start == 'PRECEDING' or start == 'FOLLOWING':
            self.frame_start_spin.setVisible(True)
        else:
            self.frame_start_spin.setVisible(False)
        end = self.frame_end_cb.currentText()
        if end == 'PRECEDING' or end == 'FOLLOWING':
            self.frame_end_spin.setVisible(True)
        else:
            self.frame_end_spin.setVisible(False)

    def on_op_changed(self):
        op = self.op_cb.currentText()
        if op == 'RANK' or op == 'ROW_NUMBER':
            self.col_cb.setVisible(False)
            layout = self.layout()
            for i in range(layout.rowCount()):
                item = layout.itemAt(i, QFormLayout.LabelRole)
                if item and item.widget() and item.widget().text() == 'Столбец':
                    item.widget().setVisible(False)
                    break
        else:
            self.col_cb.setVisible(True)
            layout = self.layout()
            for i in range(layout.rowCount()):
                item = layout.itemAt(i, QFormLayout.LabelRole)
                if item and item.widget() and item.widget().text() == 'Столбец':
                    item.widget().setVisible(True)
                    break
        if op in ['LAG', 'LEAD']:
            self.lag_lead_container.setVisible(True)
            self.params_label.setVisible(True)
        else:
            self.lag_lead_container.setVisible(False)
            self.params_label.setVisible(False)
        self.sort_container.setVisible(True)

    def on_add(self):
        func = self.op_cb.currentText()
        alias_text = self.alias_le.text().strip()
        column_expr = ""
        if func != 'RANK' and self.col_cb.currentText():
            column_expr = self.quot(self.col_cb.currentText())
        offset_expr = ""
        default_expr = ""
        if func in ['LAG', 'LEAD']:
            offset_value = self.offset_spin.value()
            if offset_value != 1:
                offset_expr = f", {offset_value}"
            default_text = self.default_le.text().strip()
            if default_text:
                if default_text.replace('.', '').isdigit():
                    default_expr = f", {default_text}"
                else:
                    default_escaped = default_text.replace("'", "''")
                    default_expr = f", '{default_escaped}'"
        args_parts = []
        if column_expr:
            args_parts.append(column_expr)
        if offset_expr:
            args_parts.append(offset_expr.lstrip(", "))
        if default_expr:
            args_parts.append(default_expr.lstrip(", "))
        func_args = ", ".join(args_parts) if args_parts else ""
        partition_expr = ""
        partition_text = self.partition_cb.currentText().strip()
        if partition_text:
            partition_expr = f"PARTITION BY {self.quot(partition_text)}"
        order_expr = ""
        sort_col_text = self.sort_col_cb.currentText().strip()
        if sort_col_text:
            sort_dir = self.sort_dir_cb.currentText()
            order_expr = f"ORDER BY {self.quot(sort_col_text)} {sort_dir}"
        frame_expr = ""
        frame_type = self.frame_type_cb.currentText().strip()
        if frame_type:
            start = self.frame_start_cb.currentText()
            end = self.frame_end_cb.currentText()

            def part_text(val, spin):
                if val == 'UNBOUNDED PRECEDING':
                    return 'UNBOUNDED PRECEDING'
                if val == 'UNBOUNDED FOLLOWING':
                    return 'UNBOUNDED FOLLOWING'
                if val == 'CURRENT ROW':
                    return 'CURRENT ROW'
                if val == 'PRECEDING':
                    return f"{spin} PRECEDING"
                if val == 'FOLLOWING':
                    return f"{spin} FOLLOWING"
                return val

            sspin = str(self.frame_start_spin.value())
            espin = str(self.frame_end_spin.value())
            stext = part_text(start, sspin)
            etext = part_text(end, espin)
            frame_expr = f"{frame_type} BETWEEN {stext} AND {etext}"
        over_parts = []
        if partition_expr:
            over_parts.append(partition_expr)
        if order_expr:
            over_parts.append(order_expr)
        if frame_expr:
            over_parts.append(frame_expr)
        over_clause = "OVER(" + " ".join(over_parts) + ")" if over_parts else "OVER()"
        alias_expr = ""
        if alias_text:
            if alias_text.isidentifier():
                alias_expr = f" AS {alias_text}"
            else:
                alias_escaped = alias_text.replace('"', '""')
                alias_expr = f' AS "{alias_escaped}"'
        if func_args:
            final_expr = f"{func}({func_args}) {over_clause}{alias_expr}"
        else:
            final_expr = f"{func}() {over_clause}{alias_expr}"
        return final_expr

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


class SubqueryDialog(QDialog):
    def __init__(self, db=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle('Составить подзапрос')
        self.result_sql = ''
        self.db = db
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)
        self.sql_widget = SQLStubWindow(db=self.db, parent=self)
        for btn in self.sql_widget.findChildren(QPushButton):
            if btn.text() == 'Применить':
                btn.hide()
        layout.addWidget(self.sql_widget)
        btn_row = QHBoxLayout()
        add_btn = QPushButton('Добавить')
        cancel_btn = QPushButton('Отмена')
        add_btn.clicked.connect(self.on_add)
        cancel_btn.clicked.connect(self.reject)
        btn_row.addWidget(add_btn)
        btn_row.addWidget(cancel_btn)
        layout.addLayout(btn_row)

    def on_add(self):
        try:
            self.sql_widget.update_sql_preview()
        except Exception:
            pass
        self.result_sql = self.sql_widget.sql_preview.toPlainText().strip()
        if not self.result_sql:
            QMessageBox.warning(self, 'Пустой SQL', 'Нельзя добавить пустой подзапрос.')
            return
        self.accept()


class ConditionTypeDialog(QDialog):
    def __init__(self, columns, db=None, parent=None, title='Добавить условие'):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.columns = columns or []
        self.db = db
        self.result_condition = None
        self.setup_ui()

    def setup_ui(self):
        layout = QFormLayout(self)
        self.type_cb = QComboBox()
        self.type_cb.addItems(['Обычное условие', 'Подзапрос'])
        layout.addRow('Тип', self.type_cb)
        self.normal_btn = QPushButton('Добавить обычное условие')
        self.normal_btn.clicked.connect(self.open_normal_condition)
        self.subquery_widget = QWidget()
        sw_layout = QHBoxLayout(self.subquery_widget)
        self.op_cb = QComboBox()
        self.op_cb.addItems(['EXISTS', 'ANY', 'ALL'])
        self.any_op_cb = QComboBox()
        self.any_op_cb.addItems(['=', '!=', '<', '<=', '>', '>='] + TEXT_OPS)
        self.col_cb = QComboBox()
        self.col_cb.addItems([f"{t}.{c}" for t, c in self.columns])
        self.compose_btn = QPushButton('Составить подзапрос')
        sw_layout.addWidget(self.op_cb)
        sw_layout.addWidget(self.any_op_cb)
        sw_layout.addWidget(self.col_cb)
        sw_layout.addWidget(self.compose_btn)
        self.compose_btn.clicked.connect(self.open_subquery_builder)
        self.op_cb.currentIndexChanged.connect(self.on_op_changed)
        layout.addRow(self.normal_btn)
        layout.addRow(self.subquery_widget)
        btn_row = QHBoxLayout()
        cancel_btn = QPushButton('Отмена')
        cancel_btn.clicked.connect(self.reject)
        btn_row.addWidget(cancel_btn)
        layout.addRow(btn_row)
        self.type_cb.currentIndexChanged.connect(self.on_type_changed)
        self.on_type_changed(0)

    def on_type_changed(self, idx):
        if idx == 0:
            self.normal_btn.setVisible(True)
            self.subquery_widget.setVisible(False)
        else:
            self.normal_btn.setVisible(False)
            self.subquery_widget.setVisible(True)
            self.on_op_changed(self.op_cb.currentIndex())

    def on_op_changed(self, idx):
        op = self.op_cb.currentText()
        if op == 'EXISTS':
            self.col_cb.setVisible(False)
            self.any_op_cb.setVisible(False)
        else:
            self.col_cb.setVisible(True)
            self.any_op_cb.setVisible(True)

    def open_normal_condition(self):
        dlg = ConditionDialog(self.columns, self, title=self.windowTitle())
        if dlg.exec():
            cond = dlg.get_condition()
            if cond:
                self.result_condition = cond
                self.accept()

    def open_subquery_builder(self):
        op = self.op_cb.currentText()
        col = self.col_cb.currentText() if self.col_cb.currentIndex() >= 0 else ''
        if op in ('ANY', 'ALL') and not col:
            QMessageBox.warning(self, 'Не выбран столбец', 'Для ANY/ALL нужно выбрать столбец.')
            return
        sqd = SubqueryDialog(db=self.db, parent=self)
        if sqd.exec():
            subq = sqd.result_sql
            if not subq:
                return
            if op == 'EXISTS':
                cond = f"EXISTS ({subq})"
            else:
                operator = self.any_op_cb.currentText() if self.any_op_cb.currentIndex() >= 0 else '='
                cond = f"{col} {operator} {op} ({subq})"
            self.result_condition = cond
            self.accept()

    def get_condition(self):
        return self.result_condition


class CaseBuilderDialog(QDialog):
    def __init__(self, columns, db=None, parent=None, title='Создать CASE'):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.columns = columns or []
        self.db = db
        self.case_expression = ''
        self.when_rows = []
        self.setup_ui()

    def setup_ui(self):
        layout = QVBoxLayout(self)
        self.rows_scroll = QScrollArea()
        self.rows_scroll.setWidgetResizable(True)
        rows_container_widget = QWidget()
        self.rows_container = QVBoxLayout(rows_container_widget)
        rows_container_widget.setLayout(self.rows_container)
        self.rows_scroll.setWidget(rows_container_widget)
        layout.addWidget(self.rows_scroll)
        add_row_btn = QPushButton('Добавить условие')
        add_row_btn.clicked.connect(self.add_when_row)
        layout.addWidget(add_row_btn)
        else_layout = QFormLayout()
        self.else_le = QLineEdit()
        self.alias_le = QLineEdit()
        else_layout.addRow('ELSE', self.else_le)
        else_layout.addRow('AS', self.alias_le)
        layout.addLayout(else_layout)
        btn_row = QHBoxLayout()
        add_btn = QPushButton('Добавить')
        cancel_btn = QPushButton('Отмена')
        add_btn.clicked.connect(self.on_add)
        cancel_btn.clicked.connect(self.reject)
        btn_row.addWidget(add_btn)
        btn_row.addWidget(cancel_btn)
        layout.addLayout(btn_row)
        self.add_when_row()

    def add_when_row(self):
        row_w = QWidget()
        rlayout = QHBoxLayout(row_w)
        cond_label = QLabel('WHEN')
        set_cond_btn = QPushButton('Условие')
        then_le = QLineEdit()
        then_le.setPlaceholderText('THEN ')
        del_btn = QPushButton('Удалить')
        rlayout.addWidget(cond_label)
        rlayout.addWidget(set_cond_btn)
        rlayout.addWidget(QLabel('THEN'))
        rlayout.addWidget(then_le)
        rlayout.addWidget(del_btn)
        self.rows_container.addWidget(row_w)
        row_obj = {'widget': row_w, 'cond': None, 'cond_label': cond_label, 'then_widget': then_le}
        self.when_rows.append(row_obj)

        def set_condition():
            dlg = ConditionDialog(self.columns, self, title='Условие для WHEN')
            if dlg.exec():
                cond = dlg.get_condition()
                row_obj['cond'] = cond
                row_obj['cond_label'].setText(f"WHEN {cond}")

        def do_delete():
            try:
                self.when_rows.remove(row_obj)
            except ValueError:
                pass
            row_w.setParent(None)

        set_cond_btn.clicked.connect(set_condition)
        del_btn.clicked.connect(do_delete)

    def on_add(self):
        parts = []
        for row in list(self.when_rows):
            cond = row.get('cond')
            then_val = row.get('then_widget').text().strip() if row.get('then_widget') is not None else ''
            if cond and then_val:
                parts.append(f"WHEN {cond} THEN {then_val}")
        else_part = self.else_le.text().strip()
        alias = self.alias_le.text().strip() or 'case_expr'
        if not parts and not else_part:
            QMessageBox.warning(self, 'Пустой CASE', 'Добавьте хотя бы одно условие.')
            return
        expr = 'CASE ' + ' '.join(parts)
        if else_part:
            expr += ' ELSE ' + else_part
        expr += f' END AS {alias}'
        self.case_expression = expr
        self.accept()

    def get_expression(self):
        return self.case_expression


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
        self.custom_expressions = []
        self.window_functions = []
        self.table_to_col_cbs = {}
        self.all_col_cbs = []
        self.schema = {}
        self.ctes = []
        self.coalesce_rules = []
        self.group_mode = None
        self.grouping_sets = []
        self.table_widgets = {}
        self._load_schema_from_db()
        self.setup_ui()
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
        self.sel_layout = sel_layout

        for table in sorted(self.schema.keys()):
            cols = self.schema.get(table, [])
            self._add_table_to_ui(table, cols)

        sel_group.setLayout(sel_layout)
        main_layout.addWidget(sel_group)

        expr_group = QGroupBox('Case')
        expr_layout = QVBoxLayout()
        self.expr_list = QListWidget()
        expr_layout.addWidget(self.expr_list)
        expr_btn_row = QHBoxLayout()
        create_case_btn = QPushButton('Добавить')
        create_case_btn.clicked.connect(self.open_case_builder)
        clear_expr_btn = QPushButton('Очистить')
        clear_expr_btn.clicked.connect(self.clear_expressions)
        expr_btn_row.addWidget(create_case_btn)
        expr_btn_row.addWidget(clear_expr_btn)
        expr_btn_row.addStretch()
        expr_layout.addLayout(expr_btn_row)
        expr_group.setLayout(expr_layout)
        main_layout.addWidget(expr_group)

        window_group = QGroupBox('Оконные функции')
        window_layout = QVBoxLayout()
        self.window_list = QListWidget()
        window_layout.addWidget(self.window_list)
        window_btn_row = QHBoxLayout()
        create_window_btn = QPushButton('Добавить')
        create_window_btn.clicked.connect(self.open_window_builder)
        clear_window_btn = QPushButton('Очистить')
        clear_window_btn.clicked.connect(self.clear_window)
        window_btn_row.addWidget(create_window_btn)
        window_btn_row.addWidget(clear_window_btn)
        window_btn_row.addStretch()
        window_layout.addLayout(window_btn_row)
        window_group.setLayout(window_layout)
        main_layout.addWidget(window_group)

        cte_group = QGroupBox('CTE')
        cte_layout = QVBoxLayout()
        self.cte_list = QListWidget()
        cte_layout.addWidget(self.cte_list)
        cte_btn_row = QHBoxLayout()
        add_cte_btn = QPushButton('Добавить')
        add_cte_btn.clicked.connect(self.open_add_cte_dialog)
        remove_cte_btn = QPushButton('Очистить')
        remove_cte_btn.clicked.connect(self.remove_selected_cte)
        cte_btn_row.addWidget(add_cte_btn)
        cte_btn_row.addWidget(remove_cte_btn)
        cte_btn_row.addStretch()
        cte_layout.addLayout(cte_btn_row)
        cte_group.setLayout(cte_layout)
        main_layout.addWidget(cte_group)


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
        clear_group_btn = QPushButton('Очис.')
        clear_group_btn.clicked.connect(self.clear_group_by)
        group_opts_btn = QPushButton('Настр.')
        group_opts_btn.clicked.connect(self.open_group_options)
        gbtn_row.addWidget(add_group_btn)
        gbtn_row.addWidget(add_agg_btn)
        gbtn_row.addWidget(clear_group_btn)
        gbtn_row.addWidget(group_opts_btn)
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

        coalesce_group = QGroupBox("coalesce/nullif")
        coalesce_layout = QVBoxLayout()
        self.coalesce_list = QListWidget()
        coalesce_layout.addWidget(self.coalesce_list)
        co_btn_row = QHBoxLayout()
        add_co_btn = QPushButton("доб.")
        del_co_btn = QPushButton("уд.")
        apply_co_btn = QPushButton("прим.")
        co_btn_row.addWidget(add_co_btn)
        co_btn_row.addWidget(del_co_btn)
        co_btn_row.addWidget(apply_co_btn)
        co_btn_row.addStretch()
        coalesce_layout.addLayout(co_btn_row)
        coalesce_group.setLayout(coalesce_layout)
        main_layout.addWidget(coalesce_group)

        add_co_btn.clicked.connect(self.open_coalesce_dialog)
        del_co_btn.clicked.connect(self.remove_selected_coalesce)
        apply_co_btn.clicked.connect(self.apply_coalesce)

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

    def _add_table_to_ui(self, table, cols):

        if table in self.table_widgets:
            return

        header_widget = QWidget()
        header_l = QHBoxLayout(header_widget)
        header_l.setContentsMargins(0, 0, 0, 0)
        table_cb = QCheckBox()
        table_label = QLabel(f"<b>{table}</b>")
        header_l.addWidget(table_cb)
        header_l.addWidget(table_label)
        header_l.addStretch()
        self.sel_layout.addWidget(header_widget)

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
        self.sel_layout.addWidget(cols_widget)

        self.table_widgets[table] = (header_widget, cols_widget, table_cb)

    def _remove_table_from_ui(self, table):

        widgets = self.table_widgets.get(table)
        if not widgets:
            return
        header_widget, cols_widget, table_cb = widgets
        try:
            self.sel_layout.removeWidget(header_widget)
            header_widget.setParent(None)
        except Exception:
            pass
        try:
            self.sel_layout.removeWidget(cols_widget)
            cols_widget.setParent(None)
        except Exception:
            pass
        removed_cbs = self.table_to_col_cbs.pop(table, [])
        for cb in removed_cbs:
            try:
                if cb in self.all_col_cbs:
                    self.all_col_cbs.remove(cb)
            except Exception:
                pass
        try:
            del self.table_widgets[table]
        except Exception:
            pass

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

    def open_case_builder(self):
        cols = [(t, c) for t in self.schema for c in self.schema[t]]
        dlg = CaseBuilderDialog(cols, db=self.db, parent=self, title='Создать CASE')
        if dlg.exec():
            expr = dlg.get_expression()
            if expr:
                self.custom_expressions.append(expr)
                self.expr_list.addItem(expr)
                self.update_sql_preview()

    def clear_expressions(self):
        self.custom_expressions.clear()
        self.expr_list.clear()
        self.update_sql_preview()


    def open_window_builder(self):
        cols = [(t, c) for t in self.schema for c in self.schema[t]]
        dlg = WindowDialog(columns=cols, parent=self, title='Создать оконную функцию')
        if dlg.exec():
            expr = dlg.on_add()
            if expr:

                self.window_functions.append(expr)
                self.window_list.addItem(expr)
                self.update_sql_preview()



    def clear_window(self):
        self.window_functions.clear()
        self.window_list.clear()
        self.update_sql_preview()

    def open_add_cte_dialog(self):
        dlg = CTEDialog(db=self.db, parent=self)
        if dlg.exec():
            cte = dlg.get_cte()
            existing_names = [c['name'] for c in self.ctes]
            if cte['name'] in existing_names:
                QMessageBox.warning(self, 'Дубликат CTE', f"CTE с именем {cte['name']} уже существует.")
                return
            self.ctes.append(cte)
            self.cte_list.addItem(f"{cte['name']}")
            cols = self._infer_cte_columns(cte['sql'], cte['name']) or []
            self.schema[cte['name']] = cols
            self._add_table_to_ui(cte['name'], cols)
            QMessageBox.information(self, 'CTE добавлен', f"CTE {cte['name']} добавлен.")
            self.update_sql_preview()

    def _infer_cte_columns(self, cte_sql, cte_name):

        if self.db is not None:
            try:
                q = f"WITH {cte_name} AS (\n{cte_sql}\n)\nSELECT * FROM {cte_name} LIMIT 0"
                try:
                    exec_res = None
                    if hasattr(self.db, 'execute'):
                        exec_res = self.db.execute(q)
                        desc = getattr(exec_res, 'description', None)
                        if desc:
                            return [d[0] for d in desc]
                    if hasattr(self.db, 'cursor'):
                        cur = self.db.cursor()
                        cur.execute(q)
                        desc = getattr(cur, 'description', None)
                        if desc:
                            return [d[0] for d in desc]
                except Exception:
                    pass
            except Exception:
                pass

        try:
            m = re.search(r"select\s+(.*?)\s+from\s", cte_sql, re.I | re.S)
            if not m:
                return []
            sel_part = m.group(1).strip()
            cols = []
            cur = ''
            depth = 0
            for ch in sel_part:
                if ch == '(':
                    depth += 1
                elif ch == ')':
                    if depth > 0:
                        depth -= 1
                if ch == ',' and depth == 0:
                    cols.append(cur.strip())
                    cur = ''
                else:
                    cur += ch
            if cur.strip():
                cols.append(cur.strip())
            parsed = []
            for c in cols:
                s = c.strip()
                s = re.sub(r"--.*$", "", s).strip()
                s = s.rstrip(';').strip()
                m_as = re.search(r"\s+as\s+\"?([^\s\"]+)\"?$", s, re.I)
                if m_as:
                    parsed.append(m_as.group(1))
                    continue
                parts = s.split()
                if len(parts) >= 2:
                    last = parts[-1]
                    if '(' not in last and last.lower() not in ('distinct',):
                        parsed.append(last.strip().strip('"'))
                        continue
                if '.' in s and '(' not in s:
                    parsed.append(s.split('.')[-1].strip().strip('"'))
                    continue
                m_end = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\s*$", s)
                if m_end:
                    parsed.append(m_end.group(1))
                else:
                    parsed.append('expr')
            cleaned = []
            for p in parsed:
                p = p.strip().strip('"').strip()
                if not p:
                    continue
                if p in cleaned:
                    i = 2
                    newp = f"{p}_{i}"
                    while newp in cleaned:
                        i += 1
                        newp = f"{p}_{i}"
                    p = newp
                cleaned.append(p)
            return cleaned
        except Exception:
            return []

    def remove_selected_cte(self):
        row = self.cte_list.currentRow()
        if row >= 0:
            item = self.cte_list.takeItem(row)
            try:
                cte_item = self.ctes[row]
            except Exception:
                cte_item = None
            try:
                del self.ctes[row]
            except Exception:
                pass
            name = item.text()
            try:
                if name in self.schema:
                    try:
                        del self.schema[name]
                    except Exception:
                        pass
                self._remove_table_from_ui(name)
            except Exception:
                pass
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
        dlg = ConditionTypeDialog(cols, db=self.db, parent=self, title='Добавить WHERE-условие')
        if dlg.exec():
            cond = dlg.get_condition()
            if cond:
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
        self.group_mode = None
        self.grouping_sets = []
        self.update_sql_preview()

    def add_aggregate(self):
        dlg = QDialog(self)
        dlg.setWindowTitle('Добавить агрегат')
        layout = QFormLayout(dlg)
        col_cb = QComboBox()
        col_cb.addItems([f"{t}.{c}" for t in self.schema for c in self.schema[t]])
        col_cb.addItems("*")
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
        dlg = ConditionTypeDialog(cols, db=self.db, parent=self, title='Добавить HAVING-условие')
        if dlg.exec():
            cond = dlg.get_condition()
            if cond:
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

    def open_coalesce_dialog(self):

        dlg = QDialog(self)
        dlg.setWindowTitle("Добавить")
        layout = QFormLayout(dlg)

        op_cb = QComboBox()
        op_cb.addItems(['COALESCE', 'NULLIF'])

        col_cb = QComboBox()
        cols = []
        for t in sorted(self.schema.keys()):
            for c in self.schema[t]:
                cols.append(f"{t}.{c}")
        col_cb.addItems(cols)

        arg_le = QLineEdit()
        arg_le.setPlaceholderText("второй аргумент")

        add_btn = QPushButton("Добавить")
        cancel_btn = QPushButton("Отмена")
        btn_row = QHBoxLayout()
        btn_row.addStretch()
        btn_row.addWidget(add_btn)
        btn_row.addWidget(cancel_btn)

        layout.addRow('Оператор', op_cb)
        layout.addRow('Столбец', col_cb)
        layout.addRow('Второй аргумент', arg_le)
        layout.addRow(btn_row)

        cancel_btn.clicked.connect(dlg.reject)

        def do_add():
            op = op_cb.currentText()
            col = col_cb.currentText()
            raw_arg = arg_le.text().strip()
            if not col:
                QMessageBox.warning(dlg, "Ошибка", "Выберите столбец.")
                return
            formatted_arg = self.format_coal(raw_arg)
            expr = f"{op}({col}, {formatted_arg})"
            rule = {'op': op, 'col': col, 'arg': formatted_arg, 'expr': expr}
            self.coalesce_rules.append(rule)
            self.coalesce_list.addItem(f"{op} | {col} | {formatted_arg}")
            dlg.accept()

        add_btn.clicked.connect(do_add)
        dlg.exec()

    def format_coal(self, arg: str) -> str:

        if arg is None:
            return "NULL"
        s = str(arg).strip()
        if s == "":
            return "''"
        if s.upper() == "NULL":
            return "NULL"
        if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
            return s
        if re.fullmatch(r'[+-]?\d+(\.\d+)?', s):
            return s
        if s.lower() in ("true", "false", "t", "f", "yes", "no", "1", "0"):
            if s.lower() in ("true", "t", "yes", "1"):
                return "TRUE"
            if s.lower() in ("false", "f", "no", "0"):
                return "FALSE"
        esc = s.replace("'", "''")
        return f"'{esc}'"

    def remove_selected_coalesce(self):

        sel_items = self.coalesce_list.selectedItems()
        if not sel_items:
            return
        rows = sorted([self.coalesce_list.row(it) for it in sel_items], reverse=True)
        for r in rows:
            try:
                self.coalesce_list.takeItem(r)
            except Exception:
                pass
            try:
                del self.coalesce_rules[r]
            except Exception:
                pass


    def apply_coalesce(self):

        sql = self.sql_preview.toPlainText()
        if not sql:
            QMessageBox.warning(self, "Пустой SQL", "Нет SQL для применения замен.")
            return
        new_sql = sql
        for rule in self.coalesce_rules:
            col = rule['col']
            expr = rule['expr']
            pat = r'(?<![\w])' + re.escape(col) + r'(?![\w])'
            new_sql = re.sub(pat, expr, new_sql)
        self.sql_preview.setPlainText(new_sql)

    def apply_coalesce_to_sql(self, sql: str) -> str:
        if not self.coalesce_rules:
            return sql
        new_sql = sql
        for rule in self.coalesce_rules:
            col = rule.get('col')
            expr = rule.get('expr')
            if not col or not expr:
                continue
            pat = r'(?<![\w])' + re.escape(col) + r'(?![\w])'
            try:
                new_sql = re.sub(pat, expr, new_sql)
            except re.error:
                new_sql = new_sql.replace(col, expr)
        return new_sql




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
        if self.custom_expressions:
            parts.extend(self.custom_expressions)
        if self.window_functions:
            parts.extend(self.window_functions)
        select_clause = ', '.join(parts) if parts else '*'

        with_clause = ""
        try:
            ctes = getattr(self, 'ctes', None) or []
            if ctes:
                cte_parts = []
                for c in ctes:
                    name = (c.get('name') if isinstance(c, dict) else None) or str(c)
                    sql = (c.get('sql') if isinstance(c, dict) else '') or ''
                    sql = sql.strip()
                    if sql.endswith(';'):
                        sql = sql[:-1].rstrip()
                    if not sql:
                        sql = "/* empty cte */"
                    cte_parts.append(f"{name} AS (\n{sql}\n)")
                if cte_parts:
                    with_clause = "WITH " + ",\n".join(cte_parts)
        except Exception:
            with_clause = ""

        sql_lines = []
        if with_clause:
            sql_lines.append(with_clause)
        sql_lines.append(f"SELECT {select_clause}")

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
            if not self.group_mode:
                sql_lines.append("GROUP BY " + ", ".join(self.group_by))
            else:
                gm = self.group_mode
                if gm == 'ROLLUP':
                    sql_lines.append("GROUP BY ROLLUP(" + ", ".join(self.group_by) + ")")
                elif gm == 'CUBE':
                    sql_lines.append("GROUP BY CUBE(" + ", ".join(self.group_by) + ")")
                elif gm == 'GROUPING SETS':
                    if self.grouping_sets:
                        sets_parts = []
                        for s in self.grouping_sets:
                            if not s:
                                sets_parts.append("()")
                            else:
                                sets_parts.append("(" + ", ".join(s) + ")")
                        sql_lines.append("GROUP BY GROUPING SETS (" + ", ".join(sets_parts) + ")")
                    else:
                        sql_lines.append("GROUP BY " + ", ".join(self.group_by))
                else:
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
            QMessageBox.warning(self, "Пустой SQL", " нечего применять.")
            return
        self.apply_sql.emit(sql)

    def update_sql_preview(self):
        try:
            s = self.build_sql()
        except Exception as e:
            s = f"Error: {e}"
        s = self.apply_coalesce_to_sql(s)
        self.sql_preview.setPlainText(s)

    def clear_join(self):
        self.join_list.clear()
        self.joins.clear()
        self.update_sql_preview()

    def open_group_options(self):

        dlg = QDialog(self)
        dlg.setWindowTitle('Настройки группировки')
        layout = QVBoxLayout(dlg)
        form = QFormLayout()
        mode_cb = QComboBox()
        mode_cb.addItems(['ROLLUP', 'CUBE', 'GROUPING SETS'])
        if self.group_mode is None:
            mode_cb.setCurrentIndex(0)
        else:
            try:
                idx = ['ROLLUP', 'CUBE', 'GROUPING SETS'].index(self.group_mode if self.group_mode else 'None')
                mode_cb.setCurrentIndex(idx)
            except Exception:
                mode_cb.setCurrentIndex(0)

        form.addRow('Режим', mode_cb)
        layout.addLayout(form)

        cols_list = QListWidget()
        cols_list.setSelectionMode(QListWidget.MultiSelection)
        all_cols = [f"{t}.{c}" for t in self.schema for c in self.schema[t]]
        for col in all_cols:
            item = QListWidgetItem(col)
            cols_list.addItem(item)
            if col in self.group_by:
                item.setSelected(True)

        layout.addWidget(QLabel('Выберите столбцы'))
        layout.addWidget(cols_list)

        layout.addWidget(QLabel('GROUPING SETS по одному набору на строку через запятую. Пустая строка - пустой набор'))
        gs_text = QTextEdit()
        if self.grouping_sets:
            gs_lines = []
            for s in self.grouping_sets:
                if s:
                    gs_lines.append(", ".join(s))
                else:
                    gs_lines.append("")
            gs_text.setPlainText("\n".join(gs_lines))
        layout.addWidget(gs_text)

        btn_row = QHBoxLayout()
        ok_btn = QPushButton('Применить')
        cancel_btn = QPushButton('Отмена')
        btn_row.addWidget(ok_btn)
        btn_row.addWidget(cancel_btn)
        layout.addLayout(btn_row)

        def on_mode_changed(idx):
            mode = mode_cb.currentText()
            if mode == 'GROUPING SETS':
                cols_list.setEnabled(True)
                gs_text.setEnabled(True)
            elif mode in ('ROLLUP', 'CUBE'):
                cols_list.setEnabled(True)
                gs_text.setEnabled(False)
            else:
                cols_list.setEnabled(False)
                gs_text.setEnabled(False)

        mode_cb.currentIndexChanged.connect(on_mode_changed)
        on_mode_changed(mode_cb.currentIndex())

        def do_apply():
            mode = mode_cb.currentText()
            if mode == 'None':
                self.group_mode = None
                self.grouping_sets = []
            elif mode in ('ROLLUP', 'CUBE'):
                selected = [i.text() for i in cols_list.selectedItems()]
                if not selected:
                    if self.group_by:
                        selected = list(self.group_by)
                    else:
                        QMessageBox.warning(dlg, 'Нет столбцов', 'Выберите столбцы .')
                        return
                self.group_by = list(selected)
                self.group_list.clear()
                for g in self.group_by:
                    self.group_list.addItem(g)
                self.group_mode = mode
                self.grouping_sets = []
            elif mode == 'GROUPING SETS':
                raw = gs_text.toPlainText().splitlines()
                sets = []
                for line in raw:
                    line = line.strip()
                    if not line:
                        sets.append([])
                    else:
                        cols = [c.strip() for c in line.split(',') if c.strip()]
                        sets.append(cols)
                all_valid = True
                for s in sets:
                    for col in s:
                        if col not in all_cols:
                            all_valid = False
                if not all_valid:
                    QMessageBox.warning(dlg, 'Предупреждение', 'Некоторые столбцы в GROUPING SETS не найдены в схеме. Убедитесь, что имена столбцов полные (таблица.столбец).')
                union_cols = []
                for s in sets:
                    for c in s:
                        if c not in union_cols:
                            union_cols.append(c)
                self.group_by = list(union_cols)
                self.group_list.clear()
                for g in self.group_by:
                    self.group_list.addItem(g)
                self.group_mode = 'GROUPING SETS'
                self.grouping_sets = sets
            self.update_sql_preview()
            dlg.accept()

        ok_btn.clicked.connect(do_apply)
        cancel_btn.clicked.connect(dlg.reject)
        dlg.exec()

class CTEDialog(QDialog):

    def __init__(self, db=None, parent=None):
        super().__init__(parent)
        self.setWindowTitle('Добавить CTE')
        self.db = db
        self.cte_name = ''
        self.cte_sql = ''
        self.setup_ui()

    def setup_ui(self):
        layout = QFormLayout(self)
        self.name_le = QLineEdit()
        self.open_editor_btn = QPushButton('Составить запрос')
        self.open_editor_btn.clicked.connect(self.open_editor)
        layout.addRow(QLabel('Имя CTE'), self.name_le)
        row_w = QWidget()
        row_l = QHBoxLayout(row_w)
        row_l.setContentsMargins(0,0,0,0)
        row_l.addWidget(self.open_editor_btn)
        layout.addRow(row_w)

        btn_row = QHBoxLayout()
        add_btn = QPushButton('Добавить')
        cancel_btn = QPushButton('Отмена')
        add_btn.clicked.connect(self.on_add)
        cancel_btn.clicked.connect(self.reject)
        btn_row.addWidget(add_btn)
        btn_row.addWidget(cancel_btn)
        layout.addRow(btn_row)

    def open_editor(self):
        sd = SubqueryDialog(db=self.db, parent=self)
        if sd.exec():
            self.cte_sql = sd.result_sql

    def on_add(self):
        name = self.name_le.text().strip()
        if not name:
            QMessageBox.warning(self, 'Имя не задано', 'Укажите имя для CTE.')
            return
        if not self.cte_sql:
            QMessageBox.warning(self, 'CTE пустой', 'Составьте SQL.')
            return
        if ' ' in name:
            QMessageBox.warning(self, 'Неверное имя', 'Имя CTE не должно содержать пробелов.')
            return
        self.cte_name = name
        self.accept()

    def get_cte(self):
        return {'name': self.cte_name, 'sql': self.cte_sql}
