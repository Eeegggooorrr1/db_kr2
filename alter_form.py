from PySide6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QWidget,
    QLineEdit, QComboBox, QCheckBox, QSpinBox, QMessageBox, QScrollArea,
    QSizePolicy, QListWidget, QListWidgetItem, QPlainTextEdit, QInputDialog
)
from PySide6.QtCore import Signal, Qt
from PySide6.QtGui import QFont, QColor
import re

from sqlalchemy import text


class RenameTableDialog(QDialog):
    def __init__(self, current_name: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Переименовать таблицу")
        self.layout = QVBoxLayout(self)
        self.input = QLineEdit(current_name)
        self.layout.addWidget(QLabel("Новое имя:"))
        self.layout.addWidget(self.input)
        row = QHBoxLayout()
        btn_ok = QPushButton("OK")
        btn_cancel = QPushButton("Отмена")
        row.addStretch(1)
        row.addWidget(btn_cancel)
        row.addWidget(btn_ok)
        self.layout.addLayout(row)
        btn_ok.clicked.connect(self.accept)
        btn_cancel.clicked.connect(self.reject)

    def new_name(self):
        return self.input.text().strip()


class ColumnEditorDialog(QDialog):

    def __init__(self, column_data=None, db=None, parent=None):
        super().__init__(parent)
        self.db = db
        self.setWindowTitle("Колонка")
        self.layout = QVBoxLayout(self)
        self.form = QVBoxLayout()
        self.name_edit = QLineEdit(column_data.get('name', '') if column_data else "")
        self.type_combo = QComboBox()
        base_types = ["INTEGER", "TEXT", "REAL", "DATE", "BOOLEAN", "ARRAY"]
        if column_data is None:
            items = base_types + ["ENUM"]
        else:
            if column_data.get('type') == 'ENUM':
                items = ["ENUM"]
            else:
                items = base_types
        self.type_combo.addItems(items)
        if column_data:
            self.type_combo.setCurrentText(column_data.get('type', 'TEXT'))
        self.not_null = QCheckBox("NOT NULL")
        self.unique = QCheckBox("UNIQUE")
        self.pk = QCheckBox("PK")
        if column_data:
            self.not_null.setChecked(column_data.get('not_null', False))
            self.unique.setChecked(column_data.get('unique', False))
            self.pk.setChecked(column_data.get('primary_key', False))
        self.default_edit = QLineEdit(column_data.get('default', '') if column_data and column_data.get('default') is not None else "")
        self.default_edit.setPlaceholderText("DEFAULT value")
        self.check_edit = QLineEdit(column_data.get('check', '') if column_data else "")
        self.check_edit.setPlaceholderText("CHECK condition")
        self.length_spin = QSpinBox()
        self.length_spin.setRange(0, 1000000)
        if column_data and column_data.get('length') is not None:
            try:
                self.length_spin.setValue(int(column_data.get('length')))
            except:
                pass
        self.array_elem_combo = QComboBox()
        self.array_elem_combo.addItems(["INTEGER", "TEXT", "REAL", "DATE", "BOOLEAN"])
        if column_data and column_data.get('array_elem_type'):
            self.array_elem_combo.setCurrentText(column_data.get('array_elem_type'))
        self.fk_table = QLineEdit(column_data.get('fk_table', '') if column_data else "")
        self.fk_column = QLineEdit(column_data.get('fk_column', '') if column_data else "")

        self.enum_label = QLabel("Enum type:")
        self.enum_combo = QComboBox()
        self.enum_manage_btn = QPushButton("Управление enum")
        self.enum_manage_btn.setToolTip("Открыть менеджер enum-типов")

        self._refresh_enum_list()

        self.length_label = QLabel("Length:")
        self.array_label = QLabel("Array elem type:")
        self.default_label = QLabel("Default:")

        self.form.addWidget(QLabel("Name:"))
        self.form.addWidget(self.name_edit)
        self.form.addWidget(QLabel("Type:"))
        self.form.addWidget(self.type_combo)
        enum_row = QHBoxLayout()
        enum_row.addWidget(self.enum_label)
        enum_row.addWidget(self.enum_combo)
        enum_row.addWidget(self.enum_manage_btn)
        self.form.addLayout(enum_row)

        self.form.addWidget(self.not_null)
        self.form.addWidget(self.unique)
        self.form.addWidget(self.pk)
        self.form.addWidget(self.default_label)
        self.form.addWidget(self.default_edit)
        self.form.addWidget(QLabel("Check:"))
        self.form.addWidget(self.check_edit)
        self.form.addWidget(self.length_label)
        self.form.addWidget(self.length_spin)
        self.form.addWidget(self.array_label)
        self.form.addWidget(self.array_elem_combo)
        self.form.addWidget(QLabel("FK Table:"))
        self.form.addWidget(self.fk_table)
        self.form.addWidget(QLabel("FK Column:"))
        self.form.addWidget(self.fk_column)
        self.layout.addLayout(self.form)
        row = QHBoxLayout()
        self.btn_ok = QPushButton("OK")
        btn_cancel = QPushButton("Отмена")
        row.addStretch(1)
        row.addWidget(btn_cancel)
        row.addWidget(self.btn_ok)
        self.layout.addLayout(row)
        self.btn_ok.clicked.connect(self.accept)
        btn_cancel.clicked.connect(self.reject)

        self.type_combo.currentTextChanged.connect(self.update_fields_visibility)
        self.enum_manage_btn.clicked.connect(self.open_enum_manager)
        self.pk.toggled.connect(self.on_pk_toggled)
        self.update_fields_visibility(self.type_combo.currentText())
        if self.pk.isChecked():
            self.on_pk_toggled(True)
        if column_data and column_data.get('type') == 'ENUM':
            enum_name = column_data.get('enum_name')
            if enum_name:
                try:
                    self.enum_combo.setCurrentText(enum_name)
                except:
                    pass

    def _refresh_enum_list(self):
        self.enum_combo.clear()
        enums = []
        if self.db is not None:
            try:
                res = self.db.list_enums()
                if isinstance(res, dict):
                    enums = list(res.keys())
                elif isinstance(res, list):
                    if all(isinstance(x, dict) and 'name' in x for x in res):
                        enums = [x['name'] for x in res]
                    else:
                        enums = res
            except Exception:
                enums = []
        self.enum_combo.addItems(enums)

    def open_enum_manager(self):

        dlg = EnumManagerDialog(db=self.db, parent=self)
        dlg.exec()
        self._refresh_enum_list()

    def update_fields_visibility(self, txt):
        t = txt.upper() if txt else ""
        is_text = "TEXT" in t and "ARRAY" not in t and "ENUM" not in t
        is_array = "ARRAY" in t
        is_enum = "ENUM" in t
        self.length_label.setVisible(is_text)
        self.length_spin.setVisible(is_text)
        self.array_label.setVisible(is_array)
        self.array_elem_combo.setVisible(is_array)
        self.enum_label.setVisible(is_enum)
        self.enum_combo.setVisible(is_enum)
        self.enum_manage_btn.setVisible(is_enum)

    def on_pk_toggled(self, checked):
        if checked:
            self.not_null.setChecked(True)
            self.unique.setChecked(True)
            self.not_null.setEnabled(False)
            self.unique.setEnabled(False)
        else:
            self.not_null.setEnabled(True)
            self.unique.setEnabled(True)

    def set_enum_mode(self):
        self.type_combo.setCurrentText("ENUM")
        self.type_combo.setEnabled(False)
        self.name_edit.setEnabled(False)
        self.not_null.setEnabled(False)
        self.unique.setEnabled(False)
        self.pk.setEnabled(False)
        self.default_edit.setEnabled(False)
        self.check_edit.setEnabled(False)
        self.length_spin.setEnabled(False)
        self.array_elem_combo.setEnabled(False)
        self.fk_table.setEnabled(False)
        self.fk_column.setEnabled(False)
        self.btn_ok.setEnabled(False)

    def get_data(self):
        default_val = self.default_edit.text().strip() if self.default_edit.text().strip() != "" else None
        check_val = self.check_edit.text().strip() if self.check_edit.text().strip() != "" else None
        length_val = int(self.length_spin.value()) if self.length_spin.value() else None
        array_elem = self.array_elem_combo.currentText() if self.array_elem_combo.currentText() else None
        enum_name = None
        if self.type_combo.currentText().upper() == 'ENUM':
            enum_name = self.enum_combo.currentText() if self.enum_combo.currentText() else None
        return {
            'name': self.name_edit.text().strip(),
            'type': self.type_combo.currentText(),
            'enum_name': enum_name,
            'not_null': self.not_null.isChecked(),
            'unique': self.unique.isChecked(),
            'primary_key': self.pk.isChecked(),
            'default': default_val,
            'check': check_val,
            'length': length_val,
            'array_elem_type': array_elem,
            'fk_table': self.fk_table.text().strip() if self.fk_table.text().strip() != "" else None,
            'fk_column': self.fk_column.text().strip() if self.fk_column.text().strip() != "" else None
        }


class ConfirmDialog(QDialog):
    def __init__(self, text: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Подтвердите")
        self.layout = QVBoxLayout(self)
        self.layout.addWidget(QLabel(text))
        row = QHBoxLayout()
        btn_ok = QPushButton("Да")
        btn_cancel = QPushButton("Нет")
        row.addStretch(1)
        row.addWidget(btn_cancel)
        row.addWidget(btn_ok)
        self.layout.addLayout(row)
        btn_ok.clicked.connect(self.accept)
        btn_cancel.clicked.connect(self.reject)


class EnumManagerDialog(QDialog):
    tablesChanged = Signal(str)
    def __init__(self, db, table_name: str = None, column_name: str = None, parent=None):
        super().__init__(parent)
        self.db = db
        self.table_name = table_name
        self.column_name = column_name
        self.setWindowTitle("Enum manager")
        self.layout = QVBoxLayout(self)

        self.enum_list = QListWidget()
        self.values_view = QPlainTextEdit()
        self.values_view.setReadOnly(True)

        row = QHBoxLayout()
        row.addWidget(self.enum_list, 1)
        row.addWidget(self.values_view, 2)
        self.layout.addLayout(row)

        btn_row = QHBoxLayout()
        self.btn_new = QPushButton("Создать")
        self.btn_delete = QPushButton("Удалить")
        self.btn_assign = QPushButton("Назначить колонке")
        self.btn_close = QPushButton("Закрыть")
        btn_row.addStretch(1)
        btn_row.addWidget(self.btn_new)
        btn_row.addWidget(self.btn_delete)
        btn_row.addWidget(self.btn_assign)
        btn_row.addWidget(self.btn_close)
        self.layout.addLayout(btn_row)

        self.enum_list.currentItemChanged.connect(self.on_enum_selected)
        self.btn_new.clicked.connect(self.create_enum)
        self.btn_delete.clicked.connect(self.delete_enum)
        self.btn_assign.clicked.connect(self.assign_enum_to_column)
        self.btn_close.clicked.connect(self.accept)

        self.current_enum = None
        self.refresh()

    def refresh(self):
        self.enum_list.clear()
        self.enum_map = {}
        self.current_enum = None

        if self.db is None:
            return

        try:
            res = self.db.list_enums()
            if isinstance(res, dict):
                self.enum_map = res
            elif isinstance(res, list):
                if all(isinstance(x, dict) and 'name' in x for x in res):
                    self.enum_map = {x['name']: x.get('values', []) for x in res}
                else:
                    self.enum_map = {name: [] for name in res}
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось получить enum-ы: {e}")
            return

        if self.table_name and self.column_name:
            try:
                cur = self.db.get_column_enum(self.table_name, self.column_name)
                self.current_enum = cur
            except Exception:
                self.current_enum = None

        for name in sorted(self.enum_map.keys()):
            item = QListWidgetItem(name)
            if name == self.current_enum:
                item.setBackground(QColor(255, 255, 200))
                font = item.font()
                font.setBold(True)
                item.setFont(font)
            self.enum_list.addItem(item)

    def on_enum_selected(self, cur: QListWidgetItem):
        if cur is None:
            self.values_view.setPlainText("")
            return
        name = cur.text()
        vals = self.enum_map.get(name, [])
        if vals:
            self.values_view.setPlainText('\n'.join(map(str, vals)))
        else:
            self.values_view.setPlainText('(значений нет или тип не возвращает список значений)')

    def create_enum(self):
        name, ok = QInputDialog.getText(self, "Создать enum", "Имя enum:")
        if not ok or not name.strip():
            return
        vals_txt, ok2 = QInputDialog.getText(self, "Создать enum", "Значения (через запятую):")
        if not ok2:
            return
        vals = [v.strip() for v in vals_txt.split(',') if v.strip()]
        if not vals:
            QMessageBox.warning(self, "Внимание", "Нужно хотя бы одно значение")
            return
        try:
            self.db.create_enum(name.strip(), vals)
            QMessageBox.information(self, "Успешно", f"Enum '{name}' создан")
            self.refresh()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))

    def delete_enum(self):
        cur = self.enum_list.currentItem()
        if not cur:
            return
        name = cur.text()
        if self.current_enum and name == self.current_enum:
            QMessageBox.warning(self, "Нельзя удалить", "Сначала смените тип колонки на другой, затем удаляйте этот enum.")
            return
        dlg = QMessageBox.question(self, "Подтвердите", f"Удалить enum '{name}'?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if dlg != QMessageBox.StandardButton.Yes:
            return
        try:
            self.db.drop_enum(name)
            QMessageBox.information(self, "Успешно", f"Enum '{name}' удалён")
            self.refresh()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))

    def assign_enum_to_column(self):

        if not (self.table_name and self.column_name):
            QMessageBox.warning(self, "Не указано", "Не заданы имя таблицы или колонки")
            return

        cur = self.enum_list.currentItem()
        if not cur:
            QMessageBox.information(self, "Не выбрано", "Выберите enum в списке")
            return
        new_enum = cur.text()

        if self.current_enum and new_enum == self.current_enum:
            QMessageBox.information(self, "Ничего не менять", "Колонка уже имеет этот enum")
            return

        conf = QMessageBox.question(
            self,
            "Подтвердите",
            f"Сменить тип колонки '{self.table_name}.{self.column_name}' на enum '{new_enum}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        if conf != QMessageBox.StandardButton.Yes:
            return

        try:
            incompatible = self.db.find_incompatible_enum_values(self.table_name, self.column_name, new_enum)
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось проверить несовместимости: {e}")
            return

        chosen_default = None
        if incompatible:
            enum_values = self.enum_map.get(new_enum)
            if enum_values is None:
                try:
                    enum_values = self.db._get_enum_values(new_enum)
                except Exception:
                    QMessageBox.critical(self, "Ошибка", "Не удалось получить значения enum")
                    return
            if not enum_values:
                QMessageBox.critical(self, "Ошибка", "Целевой enum не содержит значений")
                return

            preview = ", ".join(repr(x) for x in incompatible[:20])
            if len(incompatible) > 20:
                preview += f", ... ещё {len(incompatible) - 20}"
            msg = ("В колонке обнаружены значения, которых нет в выбранном enum:\n\n"
                   f"{preview}\n\n"
                   "Все такие значения будут заменены на одно выбранное значение из enum.")
            QMessageBox.information(self, "Несовместимые значения", msg)

            item, ok = QInputDialog.getItem(self, "Выберите значение", "Значение для замены:", enum_values, 0, False)
            if not ok:
                QMessageBox.information(self, "Отмена", "Операция отменена")
                return
            chosen_default = item

        try:
            self.db.replace_column_enum_by_swap(self.table_name, self.column_name, new_enum, default=chosen_default)
        except Exception as e:
            QMessageBox.critical(self, "Не удалось изменить тип колонки", f"{e}")
            return
        self.tablesChanged.emit(self.table_name if self.table_name is not None else "")
        QMessageBox.information(self, "Готово", "Тип колонки успешно изменён")
        self.current_enum = new_enum
        self.refresh()


class AlterTableDialog(QDialog):
    tablesChanged = Signal(str)

    def __init__(self, table_name: str, db, table_manager = None, parent=None):
        super().__init__(parent)
        self.db = db
        self.table_name = table_name
        self.table_manager = table_manager
        self.setWindowTitle(f"Alter table: {table_name}")
        self.layout = QVBoxLayout(self)
        top_row = QHBoxLayout()
        self.label_table = QLabel(f"Table: {table_name}")
        btn_rename = QPushButton("Переименовать")
        top_row.addWidget(self.label_table)
        top_row.addStretch(1)
        top_row.addWidget(btn_rename)
        self.layout.addLayout(top_row)
        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.columns_container = QWidget()
        self.columns_layout = QVBoxLayout(self.columns_container)
        self.columns_layout.setContentsMargins(2,2,2,2)
        self.columns_layout.setSpacing(2)
        self.scroll.setWidget(self.columns_container)
        self.layout.addWidget(self.scroll)
        bottom_row = QHBoxLayout()
        self.btn_add = QPushButton("Добавить столбец")
        self.btn_close = QPushButton("Закрыть")
        bottom_row.addStretch(1)
        bottom_row.addWidget(self.btn_add)
        bottom_row.addWidget(self.btn_close)
        self.layout.addLayout(bottom_row)
        btn_rename.clicked.connect(self.handle_rename)
        self.btn_add.clicked.connect(self.handle_add)
        self.btn_close.clicked.connect(self.reject)
        self.refresh_from_db()

    def refresh_from_db(self):
        try:
            self.table = self.db.get_table(self.table_name)
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось получить метаданные: {e}")
            return
        self.label_table.setText(f"Table: {self.table_name}")
        for i in reversed(range(self.columns_layout.count())):
            item = self.columns_layout.itemAt(i)
            w = item.widget() if item else None
            if w:
                w.setParent(None)
                w.deleteLater()
        self.column_rows = []
        idx = 0
        uniques = []
        try:
            uniques = [i['column_names'][0] for i in self.db.insp.get_unique_constraints(self.table.name)]
        except:
            uniques = []
        for col in self.table.columns:
            row = QWidget()
            hl = QHBoxLayout(row)
            hl.setContentsMargins(6,6,6,6)
            hl.setSpacing(8)
            name_label = QLabel(col.name)
            f = QFont()
            f.setBold(True)
            name_label.setFont(f)
            name_label.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
            name_label.setMaximumWidth(140)
            try:
                type_text = str(col.type)
            except:
                type_text = ""
            is_enum = False
            enum_name = None
            try:
                if "ENUM" in type_text.upper():
                    is_enum = True
                else:
                    if hasattr(col.type, 'enums') and getattr(col.type, 'enums') is not None:
                        is_enum = True
                enum_name = getattr(col.type, 'name', None)
                if not enum_name:
                    m = re.search(r"'?(?P<ename>[a-zA-Z0-9_]+)'?\s*:\s*enum", type_text)
                    if m:
                        enum_name = m.group('ename')
            except:
                is_enum = False
            meta_text = []
            if is_enum:
                meta_text.append(f"ENUM({enum_name})" if enum_name else "ENUM")
            else:
                meta_text.append(type_text)
            meta_text.append("NOT NULL" if not col.nullable else "NULL")
            if col.name in uniques:
                meta_text.append("UNIQUE")
            if getattr(col, "primary_key", False):
                meta_text.append("PK")
            default_txt = None
            try:
                if getattr(col, "server_default", None) is not None:
                    default_txt = str(col.server_default.arg)
            except:
                default_txt = None
            if default_txt:
                meta_text.append(f"DEFAULT={default_txt}")
            fk_txt = None
            try:
                if hasattr(col, 'foreign_keys') and col.foreign_keys:
                    for fk in col.foreign_keys:
                        fk_txt = f"{fk.column.table.name}({fk.column.name})"
            except:
                fk_txt = None
            if fk_txt:
                meta_text.append(f"FK={fk_txt}")
            check_txt = None
            try:
                check_constraints = [f"({check.sqltext})" for check in col.table.constraints if hasattr(check, 'sqltext') and col.name in str(check.sqltext)]
                if check_constraints:
                    check_txt = check_constraints[0]
            except:
                check_txt = None
            if check_txt:
                meta_text.append(f"CHECK={check_txt}")
            try:
                m = re.search(r'\(\s*(\d+)\s*\)', type_text)
                if m and not is_enum:
                    meta_text.append(f"LEN={m.group(1)}")
            except:
                pass
            meta_full = " | ".join(meta_text)
            meta_line = QLineEdit(meta_full)
            meta_line.setReadOnly(True)
            meta_line.setToolTip(meta_full)
            meta_line.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
            btn_edit = QPushButton("Изменить")
            btn_delete = QPushButton("Удалить")
            btn_edit.setFixedWidth(90)
            btn_delete.setFixedWidth(90)
            hl.addWidget(name_label)
            hl.addWidget(meta_line)
            hl.addWidget(btn_edit)
            hl.addWidget(btn_delete)
            self.columns_layout.addWidget(row)
            if idx % 2 == 0:
                row.setStyleSheet("QWidget { background: #ffffff; }")
            else:
                row.setStyleSheet("QWidget { background: #fbfbfb; }")

            if is_enum:
                btn_edit.setEnabled(True)
                btn_edit.setToolTip("Управление enum")
            else:
                btn_edit.setEnabled(True)

            self.column_rows.append((col, row))
            cid = idx
            if is_enum:
                btn_edit.clicked.connect(lambda _checked, c=col, en=enum_name: self.handle_enum_edit(c, en))
            else:
                btn_edit.clicked.connect(lambda _checked, c=col: self.handle_edit(c))
            btn_delete.clicked.connect(lambda _checked, c=col: self.handle_delete(c))
            idx += 1

    def build_data_from_table(self, table):
        data = {}
        idx = 0
        uniques = []
        try:
            uniques = [i['column_names'][0] for i in self.db.insp.get_unique_constraints(table.name)]
        except:
            uniques = []
        for col in table.columns:
            try:
                type_text = str(col.type)
            except:
                type_text = "TEXT"
            array_elem = None
            if "ARRAY" in type_text.upper():
                try:
                    array_elem = str(col.type.item_type)
                except:
                    array_elem = None
            length_val = None
            try:
                m = re.search(r'\(\s*(\d+)\s*\)', type_text)
                if m:
                    length_val = int(m.group(1))
            except:
                length_val = None
            default_val = None
            try:
                if getattr(col, "server_default", None) is not None:
                    default_val = str(col.server_default.arg)
            except:
                default_val = None
            check_val = None
            try:
                check_constraints = [f"({check.sqltext})" for check in col.table.constraints if hasattr(check, 'sqltext') and col.name in str(check.sqltext)]
                if check_constraints:
                    check_val = check_constraints[0]
            except:
                check_val = None
            fk_table = None
            fk_column = None
            try:
                if hasattr(col, 'foreign_keys') and col.foreign_keys:
                    for fk in col.foreign_keys:
                        fk_table = fk.column.table.name
                        fk_column = fk.column.name
            except:
                fk_table = None
                fk_column = None
            detected_type = "TEXT"
            enum_name = None
            if "ARRAY" in type_text.upper():
                detected_type = "ARRAY"
            else:
                is_enum = False
                if "ENUM" in type_text.upper():
                    is_enum = True
                else:
                    try:
                        if hasattr(col.type, 'enums') and getattr(col.type, 'enums') is not None:
                            is_enum = True
                    except:
                        is_enum = False
                if is_enum:
                    detected_type = "ENUM"
                    enum_name = getattr(col.type, 'name', None)
                elif "TEXT" in type_text.upper():
                    detected_type = "TEXT"
                elif "INT" in type_text.upper() or "INTEGER" in type_text.upper():
                    detected_type = "INTEGER"
                elif any(x in type_text.upper() for x in ["REAL","FLOAT","DOUBLE"]):
                    detected_type = "REAL"
                elif any(x in type_text.upper() for x in ["DATE","TIME"]):
                    detected_type = "DATE"
                elif any(x in type_text.upper() for x in ["BOOL","BOOLEAN"]):
                    detected_type = "BOOLEAN"
                else:
                    detected_type = "TEXT"
            data[idx] = {
                'name': col.name,
                'type': detected_type,
                'enum_name': enum_name,
                'not_null': not col.nullable,
                'unique': col.name in uniques,
                'primary_key': bool(getattr(col, 'primary_key', False)),
                'default': default_val,
                'check': check_val,
                'length': length_val,
                'array_elem_type': array_elem,
                'fk_table': fk_table,
                'fk_column': fk_column
            }
            idx += 1
        return data

    def handle_rename(self):
        dlg = RenameTableDialog(self.table_name, self)
        if dlg.exec() == QDialog.Accepted:
            new_name = dlg.new_name()
            if not new_name:
                QMessageBox.warning(self, "Внимание", "Имя не может быть пустым")
                return
            old_data = self.build_data_from_table(self.table)
            new_data = old_data.copy()
            try:
                self.db.alter_table(self.table_name, new_name, old_data, new_data)
                print(old_data, new_data)
                QMessageBox.information(self, "Успешно", "Таблица переименована")
                self.table_name = new_name
                self.tablesChanged.emit(self.table_name)
                if self.table_manager:
                    self.table_manager.handle_external_change(self.table_name)
                self.refresh_from_db()
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", str(e))

    def handle_add(self):
        dlg = ColumnEditorDialog(parent=self, db=self.db)
        if dlg.exec() == QDialog.Accepted:
            col_data = dlg.get_data()
            if not col_data.get('name'):
                QMessageBox.warning(self, "Внимание", "Имя столбца не может быть пустым")
                return
            old_data = self.build_data_from_table(self.table)
            new_data = {}
            idx = 0
            for v in old_data.values():
                new_data[idx] = v
                idx += 1
            new_data[idx] = col_data
            try:
                self.db.alter_table(self.table_name, self.table_name, old_data, new_data)
                print(old_data, new_data)
                QMessageBox.information(self, "Успешно", "Столбец добавлен")
                self.refresh_from_db()
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", str(e))

    def handle_edit(self, col):
        cur_data = None
        try:
            tmp = self.build_data_from_table(self.table)
            for v in tmp.values():
                if v.get('name') == col.name:
                    cur_data = v
                    break
        except:
            cur_data = None
        dlg = ColumnEditorDialog(column_data=cur_data or {}, parent=self, db=self.db)
        if dlg.exec() == QDialog.Accepted:
            new_col = dlg.get_data()
            old_data = self.build_data_from_table(self.table)
            new_data = {}
            idx = 0
            replaced = False
            for k, v in old_data.items():
                if v.get('name') == col.name and not replaced:
                    new_data[idx] = new_col
                    replaced = True
                else:
                    new_data[idx] = v
                idx += 1
            try:
                self.db.alter_table(self.table_name, self.table_name, old_data, new_data)
                print(old_data, new_data)
                QMessageBox.information(self, "Успешно", "Столбец изменен")
                self.refresh_from_db()
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", str(e))

    def handle_delete(self, col):
        dlg = ConfirmDialog(f"Удалить столбец '{col.name}'?", self)
        if dlg.exec() == QDialog.Accepted:
            old_data = self.build_data_from_table(self.table)
            new_data = {}
            idx = 0
            for v in old_data.values():
                if v.get('name') == col.name:
                    continue
                new_data[idx] = v
                idx += 1
            try:
                self.db.alter_table(self.table_name, self.table_name, old_data, new_data)
                QMessageBox.information(self, "Успешно", "Столбец удален")
                self.refresh_from_db()
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", str(e))

    def handle_enum_edit(self, col, enum_name=None):
        dlg = EnumManagerDialog(db=self.db, table_name=self.table_name, column_name=col.name, parent=self)

        if dlg.exec() == QDialog.Accepted:
            self.refresh_from_db()
            dlg.tablesChanged.connect(self.table_manager.handle_external_change)
