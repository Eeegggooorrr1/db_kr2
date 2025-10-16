# from sqlalchemy import Table, Column, Integer, String, MetaData, UniqueConstraint, Boolean
# from sqlalchemy import create_engine
# import os
#
# from config import settings
#
# DATABASE_URL = settings.get_db_url()
#
# engine = create_engine(DATABASE_URL, future=True)
# metadata = MetaData()
#
# users = Table(
#     "users",
#     metadata,
#     Column("id", Integer, primary_key=True),
#     Column("username", String(50), nullable=False, unique=True),
#     Column("full_name", String(120), nullable=True),
#     Column("age", Integer, nullable=True),
# )
# test = Table(
#     "test",
#     metadata,
#     Column("id", Integer, primary_key=True),
#     Column("blob_col", Boolean),
# )
#
# products = Table(
#     "products",
#     metadata,
#     Column("id", Integer, primary_key=True),
#     Column("code", String(40), nullable=False, unique=True),
#     Column("description", String(255), nullable=True),
#     Column("quantity", Integer, nullable=False),
# )
#
# if __name__ == "__main__":
#     metadata.create_all(engine)
#     print("Demo tables created (if not existed).")

# import enum
# from sqlalchemy import (
#     MetaData, Table, Column, Integer, String, Text, Date,
#     TIMESTAMP, Float, Boolean, ForeignKey, ARRAY, Enum,
#     func, text, CheckConstraint, UniqueConstraint, ForeignKeyConstraint, create_engine
# )
# from sqlalchemy.schema import CreateTable
#
#
# # Определение enum
# class AttackTypeEnum(str, enum.Enum):
#     no_attack = "no_attack"
#     blur = "blur"
#     noise = "noise"
#     adversarial = "adversarial"
#     other = "other"
#
# from config import settings
#
# DATABASE_URL = settings.get_db_url()
#
# engine = create_engine(DATABASE_URL, future=True)
# # Метаданные
# metadata = MetaData()
#
# # Таблица experiments
# experiments = Table(
#     "experiments",
#     metadata,
#     Column("experiment_id", Integer, primary_key=True, autoincrement=True),
#     Column("name", String(255), nullable=False),
#     Column("description", Text, nullable=True),
#     Column("created_date", Date, server_default=func.current_date()),
#
#     # Констреинты на стороне сервера
#     UniqueConstraint("name", name="uq_experiments_name"),
# )
#
# # Таблица runs
# runs = Table(
#     "runs",
#     metadata,
#     Column("run_id", Integer, primary_key=True, autoincrement=True),
#     Column("experiment_id", Integer, nullable=False),
#     Column("run_date", TIMESTAMP, server_default=func.now()),
#     Column("accuracy", Float, nullable=True),
#     Column("flagged", Boolean, nullable=True),
#
#     # Констреинты на стороне сервера
#     ForeignKeyConstraint(
#         ["experiment_id"],
#         ["experiments.experiment_id"],
#         ondelete="CASCADE",
#         name="fk_runs_experiment_id"
#     ),
#     CheckConstraint(
#         "accuracy IS NULL OR (accuracy >= 0 AND accuracy <= 1)",
#         name="ck_runs_accuracy_range"
#     )
# )
#
# # Таблица images
# images = Table(
#     "images",
#     metadata,
#     Column("image_id", Integer, primary_key=True, autoincrement=True),
#     Column("run_id", Integer, nullable=False),
#     Column("file_path", String(500), nullable=False),
#     Column("original_name", String(255), nullable=True),
#     Column("attack_type", Enum(AttackTypeEnum, name="attack_type_enum"), nullable=False),
#     Column("added_date", TIMESTAMP, server_default=text("DATE_TRUNC('second', NOW()::timestamp)")),
#     Column("coordinates", ARRAY(Integer), nullable=True),
#
#     # Констреинты на стороне сервера
#     ForeignKeyConstraint(
#         ["run_id"],
#         ["runs.run_id"],
#         ondelete="CASCADE",
#         name="fk_images_run_id"
#     ),
#     UniqueConstraint("file_path", name="uq_images_file_path"),
#     CheckConstraint(
#         "array_length(coordinates, 1) IS NULL OR array_length(coordinates, 1) = 4",
#         name="ck_images_coordinates_length"
#     )
# )
#
# metadata.create_all(engine)


# идея селекта 1





# def add_column_widget(self, col=None):
#     import re
#     widget = QWidget()
#     layout = QHBoxLayout(widget)
#     layout.setContentsMargins(0, 0, 0, 0)
#
#     if not hasattr(self, 'next_column_id'):
#         self.next_column_id = 0
#
#     column_id = self.next_column_id
#     self.next_column_id += 1
#
#     widget.column_id = column_id
#
#     name_edit = QLineEdit(col.name if col is not None else "")
#     type_combo = QComboBox()
#     type_combo.addItems(["INTEGER", "TEXT", "REAL", "DATE", "BOOLEAN", "ARRAY", "ENUM"])
#
#     not_null_check = QCheckBox("NOT NULL")
#     unique_check = QCheckBox("UNIQUE")
#     pk_check = QCheckBox("PK")
#     # unique_check.toggled.connect(lambda state: print(f"Unique toggled: {state}"))
#     # pk_check.toggled.connect(lambda state: print(f"PK toggled: {state}"))
#     # not_null_check.toggled.connect(lambda state: print(f"NOT NULL toggled: {state}"))
#
#     default_edit = QLineEdit()
#     default_edit.setPlaceholderText("DEFAULT value")
#     default_bool = QCheckBox("DEFAULT")
#     default_bool.setVisible(False)
#
#     check_edit = QLineEdit()
#     check_edit.setPlaceholderText("CHECK condition")
#
#     length_label = QLabel("Length:")
#     length_spin = QSpinBox()
#     length_spin.setRange(0, 1000000)
#     length_label.setVisible(False)
#     length_spin.setVisible(False)
#
#     array_label = QLabel("Array elem type:")
#     array_elem_combo = QComboBox()
#     array_elem_combo.addItems(["INTEGER", "TEXT", "REAL", "DATE", "BOOLEAN"])
#     array_label.setVisible(False)
#     array_elem_combo.setVisible(False)
#
#     fk_edit = QLineEdit()
#     fk_edit.setPlaceholderText("FK table")
#
#     fk_column_edit = QLineEdit()
#     fk_column_edit.setPlaceholderText("FK column")
#
#     btn_remove = QPushButton("X", styleSheet="color: red;")
#     text_edits = [name_edit, default_edit, check_edit, fk_edit, fk_column_edit]
#
#     for edit in text_edits:
#         edit.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
#         edit.setMinimumWidth(95)
#
#     widget.not_null_check = not_null_check
#     widget.unique_check = unique_check
#     widget.pk_check = pk_check
#     widget.default_edit = default_edit
#     widget.default_bool = default_bool
#     widget.check_edit = check_edit
#     widget.length_spin = length_spin
#     widget.length_label = length_label
#     widget.array_elem_combo = array_elem_combo
#     widget.array_label = array_label
#
#     def enforce_pk_state():
#         widgets = list(self.column_widgets.values())
#         active = None
#         for w in widgets:
#             if hasattr(w, 'pk_check') and w.pk_check.isChecked():
#                 active = w
#                 break
#         if active:
#             for w in widgets:
#                 if w is active:
#                     if hasattr(w, 'unique_check'):
#                         if not w.unique_check.isChecked():
#                             w.unique_check.blockSignals(True)
#                             w.unique_check.setChecked(True)
#                             w.unique_check.blockSignals(False)
#                         if w.unique_check.isEnabled():
#                             w.unique_check.setEnabled(False)
#                     if hasattr(w, 'not_null_check'):
#                         if not w.not_null_check.isChecked():
#                             w.not_null_check.blockSignals(True)
#                             w.not_null_check.setChecked(True)
#                             w.not_null_check.blockSignals(False)
#                         if w.not_null_check.isEnabled():
#                             w.not_null_check.setEnabled(False)
#                     if hasattr(w, 'default_edit'):
#                         if w.default_edit.isEnabled():
#                             w.default_edit.clear()
#                             w.default_edit.setEnabled(False)
#                     if hasattr(w, 'default_bool'):
#                         if w.default_bool.isEnabled():
#                             w.default_bool.setChecked(False)
#                             w.default_bool.setEnabled(False)
#                     if hasattr(w, 'check_edit'):
#                         if w.check_edit.isEnabled():
#                             w.check_edit.clear()
#                             w.check_edit.setEnabled(False)
#                 else:
#                     if hasattr(w, 'pk_check'):
#                         if w.pk_check.isChecked():
#                             w.pk_check.blockSignals(True)
#                             w.pk_check.setChecked(False)
#                             w.pk_check.blockSignals(False)
#                         if w.pk_check.isEnabled():
#                             w.pk_check.setEnabled(False)
#         else:
#             for w in widgets:
#                 if hasattr(w, 'pk_check') and not w.pk_check.isEnabled():
#                     w.pk_check.setEnabled(True)
#                 if hasattr(w, 'unique_check'):
#                     if not w.unique_check.isEnabled():
#                         w.unique_check.setEnabled(True)
#                 if hasattr(w, 'not_null_check'):
#                     if not w.not_null_check.isEnabled():
#                         w.not_null_check.setEnabled(True)
#                 if hasattr(w, 'default_edit'):
#                     if not w.default_edit.isEnabled():
#                         w.default_edit.setEnabled(True)
#                 if hasattr(w, 'default_bool'):
#                     if not w.default_bool.isEnabled():
#                         w.default_bool.setEnabled(True)
#                 if hasattr(w, 'check_edit'):
#                     if not w.check_edit.isEnabled():
#                         w.check_edit.setEnabled(True)
#                 if hasattr(w, 'length_spin'):
#                     if not w.length_spin.isEnabled():
#                         w.length_spin.setEnabled(True)
#                 if hasattr(w, 'length_label'):
#                     if not w.length_label.isEnabled():
#                         w.length_label.setEnabled(True)
#                 if hasattr(w, 'array_elem_combo'):
#                     if not w.array_elem_combo.isEnabled():
#                         w.array_elem_combo.setEnabled(True)
#                 if hasattr(w, 'array_label'):
#                     if not w.array_label.isEnabled():
#                         w.array_label.setEnabled(True)
#
#     def pk_toggled_handler(state, w=widget):
#         if state:
#             for other in list(self.column_widgets.values()):
#                 if other is not w and hasattr(other, 'pk_check') and other.pk_check.isChecked():
#                     w.pk_check.blockSignals(True)
#                     w.pk_check.setChecked(False)
#                     w.pk_check.blockSignals(False)
#                     return
#         enforce_pk_state()
#
#     pk_check.toggled.connect(lambda state, w=widget: pk_toggled_handler(state, w))
#
#     def apply_type_logic(t):
#         t_upper = t.upper() if isinstance(t, str) else str(t).upper()
#         if t_upper == "ENUM":
#             if widget.pk_check.isChecked():
#                 widget.pk_check.blockSignals(True)
#                 widget.pk_check.setChecked(False)
#                 widget.pk_check.blockSignals(False)
#             if widget.unique_check.isEnabled():
#                 widget.unique_check.setEnabled(False)
#             if widget.not_null_check.isEnabled():
#                 widget.not_null_check.setEnabled(False)
#             if widget.pk_check.isEnabled():
#                 widget.pk_check.setEnabled(False)
#             if widget.default_edit.isEnabled():
#                 widget.default_edit.clear()
#                 widget.default_edit.setEnabled(False)
#             if widget.default_bool.isEnabled():
#                 widget.default_bool.setChecked(False)
#                 widget.default_bool.setEnabled(False)
#             if widget.check_edit.isEnabled():
#                 widget.check_edit.clear()
#                 widget.check_edit.setEnabled(False)
#             if widget.length_spin.isVisible():
#                 widget.length_spin.setVisible(False)
#             if widget.length_label.isVisible():
#                 widget.length_label.setVisible(False)
#             if widget.array_elem_combo.isVisible():
#                 widget.array_elem_combo.setVisible(False)
#             if widget.array_label.isVisible():
#                 widget.array_label.setVisible(False)
#             enforce_pk_state()
#             return
#         if t_upper == "BOOLEAN":
#             if widget.default_edit.isVisible():
#                 widget.default_edit.setVisible(False)
#                 widget.default_edit.setEnabled(False)
#             if not widget.default_bool.isVisible():
#                 widget.default_bool.setVisible(True)
#                 widget.default_bool.setEnabled(True)
#             if widget.check_edit.isVisible():
#                 widget.check_edit.setVisible(False)
#                 widget.check_edit.setEnabled(False)
#             if widget.length_spin.isVisible():
#                 widget.length_spin.setVisible(False)
#             if widget.length_label.isVisible():
#                 widget.length_label.setVisible(False)
#             if widget.array_elem_combo.isVisible():
#                 widget.array_elem_combo.setVisible(False)
#             if widget.array_label.isVisible():
#                 widget.array_label.setVisible(False)
#         elif t_upper == "TEXT":
#             if not widget.default_edit.isVisible():
#                 widget.default_edit.setVisible(True)
#                 widget.default_edit.setEnabled(True)
#             if widget.default_bool.isVisible():
#                 widget.default_bool.setVisible(False)
#                 widget.default_bool.setEnabled(False)
#             if not widget.check_edit.isVisible():
#                 widget.check_edit.setVisible(True)
#                 widget.check_edit.setEnabled(True)
#             if not widget.length_spin.isVisible():
#                 widget.length_spin.setVisible(True)
#             if not widget.length_label.isVisible():
#                 widget.length_label.setVisible(True)
#             if widget.array_elem_combo.isVisible():
#                 widget.array_elem_combo.setVisible(False)
#             if widget.array_label.isVisible():
#                 widget.array_label.setVisible(False)
#         elif t_upper == "ARRAY":
#             if not widget.default_edit.isVisible():
#                 widget.default_edit.setVisible(True)
#                 widget.default_edit.setEnabled(True)
#             if widget.default_bool.isVisible():
#                 widget.default_bool.setVisible(False)
#                 widget.default_bool.setEnabled(False)
#             if not widget.check_edit.isVisible():
#                 widget.check_edit.setVisible(True)
#                 widget.check_edit.setEnabled(True)
#             if widget.length_spin.isVisible():
#                 widget.length_spin.setVisible(False)
#             if widget.length_label.isVisible():
#                 widget.length_label.setVisible(False)
#             if not widget.array_elem_combo.isVisible():
#                 widget.array_elem_combo.setVisible(True)
#                 widget.array_elem_combo.setEnabled(True)
#             if not widget.array_label.isVisible():
#                 widget.array_label.setVisible(True)
#         else:
#             if not widget.default_edit.isVisible():
#                 widget.default_edit.setVisible(True)
#                 widget.default_edit.setEnabled(True)
#             if widget.default_bool.isVisible():
#                 widget.default_bool.setVisible(False)
#                 widget.default_bool.setEnabled(False)
#             if not widget.check_edit.isVisible():
#                 widget.check_edit.setVisible(True)
#                 widget.check_edit.setEnabled(True)
#             if widget.length_spin.isVisible():
#                 widget.length_spin.setVisible(False)
#             if widget.length_label.isVisible():
#                 widget.length_label.setVisible(False)
#             if widget.array_elem_combo.isVisible():
#                 widget.array_elem_combo.setVisible(False)
#             if widget.array_label.isVisible():
#                 widget.array_label.setVisible(False)
#         enforce_pk_state()
#
#     type_combo.currentTextChanged.connect(lambda t: apply_type_logic(t))
#
#     def on_remove_clicked(w=widget):
#         was_pk = False
#         try:
#             was_pk = bool(w.pk_check.isChecked())
#         except Exception:
#             was_pk = False
#         self.remove_column_widget(w)
#         if was_pk:
#             enforce_pk_state()
#
#     btn_remove.clicked.connect(lambda _checked, w=widget: on_remove_clicked(w))
#
#     if col is not None:
#
#         try:
#             type_text = str(col.type)
#             tt_upper = type_text.upper()
#             if "CHAR" in tt_upper or "VARCHAR" in tt_upper or "STRING" in tt_upper:
#                 type_combo.setCurrentText("TEXT")
#                 m = re.search(r'\(\s*(\d+)\s*\)', type_text)
#                 if m:
#                     length_spin.setValue(int(m.group(1)))
#                     length_label.setVisible(True)
#                     length_spin.setVisible(True)
#                 else:
#                     check_constraints = [f"({check.sqltext})" for check in col.table.constraints
#                                          if hasattr(check, 'sqltext') and col.name in str(check.sqltext)]
#                     check_text = " ".join(check_constraints)
#                     m2 = re.search(r'length\(\s*{}[^\)]*\)\s*(?:<=|<)\s*(\d+)'.format(re.escape(col.name)),
#                                    check_text, re.IGNORECASE)
#                     if not m2:
#                         m2 = re.search(r'char_length\(\s*{}[^\)]*\)\s*(?:<=|<)\s*(\d+)'.format(re.escape(col.name)),
#                                        check_text, re.IGNORECASE)
#                     if not m2:
#                         m2 = re.search(r'{}\s*(?:<=|<)\s*(\d+)'.format(re.escape(col.name)), check_text,
#                                        re.IGNORECASE)
#                     if m2:
#                         length_spin.setValue(int(m2.group(1)))
#                         length_label.setVisible(True)
#                         length_spin.setVisible(True)
#             else:
#                 mapped = "TEXT" if ("TEXT" in tt_upper) else None
#                 if mapped:
#                     type_combo.setCurrentText(mapped)
#                 else:
#                     simple = None
#                     if "INTEGER" in tt_upper or "INT" in tt_upper:
#                         simple = "INTEGER"
#                     elif "REAL" in tt_upper or "FLOAT" in tt_upper or "DOUBLE" in tt_upper:
#                         simple = "REAL"
#                     elif "DATE" in tt_upper or "TIME" in tt_upper:
#                         simple = "DATE"
#                     elif "BOOLEAN" in tt_upper or "BOOL" in tt_upper:
#                         simple = "BOOLEAN"
#                     elif "ARRAY" in tt_upper:
#                         simple = "ARRAY"
#                         array_elem_combo.setCurrentText(str(col.type.item_type))
#                     elif "ENUM" in tt_upper:
#                         simple = "ENUM"
#                     if simple:
#                         type_combo.setCurrentText(simple)
#                     else:
#                         type_combo.setCurrentText("TEXT")
#         except:
#             try:
#                 type_combo.setCurrentText(str(col.type))
#             except:
#                 type_combo.setCurrentText("TEXT")
#         not_null_check.setChecked(not col.nullable)
#         unique_check.setChecked(
#             bool(col.name in [i['column_names'][0] for i in self.db.insp.get_unique_constraints(self.table.name)]))
#         pk_check.setChecked(bool(col.primary_key))
#         try:
#             if col.server_default is not None:
#                 default_edit.setText(str(col.server_default.arg))
#
#             check_constraints = [f"({check.sqltext})" for check in col.table.constraints
#                                  if hasattr(check, 'sqltext') and col.name in str(check.sqltext)]
#             if check_constraints:
#                 check_edit.setText(check_constraints[0])
#             if hasattr(col, 'foreign_keys') and col.foreign_keys:
#                 for fk in col.foreign_keys:
#                     fk_edit.setText(fk.column.table.name)
#                     fk_column_edit.setText(fk.column.name)
#         except:
#             pass
#
#     layout.addWidget(QLabel("Name:"))
#     layout.addWidget(name_edit, 1)
#     layout.addWidget(QLabel("Type:"))
#     layout.addWidget(type_combo, 1)
#     layout.addWidget(not_null_check, 1)
#     layout.addWidget(unique_check)
#     layout.addWidget(pk_check, 1)
#     layout.addWidget(QLabel("Default:"))
#     layout.addWidget(default_edit, 1)
#     layout.addWidget(default_bool)
#     layout.addWidget(QLabel("Check:"))
#     layout.addWidget(check_edit, 1)
#     layout.addWidget(length_label)
#     layout.addWidget(length_spin)
#     layout.addWidget(array_label)
#     layout.addWidget(array_elem_combo)
#     layout.addWidget(QLabel("FK Table:"))
#     layout.addWidget(fk_edit, 1)
#     layout.addWidget(QLabel("FK Column:"))
#     layout.addWidget(fk_column_edit, 1)
#     layout.addWidget(btn_remove)
#
#     self.columns_layout.addWidget(widget)
#     self.column_widgets[column_id] = widget
#
#     apply_type_logic(type_combo.currentText())
#     enforce_pk_state()
#
#     return column_id, widget
#
#
# def get_column_data(self, widget):
#     children = widget.findChildren(QLineEdit) + widget.findChildren(QComboBox) + widget.findChildren(
#         QCheckBox) + widget.findChildren(QSpinBox)
#
#     name_edit = None
#     type_combo = None
#     not_null_check = None
#     unique_check = None
#     pk_check = None
#     default_edit = None
#     default_bool = None
#     check_edit = None
#     fk_edit = None
#     fk_column_edit = None
#     length_spin = None
#     array_elem_combo = None
#
#     for child in children:
#         if isinstance(child, QLineEdit):
#             ph = (child.placeholderText() or "").strip()
#             if ph == "DEFAULT value":
#                 default_edit = child
#             elif ph == "CHECK condition":
#                 check_edit = child
#             elif ph == "FK table":
#                 fk_edit = child
#             elif ph == "FK column":
#                 fk_column_edit = child
#             else:
#                 if name_edit is None:
#                     name_edit = child
#         elif isinstance(child, QComboBox):
#             if type_combo is None:
#                 type_combo = child
#             else:
#                 array_elem_combo = child
#         elif isinstance(child, QCheckBox):
#             text = (child.text() or "").strip()
#             if text == "NOT NULL":
#                 not_null_check = child
#             elif text == "UNIQUE":
#                 unique_check = child
#             elif text == "PK":
#                 pk_check = child
#             elif text == "DEFAULT":
#                 default_bool = child
#         elif isinstance(child, QSpinBox):
#             if length_spin is None:
#                 length_spin = child
#
#     if name_edit and type_combo:
#         if default_bool is not None and getattr(default_bool, "isVisible", lambda: True)():
#             default_val = bool(default_bool.isChecked())
#         elif default_edit is not None and getattr(default_edit, "isVisible", lambda: True)():
#             txt = default_edit.text()
#             default_val = txt if txt != "" else None
#         else:
#             print('deeeeeeeeeeeeeeeeee')
#             default_val = None
#
#         check_val = check_edit.text() if check_edit and getattr(check_edit, "isVisible",
#                                                                 lambda: True)() and check_edit.text() != "" else None
#
#         if length_spin is not None:
#             try:
#                 length_val = int(length_spin.value())
#             except Exception:
#                 length_val = None
#         else:
#             length_val = None
#
#         if array_elem_combo is not None and getattr(array_elem_combo, "isVisible", lambda: False)():
#             print(array_elem_combo.currentText(), 'dddddddddddddd')
#             array_elem = array_elem_combo.currentText()
#         else:
#             array_elem = None
#         return {
#             'name': name_edit.text(),
#             'type': type_combo.currentText(),
#             'not_null': not_null_check.isChecked() if not_null_check else False,
#             'unique': unique_check.isChecked() if unique_check else False,
#             'primary_key': pk_check.isChecked() if pk_check else False,
#             'default': default_val,
#             'check': check_val,
#             'length': length_val,
#             'array_elem_type': array_elem,
#             'fk_table': fk_edit.text() if fk_edit else None,
#             'fk_column': fk_column_edit.text() if fk_column_edit else None
#         }