from PySide6.QtCore import QStringListModel, Signal, QObject


class TableManager(QObject):
    tablesChanged = Signal(list)

    def __init__(self, db, parent=None):
        super().__init__(parent)
        self.db = db
        self.model = QStringListModel(parent=self)
        self._tables = []
        self.refresh()

    def refresh(self):
        try:
            tables = self.db.list_tables()
        except Exception:
            tables = []
        self._tables = tables
        self.model.setStringList(tables)
        self.tablesChanged.emit(tables)

    def tables(self):
        return list(self._tables)

    def handle_external_change(self, new_name: str = None):
        self.refresh()