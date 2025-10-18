import logging
from typing import Optional, Dict, Callable
import sys

from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QHBoxLayout, QVBoxLayout, QPushButton, QLabel,
    QComboBox, QStackedWidget, QFrame, QSplitter, QSizePolicy, QScrollArea, QMessageBox, QTextEdit
)
from PySide6.QtCore import Qt, QSize, Signal, QObject

class LogEmitter(QObject):
    log = Signal(str)

class QtLoggingHandler(logging.Handler):
    def __init__(self, emitter):
        super().__init__()
        self.emitter = emitter
    def emit(self, record):
        try:
            msg = self.format(record)
            self.emitter.log.emit(msg)
        except Exception:
            pass

class LogsWindow(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        self.clearBtn = QPushButton("Очистить")
        self.logView = QTextEdit()
        self.logView.setReadOnly(True)
        layout.addWidget(self.clearBtn)
        layout.addWidget(self.logView)
        self.emitter = LogEmitter()
        self.handler = QtLoggingHandler(self.emitter)
        formatter = logging.Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s")
        self.handler.setFormatter(formatter)
        rootLogger = logging.getLogger()
        rootLogger.addHandler(self.handler)
        if rootLogger.level > logging.DEBUG:
            rootLogger.setLevel(logging.DEBUG)
        sqlalchemyLogger = logging.getLogger("sqlalchemy")
        if sqlalchemyLogger.level > logging.INFO:
            sqlalchemyLogger.setLevel(logging.INFO)
        self.emitter.log.connect(self.appendLog)
        self.clearBtn.clicked.connect(self.logView.clear)

    def appendLog(self, text):
        self.logView.append(text)