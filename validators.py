# src/app/validators.py
from __future__ import annotations
from typing import Any, Dict, Optional, List, Type
from abc import ABC, abstractmethod
from sqlalchemy import Column, Table, Integer, String
from db import Database

class ValidationError(Exception):
    pass

class FieldHandler(ABC):
    @abstractmethod
    def supports(self, column: Column) -> bool:
        ...

    @abstractmethod
    def parse(self, raw: Any, column: Column) -> Any:
        ...

    @abstractmethod
    def validate(self, value: Any, column: Column) -> Optional[str]:
        ...

class IntHandler(FieldHandler):
    def supports(self, column: Column) -> bool:
        return isinstance(column.type, Integer)

    def parse(self, raw: Any, column: Column) -> Optional[int]:
        if raw is None:
            return None
        if isinstance(raw, int):
            return raw
        if isinstance(raw, str) and raw.strip() == "":
            return None
        try:
            return int(raw)
        except Exception as e:
            raise ValueError("Ожидается целое число") from e

    def validate(self, value: Any, column: Column) -> Optional[str]:
        # future: range checks could be applied here (e.g. check unsigned)
        return None

class StrHandler(FieldHandler):
    def supports(self, column: Column) -> bool:
        return isinstance(column.type, String)

    def parse(self, raw: Any, column: Column) -> Optional[str]:
        if raw is None:
            return None
        if not isinstance(raw, str):
            raw = str(raw)
        raw = raw.strip()
        if raw == "":
            return None
        return raw

    def validate(self, value: Any, column: Column) -> Optional[str]:
        if value is None:
            return None
        try:
            maxlen = getattr(column.type, "length", None)
            if maxlen and isinstance(value, str) and len(value) > maxlen:
                return f"Максимальная длина {maxlen} символов"
        except Exception:
            pass
        return None

_DEFAULT_HANDLERS: List[FieldHandler] = [IntHandler(), StrHandler()]

class TableValidator:

    def __init__(self, handlers = None):
        self.handlers = handlers or list(_DEFAULT_HANDLERS)

    def register_handler(self, handler: FieldHandler) -> None:
        self.handlers.insert(0, handler)

    def _find_handler(self, column: Column) -> Optional[FieldHandler]:
        for h in self.handlers:
            if h.supports(column):
                return h
        return None

    def validate_table_data(self, table: Table, raw_data, db = None
                           ):

        errors= {}
        validated = {}

        for col in table.columns:
            if col.autoincrement and col.primary_key:
                continue

            raw_val = raw_data.get(col.name)
            if isinstance(raw_val, str) and raw_val.strip() == "":
                raw_val = None

            if raw_val is None:
                if not col.nullable and col.default is None and not col.autoincrement:
                    errors[col.name] = "Обязательное поле"
                else:
                    validated[col.name] = None
                continue

            handler = self._find_handler(col)
            if handler is None:
                try:
                    parsed = str(raw_val)
                except Exception:
                    errors[col.name] = "Невозможно привести значение к строке"
                    continue
                validated[col.name] = parsed
                continue

            try:
                parsed = handler.parse(raw_val, col)
            except ValueError as e:
                errors[col.name] = str(e)
                continue

            if parsed is None:
                if not col.nullable and col.default is None and not col.autoincrement:
                    errors[col.name] = "Обязательное поле"
                else:
                    validated[col.name] = None
                continue

            local_err = handler.validate(parsed, col)
            if local_err:
                errors[col.name] = local_err
                continue

            validated[col.name] = parsed

        if db is not None:
            unique_candidates = {c.name: validated.get(c.name) for c in table.columns if c.unique}
            to_check = {k: v for k, v in unique_candidates.items() if v is not None and k not in errors}
            if to_check:
                db_errors = db.check_uniques(table, to_check)
                for f, msg in db_errors.items():
                    errors[f] = msg

        return validated, errors

_default_table_validator = TableValidator()

def register_handler(handler: FieldHandler) -> None:
    _default_table_validator.register_handler(handler)

def validate_table_data(table: Table, raw_data: Dict[str, Any], db: Optional[Database] = None):
    return _default_table_validator.validate_table_data(table, raw_data, db=db)
