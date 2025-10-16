from typing import Any, Dict, Optional, List, Type
from abc import ABC, abstractmethod
from sqlalchemy import Column, Table, Integer, String, Boolean, Enum as SAEnum, Float, Date, DateTime, Numeric, ARRAY, CheckConstraint
from datetime import datetime, date
import re
import ast
import operator

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

    def parse(self, raw, column):
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

    def validate(self, value, column):
        return None

class FloatHandler(FieldHandler):
    def supports(self, column: Column) -> bool:
        return isinstance(column.type, (Float, Numeric))

    def parse(self, raw, column):
        if raw is None:
            return None
        if isinstance(raw, float):
            return raw
        if isinstance(raw, int):
            return float(raw)
        if isinstance(raw, str) and raw.strip() == "":
            return None
        try:
            return float(raw)
        except Exception as e:
            raise ValueError("Ожидается число с плавающей точкой") from e

    def validate(self, value, column):
        return None

class BoolHandler(FieldHandler):
    def supports(self, column: Column) -> bool:
        return isinstance(column.type, Boolean)

    def parse(self, raw, column):
        if raw is None:
            return None
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, (int, float)):
            return bool(raw)
        if isinstance(raw, str):
            v = raw.strip().lower()
            if v in ("1", "true", "yes", "y", "on"):
                return True
            if v in ("0", "false", "no", "n", "off"):
                return False
            if v == "":
                return None
        raise ValueError("Ожидается логическое значение")

    def validate(self, value, column):
        return None

class EnumHandler(FieldHandler):
    def supports(self, column: Column) -> bool:
        return isinstance(column.type, SAEnum)

    def parse(self, raw, column):
        if raw is None:
            return None
        enums = getattr(column.type, "enums", None) or []
        if isinstance(raw, str):
            raw = raw.strip()
            if raw == "":
                return None
        if enums and raw is not None:
            if raw in enums:
                return raw
            raise ValueError(f"Ожидается одно из: {', '.join(map(str, enums))}")
        return raw

    def validate(self, value, column):
        if value is None:
            return None
        enums = getattr(column.type, "enums", None) or []
        if enums and value not in enums:
            return f"Недопустимое значение, допустимо: {', '.join(map(str, enums))}"
        return None

class DateHandler(FieldHandler):
    def supports(self, column: Column) -> bool:
        return isinstance(column.type, Date)

    def parse(self, raw, column):
        if raw is None:
            return None
        if isinstance(raw, date) and not isinstance(raw, datetime):
            return raw
        if isinstance(raw, datetime):
            return raw.date()
        if isinstance(raw, str):
            txt = raw.strip()
            if txt == "":
                return None
            try:
                return datetime.fromisoformat(txt).date()
            except Exception:
                for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
                    try:
                        return datetime.strptime(txt, fmt).date()
                    except Exception:
                        pass
        raise ValueError("Ожидается дата")

    def validate(self, value, column):
        return None

class DateTimeHandler(FieldHandler):
    def supports(self, column: Column) -> bool:
        return isinstance(column.type, DateTime)

    def parse(self, raw, column):
        if raw is None:
            return None
        if isinstance(raw, datetime):
            return raw
        if isinstance(raw, date):
            return datetime(raw.year, raw.month, raw.day)
        if isinstance(raw, str):
            txt = raw.strip()
            if txt == "":
                return None
            try:
                return datetime.fromisoformat(txt)
            except Exception:
                for fmt in ("%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M:%S"):
                    try:
                        return datetime.strptime(txt, fmt)
                    except Exception:
                        pass
        raise ValueError("Ожидается дата и время")

    def validate(self, value, column):
        return None

class StrHandler(FieldHandler):
    def supports(self, column):
        return isinstance(column.type, String)

    def parse(self, raw, column):
        if raw is None:
            return None
        if not isinstance(raw, str):
            raw = str(raw)
        raw = raw.strip()
        if raw == "":
            return None
        return raw

    def validate(self, value, column):
        if value is None:
            return None
        try:
            maxlen = getattr(column.type, "length", None)
            if maxlen and isinstance(value, str) and len(value) > maxlen:
                return f"Максимальная длина {maxlen} символов"
        except Exception:
            pass
        return None

class ArrayHandler(FieldHandler):
    def supports(self, column: Column) -> bool:
        return isinstance(column.type, ARRAY)

    def _find_item_handler(self, item_type):
        dummy = type("D", (), {})()
        dummy.type = item_type
        for h in _DEFAULT_HANDLERS:
            try:
                if h is self:
                    continue
                if h.supports(dummy):
                    return h
            except Exception:
                continue
        return None

    def parse(self, raw, column):
        if raw is None:
            return None
        item_type = getattr(column.type, "item_type", None)
        if isinstance(raw, list):
            items = raw
        else:
            if isinstance(raw, str):
                txt = raw.strip()
                if txt == "":
                    return None
                if txt.startswith("{") and txt.endswith("}"):
                    txt = txt[1:-1]
                items = [p.strip() for p in re.split(r'\s*,\s*', txt) if p.strip() != ""]
            else:
                raise ValueError("Невозможно распознать массив")
        handler = None
        if item_type is not None:
            handler = self._find_item_handler(item_type)
        parsed_items = []
        for idx, it in enumerate(items):
            try:
                if handler is not None:
                    parsed = handler.parse(it, type("C", (), {"type": item_type})())
                else:
                    parsed = it
                parsed_items.append(parsed)
            except ValueError as e:
                raise ValueError(f"Элемент {idx}: {e}") from e
        return parsed_items

    def validate(self, value, column):
        if value is None:
            return None
        item_type = getattr(column.type, "item_type", None)
        handler = None
        if item_type is not None:
            handler = self._find_item_handler(item_type)
        for idx, it in enumerate(value):
            if handler is not None:
                err = handler.validate(it, type("C", (), {"type": item_type})())
                if err:
                    return f"Элемент {idx}: {err}"
        return None

_DEFAULT_HANDLERS = [
    EnumHandler(),
    BoolHandler(),
    IntHandler(),
    FloatHandler(),
    DateTimeHandler(),
    DateHandler(),
    ArrayHandler(),
    StrHandler(),
]

class TableValidator:

    def __init__(self, handlers = None):
        self.handlers = handlers or list(_DEFAULT_HANDLERS)

    def register_handler(self, handler):
        self.handlers.insert(0, handler)

    def _find_handler(self, column):
        for h in self.handlers:
            try:
                if h.supports(column):
                    return h
            except Exception:
                continue
        return None

    def _sql_to_python_expr(self, sql_text: str, validated: Dict[str, Any], table: Table):
        txt = str(sql_text)
        txt = re.sub(r'<>', '!=', txt, flags=re.IGNORECASE)
        txt = re.sub(r'(?<![<>!])=(?!=)', '==', txt)
        txt = re.sub(r'\bAND\b', 'and', txt, flags=re.IGNORECASE)
        txt = re.sub(r'\bOR\b', 'or', txt, flags=re.IGNORECASE)
        txt = re.sub(r'\bNOT\b', 'not', txt, flags=re.IGNORECASE)
        for col in table.columns:
            pattern = r'\b' + re.escape(col.name) + r'\b'
            val = validated.get(col.name)
            txt = re.sub(pattern, repr(val), txt)
        return txt

    def _safe_eval(self, expr: str):
        node = ast.parse(expr, mode='eval')
        ops = {
            ast.Add: operator.add,
            ast.Sub: operator.sub,
            ast.Mult: operator.mul,
            ast.Div: operator.truediv,
            ast.Mod: operator.mod,
            ast.UAdd: operator.pos,
            ast.USub: operator.neg,
            ast.Not: operator.not_,
            ast.And: lambda a, b: a and b,
            ast.Or: lambda a, b: a or b,
            ast.Eq: operator.eq,
            ast.NotEq: operator.ne,
            ast.Lt: operator.lt,
            ast.LtE: operator.le,
            ast.Gt: operator.gt,
            ast.GtE: operator.ge,
            ast.In: lambda a, b: a in b,
            ast.NotIn: lambda a, b: a not in b,
        }
        def _eval(n):
            if isinstance(n, ast.Expression):
                return _eval(n.body)
            if isinstance(n, ast.Constant):
                return n.value
            if isinstance(n, ast.Num):
                return n.n
            if isinstance(n, ast.Str):
                return n.s
            if isinstance(n, ast.UnaryOp):
                op = ops[type(n.op)]
                return op(_eval(n.operand))
            if isinstance(n, ast.BinOp):
                left = _eval(n.left)
                right = _eval(n.right)
                op = ops[type(n.op)]
                return op(left, right)
            if isinstance(n, ast.BoolOp):
                vals = [_eval(v) for v in n.values]
                res = vals[0]
                for v in vals[1:]:
                    res = ops[type(n.op)](res, v)
                return res
            if isinstance(n, ast.Compare):
                left = _eval(n.left)
                for opnode, comparator in zip(n.ops, n.comparators):
                    right = _eval(comparator)
                    op = ops[type(opnode)]
                    if not op(left, right):
                        return False
                    left = right
                return True
            if isinstance(n, ast.List):
                return [_eval(e) for e in n.elts]
            if isinstance(n, ast.Tuple):
                return tuple(_eval(e) for e in n.elts)
            raise ValueError("Недопустимое выражение в check")
        return _eval(node)

    def validate_table_data(self, table, raw_data, db = None):

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

        for check in getattr(table, "constraints", []):
            if not isinstance(check, CheckConstraint):
                continue
            txt = str(check.sqltext)
            referenced = [c.name for c in table.columns if re.search(r'\b' + re.escape(c.name) + r'\b', txt)]
            if not referenced:
                continue
            try:
                expr = self._sql_to_python_expr(txt, validated, table)
                result = self._safe_eval(expr)
                if not result:
                    for colname in referenced:
                        if colname not in errors:
                            errors[colname] = "Нарушено ограничение CHECK"
            except Exception:
                for colname in referenced:
                    if colname not in errors:
                        errors[colname] = "Невозможно проверить ограничение CHECK"

        return validated, errors

_default_table_validator = TableValidator()

def register_handler(handler):
    _default_table_validator.register_handler(handler)

def validate_table_data(table, raw_data, db = None):
    return _default_table_validator.validate_table_data(table, raw_data, db=db)
