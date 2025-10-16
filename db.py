# src/app/db.py
from sqlalchemy import text
from typing import Optional, Iterable, Dict, Any
import os
from sqlalchemy import create_engine, MetaData, Table, inspect, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine

from config import settings


class UniqueConstraintViolation(Exception):
    pass


def log_returns(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        print(f"[RETURN] {func.__name__} -> {result}")
        return result
    return wrapper

class Database:

    def __init__(self, params: dict = None):
        if params is None:
            self.database_url = settings.get_db_url()
        else:
            self.database_url = self._build_url(params)
        self.engine: Engine = create_engine(self.database_url, future=True, echo=False)
        conn = self.engine.connect()
        conn.close()
        self._connected = True
        self.SessionLocal = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, future=True)
        self.metadata = MetaData()
        self.insp = inspect(self.engine)

    def _build_url(self, params: dict) -> str:
        from urllib.parse import quote_plus
        user = params.get("DB_USER") or params.get("user") or ""
        password = params.get("DB_PASSWORD") or params.get("password") or ""
        host = params.get("DB_HOST") or params.get("host") or "localhost"
        port = params.get("DB_PORT") or params.get("port") or ""
        name = params.get("DB_NAME") or params.get("database") or ""
        if password:
            pw = quote_plus(str(password))
            return f"postgresql://{user}:{pw}@{host}:{port}/{name}"
        return f"postgresql://{user}@{host}:{port}/{name}"

    def connect(self, params: dict) -> bool:
        try:
            url = self._build_url(params)
            try:
                self.engine.dispose()
            except Exception:
                pass
            self.database_url = url
            self.engine = create_engine(self.database_url, future=True, echo=False)
            conn = self.engine.connect()
            conn.close()
            self._connected = True
            self.SessionLocal = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, future=True)
            self.metadata = MetaData()
            self.insp = inspect(self.engine)
            return True
        except Exception:
            self._connected = False
            return False

    def connect_from_env(self) -> bool:
        try:
            url = settings.get_db_url()
            try:
                self.engine.dispose()
            except Exception:
                pass
            self.database_url = url
            self.engine = create_engine(self.database_url, future=True, echo=False)
            conn = self.engine.connect()
            conn.close()
            self._connected = True
            self.SessionLocal = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, future=True)
            self.metadata = MetaData()
            self.insp = inspect(self.engine)
            return True
        except Exception:
            self._connected = False
            return False

    def is_connected(self) -> bool:
        return bool(getattr(self, "_connected", False))

    def close(self):
        try:
            if getattr(self, "engine", None):
                self.engine.dispose()
        finally:
            self._connected = False

    def reflect_tables(self, table_names=None, refresh=False):
        if refresh:
            self.metadata.clear()
        if table_names is None:
            self.metadata.reflect(bind=self.engine)
        else:
            self.metadata.reflect(bind=self.engine, only=list(table_names))

    def list_tables(self):
        return self.insp.get_table_names()

    def get_table(self, table_name: str) -> Table:
        if table_name not in self.metadata.tables:
            self.reflect_tables([table_name])
        return self.metadata.tables[table_name]

    def check_uniques(self, table: Table, data):
        errors = {}
        session = self.SessionLocal()
        try:
            for col in table.columns:
                if col.unique:
                    val = data.get(col.name)
                    if val is None:
                        continue
                    stmt = select(table).where(table.c[col.name] == val).limit(1)
                    r = session.execute(stmt).first()
                    if r:
                        errors[col.name] = "Значение должно быть уникальным"
        finally:
            session.close()
        return errors

    def insert_row(self, table: Table, data):
        session = self.SessionLocal()
        try:
            ins = table.insert().values(**{k: v for k, v in data.items() if v is not None})
            session.execute(ins)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def recreate_tables(self):
        try:
            self.metadata.reflect(bind=self.engine)
            self.metadata.drop_all(bind=self.engine)
            self.metadata.create_all(bind=self.engine)
            return True
        except Exception:
            return False


    def alter_table(self, old_table_name, new_table_name, old_data, new_data):
        @log_returns
        def _change_name(old_table_name, new_table_name):
            return f'ALTER TABLE "{old_table_name}" RENAME TO "{new_table_name}"'

        @log_returns
        def _drop_column(table, column):
            return f'ALTER TABLE "{table}" DROP COLUMN "{column}"'

        @log_returns
        def _create_column(table, column):
            sql = f'ALTER TABLE "{table}" ADD COLUMN "{column["name"]}"'

            if column['type'] == 'TEXT':
                if column['length'] > 0:
                    sql += f' VARCHAR({column["length"]})'
                else:
                    sql += ' TEXT'
            elif column['type'] == 'ARRAY':
                sql += f' {column["array_elem_type"]}[]'
            else:
                sql += f' {column["type"]}'

            if column['unique']:
                sql += ' UNIQUE'
            if column.get('not_null'):
                sql += ' NOT NULL'
            if column['default'] != '' and column['default'] is not None:
                sql += f" DEFAULT {column['default']}"
            if column['check'] != '' and column['check'] is not None:
                sql += f" CHECK ({column['check']})"
            if column['fk_table'] != '' and column['fk_column'] != '':
                sql += f' REFERENCES "{column["fk_table"]}"("{column["fk_column"]}")'

            return sql

        @log_returns
        def _rename_column(table, old_name, new_name):
            return f'ALTER TABLE "{table}" RENAME COLUMN "{old_name}" TO "{new_name}"'

        @log_returns
        def _alter_column_type(table, column, new_type):
            return f'ALTER TABLE "{table}" ALTER COLUMN "{column}" TYPE {new_type}'

        @log_returns
        def _alter_column_set_not_null(table, column):
            return f'ALTER TABLE "{table}" ALTER COLUMN "{column}" SET NOT NULL'

        @log_returns
        def _alter_column_drop_not_null(table, column):
            return f'ALTER TABLE "{table}" ALTER COLUMN "{column}" DROP NOT NULL'

        @log_returns
        def _alter_column_set_default(table, column, default):
            return f'ALTER TABLE "{table}" ALTER COLUMN "{column}" SET DEFAULT {default}'

        @log_returns
        def _alter_column_drop_default(table, column):
            return f'ALTER TABLE "{table}" ALTER COLUMN "{column}" DROP DEFAULT'

        @log_returns
        def _add_check_constraint(table, column, check):
            constraint_name = f'chk_{table}_{column}'
            return f'ALTER TABLE "{table}" ADD CONSTRAINT "{constraint_name}" CHECK ({check})'

        @log_returns
        def _drop_check_constraint(table, column):
            constraint_name = f'chk_{table}_{column}'
            return f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{constraint_name}"'

        @log_returns
        def _add_foreign_key(table, column, fk_table, fk_column):
            constraint_name = f'fk_{table}_{column}'
            return f'ALTER TABLE "{table}" ADD CONSTRAINT "{constraint_name}" FOREIGN KEY ("{column}") REFERENCES "{fk_table}"("{fk_column}")'

        @log_returns
        def _drop_foreign_key(table, column):
            constraint_name = f'fk_{table}_{column}'
            return f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{constraint_name}"'

        session = self.SessionLocal()

        try:
            #session.execute(text(f'TRUNCATE "{old_table_name}"'))

            current_table_name = old_table_name
            if old_table_name != new_table_name:
                session.execute(text(_change_name(old_table_name, new_table_name)))
                current_table_name = new_table_name

            for col_id in old_data:
                if col_id not in new_data:
                    session.execute(text(_drop_column(current_table_name, old_data[col_id]['name'])))

            for col_id in new_data:
                if col_id not in old_data:
                    session.execute(text(_create_column(current_table_name, new_data[col_id])))

            for col_id in old_data:
                if col_id in new_data:
                    old_col = old_data[col_id]
                    new_col = new_data[col_id]
                    current_column_name = old_col['name']

                    if old_col['name'] != new_col['name']:
                        session.execute(text(_rename_column(current_table_name, old_col['name'], new_col['name'])))
                        current_column_name = new_col['name']

                    if (old_col['type'] != new_col['type']
                            or old_col['length'] != new_col['length']
                            or old_col['array_elem_type'] != new_col['array_elem_type']):

                        if new_col['type'] == 'TEXT':
                            if new_col['length'] > 0:
                                session.execute(
                                    text(_alter_column_type(current_table_name, current_column_name,
                                                            f'VARCHAR({new_col["length"]})')))
                            else:
                                session.execute(
                                    text(_alter_column_type(current_table_name, current_column_name, new_col['type'])))

                        elif new_col['type'] == 'ARRAY' and old_col['type'] != 'ARRAY':

                            session.execute(
                                text(_alter_column_type(current_table_name, current_column_name,
                                                        f'{new_col["array_elem_type"]}[]')
                                     + f' USING ARRAY[{current_column_name}]'))

                        elif new_col['type'] == 'ARRAY' and old_col['type'] == 'ARRAY':

                            session.execute(
                                text(_alter_column_type(current_table_name, current_column_name,
                                                        f'{new_col["array_elem_type"]}[]')
                                     + f' USING {current_column_name}::{new_col['array_elem_type']}[]'))

                        elif new_col['type'] != 'ARRAY' and old_col['type'] == 'ARRAY':

                            session.execute(
                                text(_alter_column_type(current_table_name, current_column_name,
                                                        f'{new_col["type"]}')
                                     + f' USING {current_column_name}[1]::{new_col['type']}'))

                        else:
                            session.execute(
                                text(_alter_column_type(current_table_name, current_column_name, new_col['type'])))

                    if old_col['not_null'] != new_col['not_null']:
                        if new_col['not_null']:
                            session.execute(text(_alter_column_set_not_null(current_table_name, current_column_name)))
                        else:
                            session.execute(text(_alter_column_drop_not_null(current_table_name, current_column_name)))

                    if old_col['default'] != new_col['default']:
                        if new_col['default'] != '':
                            session.execute(text(
                                _alter_column_set_default(current_table_name, current_column_name, new_col['default'])))
                        else:
                            session.execute(text(_alter_column_drop_default(current_table_name, current_column_name)))

                    if old_col['check'] != new_col['check']:
                        if old_col['check'] != '':
                            session.execute(text(_drop_check_constraint(current_table_name, current_column_name)))
                        if new_col['check'] != '':
                            session.execute(
                                text(_add_check_constraint(current_table_name, current_column_name, new_col['check'])))

                    if old_col['fk_table'] != new_col['fk_table'] or old_col['fk_column'] != new_col['fk_column']:
                        if old_col['fk_table'] != '' and old_col['fk_column'] != '':
                            session.execute(text(_drop_foreign_key(current_table_name, current_column_name)))
                        if new_col['fk_table'] != '' and new_col['fk_column'] != '':
                            session.execute(text(_add_foreign_key(current_table_name, current_column_name,
                                                                  new_col['fk_table'], new_col['fk_column'])))

            session.commit()
            self.metadata.clear()
            self.insp = inspect(self.engine)
            self.reflect_tables([current_table_name], refresh=True)

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

