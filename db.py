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

class Database:

    def __init__(self):
        self.database_url = settings.get_db_url()
        self.engine: Engine = create_engine(self.database_url, future=True, echo=False)
        self.SessionLocal = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, future=True)
        self.metadata = MetaData()
        self.insp = inspect(self.engine)

    def reflect_tables(self, table_names: Optional[Iterable[str]] = None, refresh: bool = False):
        print('ddd', table_names)
        if refresh:
            self.metadata.clear()
        if table_names is None:
            self.metadata.reflect(bind=self.engine)
        else:
            #self.metadata.reflect(bind=self.engine)
            self.metadata.reflect(bind=self.engine, only=list(table_names))
        print('ddd', table_names)

    def list_tables(self):
        return self.insp.get_table_names()

    def get_table(self, table_name: str) -> Table:
        if table_name not in self.metadata.tables:
            self.reflect_tables([table_name])
        return self.metadata.tables[table_name]

    def check_uniques(self, table: Table, data):
        errors= {}
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

    def alter_table(self, old_table_name, new_table_name, old_data, new_data):
        def _change_name(old_table_name, new_table_name):
            return f'ALTER TABLE {old_table_name} RENAME TO {new_table_name}'

        def _drop_column(table, column):
            return f'ALTER TABLE {table} DROP COLUMN {column}'

        def _create_column(table, column):
            sql = f"ALTER TABLE {table} ADD COLUMN {column['name']} {column['type']}"

            if column['unique']:
                sql += ' UNIQUE'
            if column.get('not_null'):
                sql += ' NOT NULL'
            if column['default'] != '':
                sql += f" DEFAULT {column['default']}"
            if column['check'] != '':
                sql += f" CHECK ({column['check']})"
            if column['fk_table'] != '' and column['fk_column'] != '':
                sql += f" REFERENCES {column['fk_table']}({column['fk_column']})"

            return sql

        def _rename_column(table, old_name, new_name):
            return f'ALTER TABLE {table} RENAME COLUMN {old_name} TO {new_name}'

        def _alter_column_type(table, column, new_type):
            return f'ALTER TABLE {table} ALTER COLUMN {column} TYPE {new_type}'

        def _alter_column_set_not_null(table, column):
            return f'ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL'

        def _alter_column_drop_not_null(table, column):
            return f'ALTER TABLE {table} ALTER COLUMN {column} DROP NOT NULL'

        def _alter_column_set_default(table, column, default):
            return f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT {default}"

        def _alter_column_drop_default(table, column):
            return f'ALTER TABLE {table} ALTER COLUMN {column} DROP DEFAULT'

        def _add_check_constraint(table, column, check):
            constraint_name = f"chk_{table}_{column}"
            return f'ALTER TABLE {table} ADD CONSTRAINT {constraint_name} CHECK ({check})'

        def _drop_check_constraint(table, column):
            constraint_name = f"chk_{table}_{column}"
            return f'ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint_name}'

        def _add_foreign_key(table, column, fk_table, fk_column):
            constraint_name = f"fk_{table}_{column}"
            return f'ALTER TABLE {table} ADD CONSTRAINT {constraint_name} FOREIGN KEY ({column}) REFERENCES {fk_table}({fk_column})'

        def _drop_foreign_key(table, column):
            constraint_name = f"fk_{table}_{column}"
            return f'ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint_name}'

        session = self.SessionLocal()

        try:
            session.execute(text(f'TRUNCATE {old_table_name}'))

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

                    if old_col['type'] != new_col['type']:
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

