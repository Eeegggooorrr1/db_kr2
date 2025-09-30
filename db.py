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

    def reflect_tables(self, table_names = None):
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

    def alter_table(self,old_table_name, new_table_name, old_data, new_data):
        def _change_name(old_table_name, new_table_name):
            return f'alter table {old_table_name} rename to {new_table_name}'

        def _drop_column(table, column):
            return f'alter table {table} drop column {column}'

        def _create_column(table, column):
            return (f"alter table {table}"
                    f" add column {column['name']}"
                    f" {column['type']}"
                    f"{' unique' if column['unique'] else ''}"
                    f"{' not null' if column.get('not_null') else ''}")


        def _rename_column(column):
            pass

        print(old_data)
        print(new_data)
        session = self.SessionLocal()

        table = old_table_name
        try:
            session.execute(text(f'truncate {table}'))
            if  table != new_table_name:
                session.execute(text(_change_name(table, new_table_name)))
                table = new_table_name
            for i in old_data:
                if i not in new_data:
                    session.execute(text(_drop_column(table, old_data[i]['name'])))
            for i in new_data:
                if i not in old_data:
                    print(_create_column(table, new_data[i]))
                    session.execute(text(_create_column(table, new_data[i])))

            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

