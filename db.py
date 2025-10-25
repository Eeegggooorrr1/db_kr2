import re

from sqlalchemy import text
from typing import Optional, Iterable, Dict, Any
import os
from sqlalchemy import create_engine, MetaData, Table, inspect, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine

from config import settings


class Database:

    def __init__(self, params= None):
        if params is None:
            self.database_url = settings.get_db_url()
        else:
            self.database_url = self._build_url(params)
        self.engine: Engine = create_engine(self.database_url, future=True, echo=True)
        conn = self.engine.connect()
        conn.close()
        self._connected = True
        self.SessionLocal = sessionmaker(bind=self.engine, autoflush=False, autocommit=False, future=True)
        self.metadata = MetaData()
        self.insp = inspect(self.engine)

    def reset(self):
        with self.engine.begin() as conn:
            conn.execute(text("""
                DROP SCHEMA public CASCADE;
                CREATE SCHEMA public;
                CREATE TYPE attack_type_enum AS ENUM ('no_attack','blur','noise','adversarial','other');
                CREATE TABLE experiments (
                    experiment_id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    description TEXT,
                    created_date DATE DEFAULT current_date
                );
                CREATE TABLE runs (
                    run_id SERIAL PRIMARY KEY,
                    experiment_id INTEGER NOT NULL,
                    run_date TIMESTAMP DEFAULT now(),
                    accuracy DOUBLE PRECISION,
                    flagged BOOLEAN,
                    CONSTRAINT fk_runs_experiment_id FOREIGN KEY (experiment_id) REFERENCES experiments(experiment_id) ON DELETE CASCADE
                );
                CREATE TABLE images (
                    image_id SERIAL PRIMARY KEY,
                    run_id INTEGER NOT NULL,
                    file_path VARCHAR(500) NOT NULL,
                    original_name VARCHAR(255),
                    attack_type attack_type_enum NOT NULL,
                    added_date TIMESTAMP DEFAULT date_trunc('second', now()::timestamp),
                    coordinates INTEGER[],
                    CONSTRAINT fk_images_run_id FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
                );
                CREATE TABLE test (id SERIAL PRIMARY KEY);
                INSERT INTO experiments (name, description) VALUES
                ('Baseline Classification', 'Standard image classification without attacks'),
                ('Adversarial Robustness', 'Testing model resilience against adversarial attacks'),
                ('Noise Sensitivity', 'Evaluating performance under different noise conditions'),
                ('Blur Tolerance', 'Testing model accuracy with various blur levels'),
                ('Mixed Attacks', 'Combination of different attack types');
                INSERT INTO runs (experiment_id, run_date, accuracy, flagged)
                SELECT
                    (seq.run_id % 5) + 1 as experiment_id,
                    now() - interval '1 day' * random() * 30 as run_date,
                    round(random()::numeric * 0.4 + 0.6, 4) as accuracy,
                    random() > 0.9 as flagged
                FROM generate_series(1, 20) as seq(run_id);
                INSERT INTO images (run_id, file_path, original_name, attack_type, added_date, coordinates)
                SELECT
                    (seq.image_id % 20) + 1 as run_id,
                    '/data/images/' || seq.image_id || '.png' as file_path,
                    'original_' || seq.image_id || '.jpg' as original_name,
                    CASE floor(random() * 5)
                        WHEN 0 THEN 'no_attack'::attack_type_enum
                        WHEN 1 THEN 'blur'::attack_type_enum
                        WHEN 2 THEN 'noise'::attack_type_enum
                        WHEN 3 THEN 'adversarial'::attack_type_enum
                        ELSE 'other'::attack_type_enum
                    END as attack_type,
                    now() - interval '1 hour' * random() * 24 * 30 as added_date,
                    CASE
                        WHEN random() > 0.3 THEN ARRAY[floor(random()*1000), floor(random()*1000)]
                        ELSE NULL
                    END as coordinates
                FROM generate_series(1, 100) as seq(image_id);
            """))

    def _build_url(self, params):
        user = params.get("DB_USER") or params.get("user") or ""
        password = params.get("DB_PASSWORD") or params.get("password") or ""
        host = params.get("DB_HOST") or params.get("host") or "localhost"
        port = params.get("DB_PORT") or params.get("port") or ""
        name = params.get("DB_NAME") or params.get("database") or ""
        if password:
            return f"postgresql://{user}:{password}@{host}:{port}/{name}"
        return f"postgresql://{user}@{host}:{port}/{name}"

    def connect(self, params):
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

    def connect_from_env(self):
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

    def is_connected(self):
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

    def get_table(self, table_name):
        if table_name not in self.metadata.tables:
            self.reflect_tables([table_name])
        return self.metadata.tables[table_name]

    def check_uniques(self, table, data):
        errors = {}
        session = self.SessionLocal()
        try:
            for col in table.columns:
                if getattr(col, "_inspector_unique", False) or getattr(col, "unique", False):
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

    def insert_row(self, table, data):
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
        def _change_name(old_table_name, new_table_name):
            return f'ALTER TABLE "{old_table_name}" RENAME TO "{new_table_name}"'

        def _drop_column(table, column):
            return f'ALTER TABLE "{table}" DROP COLUMN "{column}"'

        def _create_column(table, column):
            sql = f'ALTER TABLE "{table}" ADD COLUMN "{column["name"]}"'
            if column['type'] == 'TEXT':
                if column.get('length'):
                    sql += f' VARCHAR({int(column["length"])})'
                else:
                    sql += ' TEXT'
            elif column['type'] == 'ARRAY':
                sql += f' {column["array_elem_type"]}[]'
            else:
                sql += f' {column["type"]}'
            if column.get('unique'):
                cname = f'uniq_{table}_{column["name"]}'
                sql += f' CONSTRAINT "{cname}" UNIQUE'
            if column.get('_add_not_null', False):
                sql += ' NOT NULL'
            default_val = column.get('default_normalized')
            if default_val is not None:
                sql += f' DEFAULT {default_val}'
            check_val = column.get('check_normalized')
            if check_val:
                cname = f'chk_{table}_{column["name"]}'
                sql += f' CONSTRAINT "{cname}" CHECK ({check_val})'
            fk_table = column.get('fk_table_normalized')
            fk_column = column.get('fk_column_normalized')
            if fk_table and fk_column:
                cname = f'fk_{table}_{column["name"]}'
                sql += f' CONSTRAINT "{cname}" REFERENCES "{fk_table}"("{fk_column}")'
            return sql

        def _rename_column(table, old_name, new_name):
            return f'ALTER TABLE "{table}" RENAME COLUMN "{old_name}" TO "{new_name}"'

        def _alter_column_type(table, column, new_type, using_expr=None):
            base = f'ALTER TABLE "{table}" ALTER COLUMN "{column}" TYPE {new_type}'
            if using_expr:
                base += f' USING {using_expr}'
            else:
                base += f' USING "{column}"::{new_type}'
            return base

        def _alter_column_set_not_null(table, column):
            return f'ALTER TABLE "{table}" ALTER COLUMN "{column}" SET NOT NULL'

        def _alter_column_drop_not_null(table, column):
            return f'ALTER TABLE "{table}" ALTER COLUMN "{column}" DROP NOT NULL'

        def _alter_column_set_default(table, column, default):
            return f'ALTER TABLE "{table}" ALTER COLUMN "{column}" SET DEFAULT {default}'

        def _alter_column_drop_default(table, column):
            return f'ALTER TABLE "{table}" ALTER COLUMN "{column}" DROP DEFAULT'

        def _add_check_constraint(table, column, check):
            constraint_name = f'chk_{table}_{column}'
            return f'ALTER TABLE "{table}" ADD CONSTRAINT "{constraint_name}" CHECK ({check})'

        def _drop_check_constraint(table, column):
            constraint_name = f'chk_{table}_{column}'
            return f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{constraint_name}"'

        def _add_foreign_key(table, column, fk_table, fk_column):
            constraint_name = f'fk_{table}_{column}'
            return f'ALTER TABLE "{table}" ADD CONSTRAINT "{constraint_name}" FOREIGN KEY ("{column}") REFERENCES "{fk_table}"("{fk_column}")'

        def _drop_foreign_key(table, column):
            constraint_name = f'fk_{table}_{column}'
            return f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{constraint_name}"'

        def _add_unique(table, column):
            constraint_name = f'uniq_{table}_{column}'
            return f'ALTER TABLE "{table}" ADD CONSTRAINT "{constraint_name}" UNIQUE ("{column}")'

        def _drop_unique(table, column):
            constraint_name = f'uniq_{table}_{column}'
            return f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{constraint_name}"'

        def _add_pk(table, column, constraint_name):
            return f'ALTER TABLE "{table}" ADD CONSTRAINT "{constraint_name}" PRIMARY KEY ("{column}")'

        def _drop_constraint(table, constraint_name):
            return f'ALTER TABLE "{table}" DROP CONSTRAINT IF EXISTS "{constraint_name}"'

        def _normalize_val(v):
            if v is None:
                return None
            if isinstance(v, bool):
                return v
            if isinstance(v, (int, float)):
                return v
            s = str(v).strip()
            if s == '':
                return None
            if s.lower() in ('true', 't', '1'):
                return True
            if s.lower() in ('false', 'f', '0'):
                return False
            return s

        def _format_default(val):
            if val is None:
                return None
            if isinstance(val, bool):
                return 'TRUE' if val else 'FALSE'
            if isinstance(val, (int, float)):
                return str(val)
            s = str(val).strip()
            if s == '':
                return None
            if s.upper() == 'NULL':
                return 'NULL'
            if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
                return s
            if re.fullmatch(r'-?\d+(\.\d+)?', s):
                return s
            escaped = s.replace("'", "''")
            return f"'{escaped}'"

        session = self.SessionLocal()

        try:
            current_table_name = old_table_name
            if old_table_name != new_table_name:
                session.execute(text(_change_name(old_table_name, new_table_name)))
                current_table_name = new_table_name

            normalized_old = {}
            normalized_new = {}
            for cid, col in (old_data or {}).items():
                normalized_old[cid] = dict(col)
                normalized_old[cid]['fk_table_normalized'] = _normalize_val(col.get('fk_table'))
                normalized_old[cid]['fk_column_normalized'] = _normalize_val(col.get('fk_column'))
                normalized_old[cid]['default_normalized'] = _format_default(col.get('default'))
                normalized_old[cid]['check_normalized'] = _normalize_val(col.get('check'))
                normalized_old[cid]['not_null'] = bool(_normalize_val(col.get('not_null')))
                normalized_old[cid]['unique'] = bool(_normalize_val(col.get('unique')))
                normalized_old[cid]['primary_key'] = bool(_normalize_val(col.get('primary_key')))
                normalized_old[cid]['length'] = int(col.get('length')) if col.get('length') is not None else None
                normalized_old[cid]['array_elem_type'] = col.get('array_elem_type')
            for cid, col in (new_data or {}).items():
                normalized_new[cid] = dict(col)
                normalized_new[cid]['fk_table_normalized'] = _normalize_val(col.get('fk_table'))
                normalized_new[cid]['fk_column_normalized'] = _normalize_val(col.get('fk_column'))
                normalized_new[cid]['default_normalized'] = _format_default(col.get('default'))
                normalized_new[cid]['check_normalized'] = _normalize_val(col.get('check'))
                normalized_new[cid]['not_null'] = bool(_normalize_val(col.get('not_null')))
                normalized_new[cid]['unique'] = bool(_normalize_val(col.get('unique')))
                normalized_new[cid]['primary_key'] = bool(_normalize_val(col.get('primary_key')))
                normalized_new[cid]['length'] = int(col.get('length')) if col.get('length') is not None else None
                normalized_new[cid]['array_elem_type'] = col.get('array_elem_type')

            def apply_changes():
                for col_id in list(normalized_old.keys()):
                    if col_id not in normalized_new:
                        od = normalized_old[col_id]
                        col_name = od.get('name')
                        if col_name:
                            session.execute(text(_drop_foreign_key(current_table_name, col_name)))
                            session.execute(text(_drop_unique(current_table_name, col_name)))
                            session.execute(text(_drop_column(current_table_name, col_name)))

                for col_id in list(normalized_new.keys()):
                    if col_id not in normalized_old:
                        nd = normalized_new[col_id]
                        col_name = nd.get('name')
                        if not col_name:
                            continue
                        table_empty = False
                        try:
                            res = session.execute(text(f'SELECT 1 FROM "{current_table_name}" LIMIT 1')).fetchone()
                            table_empty = res is None
                        except:
                            table_empty = False
                        nd['_add_not_null'] = False
                        if nd.get('not_null'):
                            if nd.get('default_normalized') is not None or table_empty:
                                nd['_add_not_null'] = True
                        session.execute(text(_create_column(current_table_name, nd)))
                        if nd.get('unique'):
                            session.execute(text(_add_unique(current_table_name, nd['name'])))

                pk_info = {}
                try:
                    cur_pk = self.insp.get_pk_constraint(current_table_name)
                    pk_cols = cur_pk.get('constrained_columns') or []
                    pk_name = cur_pk.get('name')
                    pk_info['cols'] = pk_cols
                    pk_info['name'] = pk_name
                except:
                    pk_info['cols'] = []
                    pk_info['name'] = None

                for col_id in list(normalized_old.keys()):
                    if col_id in normalized_new:
                        old_col = normalized_old[col_id]
                        new_col = normalized_new[col_id]
                        current_column_name = old_col.get('name')
                        if not current_column_name:
                            continue
                        if (old_col.get('name') != new_col.get('name')) and new_col.get('name'):
                            session.execute(
                                text(_rename_column(current_table_name, old_col.get('name'), new_col.get('name'))))
                            current_column_name = new_col.get('name')
                        old_type = (old_col.get('type') or '').upper()
                        new_type = (new_col.get('type') or '').upper()
                        old_len = old_col.get('length')
                        new_len = new_col.get('length')
                        old_arr = old_col.get('array_elem_type') or None
                        new_arr = new_col.get('array_elem_type') or None
                        type_changed = old_type != new_type or (
                                    old_type == 'TEXT' and new_type == 'TEXT' and old_len != new_len) or (
                                                   old_type == 'ARRAY' and new_type == 'ARRAY' and old_arr != new_arr)
                        if type_changed:
                            if new_type == 'TEXT':
                                if new_len:
                                    session.execute(text(_alter_column_type(current_table_name, current_column_name,
                                                                            f'VARCHAR({int(new_len)})')))
                                else:
                                    session.execute(
                                        text(_alter_column_type(current_table_name, current_column_name, 'TEXT')))
                            elif new_type == 'ARRAY' and old_type != 'ARRAY':
                                if new_arr:
                                    using_expr = f'ARRAY["{current_column_name}"]'
                                    session.execute(text(
                                        _alter_column_type(current_table_name, current_column_name, f'{new_arr}[]',
                                                           using_expr)))
                                else:
                                    session.execute(text(
                                        _alter_column_type(current_table_name, current_column_name, 'TEXT[]',
                                                           f'ARRAY["{current_column_name}"]')))
                            elif new_type == 'ARRAY' and old_type == 'ARRAY':
                                using_expr = f'"{current_column_name}"::{new_arr}[]' if new_arr else f'"{current_column_name}"::text[]'
                                session.execute(text(_alter_column_type(current_table_name, current_column_name,
                                                                        f'{new_arr}[]' if new_arr else 'text[]',
                                                                        using_expr)))
                            elif new_type != 'ARRAY' and old_type == 'ARRAY':
                                using_expr = f'"{current_column_name}"[1]::{new_type}'
                                session.execute(text(
                                    _alter_column_type(current_table_name, current_column_name, new_type, using_expr)))
                            else:
                                session.execute(
                                    text(_alter_column_type(current_table_name, current_column_name, new_type)))
                        if bool(old_col.get('not_null')) != bool(new_col.get('not_null')):
                            if new_col.get('not_null'):
                                try:
                                    res = session.execute(text(
                                        f'SELECT 1 FROM "{current_table_name}" WHERE "{current_column_name}" IS NULL LIMIT 1')).fetchone()
                                    if res is None:
                                        session.execute(
                                            text(_alter_column_set_not_null(current_table_name, current_column_name)))
                                    else:
                                        raise Exception('NULL values exist')
                                except:
                                    raise
                            else:
                                session.execute(
                                    text(_alter_column_drop_not_null(current_table_name, current_column_name)))
                        if old_col.get('unique') != new_col.get('unique'):
                            if old_col.get('unique'):
                                session.execute(text(_drop_unique(current_table_name, current_column_name)))
                            if new_col.get('unique'):
                                session.execute(text(_add_unique(current_table_name, current_column_name)))
                        if old_col.get('default_normalized') != new_col.get('default_normalized'):
                            if new_col.get('default_normalized') is not None:
                                session.execute(text(_alter_column_set_default(current_table_name, current_column_name,
                                                                               new_col.get('default_normalized'))))
                            else:
                                session.execute(
                                    text(_alter_column_drop_default(current_table_name, current_column_name)))
                        if old_col.get('check_normalized') != new_col.get('check_normalized'):
                            if old_col.get('check_normalized'):
                                session.execute(text(_drop_check_constraint(current_table_name, current_column_name)))
                            if new_col.get('check_normalized'):
                                session.execute(text(_add_check_constraint(current_table_name, current_column_name,
                                                                           new_col.get('check_normalized'))))
                        old_fk_table = old_col.get('fk_table_normalized')
                        old_fk_col = old_col.get('fk_column_normalized')
                        new_fk_table = new_col.get('fk_table_normalized')
                        new_fk_col = new_col.get('fk_column_normalized')
                        if old_fk_table != new_fk_table or old_fk_col != new_fk_col:
                            if old_fk_table and old_fk_col:
                                session.execute(text(_drop_foreign_key(current_table_name, current_column_name)))
                            if new_fk_table and new_fk_col:
                                session.execute(text(
                                    _add_foreign_key(current_table_name, current_column_name, new_fk_table,
                                                     new_fk_col)))
                        if old_col.get('primary_key') != new_col.get('primary_key'):
                            try:
                                cur_pk = self.insp.get_pk_constraint(current_table_name)
                                existing_pk_name = cur_pk.get('name')
                                existing_pk_cols = cur_pk.get('constrained_columns') or []
                            except:
                                existing_pk_name = None
                                existing_pk_cols = []
                            if existing_pk_name and current_column_name in existing_pk_cols and not new_col.get(
                                    'primary_key'):
                                session.execute(text(_drop_constraint(current_table_name, existing_pk_name)))
                            elif new_col.get('primary_key'):
                                if existing_pk_name and existing_pk_cols and existing_pk_cols != [current_column_name]:
                                    session.execute(text(_drop_constraint(current_table_name, existing_pk_name)))
                                pkname = f'pk_{current_table_name}'
                                session.execute(text(_add_pk(current_table_name, current_column_name, pkname)))

            try:
                apply_changes()
                session.commit()
            except Exception:
                session.rollback()
                try:
                    session.execute(text(f'TRUNCATE TABLE "{current_table_name}" CASCADE'))
                    try:
                        apply_changes()
                        session.commit()
                    except Exception:
                        session.rollback()
                        raise
                except Exception:
                    raise
            try:
                self.metadata.clear()
                self.insp = inspect(self.engine)
                self.reflect_tables([current_table_name], refresh=True)
            except:
                pass

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
