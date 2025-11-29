import re
import uuid

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

    _ident_re = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*)?$')

    def _validate_identifier(self, ident: str):
        if not isinstance(ident, str) or not ident:
            raise ValueError("Идентификатор должен быть непустой строкой")
        if not self._ident_re.match(ident):
            raise ValueError(f"Недопустимый идентификатор: {ident!r}")
        return True

    def _split_schema_ident(self, ident: str):
        parts = ident.split('.', 1)
        if len(parts) == 1:
            return ('public', parts[0])
        return (parts[0], parts[1])

    def _qual_ident(self, ident: str):
        schema, name = self._split_schema_ident(ident)
        return f'"{schema}"."{name}"'

    def list_enums(self):

        sql = text("""
            SELECT n.nspname AS schema, t.typname AS name, e.enumlabel AS enum_value, e.enumsortorder
            FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            WHERE t.typtype = 'e'
            ORDER BY t.typname, e.enumsortorder
        """)
        with self.engine.connect() as conn:
            res = conn.execute(sql).mappings().all()
        out = {}
        for row in res:
            name = row["name"]
            val = row["enum_value"]
            out.setdefault(name, []).append(val)
        return out

    def _get_enum_values(self, enum_name: str):
        self._validate_identifier(enum_name)
        schema, nm = self._split_schema_ident(enum_name)
        sql = text("""
            SELECT e.enumlabel
            FROM pg_type t
            JOIN pg_enum e ON t.oid = e.enumtypid
            JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            WHERE t.typname = :typname AND n.nspname = :schema AND t.typtype = 'e'
            ORDER BY e.enumsortorder
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(sql, {'typname': nm, 'schema': schema}).scalars().all()
        return list(rows)
    def create_enum(self, name: str, values: list):

        if not values or not isinstance(values, (list, tuple)):
            raise ValueError("values должно быть непустым списком строк")
        self._validate_identifier(name)
        schema, nm = self._split_schema_ident(name)
        ident = f'"{schema}"."{nm}"'

        params = {}
        placeholders = []
        for i, v in enumerate(values):
            if not isinstance(v, str):
                raise ValueError("Каждое значение enum должно быть строкой")
            key = f'p{i}'
            placeholders.append(':' + key)
            params[key] = v

        sql = f"CREATE TYPE {ident} AS ENUM ({', '.join(placeholders)});"

        with self.engine.begin() as conn:
            conn.execute(text(sql), params)

    def drop_enum(self, name: str, cascade: bool = False):
        self._validate_identifier(name)
        schema, nm = self._split_schema_ident(name)
        ident = f'"{schema}"."{nm}"'
        sql = f"DROP TYPE {ident} {'CASCADE' if cascade else 'RESTRICT'};"
        with self.engine.begin() as conn:
            conn.execute(text(sql))

    def _enum_exists(self, name: str) -> bool:
        self._validate_identifier(name)
        schema, nm = self._split_schema_ident(name)
        sql = text("""
            SELECT 1
            FROM pg_type t
            JOIN pg_namespace n ON n.oid = t.typnamespace
            WHERE t.typname = :typname AND n.nspname = :schema AND t.typtype = 'e'
            LIMIT 1
        """)
        with self.engine.connect() as conn:
            row = conn.execute(sql, {'typname': nm, 'schema': schema}).first()
        return bool(row)

    def get_column_enum(self, table_name: str, column_name: str):

        try:
            tbl = self.get_table(table_name)
            col = tbl.c.get(column_name)
            if col is None:
                return None
            et = getattr(col.type, 'name', None)
            if et:
                return et
            tt = str(col.type)
            m = re.search(r'\"?(?P<ename>[a-zA-Z0-9_]+)\"?', tt)
            if m:
                return m.group('ename')
        except Exception:
            return None
        return None

    def alter_column_enum(self, table_name: str, column_name: str, new_enum: str):

        self._validate_identifier(table_name)
        if not isinstance(column_name, str) or not column_name:
            raise ValueError("column_name должен быть непустой строкой")
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
            raise ValueError(f"Недопустимое имя колонки: {column_name!r}")

        if not self._enum_exists(new_enum):
            raise ValueError(f"Enum '{new_enum}' не найден в базе")

        tbl_schema, tbl_name = self._split_schema_ident(table_name)
        tbl_ident = f'"{tbl_schema}"."{tbl_name}"'
        enum_schema, enum_name = self._split_schema_ident(new_enum)
        enum_ident = f'"{enum_schema}"."{enum_name}"'
        col_ident = f'"{column_name}"'


        sql = text(f"""
            ALTER TABLE {tbl_ident}
            ALTER COLUMN {col_ident}
            TYPE {enum_ident}
            USING ({col_ident}::text)::{enum_ident};
        """)
        with self.engine.begin() as conn:
            conn.execute(sql)

    def replace_column_enum_by_swap(self, table_name: str, column_name: str, new_enum: str,
                                    default: str = None):

        self._validate_identifier(table_name)
        if not isinstance(column_name, str) or not column_name:
            raise ValueError("column_name должен быть непустой строкой")
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
            raise ValueError(f"Недопустимое имя колонки: {column_name!r}")

        if not self._enum_exists(new_enum):
            raise ValueError(f"Enum '{new_enum}' не найден")

        enum_values = set(self._get_enum_values(new_enum))
        if default is not None and default not in enum_values:
            raise ValueError(f"default '{default}' не входит в enum '{new_enum}'")

        tbl_schema, tbl_name = self._split_schema_ident(table_name)
        tbl_ident = f'"{tbl_schema}"."{tbl_name}"'
        enum_schema, enum_nm = self._split_schema_ident(new_enum)
        enum_ident = f'"{enum_schema}"."{enum_nm}"'
        col_ident = f'"{column_name}"'

        meta_sql = text("""
            SELECT is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :tname AND column_name = :cname
        """)
        with self.engine.connect() as conn:
            meta_row = conn.execute(meta_sql,
                                    {'schema': tbl_schema, 'tname': tbl_name, 'cname': column_name}).mappings().first()
        if not meta_row:
            raise ValueError(f"Колонка {table_name}.{column_name} не найдена")

        is_nullable = (meta_row['is_nullable'] == 'YES')
        orig_default = meta_row['column_default']

        tmp_text_col = f'__{column_name}_tmp_text'
        tmp_enum_col = f'__{column_name}_tmp_enum'
        tmp_text_ident = f'"{tmp_text_col}"'
        tmp_enum_ident = f'"{tmp_enum_col}"'

        with self.engine.begin() as conn:
            conn.execute(text(f'ALTER TABLE {tbl_ident} ADD COLUMN {tmp_text_ident} text;'))

            conn.execute(
                text(f'UPDATE {tbl_ident} SET {tmp_text_ident} = ({col_ident}::text) WHERE {col_ident} IS NOT NULL;'))

            distinct_sql = text(
                f"SELECT DISTINCT {tmp_text_ident} AS v FROM {tbl_ident} WHERE {tmp_text_ident} IS NOT NULL;")
            rows = conn.execute(distinct_sql).scalars().all()
            found_values = [r for r in rows if r is not None]

            incompatible = [v for v in found_values if v not in enum_values]

            if incompatible and default is None:
                raise ValueError(f"Найдены несовместимые значения и не задан default: {incompatible}")

            if incompatible and default is not None:
                for v in incompatible:
                    u = text(f"""
                        UPDATE {tbl_ident}
                        SET {tmp_text_ident} = :dval
                        WHERE {tmp_text_ident} = :oldv
                    """)
                    conn.execute(u, {'dval': default, 'oldv': v})

            conn.execute(text(f'ALTER TABLE {tbl_ident} ADD COLUMN {tmp_enum_ident} {enum_ident};'))

            conn.execute(text(f"""
                UPDATE {tbl_ident}
                SET {tmp_enum_ident} = ({tmp_text_ident})::{enum_ident}
                WHERE {tmp_text_ident} IS NOT NULL;
            """))

            if not is_nullable:
                null_count = conn.execute(
                    text(f"SELECT count(1) FROM {tbl_ident} WHERE {tmp_enum_ident} IS NULL;")).scalar()
                if null_count and null_count > 0:
                    raise ValueError(
                        f"После подготовки временной колонки обнаружено {null_count} NULL значений, а исходная колонка NOT NULL. Укажите корректный default.")

            conn.execute(text(f'ALTER TABLE {tbl_ident} DROP COLUMN {col_ident} CASCADE;'))

            conn.execute(text(f'ALTER TABLE {tbl_ident} RENAME COLUMN {tmp_enum_ident} TO {col_ident};'))

            conn.execute(text(f'ALTER TABLE {tbl_ident} DROP COLUMN {tmp_text_ident};'))

            if orig_default is not None:
                conn.execute(text(f'ALTER TABLE {tbl_ident} ALTER COLUMN {col_ident} SET DEFAULT {orig_default};'))

            if not is_nullable:
                conn.execute(text(f'ALTER TABLE {tbl_ident} ALTER COLUMN {col_ident} SET NOT NULL;'))
        self.metadata.clear()
        self.insp = inspect(self.engine)
        self.reflect_tables([table_name], refresh=True)

    def find_incompatible_enum_values(self, table_name: str, column_name: str, new_enum: str):

        self._validate_identifier(table_name)
        if not isinstance(column_name, str) or not column_name:
            raise ValueError("column_name должен быть непустой строкой")
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', column_name):
            raise ValueError(f"Недопустимое имя колонки: {column_name!r}")

        enum_values = self._get_enum_values(new_enum)
        enum_set = set(enum_values)

        tbl_schema, tbl_name = self._split_schema_ident(table_name)
        tbl_ident = f'"{tbl_schema}"."{tbl_name}"'
        col_ident = f'"{column_name}"'

        sql = text(f"SELECT DISTINCT ({col_ident}::text) AS v FROM {tbl_ident} WHERE {col_ident} IS NOT NULL;")
        with self.engine.connect() as conn:
            rows = conn.execute(sql).scalars().all()

        found = [r for r in rows if r is not None]
        incompatible = [v for v in found if v not in enum_set]
        return incompatible

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
            elif column['type'] == 'ENUM':
                sql += f' {column['enum_name']}'
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
