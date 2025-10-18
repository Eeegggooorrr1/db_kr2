from psycopg2._psycopg import connection
from sqlalchemy import Table, Column, Integer, String, MetaData, UniqueConstraint, Boolean
from sqlalchemy import create_engine
import os

from sqlalchemy.orm import sessionmaker

from config import settings

DATABASE_URL = settings.get_db_url()

engine = create_engine(DATABASE_URL, future=True)
metadata = MetaData()

import enum
from sqlalchemy import (
    MetaData, Table, Column, Integer, String, Text, Date,
    TIMESTAMP, Float, Boolean, ForeignKey, ARRAY, Enum,
    func, text, CheckConstraint, UniqueConstraint, ForeignKeyConstraint, create_engine
)
from sqlalchemy.schema import CreateTable


class AttackTypeEnum(str, enum.Enum):
    no_attack = "no_attack"
    blur = "blur"
    noise = "noise"
    adversarial = "adversarial"
    other = "other"

from config import settings

# experiments = Table(
#     "experiments",
#     metadata,
#     Column("experiment_id", Integer, primary_key=True, autoincrement=True),
#     Column("name", String(255), nullable=False),
#     Column("description", Text, nullable=True),
#     Column("created_date", Date, server_default=func.current_date()),
# )
#
# runs = Table(
#     "runs",
#     metadata,
#     Column("run_id", Integer, primary_key=True, autoincrement=True),
#     Column("experiment_id", Integer, nullable=False),
#     Column("run_date", TIMESTAMP, server_default=func.now()),
#     Column("accuracy", Float, nullable=True),
#     Column("flagged", Boolean, nullable=True),
#
#     ForeignKeyConstraint(
#         ["experiment_id"],
#         ["experiments.experiment_id"],
#         ondelete="CASCADE",
#         name="fk_runs_experiment_id"
#     )
# )
#
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
#     ForeignKeyConstraint(
#         ["run_id"],
#         ["runs.run_id"],
#         ondelete="CASCADE",
#         name="fk_images_run_id"
#     )
# )
#
# metadata.create_all(engine)

from sqlalchemy import text
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
with SessionLocal() as session:
    session.execute(text("ALTER SEQUENCE experiments_experiment_id_seq RESTART WITH 1;"))
    session.execute(text("ALTER SEQUENCE runs_run_id_seq RESTART WITH 1;"))
    session.execute(text("ALTER SEQUENCE images_image_id_seq RESTART WITH 1;"))

    session.execute(text("""
        INSERT INTO experiments (name, description) VALUES
        ('Baseline Classification', 'Standard image classification without attacks'),
        ('Adversarial Robustness', 'Testing model resilience against adversarial attacks'),
        ('Noise Sensitivity', 'Evaluating performance under different noise conditions'),
        ('Blur Tolerance', 'Testing model accuracy with various blur levels'),
        ('Mixed Attacks', 'Combination of different attack types');
    """))

    session.execute(text("""
        INSERT INTO runs (experiment_id, run_date, accuracy, flagged)
        SELECT
            (seq.run_id % 5) + 1 as experiment_id,
            now() - interval '1 day' * random() * 30 as run_date,
            round(random()::numeric * 0.4 + 0.6, 4) as accuracy,
            random() > 0.9 as flagged
        FROM generate_series(1, 20) as seq(run_id);
    """))

    session.execute(text("""
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
    session.commit()