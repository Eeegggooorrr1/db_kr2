from sqlalchemy import Table, Column, Integer, String, MetaData, UniqueConstraint
from sqlalchemy import create_engine
import os

from config import settings

DATABASE_URL = settings.get_db_url()

engine = create_engine(DATABASE_URL, future=True)
metadata = MetaData()

users = Table(
    "users",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("username", String(50), nullable=False, unique=True),
    Column("full_name", String(120), nullable=True),
    Column("age", Integer, nullable=True),
)

products = Table(
    "products",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("code", String(40), nullable=False, unique=True),
    Column("description", String(255), nullable=True),
    Column("quantity", Integer, nullable=False),
)

if __name__ == "__main__":
    metadata.create_all(engine)
    print("Demo tables created (if not existed).")