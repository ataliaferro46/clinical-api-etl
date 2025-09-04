# etl-service/src/db.py
import asyncpg, os

async def get_pool():
    return await asyncpg.create_pool(
        user=os.getenv("POSTGRES_USER", "user"),
        password=os.getenv("POSTGRES_PASSWORD", "password"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "clinical_data"),
        min_size=1, max_size=10,
    )
