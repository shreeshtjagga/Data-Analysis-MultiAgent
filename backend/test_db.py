import asyncio
import os
import json
from dotenv import load_dotenv
import asyncpg

load_dotenv(".env")

async def main():
    conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
    rows = await conn.fetch("SELECT id, file_name, file_hash, errors, completed_agents FROM analysis_history ORDER BY analysis_date DESC LIMIT 3")
    for row in rows:
        print(dict(row))
    await conn.close()

asyncio.run(main())
