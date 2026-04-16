
import asyncio
import os
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from dotenv import load_dotenv

# Path to the actual .env file
dotenv_path = r"c:\Users\NAGABALAJI\Downloads\dAAAAA\Data-Analysis-MultiAgent\.env"
load_dotenv(dotenv_path)

DATABASE_URL = os.getenv("DATABASE_URL")
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

async def check_users():
    engine = create_async_engine(DATABASE_URL)
    async_session = async_sessionmaker(engine, expire_on_commit=False)
    
    from sqlalchemy import Table, MetaData
    metadata = MetaData()
    
    async with engine.connect() as conn:
        try:
            # Just try to see if 'users' table exists and count them
            result = await conn.execute(select(func.count()).select_from(Table('users', metadata, autoload_with=engine)))
            count = result.scalar()
            print(f"Total users in DB: {count}")
            
            # List email of users
            result = await conn.execute(select(Table('users', metadata, autoload_with=engine).c.email))
            emails = result.scalars().all()
            print(f"User emails: {emails}")
        except Exception as e:
            print(f"Error checking users: {e}")
    
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(check_users())
