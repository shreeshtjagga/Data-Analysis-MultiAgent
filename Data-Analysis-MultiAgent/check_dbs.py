import asyncio
import asyncpg
import redis.asyncio as redis

async def check():
    print("--- Checking PostgreSQL ---")
    try:
        conn = await asyncpg.connect('postgresql://datapulse:datapulse_secret@localhost:5432/datapulse')
        print("Connected to PostgreSQL successfully!")
        
        tables = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
        if tables:
            print("Tables found:")
            for t in tables:
                print(f" - {t['tablename']}")
        else:
            print("No tables found in public schema.")
            
        await conn.close()
    except Exception as e:
        print(f"PostgreSQL connection failed: {e}")

    print("\n--- Checking Redis ---")
    try:
        r = redis.from_url('redis://localhost:6379')
        ping = await r.ping()
        if ping:
            print("Connected to Redis successfully!")
            keys = await r.keys('*')
            print(f"Total keys in Redis: {len(keys)}")
            for k in keys[:5]:
                print(f" - {k.decode('utf-8')}")
            if len(keys) > 5:
                print(" - ...")
        await r.close()
    except Exception as e:
        print(f"Redis connection failed: {e}")

asyncio.run(check())
