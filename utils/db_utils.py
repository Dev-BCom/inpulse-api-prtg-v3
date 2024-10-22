import aiomysql
from utils.config import get_config

config = get_config()

# Connection pool (global)
_pool = None

async def init_mysql_pool():
    global _pool
    if _pool is None:
        _pool = await aiomysql.create_pool(
            host=config['database']['host'],
            port=config['database']['port'],
            user=config['database']['user'],
            password=config['database']['password'],
            db=config['database']['database'],
            maxsize=config['database']['pool_size'],  # Use single pool_size
        )
        print(f"MySQL pool initialized with pool_size={config['database']['pool_size']}")
    return _pool

def get_pool():
    global _pool
    if _pool is None:
        raise RuntimeError("MySQL pool is not initialized. Call init_mysql_pool() first.")
    return _pool

async def close_mysql_pool():
    global _pool
    if _pool is not None:
        _pool.close()
        await _pool.wait_closed()
        _pool = None
        print("MySQL pool closed.")

async def update_import_filled_until(conn, sensor_db_id, import_filled_until):
    async with conn.cursor() as cur:
        await cur.execute("""
            UPDATE api_connections
            SET import_filled_until = %s
            WHERE id = %s
        """, (import_filled_until.strftime("%Y-%m-%d %H:%M:%S"), sensor_db_id))
        await conn.commit()
