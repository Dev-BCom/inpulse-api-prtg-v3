import aiomysql
import logging
from utils.config import get_config

config = get_config()
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
            maxsize=config['database']['pool_size'],
            autocommit=True
        )
        logging.info(f"MySQL pool initialized with pool_size={config['database']['pool_size']}")

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
        logging.info("MySQL pool closed.")

async def get_sensors():
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute("""
                SELECT * FROM api_connections
                WHERE (api_data_type NOT IN ('link', 'device', 'group') OR api_data_type IS NULL)
                AND api = 'prtg'
                AND api_needs_connection_checking = 'n'
                AND api_connected = 'y'
            """)
            sensors = await cur.fetchall()
            return sensors

async def update_import_filled_until(sensor_db_id, import_filled_until):
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute("""
                UPDATE api_connections
                SET import_filled_until = %s
                WHERE id = %s
            """, (import_filled_until.strftime("%Y-%m-%d %H:%M:%S"), sensor_db_id))
