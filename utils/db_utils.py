import aiomysql
from utils.config import get_config

config = get_config()

async def get_connection():
    conn = await aiomysql.connect(
        host=config['database']['host'],
        port=config['database']['port'],
        user=config['database']['user'],
        password=config['database']['password'],
        db=config['database']['database']
    )
    return conn

async def update_import_filled_until(conn, sensor_db_id, import_filled_until):
    async with conn.cursor() as cur:
        await cur.execute("""
            UPDATE api_connections
            SET import_filled_until = %s
            WHERE id = %s
        """, (import_filled_until.strftime("%Y-%m-%d %H:%M:%S"), sensor_db_id))
        await conn.commit()
