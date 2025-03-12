import asyncpg
import pandas as pd

class Database:
    def __init__(self, db_config):
        self.db_config = db_config
        self.pool = None

    async def create_pool(self):
        if not self.pool:
            self.pool = await asyncpg.create_pool(**self.db_config)

    async def close_pool(self):
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def fetch(self, query, *args):
        await self.create_pool()
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(query, *args)
        return rows
    
    async def fetchone(self, query, *args):
        await self.create_pool()
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(query, *args)
        return row
    
    async def fetch_to_df(self, query, *args):
        await self.create_pool()
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(query, *args)
        
        if rows:
            # Mendapatkan nama kolom dari hasil kueri
            columns = list(rows[0].keys())
            # Membuat DataFrame
            df = pd.DataFrame(rows, columns=columns)
        else:
            # Jika tidak ada hasil, buat DataFrame kosong
            df = pd.DataFrame()

        return df

    async def execute(self, query, *args):
        await self.create_pool()
        async with self.pool.acquire() as connection:
            await connection.execute(query, *args)

    async def execute_return(self, query, *args):
        await self.create_pool()
        async with self.pool.acquire() as connection:
            res = await connection.fetchval(query, *args)
        return res
