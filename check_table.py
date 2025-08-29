#!/usr/bin/env python3
import aiomysql
import asyncio

async def main():
    pool = await aiomysql.create_pool(
        host='ritstest.cnfwdrtgyxew.us-west-2.rds.amazonaws.com',
        port=3306,
        user='sarvesh',
        password='Saved6-Hydrogen-Smirk-Paltry-Trimmer',
        db='logcomex'
    )
    
    async with pool.acquire() as conn:
        async with conn.cursor() as cursor:
            await cursor.execute('DESCRIBE import_summaries')
            result = await cursor.fetchall()
            print('Columns:')
            for i, row in enumerate(result):
                print(f'{i+1}: {row[0]}')
    
    pool.close()
    await pool.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())


