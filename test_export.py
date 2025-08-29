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
            # Test SELECT query
            await cursor.execute('SELECT COUNT(*) FROM import_summaries')
            count_result = await cursor.fetchall()
            print(f'Summary count: {count_result[0][0]}')
            
            # Test actual data
            await cursor.execute('SELECT * FROM import_summaries LIMIT 1')
            data_result = await cursor.fetchall()
            print(f'Data result type: {type(data_result)}')
            print(f'Data result length: {len(data_result) if data_result else 0}')
            if data_result:
                print(f'First row type: {type(data_result[0])}')
                print(f'First row length: {len(data_result[0])}')
    
    pool.close()
    await pool.wait_closed()

if __name__ == "__main__":
    asyncio.run(main())
