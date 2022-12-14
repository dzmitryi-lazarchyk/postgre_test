import logging
from typing import Union

import asyncpg
from asyncpg import Pool, Connection

from tgbot.config import load_config

db = load_config().db

class Database:

    def __init__(self):
        self.pool : Union[Pool, None] = None

    async def create(self):
        logging.info("Database startup!")
        self.pool = await asyncpg.create_pool(
            user=db.user,
            password=db.password,
            host=db.host,
            database=db.database
        )


    async def execute(self, command, *args,
                      fetch:bool=False,
                      fetchval:bool=False,
                      fetchrow:bool=False,
                      execute:bool=False
                      ):
        async with self.pool.acquire() as connection:
            connection: Connection
            async with connection.transaction():
                if fetch:
                    result = await connection.fetch(command, *args)
                elif fetchval:
                    result = await connection.fetchval(command, *args)
                elif fetchrow:
                    result= await connection.fetchrow(command, *args)
                elif execute:
                    result = await connection.execute(command, *args)
                return result

    async def create_table_users(self):
        logging.info("Table users creation")
        sql = """
        CREATE TABLE IF NOT EXISTS Users (
        id SERIAL PRIMARY KEY,
        full_name VARCHAR(255) NOT NULL,
        username varchar(255) NULL,
        telegram_id BIGINT NOT NULL UNIQUE
        );
        """
        await self.execute(sql, execute=True)
        logging.info("Done!")


    @staticmethod
    def format_args(sql, parameters:dict):
        sql+=" AND ".join([
            f"{item}=${num}" for num,item in enumerate(parameters.keys(),
                                                       start=1)
        ])
        return sql, tuple(parameters.values())

    async def insert_user(self, **kwargs):
        print(kwargs)
        values = ", ".join([f'${num}' for num, value in enumerate(kwargs.values(),
                                                         start=1)])
        sql = f"INSERT INTO Users ({', '.join(kwargs.keys())}) Values({values}) returning *"
        print(sql)
        parameters = tuple(kwargs.values())
        print(parameters)
        return await self.execute(sql, *parameters, fetchrow=True)

    async def select_all_users(self):
        sql = "SELECT * FROM Users"
        return await self.execute(sql, fetch=True)

    async def select_user(self, **kwargs):
        sql = "SELECT * FROM Users WHERE "
        sql, parameters = self.format_args(sql, kwargs)
        return await self.execute(sql, *parameters, fetchrow=True)

    async def count_users(self):
        sql = "SELECT COUNT(*) FROM Users"
        return await self.execute(sql, fetchval=True)

    async def update_user_username(self, username, telegram_id):
        sql = "UPDATE Users SET username=$1 WHERE telegram_id=$2"
        return await self.execute(sql, username, telegram_id, execute=True)

    async def delete_users(self):
        await self.execute("DELETE FROM Users WHERE True", execute=True)

    async def drop_table_users(self):
        await self.execute("DROP TABLE Users", execute=True)