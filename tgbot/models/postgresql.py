from typing import Union

import asyncpg
from asyncpg import Pool

from tgbot.config import load_config

db = load_config().config

class Database:

    def __init__(self):
        self.pool : Union[Pool, None] = None

    async  def create(self):
        self.pool = await asyncpg.create_pool(
            user=db.user,
            password=db.password,
            host=db.host,
        )