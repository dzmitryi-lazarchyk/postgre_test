import asyncio
import logging
import re

import asyncpg
from aiogram import Dispatcher
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters import Command
from aiogram.types import Message

from tgbot.models.postgresql import Database





async def user_start(message: Message):
    await message.reply("Hello, user!")

async def add_user(message:Message, state:FSMContext):
    await message.answer("Введи данные юзера")
    await state.set_state('user_data')

async def user_data(message:Message, db:Database ,state:FSMContext):
    data = re.split(r'[\s+,.]', message.text)
    print(data)
    kwargs = {"full_name":data[0], "username":data[1], "telegram_id":message.from_user.id}
    try:
        result = await db.insert_user(**kwargs)
        print(result)
        await message.answer(result[0])
    except asyncpg.exceptions.UniqueViolationError as e:
        logging.info(e)
    await state.finish()

async def all_users(message:Message, db:Database, dp:Dispatcher):
    result = await db.select_all_users()
    text = ''
    for record in result:
        text+=f'{record[0]}:{record[1]}:{record[2]}:{record[3]}\n'
    print(text)
    await dp.bot.send_message(chat_id=message.from_user.id, text=text)
    # await message.answer(text, parse_mode=None)




def register_user(dp: Dispatcher):
    dp.register_message_handler(user_start, commands=["start"], state="*")
    dp.register_message_handler(add_user, commands=["add_user"], state="*")
    dp.register_message_handler(user_data, state="user_data")
    dp.register_message_handler(all_users, Command('all_users'))
