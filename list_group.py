from telethon import TelegramClient
import asyncio
import config


async def list_groups():
    client = TelegramClient(config.session_path, config.api_id, config.api_hash)
    await client.start(phone=config.phone, password=config.password)

    with open("groups.txt", "w", encoding="utf-8") as f:
        f.write("你加入的会话列表:\n")
        async for dialog in client.iter_dialogs():
            print(f"{dialog.name} (ID: {dialog.id})")
            f.write(f"{dialog.name} (ID: {dialog.id})\n")
    await client.disconnect()

asyncio.run(list_groups())
