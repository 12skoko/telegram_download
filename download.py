import traceback

from telethon import TelegramClient

import config

import os
import asyncio
import time
from datetime import datetime
from telethon.tl.types import (
    DocumentAttributeVideo,
    DocumentAttributeAudio,
    DocumentAttributeFilename,
    DocumentAttributeImageSize,
    PhotoSize,
    Document
)
import shutil
from telethon import utils


async def download_media(client, conn, chat_id, message_id, semaphore, progress_dict):
    async with semaphore:
        try:
            # 获取消息
            message = await client.get_messages(chat_id, ids=message_id)

            if not message or not message.media:
                return

            # 准备媒体信息
            media = message.media
            media_id = None

            # 根据媒体类型获取正确的media_id
            if hasattr(media, 'document'):
                media_id = media.document.id
            elif hasattr(media, 'photo'):
                media_id = media.photo.id
            media_info = {
                'media_id': media_id,
                'msg_id': message_id,
                'chat_id': chat_id,
                'access_hash': None,
                'media_type': None,
                'mime_type': None,
                'file_size': None,
                'width': None,
                'height': None,
                'duration': None,
                'date': message.date,
                'file_path': None,
                'attributes': str(getattr(media, 'attributes', [])),
                'file_name': None,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            }

            # 根据媒体类型提取信息
            if hasattr(media, 'document'):
                doc = media.document
                media_info.update({
                    'media_type': 'document',
                    'access_hash': doc.access_hash,
                    'mime_type': doc.mime_type,
                    'file_size': doc.size,
                })

                # 提取文件名和其他属性
                for attr in doc.attributes:
                    if isinstance(attr, DocumentAttributeFilename):
                        media_info['file_name'] = attr.file_name
                    elif isinstance(attr, DocumentAttributeVideo):
                        media_info.update({
                            'media_type': 'video',
                            'duration': attr.duration,
                            'width': attr.w,
                            'height': attr.h
                        })
                    elif isinstance(attr, DocumentAttributeAudio):
                        media_info.update({
                            'media_type': 'audio' if attr.voice else 'voice',
                            'duration': attr.duration
                        })
                    elif isinstance(attr, DocumentAttributeImageSize):
                        media_info.update({
                            'width': attr.w,
                            'height': attr.h
                        })

                # 如果没有文件名，生成一个
                if not media_info['file_name']:
                    ext = doc.mime_type.split('/')[-1] if doc.mime_type else 'bin'
                    media_info['file_name'] = f'document_{message.id}.{ext}'

            elif hasattr(media, 'photo'):
                photo = media.photo
                # 对于照片，我们需要手动获取大小
                photo_size = None
                for size in photo.sizes:
                    if hasattr(size, 'size'):
                        photo_size = size.size
                        break

                media_info.update({
                    'media_type': 'photo',
                    'access_hash': photo.access_hash,
                    'file_size': photo_size,
                    'file_name': f'photo_{message.id}.jpg'
                })

                # 获取照片的宽高
                for size in photo.sizes:
                    if hasattr(size, 'w') and hasattr(size, 'h'):
                        media_info.update({
                            'width': size.w,
                            'height': size.h
                        })
                        break

            # 创建临时目录和目标目录
            chat_dir = os.path.join(MEDIA_DIR, str(chat_id))
            os.makedirs(chat_dir, exist_ok=True)
            os.makedirs(TEMP_DIR, exist_ok=True)

            # 临时文件路径和最终文件路径
            temp_path = os.path.join(TEMP_DIR, media_info['file_name'])
            final_path = os.path.join(chat_dir, media_info['file_name'])

            # 检查文件是否已存在
            if os.path.exists(final_path):
                media_info['file_path'] = final_path
                update_media_in_db(conn, media_info)
                progress_dict[message_id] = {'status': 'skipped', 'progress': 100}
                return

            # 下载文件
            progress_dict[message_id] = {'status': 'downloading', 'progress': 0}

            def progress_callback(current, total):
                progress = int((current / total) * 100)
                progress_dict[message_id]['progress'] = progress

            await client.download_media(
                message,
                file=temp_path,
                progress_callback=progress_callback
            )

            # 移动文件到最终目录
            # os.rename(temp_path, final_path)
            shutil.move(str(temp_path), final_path)

            # 更新数据库
            media_info['file_path'] = final_path
            update_media_in_db(conn, media_info)

            # 更新message表的state字段为0
            update_message_state(conn, chat_id, message_id, 0)

            progress_dict[message_id] = {'status': 'completed', 'progress': 100}

        except Exception as e:
            print(f"Error downloading media {chat_id}/{message_id}: {str(e)}")
            progress_dict[message_id] = {'status': 'failed', 'progress': 0, 'error': str(e)}

def update_message_state(conn, chat_id, message_id, state):
    """更新message表中指定记录的state字段"""
    cursor = conn.cursor()
    try:
        sql = "UPDATE message SET state = %s WHERE chat_id = %s AND msg_id = %s"
        cursor.execute(sql, (state, chat_id, message_id))
        conn.commit()
    except Exception as e:
        print(f"Error updating message state for {chat_id}/{message_id}: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()

def update_media_in_db(conn, media_info):
    cursor = conn.cursor()

    # 检查记录是否已存在
    cursor.execute("SELECT 1 FROM media WHERE media_id = %s", (media_info['media_id'],))
    exists = cursor.fetchone()

    if exists:
        # 更新现有记录
        sql = """
        UPDATE media SET
            msg_id = %s,
            chat_id = %s,
            access_hash = %s,
            media_type = %s,
            mime_type = %s,
            file_size = %s,
            width = %s,
            height = %s,
            duration = %s,
            date = %s,
            file_path = %s,
            attributes = %s,
            file_name = %s,
            updated_at = %s
        WHERE media_id = %s
        """
        params = (
            media_info['msg_id'],
            media_info['chat_id'],
            media_info['access_hash'],
            media_info['media_type'],
            media_info['mime_type'],
            media_info['file_size'],
            media_info['width'],
            media_info['height'],
            media_info['duration'],
            media_info['date'],
            media_info['file_path'],
            media_info['attributes'],
            media_info['file_name'],
            media_info['updated_at'],
            media_info['media_id']
        )
    else:
        # 插入新记录
        sql = """
        INSERT INTO media (
            media_id, msg_id, chat_id, access_hash, media_type, mime_type,
            file_size, width, height, duration, date, file_path,
            attributes, file_name, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            media_info['media_id'],
            media_info['msg_id'],
            media_info['chat_id'],
            media_info['access_hash'],
            media_info['media_type'],
            media_info['mime_type'],
            media_info['file_size'],
            media_info['width'],
            media_info['height'],
            media_info['duration'],
            media_info['date'],
            media_info['file_path'],
            media_info['attributes'],
            media_info['file_name'],
            media_info['created_at'],
            media_info['updated_at']
        )

    cursor.execute(sql, params)
    conn.commit()


async def download_medias_concurrently(client, conn, medias):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    progress_dict = {}
    tasks = []

    for chat_id, message_id in medias:
        task = asyncio.create_task(
            download_media(client, conn, chat_id, message_id, semaphore, progress_dict)
        )
        tasks.append(task)

    # 显示进度
    total = len(medias)
    completed = 0

    while completed < total:
        completed = sum(
            1 for status in progress_dict.values()
            if status['status'] in ('completed', 'skipped', 'failed')
        )

        downloading = [
            f"{msg_id}: {status['progress']}%"
            for msg_id, status in progress_dict.items()
            if status['status'] == 'downloading'
        ]

        print(f"\rProgress: {completed}/{total} ({int(completed / total * 100)}%) | Downloading: {', '.join(downloading)}", end='')
        await asyncio.sleep(1)

    print("\nAll downloads completed!")
    await asyncio.gather(*tasks, return_exceptions=True)


def get_medias(conn):
    sqlstr='SELECT chat_id, msg_id FROM telegram.message WHERE chat_id = -1002444549090 AND media_type != "sticker" AND media_type != "MessageMediaWebPage" AND state is NULL'
    c=conn.cursor()
    c.execute(sqlstr)
    res=c.fetchall()
    return res

# 主程序
async def main():
    # 初始化数据库
    conn = config.createDBconn()

    # 创建Telegram客户端
    client = TelegramClient(
        config.session_path,
        api_id=config.api_id,  # 替换为你的API ID
        api_hash=config.api_hash  # 替换为你的API Hash
    )
    await client.start(phone=config.phone)

    # 获取需要下载的媒体列表 (使用你已有的get_medias()函数)
    medias = get_medias(conn)  # 返回 [(chat_id, message_id), ...]

    # 创建媒体存储目录
    os.makedirs(MEDIA_DIR, exist_ok=True)

    # 并发下载所有媒体文件
    await download_medias_concurrently(client, conn, medias)

    # 关闭连接
    await client.disconnect()
    conn.close()


if __name__ == '__main__':
    # 配置信息
    API_ID = config.api_id
    API_HASH = config.api_hash
    SESSION_NAME = config.session_path
    MEDIA_DIR = config.download_path
    TEMP_DIR = config.download_temp_path
    MAX_CONCURRENT_DOWNLOADS = 5  # 最大并发下载数

    asyncio.run(main())