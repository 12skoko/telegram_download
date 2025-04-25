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


def format_speed(speed_bytes):
    """格式化下载速度显示"""
    for unit in ['B/s', 'KB/s', 'MB/s', 'GB/s']:
        if speed_bytes < 1024.0:
            return f"{speed_bytes:.2f} {unit}"
        speed_bytes /= 1024.0
    return f"{speed_bytes:.2f} TB/s"


def format_size(size_bytes):
    """格式化文件大小显示"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"


def format_time(seconds):
    """格式化时间显示"""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds // 60:.0f}m {seconds % 60:.0f}s"
    else:
        return f"{seconds // 3600:.0f}h {(seconds % 3600) // 60:.0f}m"


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

            start_time = time.time()
            last_time = start_time
            last_bytes = 0
            speed = "0 B/s"

            def progress_callback(current, total):
                nonlocal last_time, last_bytes, speed

                now = time.time()
                elapsed = now - last_time

                # 每秒更新一次速度
                if elapsed >= 1.0:
                    downloaded = current - last_bytes
                    speed = format_speed(downloaded / elapsed)
                    last_time = now
                    last_bytes = current

                progress = int((current / total) * 100)
                progress_dict[message_id] = {
                    'status': 'downloading',
                    'progress': progress,
                    'speed': speed,
                    'downloaded': format_size(current),
                    'total': format_size(total),
                    'elapsed': format_time(now - start_time)
                }

            # 下载文件
            progress_dict[message_id] = {'status': 'downloading', 'progress': 0}

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
        cursor.execute(f"UPDATE message SET state = {state} WHERE chat_id = {chat_id} AND msg_id = {message_id}")
        conn.commit()
    except Exception as e:
        print(f"Error updating message state for {chat_id}/{message_id}: {str(e)}")
        conn.rollback()
    finally:
        cursor.close()


def update_media_in_db(conn, media_info):
    cursor = conn.cursor()

    cursor.execute(f"SELECT 1 FROM media WHERE media_id = {media_info['media_id']}")
    exists = cursor.fetchone()

    if exists:
        if config.db_type == 'sqlite':
            sql = """
            UPDATE media SET
                msg_id = ?,
                chat_id = ?,
                access_hash = ?,
                media_type = ?,
                mime_type = ?,
                file_size = ?,
                width = ?,
                height = ?,
                duration = ?,
                date = ?,
                file_path = ?,
                attributes = ?,
                file_name = ?,
                updated_at = ?
            WHERE media_id = ?
            """
        else:
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
        if config.db_type == 'sqlite':
            sql = """
            INSERT INTO media (
                media_id, msg_id, chat_id, access_hash, media_type, mime_type,
                file_size, width, height, duration, date, file_path,
                attributes, file_name, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        else:
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

        downloading = []
        for msg_id, status in progress_dict.items():
            if status['status'] == 'downloading':
                # 构建更详细的下载信息
                info = f"{msg_id}: {status['progress']}%"
                if 'speed' in status:
                    info += f" ({status['speed']})"
                if 'downloaded' in status and 'total' in status:
                    info += f" [{status['downloaded']}/{status['total']}]"
                downloading.append(info)

        print(f"\rProgress: {completed}/{total} ({int(completed / total * 100)}%) | Downloading: {', '.join(downloading)}", end='')
        await asyncio.sleep(1)

    print("\nAll downloads completed!")
    await asyncio.gather(*tasks, return_exceptions=True)


def get_medias(conn):
    if config.chat_id > 0:
        chat_id = -config.chat_id
    else:
        chat_id = config.chat_id
    sqlstr = f'SELECT chat_id, msg_id FROM message WHERE chat_id = {chat_id} AND media_type != "sticker" AND media_type != "MessageMediaWebPage" AND state is NULL'
    c = conn.cursor()
    c.execute(sqlstr)
    res = c.fetchall()
    return res


async def main():
    conn = config.createDBconn()

    client = TelegramClient(
        config.session_path,
        api_id=config.api_id,
        api_hash=config.api_hash
    )
    await client.start(phone=config.phone)

    medias = get_medias(conn)

    os.makedirs(MEDIA_DIR, exist_ok=True)

    await download_medias_concurrently(client, conn, medias)

    await client.disconnect()
    conn.close()


if __name__ == '__main__':
    API_ID = config.api_id
    API_HASH = config.api_hash
    SESSION_NAME = config.session_path
    MEDIA_DIR = config.download_path
    TEMP_DIR = config.download_temp_path
    MAX_CONCURRENT_DOWNLOADS = 5  # 最大并发下载数

    asyncio.run(main())
