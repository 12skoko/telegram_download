import asyncio
import logging
from datetime import datetime
from telethon import TelegramClient
from telethon.tl.types import Message, DocumentAttributeSticker
from typing import Optional, Dict, Any

import config

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
class TelegramMessageExporter:
    def __init__(self, client: TelegramClient):
        self.client = client
        self.conn = config.createDBconn()
        self.cursor = self.conn.cursor()
        self.batch_size = 1000  # 每批处理的消息数量
        self.max_retries = 3    # 最大重试次数
        self.retry_delay = 5     # 重试延迟(秒)
    async def get_last_processed_id(self, chat_id: int) -> Optional[int]:
        """获取最后处理的message_id"""
        try:
            self.cursor.execute(
                "SELECT MAX(msg_id) FROM message WHERE chat_id = %s",
                (-chat_id,)
            )
            result = self.cursor.fetchone()
            return result[0] if result and result[0] else None
        except Exception as e:
            logger.error(f"获取最后处理ID失败: {e}")
            return None
    def message_to_dict(self, message: Message) -> Dict[str, Any]:
        """将Message对象转换为字典"""
        if not isinstance(message, Message):
            return None
        # 处理转发信息
        fwd_from = message.fwd_from if hasattr(message, 'fwd_from') else None
        reply_to = message.reply_to if hasattr(message, 'reply_to') else None
        # 处理媒体信息
        media = message.media if hasattr(message, 'media') else None
        media_id = None
        media_type = None
        if media:
            if type(media).__name__ == "MessageMediaDocument":
                for attr in media.document.attributes:
                    if isinstance(attr, DocumentAttributeSticker):
                        media_type = 'sticker'
                        break
                else:
                    media_type = media.document.mime_type
                media_id = media.document.id
            elif type(media).__name__ == "MessageMediaPhoto":

                media_type='MessageMediaPhoto'
                media_id = media.photo.id

            else:
                media_type = type(media).__name__

        # 处理反应
        reactions = ''
        if hasattr(message, 'reactions') and message.reactions:
            reactions = {
                str(reaction.reaction.emoticon if hasattr(reaction.reaction, 'emoticon') else reaction.reaction):
                    reaction.count
                for reaction in message.reactions.results
            }
        reactions=str(reactions)

        return {
            'msg_id': message.id,
            'chat_id': message.chat_id if hasattr(message, 'chat_id') else None,
            'user_id': message.from_id.user_id if hasattr(message, 'from_id') and hasattr(message.from_id, 'user_id') else None,
            'msg_date': message.date,
            'edit_date': message.edit_date if hasattr(message, 'edit_date') else None,
            'message': message.message,
            'media_id': media_id,
            'media_type': media_type,
            'fwd_from_chat_id': fwd_from.from_id.channel_id if fwd_from and hasattr(fwd_from.from_id, 'channel_id') else None,
            'fwd_from_user_id': fwd_from.from_id.user_id if fwd_from and hasattr(fwd_from.from_id, 'user_id') else None,
            'fwd_from_channel_post': fwd_from.channel_post if fwd_from else None,
            'fwd_from_post_author': fwd_from.post_author if fwd_from else None,
            'fwd_from_date': fwd_from.date if fwd_from else None,
            'reply_to_msg_id': reply_to.reply_to_msg_id if reply_to else None,
            'reply_to_user_id': reply_to.reply_to_peer_id if reply_to else None,
            'views': message.views if hasattr(message, 'views') else None,
            'forwards': message.forwards if hasattr(message, 'forwards') else None,
            'grouped_id': message.grouped_id if hasattr(message, 'grouped_id') else None,
            'post_author': message.post_author if hasattr(message, 'post_author') else None,
            'reactions': reactions,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
    async def insert_messages_batch(self, messages: list) -> bool:
        """批量插入消息到MySQL"""
        if not messages:
            return True
        values = []
        for msg_dict in messages:

            values.append((
                msg_dict['msg_id'],
                msg_dict['chat_id'],
                msg_dict['user_id'],
                msg_dict['msg_date'],
                msg_dict['edit_date'],
                msg_dict['message'],
                msg_dict['media_id'],
                msg_dict['media_type'],
                msg_dict['fwd_from_chat_id'],
                msg_dict['fwd_from_user_id'],
                msg_dict['fwd_from_channel_post'],
                msg_dict['fwd_from_post_author'],
                msg_dict['fwd_from_date'],
                msg_dict['reply_to_msg_id'],
                msg_dict['reply_to_user_id'],
                msg_dict['views'],
                msg_dict['forwards'],
                msg_dict['grouped_id'],
                msg_dict['post_author'],
                msg_dict['reactions'],
                msg_dict['created_at'],
                msg_dict['updated_at']
            ))
        query = """
        INSERT INTO message (
            msg_id, chat_id, user_id, msg_date, edit_date, message, media_id, media_type,
            fwd_from_chat_id, fwd_from_user_id, fwd_from_channel_post, fwd_from_post_author, fwd_from_date,
            reply_to_msg_id, reply_to_user_id, views, forwards, grouped_id, post_author, reactions,
            created_at, updated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            edit_date = VALUES(edit_date),
            message = VALUES(message),
            media_id = VALUES(media_id),
            media_type = VALUES(media_type),
            views = VALUES(views),
            forwards = VALUES(forwards),
            reactions = VALUES(reactions),
            updated_at = VALUES(updated_at)
        """
        retries = 0
        while retries < self.max_retries:
            try:
                self.cursor.executemany(query, values)
                self.conn.commit()
                logger.info(f"成功插入/更新 {len(messages)} 条消息")
                return True
            except Exception as e:
                retries += 1
                logger.error(f"插入消息失败(尝试 {retries}/{self.max_retries}): {e}")
                if retries < self.max_retries:
                    await asyncio.sleep(self.retry_delay)

        logger.error(f"插入消息失败，已达到最大重试次数 {self.max_retries}")
        return False
    async def export_messages(self, chat_id: int):
        """导出消息主函数"""
        last_processed_id = await self.get_last_processed_id(chat_id)
        offset_id = 0 if last_processed_id is None else last_processed_id
        total_processed = 0
        logger.info(f"开始导出消息，最后处理的ID: {last_processed_id}")
        while True:
            try:
                messages = []
                async for message in self.client.iter_messages(
                    chat_id,
                    limit=self.batch_size,
                    offset_id=offset_id,
                    reverse=True  # 从旧到新处理
                ):
                    msg_dict = self.message_to_dict(message)
                    if msg_dict:
                        messages.append(msg_dict)
                if not messages:
                    logger.info("没有更多消息可处理")
                    break
                # 处理批量插入
                success = await self.insert_messages_batch(messages)
                if not success:
                    logger.error("批量插入失败，终止处理")
                    break
                total_processed += len(messages)
                offset_id = messages[-1]['msg_id']  # 更新offset_id为最后一条消息的ID
                logger.info(f"已处理 {total_processed} 条消息，当前offset_id: {offset_id}")
                # 避免触发API限制
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"处理消息时发生错误: {e}")
                await asyncio.sleep(self.retry_delay)
                continue
        logger.info(f"消息导出完成，总共处理了 {total_processed} 条消息")
# 使用示例
async def main():
    # Telegram客户端配置
    client = TelegramClient(
        config.session_path,
        api_id=config.api_id,       # 替换为你的API ID
        api_hash=config.api_hash   # 替换为你的API Hash
    )
    # 创建导出器实例
    exporter = TelegramMessageExporter(client)
    # 连接到Telegram
    await client.start(phone=config.phone)
    # 导出指定聊天ID的消息
    chat_id = 1002444549090  # 替换为你的群组ID
    await exporter.export_messages(chat_id)
    # 断开连接
    await client.disconnect()
if __name__ == '__main__':
    asyncio.run(main())