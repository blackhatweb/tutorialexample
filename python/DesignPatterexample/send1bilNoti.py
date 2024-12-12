import asyncio
import logging
from typing import List, Dict, Any, Protocol
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from enum import Enum, auto
import aiohttp
import json

# Logging Configuration
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

# Enum cho các kênh thông báo
class NotificationChannel(Enum):
    SMS = auto()
    PUSH = auto()

# Observer Pattern: Theo dõi trạng thái gửi thông báo
class NotificationObserver:
    def __init__(self):
        self.total_sent = 0
        self.total_failed = 0
        self.channel_stats = {
            channel: {'sent': 0, 'failed': 0} 
            for channel in NotificationChannel
        }
        self.lock = asyncio.Lock()

    async def record_success(self, channel: NotificationChannel):
        async with self.lock:
            self.total_sent += 1
            self.channel_stats[channel]['sent'] += 1
    
    async def record_failure(self, channel: NotificationChannel):
        async with self.lock:
            self.total_failed += 1
            self.channel_stats[channel]['failed'] += 1
    
    def get_status(self):
        return {
            "total_sent": self.total_sent,
            "total_failed": self.total_failed,
            "channel_stats": self.channel_stats
        }

# Strategy Pattern: Interface gửi thông báo
class NotificationStrategy(Protocol):
    async def send_notification(
        self, 
        recipient: str, 
        message: Dict[str, Any]
    ) -> bool:
        ...

# Concrete Strategies cho từng kênh
class SMSNotificationStrategy:
    def __init__(self, provider_api):
        self.provider_api = provider_api
    
    async def send_notification(
        self, 
        recipient: str, 
        message: Dict[str, Any]
    ) -> bool:
        try:
            # Simulate SMS sending
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.provider_api, 
                    json={
                        'phone': recipient,
                        'message': message['content']
                    }
                ) as response:
                    return response.status == 200
        except Exception as e:
            logger.error(f"SMS send failed: {e}")
            return False

class PushNotificationStrategy:
    def __init__(self, firebase_key):
        self.firebase_key = firebase_key
    
    async def send_notification(
        self, 
        recipient: str, 
        message: Dict[str, Any]
    ) -> bool:
        try:
            # Simulate Firebase Push Notification
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    'https://fcm.googleapis.com/fcm/send',
                    headers={
                        'Authorization': f'key={self.firebase_key}',
                        'Content-Type': 'application/json'
                    },
                    json={
                        'to': recipient,
                        'notification': message
                    }
                ) as response:
                    return response.status == 200
        except Exception as e:
            logger.error(f"Push notification failed: {e}")
            return False

# Builder Pattern: Xây dựng thông báo
class NotificationBuilder:
    def __init__(self):
        self.notification = {
            'content': '',
            'title': '',
            'data': {}
        }
        self.channels = []
    
    def set_content(self, content: str):
        self.notification['content'] = content
        return self
    
    def set_title(self, title: str):
        self.notification['title'] = title
        return self
    
    def add_data(self, key: str, value: Any):
        self.notification['data'][key] = value
        return self
    
    def add_channel(self, channel: NotificationChannel):
        self.channels.append(channel)
        return self
    
    def build(self):
        return Notification(
            content=self.notification['content'],
            title=self.notification['title'],
            data=self.notification['data'],
            channels=self.channels
        )

# Data Class cho Notification
@dataclass
class Notification:
    content: str
    title: str
    data: Dict[str, Any] = field(default_factory=dict)
    channels: List[NotificationChannel] = field(default_factory=list)

# Factory Pattern: Tạo notification strategy
class NotificationStrategyFactory:
    @staticmethod
    def create_strategy(
        channel: NotificationChannel, 
        config: Dict[str, Any]
    ) -> NotificationStrategy:
        strategies = {
            NotificationChannel.SMS: SMSNotificationStrategy,
            NotificationChannel.PUSH: PushNotificationStrategy
        }
        return strategies[channel](config.get('api_key'))

# Facade Pattern: Quản lý gửi thông báo
class NotificationSenderFacade:
    def __init__(
        self, 
        observer: NotificationObserver,
        config: Dict[str, Any]
    ):
        self.observer = observer
        self.config = config
        self.strategies = {
            channel: NotificationStrategyFactory.create_strategy(channel, config)
            for channel in NotificationChannel
        }
    
    async def send_batch_notifications(
        self, 
        notification: Notification, 
        recipients: List[str]
    ):
        async def send_single_notification(recipient, channel):
            try:
                strategy = self.strategies[channel]
                success = await strategy.send_notification(recipient, {
                    'content': notification.content,
                    'title': notification.title,
                    **notification.data
                })
                
                if success:
                    await self.observer.record_success(channel)
                else:
                    await self.observer.record_failure(channel)
            except Exception as e:
                logger.error(f"Notification send error: {e}")
                await self.observer.record_failure(channel)
        
        # Gửi song song theo từng kênh
        for channel in notification.channels:
            channel_tasks = [
                send_single_notification(recipient, channel) 
                for recipient in recipients
            ]
            await asyncio.gather(*channel_tasks)

# Ứng dụng chính
async def main():
    # Cấu hình
    config = {
        'sms': {'api_key': 'sms_provider_key'},
        'push': {'api_key': 'firebase_key'}
    }
    
    # Khởi tạo observer
    observer = NotificationObserver()
    
    # Tạo facade
    notification_sender = NotificationSenderFacade(observer, config)
    
    # Xây dựng thông báo
    notification_builder = NotificationBuilder()
    notification = (notification_builder
        .set_title("Khuyến Mãi Mới")
        .set_content("Ưu đãi độc quyền dành cho bạn...")
        .add_data("campaign_id", "summer_2024")
        .add_channel(NotificationChannel.SMS)
        .add_channel(NotificationChannel.PUSH)
        .build()
    )
    
    # Sinh dữ liệu người nhận
    recipients = [f"user{i}@example.com" for i in range(1_000_000)]
    
    # Xử lý batch
    batch_size = 10000
    for i in range(0, len(recipients), batch_size):
        batch = recipients[i:i+batch_size]
        
        # Gửi batch
        await notification_sender.send_batch_notifications(
            notification, batch
        )
        
        # Log trạng thái
        logger.info(f"Batch {i//batch_size + 1} completed")
        logger.info(json.dumps(observer.get_status(), indent=2))

# Chạy ứng dụng
if __name__ == "__main__":
    asyncio.run(main())