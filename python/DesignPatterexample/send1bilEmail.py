import asyncio
import logging
from typing import List, Dict
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod

# Logger Configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Observer Pattern: Tracking Email Send Status
class EmailObserver:
    def __init__(self):
        self.total_sent = 0
        self.total_failed = 0
        self.lock = asyncio.Lock()

    async def record_success(self):
        async with self.lock:
            self.total_sent += 1
    
    async def record_failure(self):
        async with self.lock:
            self.total_failed += 1
    
    def get_status(self):
        return {
            "sent": self.total_sent,
            "failed": self.total_failed
        }

# Strategy Pattern: Email Sending Strategies
class EmailSendStrategy(ABC):
    @abstractmethod
    async def send_email(self, email_context: 'EmailContext') -> bool:
        pass

class SendGridEmailStrategy(EmailSendStrategy):
    def __init__(self, api_key):
        self.api_key = api_key
    
    async def send_email(self, email_context):
        try:
            # Mô phỏng gửi email qua SendGrid
            await asyncio.sleep(0.01)  # Giả lập thời gian gửi email
            return True
        except Exception as e:
            logger.error(f"SendGrid email send failed: {e}")
            return False

class AWSEmailStrategy(EmailSendStrategy):
    def __init__(self, aws_config):
        self.aws_config = aws_config
    
    async def send_email(self, email_context):
        try:
            # Mô phỏng gửi email qua AWS SES
            await asyncio.sleep(0.01)  # Giả lập thời gian gửi email
            return True
        except Exception as e:
            logger.error(f"AWS email send failed: {e}")
            return False

# Builder Pattern: Email Template Construction
class EmailTemplateBuilder:
    def __init__(self):
        self.subject = ""
        self.body = ""
        self.personalizations = {}
    
    def set_subject(self, subject):
        self.subject = subject
        return self
    
    def set_body(self, body):
        self.body = body
        return self
    
    def personalize(self, key, value):
        self.personalizations[key] = value
        return self
    
    def build(self):
        return EmailContext(
            subject=self.subject,
            body=self.body,
            personalizations=self.personalizations
        )

# Data Class for Email Context
@dataclass
class EmailContext:
    subject: str
    body: str
    personalizations: Dict[str, str] = field(default_factory=dict)
    recipient: str = ""

# Factory Pattern: Email Sender Factory
class EmailSenderFactory:
    @staticmethod
    def create_sender(strategy_type: str, config):
        strategies = {
            'sendgrid': SendGridEmailStrategy,
            'aws': AWSEmailStrategy
        }
        return strategies.get(strategy_type, SendGridEmailStrategy)(config)

# Facade Pattern: Unified Email Sending Interface
class EmailSenderFacade:
    def __init__(self, strategy: EmailSendStrategy, observer: EmailObserver):
        self.strategy = strategy
        self.observer = observer
        self.executor = ThreadPoolExecutor(max_workers=20)
    
    async def send_batch_emails(self, email_contexts: List[EmailContext]):
        async def send_single_email(email_context):
            try:
                success = await self.strategy.send_email(email_context)
                if success:
                    await self.observer.record_success()
                else:
                    await self.observer.record_failure()
            except Exception as e:
                logger.error(f"Email send error: {e}")
                await self.observer.record_failure()
        
        # Sử dụng asyncio để gửi song song
        await asyncio.gather(*[send_single_email(email) for email in email_contexts])

# Main Application Logic
async def main():
    # Khởi tạo observer
    email_observer = EmailObserver()
    
    # Tạo email sender strategy
    email_strategy = EmailSenderFactory.create_sender(
        'sendgrid', 
        api_key='your_sendgrid_api_key'
    )
    
    # Tạo email sender facade
    email_sender = EmailSenderFacade(email_strategy, email_observer)
    
    # Tạo template email
    template_builder = EmailTemplateBuilder()
    template = (template_builder
        .set_subject("Hướng Dẫn Cài Đặt")
        .set_body("Chi tiết cài đặt chi tiết...")
    )
    
    # Sinh dữ liệu email mẫu
    recipients = [f"user{i}@example.com" for i in range(1_000_000)]
    
    # Xử lý batch emails
    batch_size = 10000
    for i in range(0, len(recipients), batch_size):
        batch = recipients[i:i+batch_size]
        
        # Tạo context emails cho batch
        email_contexts = [
            template.personalize("name", recipient)
                    .build() 
            for recipient in batch
        ]
        
        # Gửi batch
        await email_sender.send_batch_emails(email_contexts)
        
        # Log trạng thái
        logger.info(f"Batch {i//batch_size + 1} completed")
        logger.info(email_observer.get_status())

# Chạy ứng dụng
if __name__ == "__main__":
    asyncio.run(main())