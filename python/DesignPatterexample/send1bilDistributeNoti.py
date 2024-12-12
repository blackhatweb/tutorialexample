import asyncio
import aiohttp
import redis
import json
from typing import List, Dict
from concurrent.futures import ThreadPoolExecutor
from ratelimiter import RateLimiter

class NotificationDistributor:
    def __init__(self, redis_host='localhost', redis_port=6379, max_workers=50):
        # Kết nối Redis làm message queue
        self.redis_client = redis.Redis(host=redis_host, port=redis_port)
        
        # Queue để lưu danh sách khách hàng cần gửi
        self.NOTIFICATION_QUEUE = 'customer_notification_queue'
        
        # Queue để lưu các notification thất bại
        self.FAILED_QUEUE = 'failed_notification_queue'
        
        # Giới hạn số lượng worker
        self.max_workers = max_workers

    def prepare_notification_data(self, customers: List[Dict], message: str):
        """
        Chuẩn bị dữ liệu notification và đưa vào Redis queue
        """
        for customer in customers:
            notification_data = {
                'customer_id': customer['id'],
                'phone': customer.get('phone'),
                'email': customer.get('email'),
                'message': message,
                'attempts': 0
            }
            # Đưa thông tin vào queue
            self.redis_client.rpush(self.NOTIFICATION_QUEUE, json.dumps(notification_data))

    @RateLimiter(max_calls=10, period=1)  # Giới hạn tốc độ gửi
    async def send_notification(self, session, notification_data):
        """
        Gửi notification qua nhiều kênh (SMS, Email)
        """
        try:
            # Gửi SMS
            if notification_data['phone']:
                await self.send_sms(session, notification_data['phone'], notification_data['message'])
            
            # Gửi Email
            if notification_data['email']:
                await self.send_email(session, notification_data['email'], notification_data['message'])
            
            return True
        except Exception as e:
            # Ghi log lỗi
            print(f"Notification failed: {e}")
            return False

    async def send_sms(self, session, phone, message):
        """
        Mô phỏng việc gửi SMS
        """
        async with session.post('https://sms-provider.com/send', 
                                json={'phone': phone, 'message': message}) as response:
            return response.status == 200

    async def send_email(self, session, email, message):
        """
        Mô phỏng việc gửi Email
        """
        async with session.post('https://email-provider.com/send', 
                                json={'email': email, 'message': message}) as response:
            return response.status == 200

    async def worker(self, worker_id):
        """
        Worker xử lý gửi notification
        """
        async with aiohttp.ClientSession() as session:
            while True:
                # Lấy notification từ queue
                raw_notification = self.redis_client.blpop(self.NOTIFICATION_QUEUE)
                if not raw_notification:
                    await asyncio.sleep(1)
                    continue
                
                notification_data = json.loads(raw_notification[1])
                
                # Thử gửi notification
                success = await self.send_notification(session, notification_data)
                
                if not success:
                    # Nếu gửi thất bại, tăng số lần thử và đưa vào failed queue
                    notification_data['attempts'] += 1
                    if notification_data['attempts'] < 3:
                        self.redis_client.rpush(self.NOTIFICATION_QUEUE, json.dumps(notification_data))
                    else:
                        self.redis_client.rpush(self.FAILED_QUEUE, json.dumps(notification_data))

    async def start_distribution(self):
        """
        Bắt đầu quá trình phân phối notification
        """
        # Tạo worker pool
        workers = [
            asyncio.create_task(self.worker(i)) 
            for i in range(self.max_workers)
        ]
        
        # Chờ cho đến khi tất cả workers hoàn thành
        await asyncio.gather(*workers)

# Sử dụng hệ thống
async def main():
    # Danh sách khách hàng (giả định)
    customers = [
        {'id': i, 'phone': f'0987654{i:03d}', 'email': f'customer{i}@example.com'} 
        for i in range(1_000_000)
    ]

    distributor = NotificationDistributor()
    
    # Chuẩn bị dữ liệu notification
    distributor.prepare_notification_data(
        customers, 
        "Chúc mừng năm mới! Cảm ơn quý khách đã tin dùng dịch vụ của chúng tôi."
    )
    
    # Bắt đầu phân phối
    await distributor.start_distribution()

if __name__ == "__main__":
    asyncio.run(main())