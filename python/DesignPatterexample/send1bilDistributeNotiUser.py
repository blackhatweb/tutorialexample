import os
import time
import json
import logging
from typing import List, Dict
import redis
import rq
from rq import Queue
from rq.job import Job
from redis import Redis
from concurrent.futures import ThreadPoolExecutor, as_completed

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NotificationService:
    def __init__(self, 
                 redis_host='localhost', 
                 redis_port=6379, 
                 max_workers=10,
                 batch_size=10000):
        """
        Khởi tạo dịch vụ gửi thông báo phân tán
        
        :param redis_host: Địa chỉ host Redis
        :param redis_port: Cổng Redis
        :param max_workers: Số lượng worker đồng thời
        :param batch_size: Số lượng thông báo xử lý trong mỗi batch
        """
        self.redis_conn = Redis(host=redis_host, port=redis_port)
        self.notification_queue = Queue(connection=self.redis_conn)
        self.max_workers = max_workers
        self.batch_size = batch_size

    def send_notification(self, customers: List[Dict], message: str):
        """
        Gửi thông báo cho danh sách khách hàng
        
        :param customers: Danh sách khách hàng
        :param message: Nội dung thông báo
        """
        #Chia danh sách khách hàng thành các batch
        customer_batches = [
            customers[i:i + self.batch_size] 
            for i in range(0, len(customers), self.batch_size)
        ]

        total_jobs = []
        for batch in customer_batches:
            # Tạo job cho mỗi batch
            job = self.notification_queue.enqueue(
                self._process_notification_batch, 
                args=(batch, message),
                retry=rq.Retry(max=3, interval=[10, 30, 60])
            )
            total_jobs.append(job)

        # Theo dõi trạng thái job
        self._monitor_job_status(total_jobs)

    def _process_notification_batch(self, customers: List[Dict], message: str):
        """
        Xử lý gửi thông báo cho từng batch
        
        :param customers: Batch khách hàng
        :param message: Nội dung thông báo
        """
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(self._send_individual_notification, customer, message): customer 
                for customer in customers
            }

            for future in as_completed(futures):
                customer = futures[future]
                try:
                    future.result()
                    logger.info(f"Gửi thông báo thành công cho {customer['id']}")
                except Exception as e:
                    logger.error(f"Lỗi gửi thông báo cho {customer['id']}: {e}")

    def _send_individual_notification(self, customer: Dict, message: str):
        """
        Gửi thông báo cho từng khách hàng
        
        :param customer: Thông tin khách hàng
        :param message: Nội dung thông báo
        """
        # Mô phỏng việc gửi thông báo (thay thế bằng service thực tế)
        try:
            # Ví dụ: gửi email, SMS, push notification
            # send_email(customer['email'], message)
            # send_sms(customer['phone'], message)
            time.sleep(0.1)  # Mô phỏng độ trễ
        except Exception as e:
            logger.error(f"Gửi thông báo thất bại: {e}")
            raise

    def _monitor_job_status(self, jobs: List[Job]):
        """
        Theo dõi trạng thái các job
        
        :param jobs: Danh sách job
        """
        completed_jobs = 0
        failed_jobs = 0

        for job in jobs:
            job.refresh()
            if job.is_finished:
                completed_jobs += 1
            elif job.is_failed:
                failed_jobs += 1

        logger.info(f"Tổng số job: {len(jobs)}")
        logger.info(f"Job hoàn thành: {completed_jobs}")
        logger.info(f"Job thất bại: {failed_jobs}")

def generate_sample_customers(num_customers: int) -> List[Dict]:
    """
    Sinh dữ liệu mẫu khách hàng
    
    :param num_customers: Số lượng khách hàng
    :return: Danh sách khách hàng
    """
    return [
        {
            'id': f'customer_{i}',
            'email': f'customer{i}@example.com',
            'phone': f'+84{9000000 + i}'
        } for i in range(1_000_000)
    ]

def main():
    # Khởi tạo dịch vụ
    notification_service = NotificationService(
        redis_host='localhost', 
        redis_port=6379,
        max_workers=20,
        batch_size=10000
    )

    # Sinh dữ liệu mẫu
    customers = generate_sample_customers(1_000_000)

    # Gửi thông báo
    notification_service.send_notification(
        customers, 
        "Thông báo quan trọng: Chương trình khuyến mãi mới!"
    )

if __name__ == '__main__':
    main()