import os
import uuid
import time
import json
import logging
from typing import List, Dict, Optional
import redis
import rq
from rq import Queue, Worker
from rq.job import Job
from redis import Redis
from flask import Flask, request, jsonify
from rq.registry import StartedJobRegistry, FinishedJobRegistry, FailedJobRegistry
from concurrent.futures import ThreadPoolExecutor

# Cấu hình logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class NotificationJobController:
    def __init__(self, 
                 redis_host='localhost', 
                 redis_port=6379, 
                 queue_name='notification_queue'):
        """
        Khởi tạo bộ điều khiển job gửi thông báo
        
        :param redis_host: Địa chỉ host Redis
        :param redis_port: Cổng Redis 
        :param queue_name: Tên queue
        """
        self.redis_conn = Redis(host=redis_host, port=redis_port)
        self.queue = Queue(queue_name, connection=self.redis_conn)
        
        # Các registry để theo dõi trạng thái job
        self.started_registry = StartedJobRegistry(queue_name, self.redis_conn)
        self.finished_registry = FinishedJobRegistry(queue_name, self.redis_conn)
        self.failed_registry = FailedJobRegistry(queue_name, self.redis_conn)
        
        # Lưu trữ job hiện tại
        self.current_job_id = None
        
    def start_notification_job(self, 
                                customers: List[Dict], 
                                message: str, 
                                batch_size: int = 10000,
                                max_workers: int = 20) -> str:
        """
        Bắt đầu job gửi thông báo
        
        :param customers: Danh sách khách hàng
        :param message: Nội dung thông báo
        :param batch_size: Số lượng khách hàng mỗi batch
        :param max_workers: Số worker tối đa
        :return: Job ID
        """
        # Tạo job ID duy nhất
        job_id = str(uuid.uuid4())
        
        # Chia danh sách khách hàng thành các batch
        customer_batches = [
            customers[i:i + batch_size] 
            for i in range(0, len(customers), batch_size)
        ]
        
        # Enqueue job chính
        job = self.queue.enqueue(
            self._process_notification_job, 
            args=(customer_batches, message, max_workers, job_id),
            job_id=job_id,
            retry=rq.Retry(max=3, interval=[10, 30, 60])
        )
        
        # Lưu lại job ID hiện tại
        self.current_job_id = job_id
        
        return job_id
    
    def _process_notification_job(self, 
                                   customer_batches: List[List[Dict]], 
                                   message: str, 
                                   max_workers: int,
                                   job_id: str):
        """
        Xử lý job gửi thông báo
        
        :param customer_batches: Các batch khách hàng
        :param message: Nội dung thông báo
        :param max_workers: Số worker tối đa
        :param job_id: ID của job
        """
        total_batches = len(customer_batches)
        processed_batches = 0
        
        for batch in customer_batches:
            # Kiểm tra xem job có bị dừng không
            if self._is_job_stopped(job_id):
                logger.warning(f"Job {job_id} đã bị dừng.")
                break
            
            # Xử lý batch
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(self._send_individual_notification, customer, message): customer 
                    for customer in batch
                }
                
                for future in futures:
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Lỗi gửi thông báo: {e}")
            
            processed_batches += 1
            logger.info(f"Đã xử lý batch {processed_batches}/{total_batches}")
    
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
    
    def stop_job(self, job_id: Optional[str] = None):
        """
        Dừng job đang chạy
        
        :param job_id: ID của job (nếu không truyền sẽ dùng job hiện tại)
        """
        job_id = job_id or self.current_job_id
        if not job_id:
            raise ValueError("Không có job nào đang chạy")
        
        # Đánh dấu job bị dừng trong Redis
        stop_key = f"job_stopped:{job_id}"
        self.redis_conn.set(stop_key, "true")
        
        # Huỷ job nếu còn đang chạy
        job = Job.fetch(job_id, connection=self.redis_conn)
        if job and job.is_started:
            job.cancel()
    
    def _is_job_stopped(self, job_id: str) -> bool:
        """
        Kiểm tra xem job có bị dừng không
        
        :param job_id: ID của job
        :return: Trạng thái dừng job
        """
        stop_key = f"job_stopped:{job_id}"
        return self.redis_conn.get(stop_key) == b"true"
    
    def get_job_status(self, job_id: Optional[str] = None) -> Dict:
        """
        Lấy trạng thái của job
        
        :param job_id: ID của job (nếu không truyền sẽ dùng job hiện tại)
        :return: Thông tin trạng thái job
        """
        job_id = job_id or self.current_job_id
        if not job_id:
            raise ValueError("Không có job nào")
        
        job = Job.fetch(job_id, connection=self.redis_conn)
        
        return {
            "job_id": job_id,
            "status": job.get_status(),
            "started_at": job.started_at.isoformat() if job.started_at else None,
            "ended_at": job.ended_at.isoformat() if job.ended_at else None,
            "is_stopped": self._is_job_stopped(job_id),
            "started_jobs_count": len(self.started_registry.get_job_ids()),
            "finished_jobs_count": len(self.finished_registry.get_job_ids()),
            "failed_jobs_count": len(self.failed_registry.get_job_ids())
        }

# Flask API để điều khiển job
app = Flask(__name__)
job_controller = NotificationJobController()

@app.route('/notifications/start', methods=['GET'])
def start_notification_job():
    """API endpoint để bắt đầu job gửi thông báo"""
    # data = request.json
    data = {}
    
    # Sinh dữ liệu mẫu khách hàng (thay thế bằng dữ liệu thực tế)
    customers = [
        {
            'id': f'customer_{i}',
            'email': f'customer{i}@example.com',
            'phone': f'+84{9000000 + i}'
        } for i in range(1_000_000)
    ]
    
    message = data.get('message', 'Thông báo mặc định')
    batch_size = data.get('batch_size', 10000)
    max_workers = data.get('max_workers', 20)
    
    try:
        job_id = job_controller.start_notification_job(
            customers, 
            message, 
            batch_size, 
            max_workers
        )
        return jsonify({"job_id": job_id, "status": "started"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/notifications/stop', methods=['GET'])
def stop_notification_job():
    """API endpoint để dừng job gửi thông báo"""
    # data = request.json
    data = {}
    job_id = data.get('job_id')
    
    try:
        job_controller.stop_job(job_id)
        return jsonify({"status": "stopped"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/notifications/status', methods=['GET'])
def get_job_status():
    """API endpoint để lấy trạng thái job"""
    job_id = request.args.get('job_id')
    
    try:
        status = job_controller.get_job_status(job_id)
        return jsonify(status), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

def main():
    app.run(host='0.0.0.0', port=5000, debug=True)

if __name__ == '__main__':
    main()