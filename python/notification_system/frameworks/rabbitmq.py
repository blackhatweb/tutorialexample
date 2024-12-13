import pika
import json
from interfaces.message_queue import MessageQueue

class RabbitMQ(MessageQueue):
    def __init__(self, host='localhost'): # Có thể cấu hình host, port, credentials
        self.host = host
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='notifications') # Khai báo queue

    def publish(self, message):
      try:
        self.channel.basic_publish(exchange='', routing_key='notifications', body=json.dumps(message.__dict__))
        print(f" [x] Sent {message}")
      except pika.exceptions.AMQPConnectionError as e:
        print(f"Error connecting to RabbitMQ: {e}")
        raise

    def close(self): # Đảm bảo đóng kết nối khi hoàn tất
        self.connection.close()