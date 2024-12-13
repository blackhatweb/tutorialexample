from entities.notification import Notification
from interfaces.message_queue import MessageQueue

class SendNotificationUseCase:
    def __init__(self, message_queue: MessageQueue):
        self.message_queue = message_queue

    def execute(self, notification_data: dict):
        try:
            notification = Notification(**notification_data)
            notification.validate()
        except ValueError as e:
            return {"status": "error", "message": str(e)}

        try:
            self.message_queue.publish(notification)
            return {"status": "success", "message": "Notification queued"}
        except Exception as e:  # Bắt lỗi kết nối RabbitMQ
            return {"status": "error", "message": f"Error publishing message: {e}"}