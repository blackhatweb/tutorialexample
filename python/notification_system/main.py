import json
from use_cases.send_notification import SendNotificationUseCase
from frameworks.rabbitmq import RabbitMQ
from interfaces.notification_presenter import present_response

def main():
    message_queue = RabbitMQ()
    send_notification_use_case = SendNotificationUseCase(message_queue)

    notification_data = {
        "user_id": "user123",
        "message": "Hello from notification system!",
        "notification_type": "push"
    }

    response = send_notification_use_case.execute(notification_data)
    present_response(response)

    notification_data_invalid = {
        "user_id": "",
        "message": "",
        "notification_type": "push"
    }

    response_invalid = send_notification_use_case.execute(notification_data_invalid)
    present_response(response_invalid)
    message_queue.close()

if __name__ == "__main__":
    main()