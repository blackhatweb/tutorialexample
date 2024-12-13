import pika
import json

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='notifications')

def callback(ch, method, properties, body):
    notification = json.loads(body) # Giải mã JSON
    print(f" [x] Received {notification}")
    # Gửi thông báo thực sự bằng FCM/APNs ở đây
    print(f"Sending notification to user {notification['user_id']}: {notification['message']}")

channel.basic_consume(queue='notifications', on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()