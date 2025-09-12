import paho.mqtt.client as mqtt

# MQTT broker qua WebSocket
BROKER = "price-streaming-free.vndirect.com.vn"
PORT = 443  # wss dùng port 443
TOPIC = "DVX/VN30F2509"   # ví dụ subscribe giá VNM

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("✅ Kết nối MQTT thành công")
        # subscribe topic
        client.subscribe(TOPIC)
        print("📡 Subscribed:", TOPIC)
    else:
        print("❌ Lỗi kết nối, mã lỗi:", rc)

def on_message(client, userdata, msg):
    print(f"📩 Nhận dữ liệu từ {msg.topic}: {msg.payload}")

# Khởi tạo client MQTT với WebSocket
client = mqtt.Client(transport="websockets")

# Nếu broker yêu cầu SSL
client.tls_set()

# Đăng ký callback
client.on_connect = on_connect
client.on_message = on_message

# Kết nối
client.connect(BROKER, PORT, 60)

# Loop lắng nghe
client.loop_forever()
