import socketio

# Khởi tạo client
sio = socketio.Client()

@sio.event
def connect():
    print("✅ Đã kết nối tới VPS DataFeed")

    # Payload bạn muốn gửi
    payload = [
        "regs",
        "{\"action\":\"leave\",\"list\":\"VN30F2509,41I1FA000,VN30F2512,41I1G3000,GB05F2509,GB05F2512,41B5G3000,GB10F2512,GB10F2509,41BAG3000\"}"
    ]

    # Gửi message tới server
    sio.send(payload)   # gửi qua channel "message"
    print("📡 Đã gửi đăng ký:", payload)

@sio.event
def disconnect():
    print("🔌 Mất kết nối")

@sio.on("message")
def on_message(data):
    print("📩 Nhận dữ liệu:", data)

if __name__ == "__main__":
    url = "https://bgdatafeed.vps.com.vn"
    sio.connect(url, transports=["websocket"])
    sio.wait()
