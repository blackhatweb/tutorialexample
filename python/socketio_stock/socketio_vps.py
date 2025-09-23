import socketio

sio = socketio.Client()

@sio.event
def connect():
    print("✅ Kết nối thành công")
    # Gửi sự kiện regs với payload
    sio.emit("regs", {
        "action": "leave",
        "list": "41I1FA000,VN30F2512,41I1G3000"
    })

@sio.on("message")
def on_message(data):
    print("📩 Nhận:", data)

sio.connect("wss://bgdatafeed.vps.com.vn/socket.io/?EIO=4&transport=websocket")
sio.wait()
