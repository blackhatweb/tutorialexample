import socketio

# Khởi tạo một đối tượng Socket.IO client
sio = socketio.Client()

# Xử lý sự kiện 'connect'
@sio.event
def connect():
    print('Connection established')
    # Gửi một sự kiện tới server ngay sau khi kết nối
    sio.emit('my_event', {'data': 'Hello from Python!'})

# Xử lý sự kiện 'disconnect'
@sio.event
def disconnect():
    print('Disconnected from server')

# Xử lý sự kiện tùy chỉnh từ server
@sio.event
def my_response(data):
    print('Received response:', data)
    # Tắt kết nối sau khi nhận phản hồi
    sio.disconnect()

# Kết nối tới máy chủ Node.js
try:
    sio.connect('wss://bgdatafeed.vps.com.vn/socket.io/?EIO=3&transport=websocket')
    # Giữ cho client chạy để lắng nghe sự kiện
    sio.wait()
except Exception as e:
    print(f"Failed to connect: {e}")