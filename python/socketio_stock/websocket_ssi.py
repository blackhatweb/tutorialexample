# #pip install websocket-client
# import websocket
# import json

# def on_message(ws, message):
#     print("Nhận:", message)

# def on_error(ws, error):
#     print("Lỗi:", error)

# def on_close(ws, close_status_code, close_msg):
#     print("Đóng kết nối")

# def on_open(ws):
#     print("Kết nối thành công")
#     # Ví dụ: đăng ký 1 mã chứng khoán, tuỳ theo format của SSI
#     ws.send(json.dumps({"type": "sub", "topic": "systemStatusChangedV2"}))
#     ws.send(json.dumps({"type":"init"}))
#     ws.send(json.dumps({"type":"sub","topic":"stockRealtimeByListV2","variables":["41I1FA000"],"component":"SSI-TradingView-Chart"}))
#     # ws.send(json.dumps('{type: "sub", topic: "notifyIndexRealtimeByListV2", variables: ["VN30"], component: "indexChart"}'))

# if __name__ == "__main__":
#     ws = websocket.WebSocketApp(
#         "wss://iboard-pushstream.ssi.com.vn/realtime",
#         on_open=on_open,
#         on_message=on_message,
#         on_error=on_error,
#         on_close=on_close,
#     )
#     ws.run_forever()


import tkinter as tk
import websocket
import threading
import time
import json

# Biến toàn cục để lưu trữ tin nhắn mới nhất
latest_message = "Đang chờ tin nhắn từ WebSocket..."

def on_message(ws, message):
    """
    Xử lý khi nhận được tin nhắn. Cập nhật trực tiếp biến toàn cục.
    """
    global latest_message
    print(f"Nhận được tin nhắn: {message}")
    latest_message = message

def on_error(ws, error):
    print(f"Lỗi WebSocket: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Kết nối WebSocket đã đóng")

def on_open(ws):
    print("Kết nối WebSocket đã mở")
    ws.send(json.dumps({"type":"init"}))
    ws.send(json.dumps({"type":"sub","topic":"stockRealtimeByListV2","variables":["41I1FA000"],"component":"SSI-TradingView-Chart"}))

def run_websocket_client(url):
    """Hàm chạy client WebSocket trong một luồng riêng biệt."""
    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()

def check_for_updates(root, label_text):
    """
    Hàm này kiểm tra giá trị của biến toàn cục và cập nhật label.
    """
    global latest_message
    if label_text.get() != latest_message:
        # Chỉ cập nhật khi nội dung thay đổi để tránh lặp lại không cần thiết
        label_text.set(f"Tin nhắn mới:\n{latest_message}")
    
    # Lên lịch để hàm này chạy lại sau 100ms
    root.after(100, check_for_updates, root, label_text)

def main():
    root = tk.Tk()
    root.title("Cửa sổ Real-time Update")
    root.geometry("400x200")
    
    # Đặt thuộc tính "luôn hiển thị trên cùng"
    root.attributes('-topmost', True)

    label_text = tk.StringVar()
    label_text.set("Đang chờ tin nhắn từ WebSocket...")

    label = tk.Label(root, textvariable=label_text, font=("Arial", 12))
    label.pack(expand=True, padx=20, pady=20)

    websocket_url = "wss://iboard-pushstream.ssi.com.vn/realtime"
    
    # Bắt đầu luồng WebSocket riêng biệt
    websocket_thread = threading.Thread(
        target=run_websocket_client, 
        args=(websocket_url,),
        daemon=True
    )
    websocket_thread.start()

    # Bắt đầu quá trình kiểm tra cập nhật
    check_for_updates(root, label_text)

    # Chạy vòng lặp chính của Tkinter
    root.mainloop()

if __name__ == "__main__":
    main()