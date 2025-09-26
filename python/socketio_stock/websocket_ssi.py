# # #pip install websocket-client
# # import websocket
# # import json

# # def on_message(ws, message):
# #     print("Nhận:", message)

# # def on_error(ws, error):
# #     print("Lỗi:", error)

# # def on_close(ws, close_status_code, close_msg):
# #     print("Đóng kết nối")

# # def on_open(ws):
# #     print("Kết nối thành công")
# #     # Ví dụ: đăng ký 1 mã chứng khoán, tuỳ theo format của SSI
# #     ws.send(json.dumps({"type": "sub", "topic": "systemStatusChangedV2"}))
# #     ws.send(json.dumps({"type":"init"}))
# #     ws.send(json.dumps({"type":"sub","topic":"stockRealtimeByListV2","variables":["41I1FA000"],"component":"SSI-TradingView-Chart"}))
# #     # ws.send(json.dumps('{type: "sub", topic: "notifyIndexRealtimeByListV2", variables: ["VN30"], component: "indexChart"}'))

# # if __name__ == "__main__":
# #     ws = websocket.WebSocketApp(
# #         "wss://iboard-pushstream.ssi.com.vn/realtime",
# #         on_open=on_open,
# #         on_message=on_message,
# #         on_error=on_error,
# #         on_close=on_close,
# #     )
# #     ws.run_forever()


# import tkinter as tk
# import websocket
# import threading
# import time
# import json

# # Biến toàn cục để lưu trữ tin nhắn mới nhất
# latest_message = "Đang chờ tin nhắn từ WebSocket..."

# def on_message(ws, message):
#     """
#     Xử lý khi nhận được tin nhắn. Cập nhật trực tiếp biến toàn cục.
#     """
#     global latest_message
#     print(f"Nhận được tin nhắn: {message}")
#     latest_message = message

# def on_error(ws, error):
#     print(f"Lỗi WebSocket: {error}")

# def on_close(ws, close_status_code, close_msg):
#     print("Kết nối WebSocket đã đóng")

# def on_open(ws):
#     print("Kết nối WebSocket đã mở")
#     ws.send(json.dumps({"type":"init"}))
#     ws.send(json.dumps({"type":"sub","topic":"stockRealtimeByListV2","variables":["41I1FA000"],"component":"SSI-TradingView-Chart"}))

# def run_websocket_client(url):
#     """Hàm chạy client WebSocket trong một luồng riêng biệt."""
#     ws = websocket.WebSocketApp(
#         url,
#         on_open=on_open,
#         on_message=on_message,
#         on_error=on_error,
#         on_close=on_close
#     )
#     ws.run_forever()

# def check_for_updates(root, label_text):
#     """
#     Hàm này kiểm tra giá trị của biến toàn cục và cập nhật label.
#     """
#     global latest_message
#     if label_text.get() != latest_message:
#         # Chỉ cập nhật khi nội dung thay đổi để tránh lặp lại không cần thiết
#         label_text.set(f"Tin nhắn mới:\n{latest_message}")
    
#     # Lên lịch để hàm này chạy lại sau 100ms
#     root.after(100, check_for_updates, root, label_text)

# def main():
#     root = tk.Tk()
#     root.title("Cửa sổ Real-time Update")
#     root.geometry("400x200")
    
#     # Đặt thuộc tính "luôn hiển thị trên cùng"
#     root.attributes('-topmost', True)

#     label_text = tk.StringVar()
#     label_text.set("Đang chờ tin nhắn từ WebSocket...")

#     label = tk.Label(root, textvariable=label_text, font=("Arial", 12))
#     label.pack(expand=True, padx=20, pady=20)

#     websocket_url = "wss://iboard-pushstream.ssi.com.vn/realtime"
    
#     # Bắt đầu luồng WebSocket riêng biệt
#     websocket_thread = threading.Thread(
#         target=run_websocket_client, 
#         args=(websocket_url,),
#         daemon=True
#     )
#     websocket_thread.start()

#     # Bắt đầu quá trình kiểm tra cập nhật
#     check_for_updates(root, label_text)

#     # Chạy vòng lặp chính của Tkinter
#     root.mainloop()

# if __name__ == "__main__":
#     main()


# pip install lightweight-charts websocket-client pandas
import json
import threading
import time
from lightweight_charts import Chart
import websocket

# --- Cấu hình WebSocket và Xử lý Dữ liệu ---
# Đổi YOUR_WEBSOCKET_URL thành URL thực tế của sàn giao dịch
WS_URL = "wss://echo.websocket.org" # URL mô phỏng

def on_message(ws, message, chart):
    """
    Hàm xử lý khi nhận được message từ WebSocket
    """
    try:
        # Giả sử message là JSON string và chứa dữ liệu tick
        # Định dạng này chỉ là mô phỏng, cần thay đổi theo API thực tế
        data = json.loads(message)

        # Mô phỏng dữ liệu tick mới (thời gian là Unix timestamp)
        new_tick = {
            'time': data.get('time', int(time.time())),
            'open': data.get('o'),
            'high': data.get('h'),
            'low': data.get('l'),
            'close': data.get('c'),
            'volume': data.get('v', 100)
        }

        # *************** ĐIỂM QUAN TRỌNG ***************
        # Gọi update_from_tick() để cập nhật nến hiện tại hoặc bắt đầu nến mới
        chart.update_from_tick(new_tick)
        print(f"Biểu đồ đã được cập nhật với tick: {new_tick['close']}")

    except Exception as e:
        # Thường xảy ra nếu message không phải là dữ liệu giá
        # print(f"Lỗi xử lý message: {e}")
        pass

def on_open(ws):
    """
    Hàm được gọi khi kết nối WebSocket mở
    """
    print("Kết nối WebSocket đã mở. Bắt đầu gửi lệnh đăng ký...")
    # *************** ĐIỂM QUAN TRỌNG: Lệnh Đăng ký ***************
    # Gửi lệnh đăng ký (subscribe) để nhận dữ liệu giá
    # Lệnh này tùy thuộc vào API của sàn (ví dụ: Binance, Bybit)
    # ws.send(json.dumps({"op": "subscribe", "channel": "market.btcusdt.kline.1min"}))


def run_websocket(chart):
    """
    Chạy WebSocket Client trong một luồng riêng
    """
    # Tạo một class instance để truyền đối tượng chart
    class WebSocketApp(websocket.WebSocketApp):
        def on_message(self, ws, message):
            on_message(ws, message, chart)
        def on_open(self, ws):
            on_open(ws)
        def on_error(self, ws, error):
            print(f"Lỗi WebSocket: {error}")
        def on_close(self, ws, close_status_code, close_msg):
            print("Kết nối WebSocket đã đóng")

    ws_app = WebSocketApp(WS_URL)
    ws_app.run_forever()


# --- Khởi tạo Biểu đồ và Luồng ---

if __name__ == '__main__':
    # 1. Khởi tạo biểu đồ
    chart = Chart(toolbox=True)
    
    # 2. Tạo dữ liệu ban đầu (Rất quan trọng! Biểu đồ cần dữ liệu khởi tạo)
    # Nếu không có dữ liệu ban đầu, update_from_tick() có thể không hoạt động.
    initial_data = [{'time': int(time.time()), 'open': 100, 'high': 101, 'low': 99, 'close': 100, 'volume': 1000}]
    chart.set(initial_data)

    # 3. Chạy WebSocket trong một luồng độc lập
    # Bắt đầu luồng, truyền đối tượng chart vào
    ws_thread = threading.Thread(target=run_websocket, args=(chart,))
    ws_thread.daemon = True  # Đảm bảo luồng kết thúc khi chương trình chính kết thúc
    ws_thread.start()

    # 4. Hiển thị biểu đồ (Đây là hàm blocking)
    chart.show(block=True)