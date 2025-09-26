import pandas as pd
import threading
import json
import time
import random
import websocket
from datetime import datetime
from lightweight_charts import Chart
from lightweight_charts.abstract import AbstractChart

# --- Cấu hình Websocket ---
# Thay thế bằng URL Websocket thực tế của sàn giao dịch
WS_URL = "ws://echo.websocket.org" # Dùng một URL mẫu
TICKER_SYMBOL = "BTC/USDT"
CHART: AbstractChart = None # Biến toàn cục để giữ đối tượng Chart

# --- 1. Hàm Xử lý Websocket ---

def on_message(ws, message):
    """Xử lý tin nhắn nhận được từ Websocket."""
    global CHART
    
    # Giả lập xử lý dữ liệu tick thực tế (thường là JSON)
    # Ở đây ta giả lập một tick đơn giản:
    # { "time": <timestamp>, "price": <giá> }
    
    try:
        # Giả sử Websocket trả về JSON, nhưng ở đây ta mô phỏng tick ngẫu nhiên
        # Nếu là dữ liệu thực, bạn sẽ parse JSON và trích xuất thời gian/giá
        
        # Mô phỏng tạo một tick data mới
        current_time = datetime.now()
        # Giả lập giá mới dựa trên giá đóng gần nhất (nếu có)
        last_price = 1000.0 # Giá khởi điểm giả định
        try:
             # Cố gắng lấy giá đóng cuối cùng từ biểu đồ nếu nó đã được thiết lập
            last_bar = CHART.get_data().iloc[-1]
            last_price = last_bar['close']
        except Exception:
            pass # Giữ giá khởi điểm nếu chưa có data

        new_price = last_price + (random.random() - 0.5) * 5 # Thay đổi ngẫu nhiên
        
        # Tạo pd.Series theo định dạng tick yêu cầu: 'time' | 'price'
        new_tick = pd.Series({
            'time': current_time.strftime('%Y-%m-%d %H:%M:%S'),
            'price': new_price,
            # Nếu bạn muốn thêm Volume, định dạng là: 'time' | 'price' | 'volume'
            # 'volume': random.randint(1, 10) 
        })
        
        # Cập nhật biểu đồ bằng tick mới
        if CHART:
            # Phương thức update_from_tick() sẽ tự động tổng hợp tick data thành
            # các thanh (bars) OHLCV dựa trên khung thời gian (timeframe) của biểu đồ.
            CHART.update_from_tick(new_tick) 
            print(f"Tick received & updated: Time={new_tick['time']}, Price={new_tick['price']:.2f}")

    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    """Xử lý lỗi Websocket."""
    print(f"Websocket Error: {error}")

def on_close(ws, close_status_code, close_msg):
    """Xử lý khi kết nối Websocket đóng."""
    print("Websocket closed")

def on_open(ws):
    """Xử lý khi kết nối Websocket mở."""
    print(f"Websocket connected to {WS_URL}")
    
    # Gửi thông điệp đăng ký (subscribe) nếu cần
    # Ví dụ (thay thế bằng payload thực tế):
    # subscribe_msg = json.dumps({"op": "subscribe", "channel": f"trades.{TICKER_SYMBOL}"})
    # ws.send(subscribe_msg)
    # print(f"Sent subscription request: {subscribe_msg}")

    # Bắt đầu luồng gửi tin nhắn giả định để mô phỏng tick data
    def run(*args):
        # Đây là một mô phỏng. Trong thực tế, sàn giao dịch sẽ gửi data.
        # Ta gửi tin nhắn rác để kích hoạt on_message() 
        # (Chỉ hoạt động với echo.websocket.org)
        # Nếu bạn dùng một sàn giao dịch thực, bạn KHÔNG cần vòng lặp này, 
        # data sẽ tự động đến on_message() sau khi đăng ký.
        
        # Vòng lặp mô phỏng việc nhận dữ liệu tick liên tục
        while ws.sock and ws.sock.connected:
            # Gửi một tin nhắn bất kỳ để kích hoạt hàm on_message
            ws.send(f"Simulating tick for {TICKER_SYMBOL}") 
            time.sleep(0.5) # Độ trễ giữa các tick (0.5 giây)
        
    threading.Thread(target=run).start()

def connect_websocket():
    """Thiết lập và chạy kết nối Websocket."""
    # Tạo một đối tượng Websocket App
    ws_app = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    # Chạy vòng lặp chính của Websocket trong một luồng riêng
    # run_forever() là blocking, nên cần luồng riêng để UI vẫn chạy
    ws_app.run_forever()

# --- 2. Khởi tạo Biểu đồ và Dữ liệu Khởi tạo ---

def generate_initial_data(num_bars=50):
    """Tạo dữ liệu OHLCV khởi tạo (giả định) cho biểu đồ."""
    data = []
    start_time = datetime(2025, 1, 1, 9, 0)
    open_price = 1000.0
    
    for i in range(num_bars):
        current_time = start_time + pd.Timedelta(minutes=5 * (i+1)) # Giả lập 5 phút
        change = (random.random() - 0.5) * 10
        close_price = open_price + change
        high = max(open_price, close_price) + random.random() * 5
        low = min(open_price, close_price) - random.random() * 5
        volume = random.randint(100, 500)
        
        data.append({
            'time': current_time.strftime('%Y-%m-%d %H:%M:%S'),
            'open': open_price,
            'high': high,
            'low': low,
            'close': close_price,
            'volume': volume
        })
        open_price = close_price # Cập nhật giá mở cho thanh tiếp theo
        
    df = pd.DataFrame(data)
    return df

# --- 3. Hàm Chính ---

if __name__ == '__main__':
    # 1. Tạo biểu đồ
    CHART = Chart()
    CHART.layout(background_color='#131722', text_color='#d1d4dc')
    CHART.set_title(f'Real-Time Tick Update for {TICKER_SYMBOL}')

    # 2. Chuẩn bị dữ liệu khởi tạo và thiết lập
    initial_data = generate_initial_data()
    CHART.set(initial_data)

    # 3. Chạy Websocket trong một luồng riêng
    ws_thread = threading.Thread(target=connect_websocket)
    ws_thread.daemon = True # Cho phép luồng kết thúc khi chương trình chính kết thúc
    ws_thread.start()
    
    print("Starting Lightweight Charts UI...")

    # 4. Hiển thị biểu đồ (blocking call - chặn cho đến khi cửa sổ đóng)
    CHART.show(block=True) 

    # 5. Dọn dẹp (nếu cần)
    print("Chart closed. Program exiting.")