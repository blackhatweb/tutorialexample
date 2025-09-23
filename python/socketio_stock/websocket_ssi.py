#pip install websocket-client
import websocket
import json

def on_message(ws, message):
    print("Nhận:", message)

def on_error(ws, error):
    print("Lỗi:", error)

def on_close(ws, close_status_code, close_msg):
    print("Đóng kết nối")

def on_open(ws):
    print("Kết nối thành công")
    # Ví dụ: đăng ký 1 mã chứng khoán, tuỳ theo format của SSI
    ws.send(json.dumps({"type": "sub", "topic": "systemStatusChangedV2"}))
    # ws.send(json.dumps('{type: "sub", topic: "notifyIndexRealtimeByListV2", variables: ["VN30"], component: "indexChart"}'))

if __name__ == "__main__":
    ws = websocket.WebSocketApp(
        "wss://iboard-pushstream.ssi.com.vn/realtime",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()
