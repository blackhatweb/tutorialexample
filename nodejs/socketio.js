const { io } = require("socket.io-client");

// URL socket.io
const socket = io("wss://bgdatafeed.vps.com.vn", {
  path: "/socket.io",
  transports: ["websocket"], // ép dùng websocket
});

socket.on("connect", () => {
  console.log("✅ Đã kết nối:", socket.id);

  // Gửi message đăng ký
  socket.emit("regs", {
    action: "join",
    list: "VN30FA000"
  });
});

socket.on("message", (msg) => {
  console.log("📩 Nhận:", msg);
});

socket.on("disconnect", () => {
  console.log("❌ Mất kết nối");
});
