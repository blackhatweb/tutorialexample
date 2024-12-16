CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(), -- ID duy nhất của job
    name VARCHAR(255) NOT NULL,               -- Tên job (ví dụ: "send_daily_notifications")
    status VARCHAR(50) NOT NULL DEFAULT 'pending', -- Trạng thái (pending, running, stopped, completed, failed)
    schedule VARCHAR(255),                      -- Lịch trình (Cron expression)
    last_run TIMESTAMP WITH TIME ZONE,          -- Thời điểm chạy gần nhất
    next_run TIMESTAMP WITH TIME ZONE,           -- Thời điểm chạy tiếp theo (có thể null nếu job đã dừng)
    message TEXT,                                -- Nội dung tin nhắn
    target_audience TEXT,                       -- Đối tượng nhận tin (ví dụ: "all", hoặc một query SQL)
    notification_type VARCHAR(50),               -- Loại thông báo (push, email, sms...)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);