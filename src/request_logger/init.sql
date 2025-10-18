-- Kích hoạt extension để có thể tạo UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Tạo bảng để lưu lại mapping giữa request_id và query_text
-- Bảng này rất quan trọng cho việc huấn luyện lại (fine-tuning) mô hình sau này
CREATE TABLE IF NOT EXISTS request_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    request_id UUID NOT NULL,
    query_text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tạo index trên cột request_id để tăng tốc độ truy vấn
CREATE INDEX IF NOT EXISTS idx_request_logs_request_id ON request_logs(request_id);


