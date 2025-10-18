-- Kích hoạt extension pgcrypto để sử dụng gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Tạo bảng để lưu trữ phản hồi của người dùng
CREATE TABLE IF NOT EXISTS feedback (
    -- ID duy nhất cho mỗi feedback, tự động tạo
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- ID của request ban đầu mà người dùng đang phản hồi
    request_id UUID NOT NULL,

    -- Đánh giá của người dùng: true = hữu ích, false = không chính xác
    rating BOOLEAN NOT NULL,

    -- Bình luận chi tiết từ người dùng (tùy chọn)
    comment TEXT,

    -- Thời gian feedback được tạo, tự động điền
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tạo index trên request_id để tăng tốc độ truy vấn
CREATE INDEX IF NOT EXISTS idx_feedback_request_id ON feedback(request_id);

