import os
import logging
from typing import List, Optional
from uuid import UUID
from datetime import datetime

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import psycopg2
from psycopg2.pool import SimpleConnectionPool

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Cấu hình Database ---
DB_HOST = os.getenv("POSTGRES_HOST", "postgres")
DB_NAME = os.getenv("POSTGRES_DB", "legal_rag")
DB_USER = os.getenv("POSTGRES_USER", "legalai")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "legalai123")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Khởi tạo Connection Pool ---
# Tạo một "bể" kết nối để tái sử dụng, giúp tăng hiệu suất.
try:
    db_pool = SimpleConnectionPool(minconn=1, maxconn=10, dsn=DATABASE_URL)
    logger.info("✅ Database connection pool created successfully.")
except psycopg2.OperationalError as e:
    logger.error(f"❌ Could not connect to PostgreSQL database: {e}")
    db_pool = None

# --- Khởi tạo FastAPI App ---
app = FastAPI(
    title="Feedback Service",
    version="1.1.0",
    description="Dịch vụ thu thập và truy xuất phản hồi của người dùng."
)

# --- Pydantic Models ---
class FeedbackIn(BaseModel):
    request_id: UUID
    rating: bool = Field(..., description="True cho 'hữu ích', False cho 'không chính xác'")
    comment: Optional[str] = Field(None, description="Bình luận chi tiết của người dùng")

class FeedbackOut(BaseModel):
    id: UUID
    request_id: UUID
    rating: bool
    comment: Optional[str]
    created_at: datetime

    class Config:
        orm_mode = True

# --- API Endpoints ---
@app.get("/health")
async def health_check():
    """Kiểm tra sức khỏe của dịch vụ."""
    return {"status": "healthy", "service": "feedback_service"}

@app.post("/api/v1/feedback", status_code=201)
async def submit_feedback(feedback: FeedbackIn):
    """
    Endpoint để nhận phản hồi từ người dùng và lưu vào database.
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database connection is not available.")

    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO feedback (request_id, rating, comment)
                VALUES (%s, %s, %s)
                """,
                (str(feedback.request_id), feedback.rating, feedback.comment)
            )
            conn.commit()
        logger.info(f"📝 New feedback received and saved for request_id: {feedback.request_id}")
        return {"status": "success", "message": "Feedback submitted successfully."}
    except Exception as e:
        logger.error(f"Error while saving feedback: {e}")
        raise HTTPException(status_code=500, detail="Could not save feedback to the database.")
    finally:
        if conn:
            db_pool.putconn(conn)

@app.get("/api/v1/feedback", response_model=List[FeedbackOut])
async def get_all_feedback():
    """
    Endpoint để truy xuất toàn bộ dữ liệu phản hồi đã thu thập.
    Sắp xếp theo thời gian tạo mới nhất trước.
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database connection is not available.")
    
    conn = None
    try:
        conn = db_pool.getconn()
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, request_id, rating, comment, created_at FROM feedback ORDER BY created_at DESC"
            )
            records = cursor.fetchall()
            
            # Chuyển đổi tuple từ database thành Pydantic model
            feedback_list = [
                FeedbackOut(
                    id=row[0],
                    request_id=row[1],
                    rating=row[2],
                    comment=row[3],
                    created_at=row[4]
                ) for row in records
            ]
        logger.info(f"📊 Retrieved {len(feedback_list)} feedback records.")
        return feedback_list
    except Exception as e:
        logger.error(f"Error while fetching feedback: {e}")
        raise HTTPException(status_code=500, detail="Could not fetch feedback from the database.")
    finally:
        if conn:
            db_pool.putconn(conn)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


