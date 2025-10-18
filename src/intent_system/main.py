import os
import logging
import uuid
import psycopg2.pool
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from prometheus_fastapi_instrumentator import Instrumentator

# Xử lý import để chạy được cả local và trong container
try:
    from src.common.kafka_client import KafkaProducer
except ImportError:
    import sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
    from src.common.kafka_client import KafkaProducer

# --- Cấu hình logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Cấu hình Database Connection Pool ---
DB_POOL = None

def get_db_pool():
    global DB_POOL
    if DB_POOL is None:
        try:
            DB_POOL = psycopg2.pool.SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                host=os.getenv("POSTGRES_HOST"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                database=os.getenv("POSTGRES_DB")
            )
            logger.info("✅ Database connection pool đã được tạo thành công.")
        except psycopg2.OperationalError as e:
            logger.error(f"❌ Không thể tạo database connection pool: {e}", exc_info=True)
            raise
    return DB_POOL

# --- Vòng đời ứng dụng (Lifespan) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Khởi tạo các kết nối khi ứng dụng khởi động
    app.state.kafka_producer = KafkaProducer()
    app.state.db_pool = get_db_pool()
    logger.info("Kafka Producer và DB Pool đã được khởi tạo.")
    
    yield
    
    # Đóng các kết nối khi ứng dụng tắt
    if app.state.kafka_producer:
        app.state.kafka_producer.close()
        logger.info("Kafka Producer đã được đóng.")
    if app.state.db_pool:
        app.state.db_pool.closeall()
        logger.info("Database connection pool đã được đóng.")

app = FastAPI(
    title="Intent Classification Service with Logging",
    version="1.1.0",
    lifespan=lifespan
)

Instrumentator().instrument(app).expose(app)

# --- Các Pydantic Model ---
class LegalQuery(BaseModel):
    text: str

class ProcessingResponse(BaseModel):
    status: str
    request_id: str

# --- Hàm ghi log request ---
def log_request_to_db(pool, request_id: str, query_text: str):
    conn = None
    try:
        conn = pool.getconn()
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO request_logs (request_id, query_text) VALUES (%s, %s)",
                (request_id, query_text)
            )
            conn.commit()
            logger.info(f"Đã ghi log cho request_id: {request_id}")
    except Exception as e:
        logger.error(f"Lỗi khi ghi log request vào DB: {e}", exc_info=True)
        if conn:
            conn.rollback() # Rollback nếu có lỗi
    finally:
        if conn:
            pool.putconn(conn)

# --- Các API Endpoint ---
@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "intent_system"}

@app.post("/api/v1/intent/classify", response_model=ProcessingResponse)
async def classify_intent(query: LegalQuery, request: Request):
    request_id = str(uuid.uuid4())
    try:
        # 1. Ghi log request vào DB
        log_request_to_db(request.app.state.db_pool, request_id, query.text)
        
        # 2. Phân loại intent (Mock logic)
        text_lower = query.text.lower()
        if any(word in text_lower for word in ["hợp đồng", "contract", "thỏa thuận"]):
            domain = "d1_contract_law"
            confidence = 0.92
        elif any(word in text_lower for word in ["lao động", "labor", "tiền lương"]):
            domain = "d2_labor_law" 
            confidence = 0.88
        else:
            domain = "d10_general_legal"
            confidence = 0.70
        
        # 3. Tạo message để gửi đi
        message = {
            "request_id": request_id,
            "query_text": query.text,
            "domain": domain,
            "confidence": confidence
        }
        
        # 4. Gửi message tới Kafka
        kafka_producer = request.app.state.kafka_producer
        kafka_producer.send_message("intent.classified", message)

        return ProcessingResponse(status="processing", request_id=request_id)
        
    except Exception as e:
        logger.error(f"Intent classification failed for request_id {request_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Classification error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="info")


