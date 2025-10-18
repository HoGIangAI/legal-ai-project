import os
import logging
import json
import psycopg2
from psycopg2.extras import RealDictCursor
from pgvector.psycopg2 import register_vector
from sentence_transformers import SentenceTransformer
import openai

# Xử lý import để chạy được cả local và trong container
try:
    from src.common.kafka_client import KafkaConsumer, KafkaProducer
except ImportError:
    import sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
    from src.common.kafka_client import KafkaConsumer, KafkaProducer

# --- Cấu hình logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Khởi tạo các tài nguyên dùng chung ---
# Tải model embedding một lần duy nhất khi worker khởi động
logger.info("Đang tải model embedding 'all-MiniLM-L6-v2'...")
embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
logger.info("Tải model embedding thành công.")

# Cấu hình OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")
if not openai.api_key:
    logger.warning("OPENAI_API_KEY chưa được thiết lập. Các lời gọi LLM sẽ thất bại.")

# Khởi tạo Kafka Producer để gửi kết quả đi
kafka_producer = KafkaProducer()

# --- Hàm xử lý chính ---

def get_db_connection():
    """Tạo kết nối tới database PostgreSQL."""
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB", "legal_rag"),
        user=os.getenv("POSTGRES_USER", "legalai"),
        password=os.getenv("POSTGRES_PASSWORD", "legalai123"),
        port=os.getenv("POSTGRES_PORT", "5432")
    )
    register_vector(conn)
    return conn

def find_relevant_chunks(query_text: str, top_k: int = 5):
    """Vector hóa câu hỏi và tìm kiếm các chunk liên quan trong database."""
    logger.info(f"Đang tìm {top_k} chunk liên quan cho câu hỏi: '{query_text[:50]}...'")
    query_embedding = embedding_model.encode(query_text)
    
    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Sử dụng toán tử <=> (cosine distance) của pgvector để tìm kiếm
            cur.execute(
                "SELECT id, content, metadata FROM legal_documents ORDER BY embedding <=> %s LIMIT %s",
                (query_embedding, top_k)
            )
            results = cur.fetchall()
            logger.info(f"Tìm thấy {len(results)} chunk liên quan.")
            return results
    except Exception as e:
        logger.error(f"Lỗi khi tìm kiếm trong database: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()

def generate_answer_with_llm(query_text: str, context_chunks: list):
    """Sử dụng LLM (OpenAI) để sinh câu trả lời dựa trên ngữ cảnh."""
    if not openai.api_key:
        logger.error("Không thể sinh câu trả lời do thiếu OPENAI_API_KEY.")
        return "Không thể kết nối đến dịch vụ AI do lỗi cấu hình."

    logger.info("Đang gửi yêu cầu đến LLM để sinh câu trả lời...")
    context_str = "\n---\n".join([chunk['content'] for chunk in context_chunks])
    
    prompt = f"""
    Bạn là một trợ lý pháp lý AI chuyên nghiệp tại Việt Nam. Dựa vào các văn bản pháp luật được cung cấp dưới đây, hãy trả lời câu hỏi của người dùng một cách chính xác và chỉ dựa trên thông tin có trong văn bản.

    **Văn bản tham khảo:**
    {context_str}

    **Câu hỏi của người dùng:**
    {query_text}

    **Câu trả lời của bạn:**
    """
    
    try:
        response = openai.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Bạn là một trợ lý pháp lý AI chuyên nghiệp tại Việt Nam."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
        )
        answer = response.choices[0].message.content
        logger.info("Nhận được câu trả lời từ LLM thành công.")
        return answer
    except Exception as e:
        logger.error(f"Lỗi khi gọi API của LLM: {e}", exc_info=True)
        return "Xin lỗi, đã có lỗi xảy ra trong quá trình tạo câu trả lời."


def process_intent_message(message: dict):
    """
    Hàm callback để xử lý mỗi message nhận được từ topic 'intent.classified'.
    """
    try:
        request_id = message.get("request_id")
        query_text = message.get("query_text")

        if not all([request_id, query_text]):
            logger.error(f"Message không hợp lệ, thiếu request_id hoặc query_text: {message}")
            return

        # 1. Tìm kiếm các chunk văn bản liên quan (Retrieve)
        relevant_chunks = find_relevant_chunks(query_text)
        
        # 2. Sinh câu trả lời từ LLM (Generate)
        answer = generate_answer_with_llm(query_text, relevant_chunks)
        
        # 3. Chuẩn bị message kết quả
        result_message = {
            "request_id": request_id,
            "original_query": query_text,
            "answer": answer,
            "sources": relevant_chunks  # Gửi kèm các nguồn đã sử dụng
        }
        
        # 4. Gửi kết quả đến topic tiếp theo
        kafka_producer.send_message("rag.answered", result_message)

    except Exception as e:
        logger.error(f"Lỗi không xác định trong quá trình xử lý message RAG: {e}", exc_info=True)


# --- Điểm khởi chạy của Worker ---
if __name__ == "__main__":
    logger.info("Khởi động RAG System Consumer Worker...")
    consumer = KafkaConsumer(
        topics=["intent.classified"], 
        group_id="rag_system_group"
    )
    # Bắt đầu vòng lặp lắng nghe và xử lý
    consumer.consume_messages(callback_function=process_intent_message)


