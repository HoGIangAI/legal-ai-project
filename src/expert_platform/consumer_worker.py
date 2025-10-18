import os
import logging
import json
import openai

# Xử lý import để chạy được cả local và trong container
try:
    from src.common.kafka_client import KafkaConsumer, KafkaProducer
except ImportError:
    import sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
    from src.common.kafka_client import KafkaConsumer, KafkaProducer

# --- Cấu hình logging và OpenAI ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Đọc API key từ biến môi trường
openai.api_key = os.getenv("OPENAI_API_KEY")
if not openai.api_key:
    logger.warning("OPENAI_API_KEY không được thiết lập. Dịch vụ sẽ không thể hoạt động.")

# --- PROMPT TEMPLATE ---
# Đây là "bộ não" của chuyên gia AI. Chúng ta ra lệnh cho LLM cách hành xử.
EXPERT_PROMPT_TEMPLATE = """
Bạn là một chuyên gia pháp lý cao cấp của Việt Nam với nhiều năm kinh nghiệm.
Nhiệm vụ của bạn là xem xét một câu trả lời pháp lý sơ bộ được tạo ra bởi một hệ thống RAG và làm giàu nó bằng kiến thức chuyên môn của mình.

**BỐI CẢNH:**
- **Câu trả lời sơ bộ từ RAG:** "{rag_answer}"
- **Các nguồn tài liệu tham khảo:**
{sources}

**YÊU CẦU:**
Dựa trên bối cảnh trên, hãy đưa ra một câu trả lời hoàn chỉnh và chuyên sâu hơn. Câu trả lời của bạn phải có cấu trúc rõ ràng như sau:

### PHÂN TÍCH CHUYÊN SÂU
(Viết lại và mở rộng câu trả lời từ RAG một cách chi tiết, mạch lạc, với văn phong của một chuyên gia. Giải thích rõ các thuật ngữ và logic pháp lý. Tuyệt đối chỉ dựa vào các nguồn tài liệu được cung cấp.)

### CÁC YẾU TỐ CẦN LƯU Ý
(Liệt kê các điểm quan trọng, các ngoại lệ, hoặc các vấn đề liên quan mà người hỏi cần phải đặc biệt chú ý, dựa trên kinh nghiệm của bạn. Ví dụ: "Cần lưu ý về thời hiệu khởi kiện", "Hình thức của hợp đồng này phải được công chứng", v.v.)
"""

# --- Logic xử lý message ---
kafka_producer = KafkaProducer()

def process_rag_answer(message: dict):
    """
    Hàm callback để xử lý câu trả lời từ rag-system, làm giàu nó bằng LLM.
    """
    global kafka_producer
    try:
        request_id = message.get("request_id")
        rag_answer = message.get("answer", "")
        sources = message.get("sources", [])

        if not rag_answer or not openai.api_key:
            logger.warning(f"Bỏ qua xử lý cho request_id {request_id} do thiếu câu trả lời hoặc API key.")
            # Có thể đẩy thẳng message đi tiếp hoặc vào một topic lỗi riêng
            return

        # Định dạng lại các nguồn để đưa vào prompt
        formatted_sources = "\n".join([f"- Nguồn: {s.get('metadata', {}).get('source', 'N/A')}, Nội dung: {s.get('content_preview', 'N/A')}" for s in sources])

        # Tạo prompt hoàn chỉnh
        prompt = EXPERT_PROMPT_TEMPLATE.format(rag_answer=rag_answer, sources=formatted_sources)

        logger.info(f"Đang gửi yêu cầu phân tích chuyên sâu cho request_id: {request_id}")

        # Gọi API của LLM
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo", # Hoặc "gpt-4"
            messages=[
                {"role": "system", "content": "Bạn là một chuyên gia pháp lý cao cấp của Việt Nam."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
        )

        llm_response_text = response.choices[0].message['content']

        # Phân tích cú pháp câu trả lời từ LLM
        expert_answer = "Không thể phân tích câu trả lời từ LLM."
        domain_analysis = {"notes": "Không thể phân tích các yếu tố cần lưu ý."}
        
        if "### CÁC YẾU TỐ CẦN LƯU Ý" in llm_response_text:
            parts = llm_response_text.split("### CÁC YẾU TỐ CẦN LƯU Ý")
            expert_answer = parts[0].replace("### PHÂN TÍCH CHUYÊN SÂU", "").strip()
            domain_analysis_text = parts[1].strip()
            domain_analysis = {"key_considerations": [line.strip() for line in domain_analysis_text.split('\n') if line.strip()]}
        elif "### PHÂN TÍCH CHUYÊN SÂU" in llm_response_text:
             expert_answer = llm_response_text.replace("### PHÂN TÍCH CHUYÊN SÂU", "").strip()

        logger.info(f"Phân tích chuyên sâu thành công cho request_id: {request_id}")

        # Tạo message mới để gửi đi
        output_message = {
            "request_id": request_id,
            "original_rag_answer": rag_answer,
            "expert_answer": expert_answer,
            "domain_specific_analysis": domain_analysis,
            "sources": sources # Giữ nguyên sources để các bước sau có thể dùng
        }

        # Gửi kết quả đã được làm giàu vào topic tiếp theo
        kafka_producer.send_message("expert.analyzed", output_message)

    except Exception as e:
        logger.error(f"Lỗi khi xử lý message tại expert-platform: {e}", exc_info=True)
        # Ném lỗi ra ngoài để logic retry/DLQ trong kafka_client xử lý
        raise e

# --- Điểm khởi chạy của worker ---
if __name__ == "__main__":
    logger.info("Khởi chạy Expert Platform Worker...")
    
    rag_consumer = KafkaConsumer(
        topics=['rag.answered'],
        group_id='expert_platform_group'
    )
    
    rag_consumer.consume_messages(process_rag_answer)


