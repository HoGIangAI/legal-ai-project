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
# Ra lệnh cho LLM đóng vai một trợ lý cẩn thận và trả về kết quả dưới dạng JSON.
DOUBLE_CHECK_PROMPT_TEMPLATE = """
Bạn là một trợ lý pháp lý cực kỳ cẩn thận và chi tiết.
Nhiệm vụ của bạn là kiểm tra lại câu trả lời do một chuyên gia pháp lý soạn thảo để đảm bảo chất lượng cao nhất trước khi gửi cho khách hàng.

**BỐI CẢNH:**
- **Câu hỏi gốc của khách hàng:** "{query_text}"
- **Câu trả lời do chuyên gia soạn thảo:** "{expert_answer}"

**YÊU CẦU KIỂM TRA:**
Hãy kiểm tra các yếu tố sau:
1. **Tính phù hợp:** Câu trả lời có trực tiếp giải quyết câu hỏi của khách hàng không?
2. **Tính logic:** Các luận điểm trong câu trả lời có logic, không mâu thuẫn nội tại không?
3. **Tính đầy đủ:** Câu trả lời có bỏ sót khía cạnh quan trọng nào của câu hỏi không?

**ĐỊNH DẠNG ĐẦU RA (RẤT QUAN TRỌNG):**
Hãy trả về kết quả đánh giá của bạn dưới dạng một chuỗi JSON **DUY NHẤT** và **KHÔNG CÓ GÌ KHÁC**. Chuỗi JSON phải có cấu trúc như sau:
{{
  "is_verified": <boolean>,
  "confidence": <float>,
  "issues": ["<vấn đề 1 nếu có>", "<vấn đề 2 nếu có>", ...]
}}

**Giải thích các trường:**
- `is_verified`: `true` nếu câu trả lời tốt và sẵn sàng gửi đi, `false` nếu có vấn đề nghiêm trọng.
- `confidence`: Một con số từ 0.0 đến 1.0 thể hiện mức độ tự tin của bạn vào chất lượng câu trả lời.
- `issues`: Một danh sách các vấn đề bạn tìm thấy. Nếu không có vấn đề gì, hãy để danh sách này rỗng `[]`.
"""

# --- Logic xử lý message ---
kafka_producer = KafkaProducer()

def process_expert_answer(message: dict):
    """
    Hàm callback để xử lý câu trả lời từ expert-platform, kiểm tra chéo bằng LLM.
    """
    global kafka_producer
    try:
        # Giả định rằng query_text đã được truyền qua các bước trước đó
        # Đây là một cải tiến cần thực hiện: đảm bảo message chứa query_text
        query_text = message.get("query_text", "Không có câu hỏi gốc.")
        request_id = message.get("request_id")
        expert_answer = message.get("expert_answer", "")

        if not expert_answer or not openai.api_key:
            logger.warning(f"Bỏ qua kiểm tra cho request_id {request_id} do thiếu câu trả lời hoặc API key.")
            return

        # Tạo prompt hoàn chỉnh
        prompt = DOUBLE_CHECK_PROMPT_TEMPLATE.format(query_text=query_text, expert_answer=expert_answer)

        logger.info(f"Đang gửi yêu cầu kiểm tra chéo cho request_id: {request_id}")

        # Gọi API của LLM
        response = openai.ChatCompletion.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "Bạn là một trợ lý pháp lý, chỉ trả lời bằng định dạng JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
        )

        llm_response_text = response.choices[0].message['content']

        # Phân tích cú pháp chuỗi JSON nhận về
        verification_result = {
            "is_verified": False,
            "confidence": 0.0,
            "issues": ["Lỗi phân tích cú pháp phản hồi từ LLM."]
        }
        try:
            verification_result = json.loads(llm_response_text)
        except json.JSONDecodeError:
            logger.error(f"Không thể phân tích JSON từ LLM cho request_id {request_id}. Phản hồi: {llm_response_text}")

        logger.info(f"Kiểm tra chéo thành công cho request_id: {request_id}")

        # Tạo message kết quả cuối cùng
        final_message = message.copy() # Sao chép toàn bộ message từ bước trước
        final_message["verification_result"] = verification_result

        # Gửi kết quả cuối cùng vào topic final.answer.ready
        kafka_producer.send_message("final.answer.ready", final_message)

    except Exception as e:
        logger.error(f"Lỗi khi xử lý message tại double-check: {e}", exc_info=True)
        raise e

# --- Điểm khởi chạy của worker ---
if __name__ == "__main__":
    logger.info("Khởi chạy Double-Check Worker...")
    
    expert_consumer = KafkaConsumer(
        topics=['expert.analyzed'],
        group_id='double_check_group'
    )
    
    expert_consumer.consume_messages(process_expert_answer)


