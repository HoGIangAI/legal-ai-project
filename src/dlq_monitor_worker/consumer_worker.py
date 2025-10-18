import os
import logging
import json

# Xử lý import để chạy được cả local và trong container
try:
    from src.common.kafka_client import KafkaConsumer
except ImportError:
    import sys
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
    from src.common.kafka_client import KafkaConsumer

# --- Cấu hình logging ---
# Cấu hình để hiển thị cả các log mức CRITICAL
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Logic xử lý message lỗi ---
def process_dlq_message(message: dict):
    """
    Hàm callback để xử lý các message từ Dead Letter Queue.
    Nhiệm vụ chính là ghi log ở mức độ CRITICAL để gây sự chú ý.
    """
    try:
        # Ghi log toàn bộ message lỗi với mức độ CRITICAL
        # Các hệ thống giám sát log như Loki/Grafana có thể được cấu hình
        # để gửi cảnh báo (alert) khi phát hiện log ở mức độ này.
        logger.critical(
            "🚨 DETECTED FAILED MESSAGE IN DLQ 🚨\n%s",
            json.dumps(message, indent=2, ensure_ascii=False)
        )
    except Exception as e:
        logger.error(f"Lỗi khi xử lý DLQ message: {e}", exc_info=True)


# --- Điểm khởi chạy của worker ---
if __name__ == "__main__":
    logger.info("Khởi chạy DLQ Monitor Worker...")
    
    # Khởi tạo consumer và đăng ký lắng nghe các topic có đuôi .dlq
    # bằng cách sử dụng topic_pattern (biểu thức chính quy)
    dlq_consumer = KafkaConsumer(
        topic_pattern='.*\\.dlq$',
        group_id='dlq-monitor-group'
    )
    
    # Bắt đầu vòng lặp lắng nghe và xử lý message
    dlq_consumer.consume_messages(process_dlq_message)

