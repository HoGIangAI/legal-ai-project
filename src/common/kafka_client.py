import os
import json
import logging
import time
import traceback
from kafka import KafkaProducer as KafkaProducerLib
from kafka import KafkaConsumer as KafkaConsumerLib
from kafka.errors import NoBrokersAvailable

# --- Cấu hình chung ---
# Thiết lập logging chi tiết
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Đọc cấu hình Kafka từ biến môi trường, có giá trị mặc định cho local dev
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
MAX_CONNECTION_RETRIES = 5
RETRY_DELAY_SECONDS = 5


class KafkaProducer:
    """
    Một client Kafka Producer đã được cấu hình để gửi message dạng JSON
    và có sẵn cơ chế retry khi kết nối.
    """

    def __init__(self):
        """Khởi tạo producer và thực hiện kết nối tới Kafka server."""
        self.producer = None
        retries = 0
        while retries < MAX_CONNECTION_RETRIES and self.producer is None:
            try:
                self.producer = KafkaProducerLib(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    api_version=(0, 10, 1)  # Đảm bảo tương thích với nhiều phiên bản Kafka
                )
                logger.info("✅ Kết nối thành công tới Kafka với vai trò Producer.")
            except NoBrokersAvailable:
                retries += 1
                logger.warning(
                    f"⚠️ Không tìm thấy Kafka broker. "
                    f"Thử lại sau {RETRY_DELAY_SECONDS} giây... "
                    f"(Lần thử {retries}/{MAX_CONNECTION_RETRIES})"
                )
                time.sleep(RETRY_DELAY_SECONDS)

        if self.producer is None:
            logger.error("❌ Không thể kết nối tới Kafka sau nhiều lần thử. Vui lòng kiểm tra lại cấu hình.")
            raise ConnectionError("Không thể khởi tạo Kafka Producer.")

    def send_message(self, topic: str, message: dict):
        """
        Gửi một message (dạng dict) tới một topic Kafka cụ thể.

        Args:
            topic (str): Tên của topic Kafka.
            message (dict): Nội dung message cần gửi.
        """
        if not isinstance(message, dict):
            logger.error("Lỗi: Message phải là một dictionary.")
            return

        try:
            logger.info(f"🚀 Đang gửi message tới topic '{topic}': {message}")
            self.producer.send(topic, value=message)
            self.producer.flush()  # Đảm bảo message được gửi đi ngay lập tức
            logger.info(f"✔️ Gửi message tới topic '{topic}' thành công.")
        except Exception as e:
            logger.error(f"❌ Gặp lỗi khi gửi message tới topic '{topic}': {e}")

    def close(self):
        """Đóng kết nối producer."""
        if self.producer:
            logger.info("🔌 Đang đóng kết nối Kafka Producer.")
            self.producer.close()


class KafkaConsumer:
    """
    Một client Kafka Consumer được cấu hình để nhận message dạng JSON,
    xử lý qua một hàm callback và có sẵn cơ chế retry.
    """

    def __init__(self, topics: list, group_id: str):
        """
        Khởi tạo consumer, kết nối và subscribe vào các topic.

        Args:
            topics (list): Một danh sách các topic cần lắng nghe.
            group_id (str): ID của consumer group.
        """
        self.consumer = None
        self._producer = None # Producer nội bộ cho DLQ
        retries = 0
        while retries < MAX_CONNECTION_RETRIES and self.consumer is None:
            try:
                self.consumer = KafkaConsumerLib(
                    *topics,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(','),
                    group_id=group_id,
                    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                    auto_offset_reset='earliest',  # Bắt đầu đọc từ message cũ nhất nếu là consumer mới
                    enable_auto_commit=False # Tắt auto commit để xử lý lỗi thủ công
                )
                logger.info(
                    f"✅ Kết nối thành công tới Kafka với vai trò Consumer."
                    f"\n\t- Group ID: '{group_id}'"
                    f"\n\t- Topics: {topics}"
                )
            except NoBrokersAvailable:
                retries += 1
                logger.warning(
                    f"⚠️ Không tìm thấy Kafka broker. "
                    f"Thử lại sau {RETRY_DELAY_SECONDS} giây... "
                    f"(Lần thử {retries}/{MAX_CONNECTION_RETRIES})"
                )
                time.sleep(RETRY_DELAY_SECONDS)

        if self.consumer is None:
            logger.error("❌ Không thể kết nối tới Kafka sau nhiều lần thử. Vui lòng kiểm tra lại cấu hình.")
            raise ConnectionError("Không thể khởi tạo Kafka Consumer.")

    def _get_dlq_producer(self):
        """Khởi tạo producer cho DLQ khi cần."""
        if self._producer is None:
            self._producer = KafkaProducer()
        return self._producer

    def consume_messages(self, callback_function):
        """
        Bắt đầu vòng lặp vô tận để lắng nghe và xử lý message.
        Tích hợp logic Retry và DLQ.
        """
        logger.info("🎧 Bắt đầu lắng nghe message từ Kafka...")
        try:
            for message in self.consumer:
                logger.info(
                    f"📩 Nhận được message từ topic '{message.topic}' "
                    f"(Partition: {message.partition}, Offset: {message.offset})"
                )
                
                is_processed = False
                retries = 0
                max_retries = 3

                while not is_processed and retries < max_retries:
                    try:
                        # Gọi hàm callback để xử lý logic nghiệp vụ
                        callback_function(message.value)
                        is_processed = True
                        # Commit offset sau khi xử lý thành công
                        self.consumer.commit()
                        logger.info(f"✔️ Xử lý message thành công (Offset: {message.offset}).")

                    except Exception as e:
                        retries += 1
                        logger.error(
                            f"❌ Gặp lỗi khi xử lý message (Offset: {message.offset}). "
                            f"Thử lại... (Lần thử {retries}/{max_retries}). Lỗi: {e}"
                        )
                        time.sleep(RETRY_DELAY_SECONDS)

                if not is_processed:
                    logger.critical(
                        f"CRITICAL: Không thể xử lý message (Offset: {message.offset}) sau {max_retries} lần thử. "
                        f"Đang chuyển tới Dead Letter Queue (DLQ)."
                    )
                    try:
                        dlq_producer = self._get_dlq_producer()
                        dlq_topic = f"{message.topic}.dlq"
                        error_message = message.value
                        error_message['error_details'] = {
                            'original_topic': message.topic,
                            'offset': message.offset,
                            'error_traceback': traceback.format_exc()
                        }
                        dlq_producer.send_message(dlq_topic, error_message)
                        # Commit offset sau khi đã gửi tới DLQ để không xử lý lại message này nữa
                        self.consumer.commit()
                    except Exception as dlq_e:
                        logger.error(f"FATAL: Không thể gửi message tới DLQ. Lỗi: {dlq_e}", exc_info=True)


        except KeyboardInterrupt:
            logger.info("... Người dùng đã dừng tiến trình consumer.")
        finally:
            self.close()

    def close(self):
        """Đóng kết nối consumer và producer (nếu có)."""
        if self.consumer:
            logger.info("🔌 Đang đóng kết nối Kafka Consumer.")
            self.consumer.close()
        if self._producer:
            self._producer.close()


