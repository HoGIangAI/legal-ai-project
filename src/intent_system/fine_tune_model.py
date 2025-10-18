import os
import logging
import argparse
import requests
import joblib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Định nghĩa các hằng số ---
MODEL_FILENAME = "intent_model.joblib"

def fetch_feedback_data(api_url: str) -> list:
    """
    Gọi đến feedback-service để lấy toàn bộ dữ liệu phản hồi.
    """
    try:
        logger.info(f"Đang lấy dữ liệu phản hồi từ: {api_url}")
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()  # Ném lỗi nếu status code không phải 2xx
        feedback_data = response.json()
        logger.info(f"Lấy thành công {len(feedback_data)} bản ghi phản hồi.")
        return feedback_data
    except requests.exceptions.RequestException as e:
        logger.error(f"Lỗi khi gọi API của feedback-service: {e}")
        return []

def process_and_prepare_data(feedback_data: list) -> tuple:
    """
    Chuyển đổi dữ liệu phản hồi thành định dạng có thể huấn luyện.
    
    LƯU Ý QUAN TRỌNG:
    Trong thực tế, bước này là phức tạp nhất. Chúng ta cần một cơ sở dữ liệu hoặc một hệ thống logging
    để có thể ánh xạ `request_id` trong feedback trở lại `query_text` ban đầu.
    
    Trong kịch bản này, chúng ta sẽ MÔ PHỎNG bước này bằng cách giả định rằng
    chúng ta có một cơ sở dữ liệu `request_logs` chứa {request_id: query_text}.
    Đồng thời, chúng ta cũng cần một cơ chế để "Knowledge Manager" gán nhãn `intent` đúng cho các
    phản hồi "không chính xác".

    Ở đây, chúng ta chỉ giả lập một vài dữ liệu mẫu để chứng minh luồng hoạt động.
    """
    logger.info("Bắt đầu xử lý và chuẩn bị dữ liệu huấn luyện mới...")
    
    # --- PHẦN MÔ PHỎNG ---
    # Giả lập một "database" chứa các request đã gửi đi
    request_logs = {
        "3a8b3e5e-6c6c-4b3a-8f3e-6c6c6c6c6c6c": "thời gian làm thêm giờ được quy định thế nào",
        "4b8b3e5e-7c7c-4b4a-8f4e-7c7c7c7c7c7c": "thủ tục để hủy hợp đồng mua bán nhà",
    }
    
    new_training_texts = []
    new_training_labels = []

    for feedback in feedback_data:
        request_id = str(feedback.get('request_id'))
        
        # Lấy lại query_text từ log (mô phỏng)
        query_text = request_logs.get(request_id)
        
        if not query_text:
            continue
            
        # Nếu người dùng đánh giá là "không chính xác", chúng ta cần một nhãn đúng.
        # Giả sử Knowledge Manager đã gán nhãn đúng là "d2_labor_law" cho feedback này.
        if not feedback.get('rating'): # rating = False
            # Trong thực tế, nhãn này sẽ đến từ một giao diện của Knowledge Manager
            correct_label = "d2_labor_law" 
            new_training_texts.append(query_text)
            new_training_labels.append(correct_label)
            
    logger.info(f"Đã chuẩn bị được {len(new_training_texts)} mẫu dữ liệu mới từ feedback.")
    # --- KẾT THÚC PHẦN MÔ PHỎNG ---

    return new_training_texts, new_training_labels

def main(feedback_api_url: str):
    """
    Hàm chính để chạy toàn bộ quy trình fine-tuning.
    """
    logger.info("===== BẮT ĐẦU QUY TRÌNH FINE-TUNING MÔ HÌNH INTENT =====")

    # 1. Lấy dữ liệu phản hồi mới
    feedback_data = fetch_feedback_data(feedback_api_url)
    if not feedback_data:
        logger.warning("Không có dữ liệu phản hồi mới để huấn luyện. Kết thúc quy trình.")
        return

    # 2. Xử lý dữ liệu
    new_texts, new_labels = process_and_prepare_data(feedback_data)
    if not new_texts:
        logger.warning("Không có dữ liệu huấn luyện hợp lệ sau khi xử lý. Kết thúc quy trình.")
        return

    # 3. Tải mô hình hiện có
    if not os.path.exists(MODEL_FILENAME):
        logger.error(f"Không tìm thấy file mô hình '{MODEL_FILENAME}'. Vui lòng chạy train_initial_model.py trước.")
        return
        
    logger.info(f"Đang tải mô hình hiện có từ '{MODEL_FILENAME}'...")
    model_pipeline: Pipeline = joblib.load(MODEL_FILENAME)

    # 4. Huấn luyện lại mô hình với dữ liệu mới
    # Cách tiếp cận đơn giản và hiệu quả nhất là huấn luyện lại từ đầu với toàn bộ dữ liệu.
    # Lấy dữ liệu cũ từ bước huấn luyện ban đầu (để mô phỏng)
    from train_initial_model import TRAINING_DATA
    
    all_texts = [item["text"] for item in TRAINING_DATA] + new_texts
    all_labels = [item["label"] for item in TRAINING_DATA] + new_labels
    
    logger.info(f"Bắt đầu huấn luyện lại mô hình trên tổng cộng {len(all_texts)} mẫu dữ liệu...")
    model_pipeline.fit(all_texts, all_labels)
    logger.info("Huấn luyện lại hoàn tất.")

    # 5. Lưu mô hình đã được cải tiến
    joblib.dump(model_pipeline, MODEL_FILENAME)
    logger.info(f"✅ Mô hình đã được cập nhật và lưu thành công vào '{MODEL_FILENAME}'.")
    logger.info("===== KẾT THÚC QUY TRÌNH FINE-TUNING =====")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kịch bản tinh chỉnh (fine-tune) mô hình phân loại Intent.")
    parser.add_argument(
        "--feedback-api-url",
        type=str,
        required=True,
        help="URL đầy đủ của endpoint GET /api/v1/feedback của feedback-service."
    )
    
    args = parser.parse_args()
    main(feedback_api_url=args.feedback_api_url)

