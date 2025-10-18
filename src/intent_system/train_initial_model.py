import joblib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
import logging

# --- Cấu hình Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Dữ liệu huấn luyện ban đầu, dựa trên logic từ khóa cũ để đảm bảo tính tương thích
TRAINING_DATA = [
    # Contract & Civil Law
    {"text": "tư vấn về hợp đồng", "label": "d1_contract_law"},
    {"text": "điều khoản trong thỏa thuận", "label": "d1_contract_law"},
    {"text": "hợp đồng có hiệu lực khi nào", "label": "d1_contract_law"},
    {"text": "vấn đề dân sự tài sản thừa kế", "label": "d1_civil_law"},


    # Labor Law
    {"text": "hợp đồng lao động", "label": "d2_labor_law"},
    {"text": "quy định về tiền lương", "label": "d2_labor_law"},
    {"text": "tôi muốn hỏi về luật lao động", "label": "d2_labor_law"},

    # Criminal Law
    {"text": "bị phạt tù bao lâu", "label": "d3_criminal_law"},
    {"text": "tội phạm hình sự", "label": "d3_criminal_law"},
    {"text": "vấn đề liên quan đến hình sự", "label": "d3_criminal_law"},

    # Business Law
    {"text": "thành lập công ty", "label": "d4_business_law"},
    {"text": "thủ tục luật doanh nghiệp", "label": "d4_business_law"},

    # Tax Law
    {"text": "hướng dẫn kê khai thuế", "label": "d5_tax_law"},
    {"text": "vấn đề về tax", "label": "d5_tax_law"},
    
    # Land Law
    {"text": "mua bán đất đai", "label": "d6_land_law"},
    {"text": "tranh chấp bất động sản sổ đỏ", "label": "d6_land_law"},

    # Family Law
    {"text": "thủ tục ly hôn", "label": "d7_family_law"},
    {"text": "kết hôn và gia đình", "label": "d7_family_law"},
    
    # ... (thêm dữ liệu cho các domain khác nếu cần)
    {"text": "một câu hỏi pháp lý chung", "label": "d10_general_legal"},
]

def train_and_save_model():
    """
    Huấn luyện mô hình phân loại intent ban đầu và lưu lại.
    """
    logger.info("Bắt đầu quá trình huấn luyện mô hình intent ban đầu...")

    # Tách dữ liệu thành văn bản và nhãn
    X_train = [item["text"] for item in TRAINING_DATA]
    y_train = [item["label"] for item in TRAINING_DATA]

    # Xây dựng pipeline xử lý và huấn luyện
    # 1. TfidfVectorizer: Chuyển văn bản thành vector số học.
    # 2. LogisticRegression: Mô hình phân loại.
    model_pipeline = Pipeline([
        ('tfidf', TfidfVectorizer()),
        ('clf', LogisticRegression(random_state=42, C=5)) # Tăng C để mô hình fit tốt hơn với dữ liệu nhỏ
    ])

    # Huấn luyện mô hình
    logger.info(f"Huấn luyện trên {len(X_train)} mẫu dữ liệu...")
    model_pipeline.fit(X_train, y_train)
    logger.info("Huấn luyện hoàn tất.")

    # Lưu pipeline đã huấn luyện ra file
    model_filename = "intent_model.joblib"
    joblib.dump(model_pipeline, model_filename)
    logger.info(f"Mô hình đã được lưu thành công vào file '{model_filename}'.")

if __name__ == "__main__":
    train_and_save_model()

