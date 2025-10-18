from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Optional
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Expert Platform", version="1.0.0")

class ExpertRequest(BaseModel):
    question: str
    domain: str
    context: Optional[Dict] = None
    chat_history: Optional[List[Dict]] = None

class ExpertResponse(BaseModel):
    answer: str
    reasoning: str
    confidence: float
    sources: List[Dict]
    domain_specific_analysis: Dict

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "expert_platform"}

# Domain-specific expert knowledge bases
DOMAIN_EXPERTS = {
    "d1_contract_law": {
        "name": "Hợp đồng & Dân sự",
        "expertise": ["hợp đồng dân sự", "giao dịch", "nghĩa vụ", "tài sản"],
        "key_concepts": ["Bộ luật Dân sự 2015", "Điều 385", "hình thức hợp đồng"]
    },
    "d2_labor_law": {
        "name": "Lao động & Việc làm", 
        "expertise": ["hợp đồng lao động", "tiền lương", "bảo hiểm", "kỷ luật"],
        "key_concepts": ["Bộ luật Lao động 2019", "Điều 15", "thời giờ làm việc"]
    },
    "d3_criminal_law": {
        "name": "Hình sự",
        "expertise": ["tội phạm", "hình phạt", "truy tố", "phòng vệ chính đáng"],
        "key_concepts": ["Bộ luật Hình sự 2015", "tội danh", "hình phạt"]
    }
}

@app.post("/api/v1/expert/analyze", response_model=ExpertResponse)
async def expert_analysis(request: ExpertRequest):
    try:
        logger.info(f"Expert analysis for domain: {request.domain}")
        
        # Get domain expert configuration
        domain_expert = DOMAIN_EXPERTS.get(request.domain, DOMAIN_EXPERTS["d1_contract_law"])
        
        # Mock expert analysis
        if request.domain == "d2_labor_law":
            answer, reasoning = analyze_labor_law(request.question)
        elif request.domain == "d1_contract_law":
            answer, reasoning = analyze_contract_law(request.question)
        else:
            answer, reasoning = analyze_general_law(request.question)
        
        return ExpertResponse(
            answer=answer,
            reasoning=reasoning,
            confidence=0.88,
            sources=[
                {
                    "type": "legal_document",
                    "reference": domain_expert["key_concepts"][0],
                    "relevance": 0.92
                }
            ],
            domain_specific_analysis={
                "domain": domain_expert["name"],
                "key_considerations": domain_expert["expertise"][:3],
                "legal_framework": domain_expert["key_concepts"]
            }
        )
        
    except Exception as e:
        logger.error(f"Expert analysis failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Analysis failed: {str(e)}")

def analyze_labor_law(question: str) -> tuple[str, str]:
    """Expert analysis for labor law domain"""
    question_lower = question.lower()
    
    if "hợp đồng lao động" in question_lower:
        answer = """Theo Điều 15 Bộ luật Lao động 2019, hợp đồng lao động phải có các nội dung chính:
1. Tên, địa chỉ người sử dụng lao động
2. Họ tên, ngày sinh, giới tính, địa chỉ người lao động  
3. Công việc và địa điểm làm việc
4. Thời hạn hợp đồng
5. Mức lương, hình thức trả lương
6. Chế độ nâng bậc, nâng lương"""
        
        reasoning = "Phân tích dựa trên quy định về hình thức và nội dung hợp đồng lao động theo Bộ luật Lao động 2019."
    
    elif "thời giờ làm việc" in question_lower:
        answer = "Thời giờ làm việc bình thường không quá 8 giờ/ngày và 48 giờ/tuần. Làm thêm giờ không quá 50% số giờ làm việc bình thường/ngày."
        reasoning = "Áp dụng quy định về thời giờ làm việc tại Điều 105 Bộ luật Lao động 2019."
    
    else:
        answer = "Dựa trên phân tích chuyên sâu về pháp luật lao động, vấn đề của bạn cần xem xét các quy định cụ thể trong Bộ luật Lao động 2019."
        reasoning = "Phân tích tổng quan dựa trên khung pháp lý về lao động hiện hành."
    
    return answer, reasoning

def analyze_contract_law(question: str) -> tuple[str, str]:
    """Expert analysis for contract law domain"""
    question_lower = question.lower()
    
    if "hiệu lực" in question_lower or "điều kiện" in question_lower:
        answer = """Hợp đồng dân sự có hiệu lực khi có đủ các điều kiện:
1. Chủ thể có năng lực pháp luật dân sự
2. Mục đích và nội dung không vi phạm điều cấm của luật
3. Chủ thể tự nguyện giao kết
4. Hình thức hợp đồng phù hợp với quy định pháp luật"""
        
        reasoning = "Phân tích dựa trên Điều 117 Bộ luật Dân sự 2015 về điều kiện có hiệu lực của hợp đồng."
    
    elif "hủy bỏ" in question_lower or "chấm dứt" in question_lower:
        answer = "Hợp đồng có thể bị hủy bỏ khi có vi phạm cơ bản, không thể thực hiện được hoặc các bên thỏa thuận chấm dứt."
        reasoning = "Áp dụng quy định về hủy bỏ và chấm dứt hợp đồng theo Bộ luật Dân sự 2015."
    
    else:
        answer = "Phân tích chuyên sâu về hợp đồng dân sự cần xem xét các yếu tố: chủ thể, đối tượng, hình thức và nội dung thỏa thuận."
        reasoning = "Phân tích tổng quan dựa trên các nguyên tắc cơ bản của pháp luật hợp đồng."
    
    return answer, reasoning

def analyze_general_law(question: str) -> tuple[str, str]:
    """General legal expert analysis"""
    answer = "Dựa trên kiến thức chuyên môn pháp lý, vấn đề của bạn cần được phân tích trong bối cảnh pháp luật hiện hành và thực tiễn áp dụng."
    reasoning = "Phân tích dựa trên nguyên tắc chung của pháp luật và thực tiễn xét xử."
    return answer, reasoning

@app.post("/api/v1/expert/domains")
async def get_supported_domains():
    """Get list of supported legal domains"""
    domains = []
    for domain_id, config in DOMAIN_EXPERTS.items():
        domains.append({
            "id": domain_id,
            "name": config["name"],
            "expertise_areas": config["expertise"],
            "key_documents": config["key_concepts"]
        })
    return {"domains": domains}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005, log_level="info")
