from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, List
import logging

app = FastAPI(
    title="Legal AI Intent System",
    description="Vietnamese Legal Intent Recognition Service",
    version="1.0.0"
)

class LegalQuery(BaseModel):
    text: str
    context: Dict[str, Any] = {}
    session_id: str = None

class IntentResult(BaseModel):
    primary_domain: str
    confidence_score: float
    domains: List[str]
    keywords: List[str]
    fallback_used: bool = False
    processing_time: float

class HealthResponse(BaseModel):
    status: str
    service: str
    version: str

@app.get("/health", response_model=HealthResponse)
async def health_check():
    return HealthResponse(
        status="healthy",
        service="intent-system",
        version="1.0.0"
    )

@app.post("/api/v1/intent/classify", response_model=IntentResult)
async def classify_intent(query: LegalQuery):
    try:
        # Simple domain detection
        domain_keywords = {
            'd1_civil_law': ['hợp đồng', 'dân sự', 'tài sản', 'thừa kế'],
            'd2_commercial_law': ['công ty', 'doanh nghiệp', 'thương mại'],
            'd3_labor_law': ['lao động', 'tiền lương', 'bảo hiểm'],
            'd4_administrative_law': ['hành chính', 'khiếu nại', 'thủ tục'],
            'd5_criminal_law': ['hình sự', 'tội phạm', 'bị cáo'],
            'd6_tax_law': ['thuế', 'kê khai', 'hoàn thuế'],
            'd7_land_law': ['đất đai', 'sổ đỏ', 'quy hoạch'],
            'd8_environment_law': ['môi trường', 'xả thải', 'xử phạt'],
            'd9_investment_law': ['đầu tư', 'dự án', 'ưu đãi'],
            'd10_finance_banking': ['ngân hàng', 'tín dụng', 'bảo hiểm']
        }

        detected_domains = []
        detected_keywords = []
        query_lower = query.text.lower()

        for domain, keywords in domain_keywords.items():
            for keyword in keywords:
                if keyword in query_lower:
                    detected_domains.append(domain)
                    detected_keywords.append(keyword)
                    break

        primary_domain = "d1_civil_law"
        confidence = 0.7
        if detected_domains:
            primary_domain = detected_domains[0]
            confidence = min(0.95, 0.7 + (len(detected_domains) * 0.1))

        return IntentResult(
            primary_domain=primary_domain,
            confidence_score=round(confidence, 2),
            domains=detected_domains[:3],
            keywords=detected_keywords[:5],
            fallback_used=len(detected_domains) == 0,
            processing_time=0.1
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Classification failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
