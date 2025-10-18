from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Double-Check System", version="1.0.0")

class VerificationRequest(BaseModel):
    answer: str
    context: List[Dict]
    question: str
    domain: str

class VerificationResponse(BaseModel):
    is_verified: bool
    confidence: float
    issues: List[str]
    corrections: List[str]
    verification_method: str
    cross_references: List[Dict]

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "double_check"}

@app.post("/api/v1/verify/answer", response_model=VerificationResponse)
async def verify_answer(request: VerificationRequest):
    try:
        logger.info(f"Verifying answer for domain: {request.domain}")
        
        # Multiple verification strategies
        consistency_check = check_consistency(request.answer, request.context)
        fact_check = check_facts(request.answer, request.domain)
        logic_check = check_logical_consistency(request.answer)
        
        # Calculate overall verification score
        verification_score = (
            consistency_check["score"] * 0.4 +
            fact_check["score"] * 0.4 + 
            logic_check["score"] * 0.2
        )
        
        # Compile issues and corrections
        all_issues = (
            consistency_check["issues"] +
            fact_check["issues"] + 
            logic_check["issues"]
        )
        
        all_corrections = (
            consistency_check["corrections"] +
            fact_check["corrections"] +
            logic_check["corrections"]
        )
        
        is_verified = verification_score >= 0.7 and len(all_issues) == 0
        
        return VerificationResponse(
            is_verified=is_verified,
            confidence=verification_score,
            issues=all_issues,
            corrections=all_corrections,
            verification_method="multi_strategy_verification",
            cross_references=[
                {
                    "source": "internal_consistency",
                    "score": consistency_check["score"],
                    "details": consistency_check["details"]
                },
                {
                    "source": "factual_accuracy", 
                    "score": fact_check["score"],
                    "details": fact_check["details"]
                }
            ]
        )
        
    except Exception as e:
        logger.error(f"Verification failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Verification failed: {str(e)}")

def check_consistency(answer: str, context: List[Dict]) -> Dict:
    """Check internal consistency with provided context"""
    issues = []
    corrections = []
    details = []
    
    # Check if answer references context properly
    context_keywords = extract_keywords_from_context(context)
    answer_keywords = set(answer.lower().split())
    
    matched_keywords = context_keywords.intersection(answer_keywords)
    match_ratio = len(matched_keywords) / len(context_keywords) if context_keywords else 1.0
    
    if match_ratio < 0.3:
        issues.append("Answer doesn't sufficiently reference provided context")
        corrections.append("Incorporate more context-specific information")
    
    details.append(f"Context keyword match: {match_ratio:.2f}")
    
    return {
        "score": min(match_ratio * 1.5, 1.0),
        "issues": issues,
        "corrections": corrections,
        "details": details
    }

def check_facts(answer: str, domain: str) -> Dict:
    """Check factual accuracy based on domain knowledge"""
    issues = []
    corrections = []
    details = []
    score = 0.8  # Base score
    
    # Domain-specific fact checking rules
    domain_rules = {
        "d2_labor_law": [
            {
                "pattern": "8 giờ",
                "check": "Thời giờ làm việc bình thường không quá 8 giờ/ngày",
                "weight": 0.1
            }
        ],
        "d1_contract_law": [
            {
                "pattern": "tự nguyện",
                "check": "Hợp đồng phải được giao kết tự nguyện",
                "weight": 0.15
            }
        ]
    }
    
    rules = domain_rules.get(domain, [])
    for rule in rules:
        if rule["pattern"] in answer.lower():
            score += rule["weight"]
            details.append(f"Matched rule: {rule['check']}")
    
    return {
        "score": min(score, 1.0),
        "issues": issues,
        "corrections": corrections,
        "details": details
    }

def check_logical_consistency(answer: str) -> Dict:
    """Check logical consistency of the answer"""
    issues = []
    corrections = []
    details = []
    score = 0.9  # Base score for logical consistency
    
    # Check for contradictions (simplified)
    contradictions = [
        ("phải", "không phải"),
        ("được", "không được")
    ]
    
    for term1, term2 in contradictions:
        if term1 in answer.lower() and term2 in answer.lower():
            issues.append(f"Possible contradiction: {term1} vs {term2}")
            score -= 0.2
    
    details.append("Basic logical consistency check completed")
    
    return {
        "score": max(score, 0.0),
        "issues": issues,
        "corrections": corrections,
        "details": details
    }

def extract_keywords_from_context(context: List[Dict]) -> set:
    """Extract important keywords from context"""
    keywords = set()
    for item in context:
        if 'content' in item:
            words = item['content'].lower().split()[:10]
            keywords.update(words)
    return keywords

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8006, log_level="info")
