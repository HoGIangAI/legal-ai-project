import pytest
from src.intent_system.main import app, LegalQuery
from fastapi.testclient import TestClient

client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_intent_classification():
    query = LegalQuery(text="Điều kiện có hiệu lực của hợp đồng dân sự")
    response = client.post("/api/v1/intent/classify", json=query.dict())
    
    assert response.status_code == 200
    data = response.json()
    assert "primary_domain" in data
    assert "confidence_score" in data
    assert data["confidence_score"] > 0
