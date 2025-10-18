import pytest
from src.semantic_chunker.main import app, ChunkingRequest
from fastapi.testclient import TestClient

client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_document_chunking():
    sample_document = "Điều 1. Phạm vi điều chỉnh. Luật này quy định về hợp đồng."
    request = ChunkingRequest(document_content=sample_document)
    response = client.post("/api/v1/chunk/document", json=request.dict())
    
    assert response.status_code == 200
    data = response.json()
    assert "chunks" in data
    assert "total_chunks" in data
