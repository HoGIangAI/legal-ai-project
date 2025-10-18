# legal-ai-platform/tests/test_integration.py
import pytest
import requests
import json

# Service endpoints
INTENT_SERVICE = "http://localhost:8001"
CHUNKER_SERVICE = "http://localhost:8002"
ONTOLOGY_SERVICE = "http://localhost:8003"
RAG_SERVICE = "http://localhost:8004"

def test_full_pipeline():
    """Test complete legal query pipeline"""
    
    # 1. Intent Classification
    intent_payload = {
        "query": "Tôi muốn tìm hiểu về điều khoản hợp đồng lao động"
    }
    
    intent_response = requests.post(
        f"{INTENT_SERVICE}/api/v1/intent/classify",
        json=intent_payload
    )
    assert intent_response.status_code == 200
    intent_data = intent_response.json()
    print(f"Intent: {intent_data}")
    
    # 2. Document Chunking
    document = """
    Điều 1. Phạm vi điều chỉnh
    Bộ luật này quy định về hợp đồng lao động, các quyền và nghĩa vụ của các bên.
    
    Điều 2. Giải thích từ ngữ
    Hợp đồng lao động là sự thỏa thuận giữa người lao động và người sử dụng lao động.
    """
    
    chunker_payload = {
        "document_text": document,
        "chunking_strategy": "legal_boundaries"
    }
    
    chunker_response = requests.post(
        f"{CHUNKER_SERVICE}/api/v1/chunk/document",
        json=chunker_payload
    )
    assert chunker_response.status_code == 200
    chunks_data = chunker_response.json()
    print(f"Chunks: {len(chunks_data['chunks'])}")
    
    # 3. Ontology Query
    ontology_payload = {
        "entity_name": "Bộ luật Dân sự 2015",
        "depth": 2
    }
    
    ontology_response = requests.post(
        f"{ONTOLOGY_SERVICE}/api/v1/ontology/query",
        json=ontology_payload
    )
    assert ontology_response.status_code == 200
    ontology_data = ontology_response.json()
    print(f"Ontology entities: {len(ontology_data['entities'])}")
    
    # 4. RAG Search and Generation
    search_payload = {
        "query": "điều khoản hợp đồng lao động",
        "top_k": 3
    }
    
    search_response = requests.post(
        f"{RAG_SERVICE}/api/v1/rag/search",
        json=search_payload
    )
    assert search_response.status_code == 200
    search_data = search_response.json()
    print(f"Search results: {len(search_data['results'])}")
    
    # 5. Answer Generation
    if search_data['results']:
        qa_payload = {
            "question": "điều khoản hợp đồng lao động",
            "context_chunks": [
                {
                    "id": result["id"],
                    "content": result["content"],
                    "metadata": result["metadata"]
                }
                for result in search_data['results'][:2]
            ]
        }
        
        qa_response = requests.post(
            f"{RAG_SERVICE}/api/v1/rag/generate",
            json=qa_payload
        )
        assert qa_response.status_code == 200
        qa_data = qa_response.json()
        print(f"Generated answer: {qa_data['answer']}")

if __name__ == "__main__":
    test_full_pipeline()
    print("✅ Integration test completed successfully!")
