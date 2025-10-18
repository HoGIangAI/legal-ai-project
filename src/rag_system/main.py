from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="RAG System", version="1.0.0")

class DocumentChunk(BaseModel):
    id: str
    content: str
    metadata: Dict[str, str]

class SearchQuery(BaseModel):
    query: str
    top_k: int = 5

class QARequest(BaseModel):
    question: str
    context_chunks: List[DocumentChunk]

class QAResponse(BaseModel):
    answer: str
    sources: List[Dict]
    confidence: float

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "rag_system"}

@app.post("/api/v1/rag/ingest")
async def ingest_documents(chunks: List[DocumentChunk]):
    try:
        logger.info(f"Ingesting {len(chunks)} documents")
        return {"status": "success", "message": f"Ingested {len(chunks)} documents"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")

@app.post("/api/v1/rag/search")
async def semantic_search(query: SearchQuery):
    try:
        logger.info(f"Searching for: {query.query}")
        
        # Mock results for now
        mock_results = [
            {
                "id": "doc_001",
                "content": "Hợp đồng dân sự là sự thỏa thuận giữa các bên về việc xác lập, thay đổi hoặc chấm dứt quyền, nghĩa vụ dân sự.",
                "metadata": {"source": "Bộ luật Dân sự 2015", "article": "Điều 385"},
                "similarity": 0.85
            }
        ]
        
        return {"query": query.query, "results": mock_results[:query.top_k]}
    except Exception as e:
        logger.error(f"Semantic search failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")

@app.post("/api/v1/rag/generate", response_model=QAResponse)
async def generate_answer(qa_request: QARequest):
    try:
        context = " ".join([chunk.content for chunk in qa_request.context_chunks])
        
        answer = f"Dựa trên các văn bản pháp luật: {context[:100]}..."
        
        sources = [
            {
                "chunk_id": chunk.id,
                "content_preview": chunk.content[:50] + "...",
                "metadata": chunk.metadata
            }
            for chunk in qa_request.context_chunks
        ]
        
        return QAResponse(
            answer=answer,
            sources=sources,
            confidence=0.82
        )
    except Exception as e:
        logger.error(f"Answer generation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Generation failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8004, log_level="info")
