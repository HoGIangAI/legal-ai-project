from fastapi import FastAPI
from pydantic import BaseModel
from typing import List, Dict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Semantic Chunker Service", version="1.0.0")

class ChunkingRequest(BaseModel):
    document_text: str
    chunking_strategy: str = "legal_boundaries"

class DocumentChunk(BaseModel):
    content: str
    metadata: Dict[str, str]

class ChunkingResponse(BaseModel):
    chunks: List[DocumentChunk]
    total_chunks: int

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "semantic_chunker"}

@app.post("/api/v1/chunk/document", response_model=ChunkingResponse)
async def chunk_document(request: ChunkingRequest):
    try:
        document_text = request.document_text
        chunks = []
        
        boundaries = ["Điều", "Chương", "Khoản", "Mục"]
        current_chunk = ""
        lines = document_text.split('\n')
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            is_boundary = any(line.startswith(boundary) for boundary in boundaries)
            
            if is_boundary and current_chunk:
                chunks.append(DocumentChunk(
                    content=current_chunk.strip(),
                    metadata={"type": "legal_article", "strategy": request.chunking_strategy}
                ))
                current_chunk = line
            else:
                current_chunk += " " + line if current_chunk else line
        
        if current_chunk:
            chunks.append(DocumentChunk(
                content=current_chunk.strip(),
                metadata={"type": "legal_article", "strategy": request.chunking_strategy}
            ))
        
        if not chunks:
            chunks.append(DocumentChunk(
                content=document_text,
                metadata={"type": "legal_document", "strategy": request.chunking_strategy}
            ))
        
        return ChunkingResponse(
            chunks=chunks,
            total_chunks=len(chunks)
        )
        
    except Exception as e:
        logger.error(f"Document chunking failed: {str(e)}")
        return ChunkingResponse(chunks=[], total_chunks=0)

if __name__ == "__main__":
    import uvicorn
    # FIX: Change port from 8000 to 8002
    uvicorn.run(app, host="0.0.0.0", port=8002, log_level="info")
