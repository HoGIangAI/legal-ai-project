from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Optional
import neo4j
import os
import logging
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Models
class LegalEntity(BaseModel):
    id: str
    name: str
    type: str
    properties: Dict[str, str]

class Relationship(BaseModel):
    source: str
    target: str  
    type: str
    properties: Dict[str, str]

class OntologyQuery(BaseModel):
    entity_name: str
    depth: int = 1

class OntologyResponse(BaseModel):
    entities: List[LegalEntity]
    relationships: List[Relationship]

def get_neo4j_driver():
    return neo4j.GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        auth=("neo4j", "legalai123")
    )

async def initialize_ontology():
    try:
        driver = get_neo4j_driver()
        with driver.session() as session:
            # Create constraints
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (e:LegalEntity) REQUIRE e.id IS UNIQUE")
            
            # Clear and create sample data
            session.run("MATCH (n) DETACH DELETE n")
            
            entities = [
                {"id": "law_001", "name": "Bộ luật Lao động 2019", "type": "LAW"},
                {"id": "article_001", "name": "Điều 1. Phạm vi điều chỉnh", "type": "ARTICLE"},
                {"id": "article_002", "name": "Điều 2. Giải thích từ ngữ", "type": "ARTICLE"},
                {"id": "concept_001", "name": "Hợp đồng lao động", "type": "CONCEPT"},
            ]
            
            for entity in entities:
                session.run("""
                    MERGE (e:LegalEntity {id: $id})
                    SET e.name = $name, e.type = $type
                """, **entity)
            
            relationships = [
                {"source": "article_001", "target": "law_001", "type": "BELONGS_TO"},
                {"source": "article_002", "target": "law_001", "type": "BELONGS_TO"}, 
                {"source": "concept_001", "target": "article_002", "type": "DEFINED_IN"},
            ]
            
            for rel in relationships:
                session.run("""
                    MATCH (a:LegalEntity {id: $source}) 
                    MATCH (b:LegalEntity {id: $target})
                    MERGE (a)-[r:RELATES_TO {type: $type}]->(b)
                """, **rel)
                
        logger.info("✅ Ontology initialized successfully")
    except Exception as e:
        logger.error(f"❌ Ontology init failed: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    await initialize_ontology()
    yield

app = FastAPI(title="Legal Ontology", version="1.0.0", lifespan=lifespan)

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "legal_ontology"}

@app.post("/api/v1/ontology/query", response_model=OntologyResponse)
async def query_ontology(query: OntologyQuery):
    try:
        driver = get_neo4j_driver()
        with driver.session() as session:
            # SIMPLE AND SAFE CYPHER QUERY
            result = session.run("""
                MATCH (start:LegalEntity {name: $name})
                OPTIONAL MATCH (start)-[r]-(related:LegalEntity)
                RETURN start, collect(DISTINCT related) as related_nodes, collect(DISTINCT r) as related_rels
            """, name=query.entity_name)
            
            record = result.single()
            if not record:
                return OntologyResponse(entities=[], relationships=[])
            
            entities = []
            relationships = []
            
            # Main entity
            main = record["start"]
            entities.append(LegalEntity(
                id=main["id"],
                name=main["name"], 
                type=main["type"],
                properties={}
            ))
            
            # Related entities
            for node in record["related_nodes"] or []:
                entities.append(LegalEntity(
                    id=node["id"],
                    name=node["name"],
                    type=node["type"], 
                    properties={}
                ))
            
            # Relationships
            for rel in record["related_rels"] or []:
                relationships.append(Relationship(
                    source=rel.start_node["id"],
                    target=rel.end_node["id"],
                    type=rel.type,
                    properties={}
                ))
            
            return OntologyResponse(
                entities=entities,
                relationships=relationships
            )
            
    except Exception as e:
        logger.error(f"Query failed: {e}")
        raise HTTPException(500, f"Query failed: {e}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8003)
