from __future__ import annotations
import os
from pydantic import BaseModel, Field

class Settings(BaseModel):
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="localhost:9092")
    SCHEMA_REGISTRY_URL: str = Field(default="http://localhost:8081")
    TOPIC_GRAPHOPS: str = Field(default="legal-ontology-graphops")
    NEO4J_URI: str = Field(default="bolt://localhost:7687")
    NEO4J_USER: str = Field(default="neo4j")
    NEO4J_PASSWORD: str = Field(default="legalai123")
    ONTOLOGY_FILE: str = Field(default="./data/ontology/ontology_thuytinh_dev.jsonl")
    LOG_LEVEL: str = Field(default="INFO")

    @classmethod
    def load(cls) -> "Settings":
        """Load settings from environment vars with defaults."""
        values = {k: os.getenv(k) for k in cls.model_fields}
        return cls(**{k: v for k, v in values.items() if v is not None})
