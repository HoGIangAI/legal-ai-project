from __future__ import annotations
from typing import Dict, List, Literal, Optional, Union
from pydantic import BaseModel, Field

class NodePayload(BaseModel):
    label: str
    id: str
    props: Dict[str, object] = Field(default_factory=dict)

class EdgeInputRef(BaseModel):
    labels: List[str]
    id: str

class EdgeInput(BaseModel):
    type: str
    from_: EdgeInputRef = Field(alias="from")
    to: EdgeInputRef
    props: Dict[str, object] = Field(default_factory=dict)

class EdgeNormalized(BaseModel):
    type: str
    src_label: str
    src_id: str
    dst_label: str
    dst_id: str
    props: Dict[str, object] = Field(default_factory=dict)

EdgeUnion = Union[EdgeInput, EdgeNormalized]

class GraphOpsItem(BaseModel):
    op: Literal["upsert"]
    node: Optional[NodePayload] = None
    edge: Optional[EdgeUnion] = None

    def normalize_edge(self) -> Optional[EdgeNormalized]:
        if not self.edge:
            return None
        if isinstance(self.edge, EdgeNormalized):
            return self.edge
        ei: EdgeInput = self.edge  # type: ignore[assignment]
        return EdgeNormalized(
            type=ei.type,
            src_label=ei.from_.labels[0],
            src_id=ei.from_.id,
            dst_label=ei.to.labels[0],
            dst_id=ei.to.id,
            props=ei.props or {},
        )
