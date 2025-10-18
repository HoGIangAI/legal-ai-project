from adapter_neo4j_v3rev2 import OntologyAdapter
adapter = OntologyAdapter()
evt = {
  "src":{"concept_id":"C001","name":"Clause A","domain":"legal"},
  "dst":{"concept_id":"C002","name":"Clause B","domain":"legal"},
  "rel":{"type":"CROSSES_DOMAIN","edge_id":"edge-C001-C002","metadata":{"note":"seed"},"version":"v1","kctx":"ctx-001"}
}
print(adapter.ingest_event(evt))
adapter.close()
