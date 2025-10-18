# Legal AI — Ontology GraphOps

## Quick start
1) docker compose -f docker-compose.kafka.yml up -d
2) docker compose -f docker-compose.neo4j.yml up -d
3) make install && make diagnose
4) make emit && python -m services.ontology.ontology_consumer --json --once --idle-timeout-sec 3 --auto-offset-reset earliest --group-id "ontology-consumer-once-$(date +%s)"
5) make audit
6) make ui  → http://localhost:8088/dashboard
