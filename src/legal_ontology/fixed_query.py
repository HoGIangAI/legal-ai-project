def build_safe_cypher_query(depth: int):
    """Safe Cypher query with proper null handling"""
    if depth == 1:
        return """
            MATCH (start:LegalEntity {name: $entity_name})
            OPTIONAL MATCH (start)-[r:RELATES_TO]-(related:LegalEntity)
            RETURN start as main_entity, 
                   COALESCE(COLLECT(DISTINCT related), []) as related_nodes,
                   COALESCE(COLLECT(DISTINCT r), []) as related_rels
        """
    elif depth == 2:
        return """
            MATCH (start:LegalEntity {name: $entity_name})
            OPTIONAL MATCH (start)-[r1:RELATES_TO]-(related1:LegalEntity)
            OPTIONAL MATCH (related1)-[r2:RELATES_TO]-(related2:LegalEntity)
            RETURN start as main_entity,
                   COALESCE(COLLECT(DISTINCT related1) + COALESCE(COLLECT(DISTINCT related2), []), []) as related_nodes,
                   COALESCE(COLLECT(DISTINCT r1) + COALESCE(COLLECT(DISTINCT r2), []), []) as related_rels
        """
    else:  # depth >= 3 - FIXED VERSION
        return """
            MATCH (start:LegalEntity {name: $entity_name})
            OPTIONAL MATCH path = (start)-[r:RELATES_TO*1..3]-(related:LegalEntity)
            WITH start, 
                 [node IN nodes(path) WHERE node <> start] as all_nodes,
                 [rel IN relationships(path) WHERE rel IS NOT NULL] as all_rels
            RETURN start as main_entity,
                   COALESCE(all_nodes, []) as related_nodes,
                   COALESCE(all_rels, []) as related_rels
        """
