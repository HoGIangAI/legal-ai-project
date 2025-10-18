# Neo4j Diagnostics (V3_Rev2 alignment)
- Generated at: 2025-10-16T05:01:51.341183+00:00

## Connectivity
```json
{
  "ok": true
}
```

## Components / Version / Edition
```json
{
  "dbms.components": [
    {
      "name": "Neo4j Kernel",
      "versions": [
        "5.13.0"
      ],
      "edition": "community"
    }
  ],
  "dbms.procedures": null
}
```

## Plugins (APOC / GDS)
```json
{
  "apoc": [
    {
      "apoc_version": "5.13.0"
    }
  ],
  "gds": null
}
```

## Config (APOC/GDS, Memory)
```json
{
  "apoc_gds": [],
  "memory": [
    {
      "name": "server.memory.heap.initial_size",
      "value": "No Value"
    },
    {
      "name": "server.memory.heap.max_size",
      "value": "No Value"
    },
    {
      "name": "server.memory.pagecache.size",
      "value": "512.00MiB"
    }
  ]
}
```

## Deployment (Databases)
```json
{
  "databases": [
    {
      "name": "neo4j",
      "type": "standard",
      "aliases": [],
      "access": "read-write",
      "address": "localhost:7687",
      "role": "primary",
      "writer": true,
      "requestedStatus": "online",
      "currentStatus": "online",
      "statusMessage": "",
      "default": true,
      "home": true,
      "constituents": []
    },
    {
      "name": "system",
      "type": "system",
      "aliases": [],
      "access": "read-write",
      "address": "localhost:7687",
      "role": "primary",
      "writer": true,
      "requestedStatus": "online",
      "currentStatus": "online",
      "statusMessage": "",
      "default": false,
      "home": false,
      "constituents": []
    }
  ],
  "current_database": null
}
```

## Schema (Labels, Relationship Types, Indexes, Constraints)
```json
{
  "labels": null,
  "relationship_types": null,
  "indexes": [
    {
      "id": 5,
      "name": "constraint_8335a696",
      "state": "ONLINE",
      "populationPercent": 100.0,
      "type": "RANGE",
      "entityType": "NODE",
      "labelsOrTypes": [
        "LegalEntity"
      ],
      "properties": [
        "name"
      ],
      "indexProvider": "range-1.0",
      "owningConstraint": "constraint_8335a696",
      "lastRead": "2025-10-12T20:06:22.147000000+00:00",
      "readCount": 46
    },
    {
      "id": 3,
      "name": "constraint_84afe147",
      "state": "ONLINE",
      "populationPercent": 100.0,
      "type": "RANGE",
      "entityType": "NODE",
      "labelsOrTypes": [
        "LegalEntity"
      ],
      "properties": [
        "id"
      ],
      "indexProvider": "range-1.0",
      "owningConstraint": "constraint_84afe147",
      "lastRead": "2025-10-12T20:06:22.360000000+00:00",
      "readCount": 137
    },
    {
      "id": 1,
      "name": "index_343aff4e",
      "state": "ONLINE",
      "populationPercent": 100.0,
      "type": "LOOKUP",
      "entityType": "NODE",
      "labelsOrTypes": null,
      "properties": null,
      "indexProvider": "token-lookup-1.0",
      "owningConstraint": null,
      "lastRead": "2025-10-16T04:58:09.877000000+00:00",
      "readCount": 2
    },
    {
      "id": 2,
      "name": "index_f7700477",
      "state": "ONLINE",
      "populationPercent": 100.0,
      "type": "LOOKUP",
      "entityType": "RELATIONSHIP",
      "labelsOrTypes": null,
      "properties": null,
      "indexProvider": "token-lookup-1.0",
      "owningConstraint": null,
      "lastRead": null,
      "readCount": 0
    }
  ],
  "constraints": [
    {
      "id": 6,
      "name": "constraint_8335a696",
      "type": "UNIQUENESS",
      "entityType": "NODE",
      "labelsOrTypes": [
        "LegalEntity"
      ],
      "properties": [
        "name"
      ],
      "ownedIndex": "constraint_8335a696",
      "propertyType": null
    },
    {
      "id": 4,
      "name": "constraint_84afe147",
      "type": "UNIQUENESS",
      "entityType": "NODE",
      "labelsOrTypes": [
        "LegalEntity"
      ],
      "properties": [
        "id"
      ],
      "ownedIndex": "constraint_84afe147",
      "propertyType": null
    }
  ],
  "apoc_meta_schema": [
    {
      "value": {
        "LegalEntity": {
          "count": 4,
          "labels": [],
          "properties": {
            "id": {
              "unique": true,
              "indexed": true,
              "type": "STRING",
              "existence": false
            },
            "name": {
              "unique": true,
              "indexed": true,
              "type": "STRING",
              "existence": false
            },
            "type": {
              "unique": false,
              "indexed": false,
              "type": "STRING",
              "existence": false
            }
          },
          "type": "node",
          "relationships": {
            "RELATES_TO": {
              "count": 1,
              "direction": "out",
              "labels": [
                "LegalEntity",
                "LegalEntity"
              ],
              "properties": {
                "type": {
                  "indexed": false,
                  "type": "STRING",
                  "existence": false,
                  "array": false
                }
              }
            }
          }
        },
        "RELATES_TO": {
          "count": 3,
          "properties": {
            "type": {
              "indexed": false,
              "type": "STRING",
              "existence": false,
              "array": false
            }
          },
          "type": "relationship"
        }
      }
    }
  ]
}
```

## Data Stats
```json
{
  "db_stats_retrieve": [
    {
      "section": "GRAPH COUNTS",
      "data": {
        "nodes": [
          {
            "count": 4
          },
          {
            "count": 4,
            "label": "LegalEntity"
          }
        ],
        "indexes": [
          {
            "indexProvider": "token-lookup-1.0",
            "indexType": "LOOKUP",
            "updatesSinceEstimation": 0,
            "labels": [],
            "properties": [],
            "totalSize": 0,
            "estimatedUniqueSize": 0
          },
          {
            "indexProvider": "token-lookup-1.0",
            "indexType": "LOOKUP",
            "updatesSinceEstimation": 0,
            "relationshipTypes": [],
            "properties": [],
            "totalSize": 0,
            "estimatedUniqueSize": 0
          },
          {
            "indexProvider": "range-1.0",
            "indexType": "RANGE",
            "updatesSinceEstimation": 0,
            "labels": [
              "LegalEntity"
            ],
            "properties": [
              "id"
            ],
            "totalSize": 4,
            "estimatedUniqueSize": 4
          },
          {
            "indexProvider": "range-1.0",
            "indexType": "RANGE",
            "updatesSinceEstimation": 0,
            "labels": [
              "LegalEntity"
            ],
            "properties": [
              "name"
            ],
            "totalSize": 4,
            "estimatedUniqueSize": 4
          }
        ],
        "constraints": [
          {
            "label": "LegalEntity",
            "properties": [
              "id"
            ],
            "type": "Uniqueness constraint"
          },
          {
            "label": "LegalEntity",
            "properties": [
              "name"
            ],
            "type": "Uniqueness constraint"
          }
        ],
        "relationships": [
          {
            "count": 3
          },
          {
            "relationshipType": "RELATES_TO",
            "count": 3
          },
          {
            "startLabel": "LegalEntity",
            "relationshipType": "RELATES_TO",
            "count": 3
          },
          {
            "relationshipType": "RELATES_TO",
            "count": 3,
            "endLabel": "LegalEntity"
          }
        ]
      }
    }
  ],
  "count_nodes": [
    {
      "nodes": 4
    }
  ],
  "count_relationships": [
    {
      "rels": 3
    }
  ]
}
```

## Security (Users/Roles)
```json
{
  "users": [
    {
      "user": "neo4j",
      "roles": null,
      "passwordChangeRequired": false,
      "suspended": null,
      "home": null
    }
  ],
  "roles": null
}
```

## Errors (non-fatal)
```json
[
  {
    "query": "CALL dbms.procedures() YIELD name RETURN name ORDER BY name LIMIT 5000",
    "error": "{neo4j_code: Neo.ClientError.Procedure.ProcedureNotFound} {message: There is no procedure with the name `dbms.procedures` registered for this database instance. Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.} {gql_status: 50N42} {gql_status_description: error: general processing exception - unexpected error. There is no procedure with the name `dbms.procedures` registered for this database instance. Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.}"
  },
  {
    "query": "CALL gds.version() YIELD version RETURN version AS gds_version",
    "error": "{neo4j_code: Neo.ClientError.Procedure.ProcedureNotFound} {message: There is no procedure with the name `gds.version` registered for this database instance. Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.} {gql_status: 50N42} {gql_status_description: error: general processing exception - unexpected error. There is no procedure with the name `gds.version` registered for this database instance. Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed.}"
  },
  {
    "query": "SHOW CURRENT DATABASE",
    "error": "{neo4j_code: Neo.ClientError.Statement.SyntaxError} {message: Invalid input 'DATABASE': expected \"USER\" (line 1, column 14 (offset: 13))\n\"SHOW CURRENT DATABASE\"\n              ^} {gql_status: 50N42} {gql_status_description: error: general processing exception - unexpected error. Invalid input 'DATABASE': expected \"USER\" (line 1, column 14 (offset: 13))\n\"SHOW CURRENT DATABASE\"\n              ^}"
  },
  {
    "query": "SHOW LABELS YIELD label RETURN label ORDER BY label",
    "error": "{neo4j_code: Neo.ClientError.Statement.SyntaxError} {message: Invalid input 'LABELS': expected\n  \"ALIAS\"\n  \"ALIASES\"\n  \"ALL\"\n  \"BTREE\"\n  \"BUILT\"\n  \"CONSTRAINT\"\n  \"CONSTRAINTS\"\n  \"CURRENT\"\n  \"DATABASE\"\n  \"DATABASES\"\n  \"DEFAULT\"\n  \"EXIST\"\n  \"EXISTENCE\"\n  \"EXISTS\"\n  \"FULLTEXT\"\n  \"FUNCTION\"\n  \"FUNCTIONS\"\n  \"HOME\"\n  \"INDEX\"\n  \"INDEXES\"\n  \"KEY\"\n  \"LOOKUP\"\n  \"NODE\"\n  \"POINT\"\n  \"POPULATED\"\n  \"PRIVILEGE\"\n  \"PRIVILEGES\"\n  \"PROCEDURE\"\n  \"PROCEDURES\"\n  \"PROPERTY\"\n  \"RANGE\"\n  \"REL\"\n  \"RELATIONSHIP\"\n  \"ROLE\"\n  \"ROLES\"\n  \"SERVER\"\n  \"SERVERS\"\n  \"SETTING\"\n  \"SETTINGS\"\n  \"SUPPORTED\"\n  \"TEXT\"\n  \"TRANSACTION\"\n  \"TRANSACTIONS\"\n  \"UNIQUE\"\n  \"UNIQUENESS\"\n  \"USER\"\n  \"USERS\" (line 1, column 6 (offset: 5))\n\"SHOW LABELS YIELD label RETURN label ORDER BY label\"\n      ^} {gql_status: 50N42} {gql_status_description: error: general processing exception - unexpected error. Invalid input 'LABELS': expected\n  \"ALIAS\"\n  \"ALIASES\"\n  \"ALL\"\n  \"BTREE\"\n  \"BUILT\"\n  \"CONSTRAINT\"\n  \"CONSTRAINTS\"\n  \"CURRENT\"\n  \"DATABASE\"\n  \"DATABASES\"\n  \"DEFAULT\"\n  \"EXIST\"\n  \"EXISTENCE\"\n  \"EXISTS\"\n  \"FULLTEXT\"\n  \"FUNCTION\"\n  \"FUNCTIONS\"\n  \"HOME\"\n  \"INDEX\"\n  \"INDEXES\"\n  \"KEY\"\n  \"LOOKUP\"\n  \"NODE\"\n  \"POINT\"\n  \"POPULATED\"\n  \"PRIVILEGE\"\n  \"PRIVILEGES\"\n  \"PROCEDURE\"\n  \"PROCEDURES\"\n  \"PROPERTY\"\n  \"RANGE\"\n  \"REL\"\n  \"RELATIONSHIP\"\n  \"ROLE\"\n  \"ROLES\"\n  \"SERVER\"\n  \"SERVERS\"\n  \"SETTING\"\n  \"SETTINGS\"\n  \"SUPPORTED\"\n  \"TEXT\"\n  \"TRANSACTION\"\n  \"TRANSACTIONS\"\n  \"UNIQUE\"\n  \"UNIQUENESS\"\n  \"USER\"\n  \"USERS\" (line 1, column 6 (offset: 5))\n\"SHOW LABELS YIELD label RETURN label ORDER BY label\"\n      ^}"
  },
  {
    "query": "SHOW RELATIONSHIP TYPES YIELD relationshipType AS type RETURN type ORDER BY type",
    "error": "{neo4j_code: Neo.ClientError.Statement.SyntaxError} {message: Invalid input 'TYPES': expected\n  \"EXIST\"\n  \"EXISTENCE\"\n  \"EXISTS\"\n  \"KEY\"\n  \"PROPERTY\"\n  \"UNIQUE\"\n  \"UNIQUENESS\" (line 1, column 19 (offset: 18))\n\"SHOW RELATIONSHIP TYPES YIELD relationshipType AS type RETURN type ORDER BY type\"\n                   ^} {gql_status: 50N42} {gql_status_description: error: general processing exception - unexpected error. Invalid input 'TYPES': expected\n  \"EXIST\"\n  \"EXISTENCE\"\n  \"EXISTS\"\n  \"KEY\"\n  \"PROPERTY\"\n  \"UNIQUE\"\n  \"UNIQUENESS\" (line 1, column 19 (offset: 18))\n\"SHOW RELATIONSHIP TYPES YIELD relationshipType AS type RETURN type ORDER BY type\"\n                   ^}"
  },
  {
    "query": "SHOW ROLES",
    "error": "{neo4j_code: Neo.ClientError.Statement.UnsupportedAdministrationCommand} {message: Unsupported administration command: SHOW ROLES} {gql_status: 50N42} {gql_status_description: error: general processing exception - unexpected error. Unsupported administration command: SHOW ROLES}"
  }
]
```