import json, time
import psycopg2
from confluent_kafka import Producer
from kafka_config import producer_conf

def fetch_outbox(conn, batch=500):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, topic, key, payload, headers
            FROM outbox_events
            WHERE published = FALSE
            ORDER BY created_at ASC
            LIMIT %s FOR UPDATE SKIP LOCKED
        """, (batch,))
        rows = cur.fetchall()
    return rows

def mark_published(conn, ids):
    with conn.cursor() as cur:
        cur.execute("UPDATE outbox_events SET published=TRUE WHERE id = ANY(%s)", (ids,))
    conn.commit()

def main():
    p = Producer(producer_conf())
    conn = psycopg2.connect(dsn=os.getenv("PG_DSN"))

    while True:
        rows = fetch_outbox(conn)
        if not rows:
            time.sleep(0.5); continue
        ids = []
        for (id_, topic, key, payload, headers) in rows:
            p.produce(topic=topic, key=key, value=json.dumps(payload), headers=[(k,str(v)) for k,v in (headers or {}).items()])
            ids.append(id_)
        p.flush()
        mark_published(conn, ids)

if __name__ == "__main__":
    main()
