import json, time, uuid
from confluent_kafka import DeserializingConsumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from kafka_config import consumer_conf, producer_conf, schema_registry_conf, topic_names

MAX_RETRIES = 5

def make_consumer(group_id: str):
    return DeserializingConsumer({
        **consumer_conf(group_id),
        "key.deserializer": StringDeserializer("utf_8"),
        "value.deserializer": None  # set per-topic below if cần
    })

def make_dlq_producer():
    return Producer(producer_conf())

def main():
    tn = topic_names()
    sr = SchemaRegistryClient(schema_registry_conf())
    # Deserializer per topic schema
    doc_deser = AvroDeserializer(sr, None)     # dynamic reading by SR
    intent_deser = AvroDeserializer(sr, None)

    c = make_consumer("cg.rag.ingest")
    c.subscribe([tn["documents"]])

    dlq_topic = tn["documents"] + tn["dlq_suffix"]
    dlq_p = make_dlq_producer()

    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        try:
            key = msg.key()
            # AvroDeserializer cần raw bytes -> confluent handles automatically if registered
            value = msg.value()  # sẽ là dict sau giải mã Avro
            # --- Business handling ---
            # TODO: xử lý idempotent (ví dụ check eventId trong store)
            # giả định đôi khi lỗi transient:
            if value and value.get("qualityScore", 1) < 0.5:
                raise RuntimeError("Low qualityScore, reprocess later")

            # xử lý OK ⇒ commit offset
            c.commit(msg)
            print(f"[OK] {msg.topic()} key={key} offset={msg.offset()}")
        except Exception as e:
            # Đọc/ghi header retry-count
            headers = dict(msg.headers() or [])
            retry_count = int(headers.get("retry-count", 0))
            if retry_count < MAX_RETRIES:
                # requeue bằng cách produce lại vào cùng topic với backoff nhẹ
                time.sleep(0.2 * (retry_count+1))
                dlv_key = msg.key()
                dlv_val = msg.value()
                dlv_headers = [(k, v) for k, v in headers.items() if k != "retry-count"]
                dlv_headers.append(("retry-count", str(retry_count+1)))
                dlq_p.produce(  # dùng producer thường để “parking” tạm thời vào DLQ? hoặc tái gửi topic gốc
                    topic=dlq_topic,  # đối sách: gửi vào DLQ ngay; hoặc gửi lại topic gốc nếu muốn retry in-place
                    key=dlv_key,
                    value=dlv_val,
                    headers=dlv_headers
                )
                dlq_p.flush()
                c.commit(msg)  # commit để tránh “kẹt”; ta sẽ có 1 consumer khác chuyên đọc DLQ
                print(f"[RETRY->{dlq_topic}] {retry_count+1}/{MAX_RETRIES} key={msg.key()}")
            else:
                # Quá số lần ⇒ để nguyên trong DLQ (đã gửi ở lần trước) hoặc gửi vào DLQ 'final'
                print(f"[FAILED] exceeded retries for key={msg.key()} err={e}")
