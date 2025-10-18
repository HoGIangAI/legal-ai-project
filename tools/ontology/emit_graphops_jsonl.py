from __future__ import annotations

import os
import sys
import json
from pathlib import Path
from typing import Iterator, Dict, Any

# ✅ Dùng wait_random_exponential với exp_base cho tương thích mọi version
from tenacity import retry, stop_after_attempt, wait_random_exponential

from common.env import Settings
from common.log import get_logger, new_correlation_id
from common.errors import DataError

# Exit codes thống nhất
EXIT_OK = 0
EXIT_ENV = 2
EXIT_CONN = 3
EXIT_SCHEMA = 4
EXIT_DATA = 5
EXIT_AUDIT = 6
EXIT_UNHANDLED = 7


def read_jsonl(p: Path) -> Iterator[Dict[str, Any]]:
    """Đọc JSONL, trả từng object; raise DataError nếu gặp dòng hỏng."""
    if not p.exists() or p.stat().st_size == 0:
        raise DataError(
            f"Input file invalid or empty: {p}",
            error_code="FILE_INVALID",
            hint="Provide non-empty JSONL file",
        )
    with p.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError as e:
                raise DataError(
                    f"Line {i}: {e}",
                    error_code="JSONL_PARSE",
                    hint="Fix malformed JSON on the indicated line",
                )


@retry(
    stop=stop_after_attempt(3),
    # ⏳ backoff mũ ngẫu nhiên, exp_base=2, max=1.0 → tương thích cả bản cũ và mới
    wait=wait_random_exponential(exp_base=2, max=1.0),
)
def emit_file(input_path: Path, *, dry_run: bool = False) -> int:
    """
    Đọc file JSONL và (tuỳ chọn) gửi lên Kafka topic GraphOps.
    - Khi dry_run=True: KHÔNG tạo Kafka Producer, chỉ log sample.
    - Khi dry_run=False: tạo producer và produce từng bản ghi.
    """
    settings = Settings.load()
    logger = get_logger(os.getenv("LOG_LEVEL", "INFO"))
    corr = new_correlation_id()

    topic = settings.TOPIC_GRAPHOPS
    logger.info(
        "emit_start",
        module="emit_graphops",
        file=str(input_path),
        topic=topic,
        correlation_id=corr,
    )

    # 🔒 CHỈ tạo producer khi KHÔNG dry-run
    producer = None
    if not dry_run:
        from services.ontology.kafka_config import json_producer
        producer = json_producer(settings)

    count = 0
    for obj in read_jsonl(input_path):
        if not isinstance(obj, dict) or obj.get("op") != "upsert" or not (
            ("node" in obj) ^ ("edge" in obj)
        ):
            logger.warning(
                "skip_unsupported_shape",
                module="emit_graphops",
                sample_keys=list(obj.keys()) if isinstance(obj, dict) else str(type(obj)),
                error_code="UNSUPPORTED_SHAPE",
                hint="Expect {'op':'upsert','node':{...}} or {'op':'upsert','edge':{...}}",
                correlation_id=corr,
            )
            continue

        if dry_run:
            logger.info("emit_dry_run", module="emit_graphops", sample=obj, correlation_id=corr)
            count += 1
            continue

        try:
            payload = json.dumps(obj, ensure_ascii=False).encode("utf-8")
            assert producer is not None
            producer.produce(topic, value=payload)
            count += 1
        except Exception as e:
            logger.error(
                "produce_failed",
                module="emit_graphops",
                error=str(e),
                error_code="KAFKA_PRODUCE",
                hint="Check broker connectivity & topic",
                correlation_id=corr,
                extra={"topic": topic},
            )

    if producer is not None:
        try:
            producer.flush()
        except Exception as e:
            logger.error(
                "producer_flush_failed",
                module="emit_graphops",
                error=str(e),
                error_code="KAFKA_FLUSH",
                correlation_id=corr,
            )

    logger.info("emit_done", module="emit_graphops", count=count, correlation_id=corr)
    return EXIT_OK


def _main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Emit GraphOps JSONL → Kafka")
    ap.add_argument(
        "--input",
        required=False,
        default=os.getenv("ONTOLOGY_FILE", "./data/ontology/ontology_thuytinh_dev.jsonl"),
        help="Path to JSONL input file",
    )
    ap.add_argument("--json", action="store_true", help="Print JSON summary to stdout")
    ap.add_argument("--verbose", action="store_true", help="Verbose logging (LOG_LEVEL=DEBUG)")
    ap.add_argument("--dry-run", action="store_true", help="Do not connect Kafka; just parse & log")
    args = ap.parse_args()

    if args.verbose:
        os.environ["LOG_LEVEL"] = "DEBUG"

    try:
        code = emit_file(Path(args.input), dry_run=args.dry_run)
        if args.json:
            print(json.dumps({"ok": code == EXIT_OK, "input": args.input, "dry_run": args.dry_run}, ensure_ascii=False))
        return code
    except DataError as e:
        print(json.dumps({
            "ok": False,
            "error": str(e),
            "error_code": getattr(e, "error_code", "DATA_INVALID"),
            "hint": getattr(e, "hint", None),
        }, ensure_ascii=False))
        return EXIT_DATA
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e), "error_code": "UNHANDLED"}, ensure_ascii=False))
        return EXIT_UNHANDLED


if __name__ == "__main__":
    sys.exit(_main())

