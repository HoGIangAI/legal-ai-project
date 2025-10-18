import json, sys, os

def to_node(label, _id, props):
    return {"op":"upsert","node":{"label":label,"id":_id,"props":props or {}}}

def to_edge(from_label, from_id, rel_type, to_label, to_id, props):
    return {"op":"upsert","edge":{
        "from":{"label":from_label,"id":from_id},
        "to":{"label":to_label,"id":to_id},
        "type":rel_type,
        "props":props or {}
    }}

def normalize(obj):
    if ("node" in obj) or ("edge" in obj):
        obj.setdefault("op","upsert")
        return obj
    act = obj.get("action","").upper()
    if act in ("CREATE_NODE","UPSERT_NODE"):
        _id = obj.get("id") or obj.get("source_id")
        name = obj.get("name") or (obj.get("properties") or {}).get("name")
        props = obj.get("properties") or {}
        if name and "name" not in props: props["name"]=name
        return to_node("Concept", _id, props)
    if act in ("CREATE_EDGE","LINK_NODE","UPSERT_EDGE"):
        from_id = obj.get("source_id") or obj.get("from_id")
        to_id   = obj.get("target_id") or obj.get("to_id")
        rel     = obj.get("relation") or obj.get("type") or "RELATED_TO"
        props   = obj.get("properties") or {}
        return to_edge("Concept", from_id, rel, "Concept", to_id, props)
    if "id" in obj and ("name" in obj or "props" in obj):
        props = obj.get("props") or {}
        if "name" in obj: props.setdefault("name", obj["name"])
        return to_node("Concept", obj["id"], props)
    raise ValueError(f"Unrecognized record shape: {obj}")

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    total, passed, failed = 0, 0, 0
    with open(args.input, "r", encoding="utf-8") as fin, \
         open(args.output, "w", encoding="utf-8") as fout:
        for line in fin:
            line = line.strip()
            if not line: continue
            total += 1
            try:
                obj = json.loads(line)
                evt = normalize(obj)
                fout.write(json.dumps(evt, ensure_ascii=False)+"\n")
                passed += 1
            except Exception as e:
                failed += 1
                sys.stderr.write(f"[WARN] line {total}: {e}\n")

    print(json.dumps({"total": total, "converted": passed, "failed": failed}, ensure_ascii=False))

if __name__ == "__main__":
    main()
