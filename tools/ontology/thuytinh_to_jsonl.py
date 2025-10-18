import re, os, json, argparse

# ---- Helpers ----
def parse_kv(s: str):
    props = {}
    s = (s or "").strip()
    if not s:
        return props
    for part in s.split(";"):
        part = part.strip()
        if not part:
            continue
        if "=" in part:
            k, v = part.split("=", 1)
            props[k.strip()] = auto_cast(v.strip())
        else:
            props[part] = True
    return props

def auto_cast(v: str):
    # cố gắng ép kiểu nhẹ
    if v.lower() in ("true","false"): return v.lower() == "true"
    try:
        if "." in v: return float(v)
        return int(v)
    except ValueError:
        return v

def labels_from(s: str):
    if not s: return []
    s = s.strip()
    if "|" in s: return [x.strip() for x in s.split("|") if x.strip()]
    if "," in s: return [x.strip() for x in s.split(",") if x.strip()]
    return [s]

# ---- Parsers ----
re_pipe_node = re.compile(r"^NODE\|(?P<labels>[^|]+)\|(?P<id>[^|]+)\|?(?P<kv>.*)$", re.IGNORECASE)
re_pipe_edge = re.compile(r"^EDGE\|(?P<etype>[^|]+)\|(?P<flabel>[^|]+)\|(?P<fid>[^|]+)\|(?P<tlabel>[^|]+)\|(?P<tid>[^|]+)\|?(?P<kv>.*)$", re.IGNORECASE)

def try_parse_pipe(line: str):
    m = re_pipe_node.match(line)
    if m:
        labels = labels_from(m.group("labels"))
        return {"op":"upsert","node":{"labels":labels,"id":m.group("id").strip(),"props":parse_kv(m.group("kv"))}}
    m = re_pipe_edge.match(line)
    if m:
        return {"op":"upsert","edge":{
            "type": m.group("etype").strip(),
            "from": {"labels": labels_from(m.group("flabel")), "id": m.group("fid").strip()},
            "to":   {"labels": labels_from(m.group("tlabel")), "id": m.group("tid").strip()},
            "props": parse_kv(m.group("kv")),
        }}
    return None

def iter_code_blocks(text: str):
    # lấy các khối ``` ... ```
    for block in re.findall(r"```(?:[^\n]*)\n(.*?)```", text, flags=re.DOTALL):
        for raw in block.splitlines():
            line = raw.strip()
            if line:
                yield line

def iter_all_lines(text: str):
    for raw in text.splitlines():
        line = raw.strip()
        if line:
            yield line

def convert(input_path: str, output_path: str):
    with open(input_path, "r", encoding="utf-8") as f:
        content = f.read()

    out = []
    # 1) ưu tiên: JSON valid từng dòng trong toàn văn
    #    (chúng ta bắt các khối code trước, nếu vẫn không có thì duyệt toàn văn)
    any_json = False

    # thử quét khối code trước
    for line in iter_code_blocks(content):
        if line.startswith("{") and line.endswith("}"):
            try:
                obj = json.loads(line)
                if "op" in obj and ("node" in obj or "edge" in obj):
                    out.append(obj); any_json = True
            except Exception:
                pass
        else:
            obj = try_parse_pipe(line)
            if obj: out.append(obj)

    # nếu không có gì từ khối code, duyệt toàn văn
    if not out:
        for line in iter_all_lines(content):
            if line.startswith("{") and line.endswith("}"):
                try:
                    obj = json.loads(line)
                    if "op" in obj and ("node" in obj or "edge" in obj):
                        out.append(obj); any_json = True
                except Exception:
                    pass
            else:
                obj = try_parse_pipe(line)
                if obj: out.append(obj)

    # ghi file
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as w:
        for obj in out:
            w.write(json.dumps(obj, ensure_ascii=False) + "\n")

    return len(out)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()
    n = convert(args.input, args.output)
    print(f"[DONE] wrote {n} lines to {args.output}")
    if n == 0:
        print("[HINT] Không phát hiện dòng hợp lệ.")
        print(" - Bạn có thể thêm dòng theo dạng:")
        print("   NODE|LawArticle|A001|title=Điều 1;year=2024")
        print("   EDGE|REFERS_TO|LawArticle|A001|LawConcept|K002|note=tham chiếu")
        print(" - Hoặc dán trực tiếp JSON mục tiêu vào file .txt (mỗi dòng 1 JSON).")

if __name__ == "__main__":
    main()

