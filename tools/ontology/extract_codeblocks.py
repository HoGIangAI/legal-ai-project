import os, re, sys, pathlib

SRC = os.path.expanduser(
    "~/legal_ai_infrastructure/legal-ai-implementation/legal-ai-platform/data/ontology/ontology_thuytinh_source.txt"
)
OUTDIR = os.path.expanduser(
    "~/legal_ai_infrastructure/legal-ai-implementation/legal-ai-platform/build/extracted"
)

def main():
    pathlib.Path(OUTDIR).mkdir(parents=True, exist_ok=True)
    with open(SRC, "r", encoding="utf-8") as f:
        text = f.read()

    # match code fences: ```lang\n...```  (lang optional)
    pattern = re.compile(r"```([^\n`]*)\n(.*?)```", re.DOTALL)
    blocks = list(pattern.finditer(text))
    if not blocks:
        print("[WARN] No fenced code blocks found in source.")
        return

    count = 0
    manifest = []
    for m in blocks:
        count += 1
        lang = (m.group(1) or "").strip()
        body = m.group(2)
        # normalize ext
        ext = "txt"
        if lang:
            # map some common languages to file extensions
            lang_l = lang.lower()
            if "python" in lang_l or lang_l == "py":
                ext = "py"
            elif "bash" in lang_l or "shell" in lang_l or lang_l == "sh":
                ext = "sh"
            elif "json" in lang_l:
                ext = "json"
            elif "txt" in lang_l or "ini" in lang_l or "env" in lang_l:
                ext = "txt"
            elif "yaml" in lang_l or "yml" in lang_l:
                ext = "yml"
            else:
                # try to use the raw lang as extension if safe
                ext = re.sub(r"[^a-z0-9]+", "", lang_l) or "txt"

        out_name = f"block{count:03d}.{ext}"
        out_path = os.path.join(OUTDIR, out_name)
        with open(out_path, "w", encoding="utf-8") as fo:
            fo.write(body)
        manifest.append((out_name, lang))

    # write manifest for quick review
    with open(os.path.join(OUTDIR, "_MANIFEST.txt"), "w", encoding="utf-8") as mf:
        for name, lang in manifest:
            mf.write(f"{name}\t{lang}\n")

    print(f"[OK] Extracted {len(manifest)} code blocks into: {OUTDIR}")
    print(f"[OK] Manifest: {os.path.join(OUTDIR, '_MANIFEST.txt')}")
    print("[TIP] You can preview first lines of each block with:")
    print(f"      head -n 5 {OUTDIR}/block*.py {OUTDIR}/block*.sh {OUTDIR}/block*.txt 2>/dev/null | sed -n '1,50p'")

if __name__ == '__main__':
    main()
