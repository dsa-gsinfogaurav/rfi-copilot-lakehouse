"""Export executed job-run notebooks (with their real outputs) into the repo as text.

For every notebook task in a job run, pulls the rendered run via `databricks jobs export-run`, decodes the embedded
notebook model, and writes:
  evidence/notebooks/<task>.ipynb   - Jupyter notebook with cell outputs (renders on GitHub)
  evidence/notebooks/<task>.md      - plain-text transcript: code + output for every cell
Usage: python3 scripts/export_evidence.py <job_run_id> [profile]
"""
import base64, html, json, os, re, subprocess, sys, urllib.parse

RUN = sys.argv[1]; PROFILE = sys.argv[2] if len(sys.argv) > 2 else "fe-vm-gauravsharma"
OUT = os.path.join(os.path.dirname(__file__), "..", "evidence", "notebooks"); os.makedirs(OUT, exist_ok=True)

def cli(*a):
    return json.loads(subprocess.run(["databricks", "-p", PROFILE, *a, "-o", "json"], capture_output=True, text=True, check=True).stdout)

EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[a-z]{2,}")
def redact(s):  # public repo: no personal identifiers in committed evidence
    return EMAIL.sub(lambda m: "build-user@example.com" if m.group(0).lstrip("n").startswith("gaurav.sharma@") else "other-principal@example.com", s or "")

def strip(s):
    s = re.sub(r"<br\s*/?>", "\n", s or ""); s = re.sub(r"<[^>]+>", "", s); return html.unescape(s)

def table_text(res):
    cols = [c.get("name") for c in res.get("schema", [])]; rows = res.get("data", [])[:30]
    if not cols: return ""
    lines = [" | ".join(cols), " | ".join("---" for _ in cols)] + [" | ".join(str(x) for x in r) for r in rows]
    if len(res.get("data", [])) > 30: lines.append(f"... ({len(res['data'])} rows total)")
    return "\n".join(lines)

def result_text(r):
    if not r: return ""
    t = r.get("type"); d = r.get("data")
    if t == "table": return table_text(r)
    if t == "listResults": return "\n\n".join(result_text(x) for x in d or [])
    if t == "error" or r.get("cause"): return "ERROR: " + strip(r.get("summary") or "") + "\n" + strip(r.get("cause") or "")[:3000]
    if isinstance(d, str): return strip(d)
    return strip(json.dumps(d))[:5000] if d else ""

run = cli("jobs", "get-run", RUN)
index = []
for t in run.get("tasks", []):
    if "notebook_task" not in t:
        index.append((t["task_key"], t["state"].get("result_state"), "pipeline/other task")); continue
    views = cli("jobs", "export-run", str(t["run_id"]), "--views-to-export", "CODE").get("views", [])
    page = views[0]["content"]
    m = re.search(r"__DATABRICKS_NOTEBOOK_MODEL = '([^']+)'", page)
    model = json.loads(urllib.parse.unquote(base64.b64decode(m.group(1)).decode("utf-8")))
    cells, md = [], [f"# {t['task_key']} — executed notebook transcript\n", f"Job run {RUN}, task run {t['run_id']}, result: **{t['state'].get('result_state')}**\n"]
    for c in sorted(model.get("commands", []), key=lambda x: x.get("position", 0)):
        src = redact(c.get("command", ""))
        out = "\n".join(l for l in result_text(c.get("results")).splitlines()
                        if not any(x in l for x in ("Effective usage policy", "application/vnd.jupyter.widget-view", "warnings.warn(", "UserWarning: Hint"))).strip()
        out = redact(out)
        if src.lstrip().startswith("%md"):
            text = re.sub(r"^\s*%md\s?", "", src)
            cells.append({"cell_type": "markdown", "metadata": {}, "source": text}); md.append(text + "\n"); continue
        cells.append({"cell_type": "code", "metadata": {}, "execution_count": None, "source": src,
                      "outputs": [{"output_type": "stream", "name": "stdout", "text": out}] if out else []})
        md.append("```python\n" + src.strip() + "\n```\n")
        if out: md.append("**Output:**\n\n```text\n" + out.strip()[:12000] + "\n```\n")
    nb = {"nbformat": 4, "nbformat_minor": 5, "metadata": {"kernelspec": {"name": "python3", "display_name": "Python 3"},
          "databricks_job_run": {"job_run_id": RUN, "task_run_id": t["run_id"], "result": t["state"].get("result_state")}}, "cells": cells}
    json.dump(nb, open(os.path.join(OUT, f"{t['task_key']}.ipynb"), "w"), indent=1)
    open(os.path.join(OUT, f"{t['task_key']}.md"), "w").write("\n".join(md))
    index.append((t["task_key"], t["state"].get("result_state"), f"{len(cells)} cells"))
    print("exported", t["task_key"], t["state"].get("result_state"), len(cells), "cells")

with open(os.path.join(OUT, "README.md"), "w") as f:
    f.write(f"# Executed notebooks — job run {RUN}\n\nStarted {run.get('start_time')} · state {run['state']}\n\n| task | result | export |\n|---|---|---|\n")
    for k, r, n in index: f.write(f"| {k} | {r} | {n} |\n")
