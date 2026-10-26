# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Gen AI — spec knowledge base, root-cause classification, cited RFI drafts
# MAGIC 1. **Vector Search** delta-sync index over the parsed specification book (managed `gte-large-en` embeddings).
# MAGIC 2. **`ai_classify`** (AI Functions, SQL) tags every RFI with a root cause -> the "why are we writing RFIs" signal
# MAGIC    for preconstruction (design conflict vs missing info vs substitution...). Evaluated against the generator's hidden label.
# MAGIC 3. **RAG**: for every open RFI, retrieve the governing spec sections and have Claude (Foundation Model API) draft a
# MAGIC    cited response. Retrieval quality is measured (hit@4 = governing section retrieved) and drafts land in `rfi_ai_drafts`.

# COMMAND ----------
# MAGIC %pip install -q "openai>=1.52" "databricks-sdk>=0.60"

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "gauravsharma_catalog")
dbutils.widgets.text("schema", "rfi_intel")
dbutils.widgets.text("vs_endpoint", "rfi_intel_vs")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-4-6")
dbutils.widgets.text("embedding_model", "databricks-gte-large-en")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema")
VS_EP = dbutils.widgets.get("vs_endpoint"); LLM = dbutils.widgets.get("llm_endpoint"); EMB = dbutils.widgets.get("embedding_model")
FQ = f"{catalog}.{schema}"; INDEX = f"{FQ}.spec_chunks_index"
import json, time, re
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
ev = {}

# COMMAND ----------
# MAGIC %md ## 1 · Knowledge base table (CDF) + Vector Search index

# COMMAND ----------
spark.sql(f"""CREATE TABLE IF NOT EXISTS {FQ}.spec_chunks (chunk_id STRING NOT NULL, doc_name STRING, division STRING,
              section STRING, title STRING, content STRING, updated_at TIMESTAMP)
              TBLPROPERTIES (delta.enableChangeDataFeed = true)
              COMMENT 'Specification knowledge base (one chunk per CSI section). Source of the Vector Search index.'""")
spark.sql(f"""MERGE INTO {FQ}.spec_chunks t USING {FQ}.silver_spec_chunks s ON t.chunk_id = s.chunk_id
              WHEN MATCHED AND t.content <> s.content THEN UPDATE SET t.content = s.content, t.title = s.title, t.updated_at = current_timestamp()
              WHEN NOT MATCHED THEN INSERT (chunk_id, doc_name, division, section, title, content, updated_at)
                   VALUES (s.chunk_id, s.doc_name, s.division, s.section, s.title, s.content, current_timestamp())""")
print("spec_chunks rows:", spark.table(f"{FQ}.spec_chunks").count())

from databricks.sdk.service.vectorsearch import (EndpointType, VectorIndexType, DeltaSyncVectorIndexSpecRequest,
                                                 EmbeddingSourceColumn, PipelineType)
if VS_EP not in {e.name for e in w.vector_search_endpoints.list_endpoints()}:
    getattr(w.vector_search_endpoints, 'create_endpoint_no_wait', None) and w.vector_search_endpoints.create_endpoint_no_wait(name=VS_EP, endpoint_type=EndpointType.STANDARD)
for _ in range(80):
    st = w.vector_search_endpoints.get_endpoint(VS_EP).endpoint_status.state.value
    if st == "ONLINE": break
    print("endpoint:", st); time.sleep(30)
print("VS endpoint", VS_EP, "ONLINE")
try:
    w.vector_search_indexes.get_index(INDEX)
    w.vector_search_indexes.sync_index(INDEX); print("index exists - sync triggered")
except Exception:
    w.vector_search_indexes.create_index(
        name=INDEX, endpoint_name=VS_EP, primary_key="chunk_id", index_type=VectorIndexType.DELTA_SYNC,
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(source_table=f"{FQ}.spec_chunks", pipeline_type=PipelineType.TRIGGERED,
            embedding_source_columns=[EmbeddingSourceColumn(name="content", embedding_model_endpoint_name=EMB)]))
    print("index created")
for _ in range(80):
    idx = w.vector_search_indexes.get_index(INDEX)
    if idx.status.ready and (idx.status.indexed_row_count or 0) > 0: break
    print("index ready:", idx.status.ready, getattr(idx.status, "message", "")); time.sleep(30)
print(f"Index {INDEX} ready, indexed rows = {idx.status.indexed_row_count}")
ev["vector_index"] = {"name": INDEX, "endpoint": VS_EP, "indexed_rows": idx.status.indexed_row_count, "embedding_model": EMB}

# COMMAND ----------
# MAGIC %md ## 2 · `ai_classify` root cause for every RFI (batch AI Functions in SQL)

# COMMAND ----------
LABELS = ["Design conflict", "Missing information", "Spec ambiguity", "Field condition", "Substitution request", "Constructability"]
spark.sql(f"""CREATE OR REPLACE TABLE {FQ}.gold_rfi_root_cause
COMMENT 'Gen-AI root cause per RFI (ai_classify over subject + question). Drives preconstruction prevention.'
AS SELECT rfi_id, project_id, discipline, spec_section,
          ai_classify(concat(subject, '. ', question), array({', '.join(repr(l) for l in LABELS)})) AS root_cause
   FROM {FQ}.gold_rfi_facts""")
rc = spark.sql(f"SELECT root_cause, count(*) n FROM {FQ}.gold_rfi_root_cause GROUP BY 1 ORDER BY 2 DESC")
display(rc); ev["root_cause_distribution"] = {r.root_cause: r.n for r in rc.collect()}

# evaluate against the synthetic generator's hidden label (only available because the data is synthetic)
acc = spark.sql(f"""SELECT round(avg(CASE WHEN c.root_cause = b._root_cause_truth THEN 1 ELSE 0 END), 3) AS accuracy, count(*) AS n
                    FROM {FQ}.gold_rfi_root_cause c JOIN (SELECT DISTINCT rfi_id, _root_cause_truth FROM {FQ}.bronze_rfis) b USING (rfi_id)""").first()
print(f"ai_classify agreement with ground-truth label: {acc.accuracy:.1%} over {acc.n:,} RFIs")
cm = spark.sql(f"""SELECT b._root_cause_truth AS truth, c.root_cause AS predicted, count(*) n
                   FROM {FQ}.gold_rfi_root_cause c JOIN (SELECT DISTINCT rfi_id, _root_cause_truth FROM {FQ}.bronze_rfis) b USING (rfi_id)
                   GROUP BY 1, 2 ORDER BY 1, 3 DESC""")
display(cm)
ev["ai_classify_accuracy"] = acc.accuracy; ev["confusion"] = [r.asDict() for r in cm.collect()]

# COMMAND ----------
# MAGIC %md ## 3 · RAG: retrieve governing spec sections + cited Claude draft for each open RFI

# COMMAND ----------
SYSTEM = ("You are a senior project engineer at a general contractor drafting a formal RFI response. Ground every statement ONLY "
          "in the supplied specification excerpts and cite section numbers inline, e.g. (per 03 30 00). If the excerpts do not answer "
          "the question, say what the design team must decide. Return ONLY JSON: {\"response\": \"<3-5 sentences>\", "
          "\"cited_sections\": [\"<section>\"], \"answerable_from_specs\": true|false, \"clarity_score\": <0-100>}")
oai = w.serving_endpoints.get_open_ai_client()

def retrieve(q, k=4):
    r = w.vector_search_indexes.query_index(index_name=INDEX, columns=["section", "title", "content"], query_text=q, num_results=k)
    cols = [c.name for c in r.manifest.columns]
    return [dict(zip(cols, x)) for x in (r.result.data_array or [])]

def _unesc(x):
    try: return json.loads('"' + x + '"')
    except Exception: return x.replace('\\"', '"').replace("\\n", "\n")

def _text(content):
    if isinstance(content, str): return content
    return "".join(p.get("text", "") if isinstance(p, dict) else (getattr(p, "text", "") or "") for p in (content or []) if (p.get("type") if isinstance(p, dict) else getattr(p, "type", "text")) == "text")

SEC_RE = re.compile(r"\b\d\d \d\d \d\d\b")
def parse_json(out):
    out = re.sub(r"^```(?:json)?\s*|\s*```$", "", out.strip())
    m = re.search(r"\{.*\}", out, re.S)
    try: return json.loads(m.group(0))
    except Exception:
        r = re.search(r'"response"\s*:\s*"((?:[^"\\]|\\.)*)', out, re.S)
        return {"response": (_unesc(r.group(1)) if r else out), "answerable_from_specs": None, "clarity_score": None}

def draft(rfi, chunks):
    ctx = "\n\n".join(f"[{c['section']} - {c['title']}]\n{c['content']}" for c in chunks)
    out = oai.chat.completions.create(model=LLM, max_tokens=1200, messages=[
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": f"RFI {rfi['rfi_id']} ({rfi['discipline']}): {rfi['subject']}\n{rfi['question']}\n\nSPEC EXCERPTS:\n{ctx}"}]).choices[0].message.content
    d = parse_json(_text(out))
    d["cited_sections"] = sorted(set(SEC_RE.findall(d.get("response") or "")))  # citations as they appear inline in the answer
    return d

open_rfis = [r.asDict() for r in spark.sql(f"SELECT rfi_id, discipline, spec_section, subject, question FROM {FQ}.gold_rfi_facts WHERE is_open ORDER BY submitted_at").collect()]
rows, hits = [], 0
for i, r in enumerate(open_rfis):
    ch = retrieve(r["question"])
    hit = r["spec_section"] in [c["section"] for c in ch]; hits += hit
    try: d = draft(r, ch)
    except Exception as e: d = {"response": f"(draft failed: {e})", "cited_sections": []}
    rows.append((r["rfi_id"], d.get("response"), ", ".join(d.get("cited_sections") or []), ", ".join(c["section"] for c in ch),
                 bool(hit), d.get("answerable_from_specs"), d.get("clarity_score"), LLM))
    if i < 3:
        print(f"\n=== {r['rfi_id']} | {r['subject']}\nQ: {r['question']}\nretrieved: {[c['section'] for c in ch]} (governing {r['spec_section']}, hit={hit})\nDRAFT: {d.get('response')}")
schema_ddl = "rfi_id string, draft_response string, cited_sections string, retrieved_sections string, retrieval_hit boolean, answerable_from_specs boolean, clarity_score int, model string"
(spark.createDataFrame(rows, schema_ddl).selectExpr("*", "current_timestamp() AS generated_at")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.rfi_ai_drafts"))
spark.sql(f"COMMENT ON TABLE {FQ}.rfi_ai_drafts IS 'Batch RAG drafts (Vector Search + Claude via Foundation Model API) for every open RFI, pending human approval'")
grounded = sum(1 for x in rows if x[2] and all(s.strip() in x[3] for s in x[2].split(",")))
print(f"\nRetrieval hit@4 (governing section retrieved): {hits}/{len(rows)} = {hits / max(len(rows), 1):.1%}")
print(f"Drafts whose citations are all within retrieved context (no hallucinated sections): {grounded}/{len(rows)}")
failed = sum(1 for x in rows if (x[1] or "").startswith("(draft failed"))
print(f"Draft failures: {failed}/{len(rows)}")
assert failed <= len(rows) * 0.1, "too many LLM draft failures"
ev["rag"] = {"draft_failures": failed, "open_rfis": len(rows), "hit_at_4": round(hits / max(len(rows), 1), 3), "grounded_citations": grounded,
             "samples": [dict(zip(["rfi_id", "draft", "cited", "retrieved", "hit"], x[:5])) for x in rows[:5]]}

# COMMAND ----------
dbutils.fs.put(f"/Volumes/{catalog}/{schema}/evidence/04_genai.json", json.dumps(ev, indent=2, default=str), overwrite=True)
