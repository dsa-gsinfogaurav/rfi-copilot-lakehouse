# genai_rag_classify — executed notebook transcript

Job run 111509992172372, task run 163765045862470, result: **SUCCESS**

# 04 · Gen AI — spec knowledge base, root-cause classification, cited RFI drafts
1. **Vector Search** delta-sync index over the parsed specification book (managed `gte-large-en` embeddings).
2. **`ai_classify`** (AI Functions, SQL) tags every RFI with a root cause -> the "why are we writing RFIs" signal
   for preconstruction (design conflict vs missing info vs substitution...). Evaluated against the generator's hidden label.
3. **RAG**: for every open RFI, retrieve the governing spec sections and have Claude (Foundation Model API) draft a
   cited response. Retrieval quality is measured (hit@4 = governing section retrieved) and drafts land in `rfi_ai_drafts`.

```python
%pip install -q "openai>=1.52" "databricks-sdk>=0.60"
```

**Output:**

```text
[43mNote: you may need to restart the kernel using %restart_python or dbutils.library.restartPython() to use updated packages.[0m
```

```python
dbutils.library.restartPython()
```

```python
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
```

## 1 · Knowledge base table (CDF) + Vector Search index

```python
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
```

**Output:**

```text
spec_chunks rows: 25
VS endpoint cdi_vs ONLINE
index exists - sync triggered
Index gauravsharma_catalog.rfi_intel.spec_chunks_index ready, indexed rows = 25
```

## 2 · `ai_classify` root cause for every RFI (batch AI Functions in SQL)

```python
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
```

**Output:**

```text
root_cause | n
--- | ---
Spec ambiguity | 1065
Design conflict | 785
Field condition | 749
Missing information | 721
Substitution request | 462
Constructability | 13

ai_classify agreement with ground-truth label: 74.0% over 3,795 RFIs


truth | predicted | n
--- | --- | ---
Constructability | Field condition | 80
Constructability | Spec ambiguity | 21
Design conflict | Design conflict | 563
Design conflict | Spec ambiguity | 189
Design conflict | Field condition | 42
Design conflict | Substitution request | 29
Design conflict | Constructability | 13
Design conflict | Missing information | 6
Field condition | Field condition | 609
Field condition | Missing information | 12
Field condition | Spec ambiguity | 11
Field condition | Design conflict | 11
Missing information | Missing information | 682
Missing information | Spec ambiguity | 208
Missing information | Design conflict | 5
Missing information | Field condition | 3
Spec ambiguity | Spec ambiguity | 522
Spec ambiguity | Design conflict | 202
Spec ambiguity | Field condition | 15
Spec ambiguity | Missing information | 4
Spec ambiguity | Substitution request | 1
Substitution request | Substitution request | 432
Substitution request | Spec ambiguity | 114
Substitution request | Missing information | 17
Substitution request | Design conflict | 4
```

## 3 · RAG: retrieve governing spec sections + cited Claude draft for each open RFI

```python
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
```

**Output:**

```text
/home/spark-f75bbd60-6a49-4d6c-9f6a-73/.ipykernel/66/command-7021091486904085-2121273687:5: DeprecationWarning: get_open_ai_client() is deprecated. Please install the databricks-openai package and use 'from databricks_openai import DatabricksOpenAI' instead. See https://api-docs.databricks.com/python/databricks-ai-bridge/latest/databricks_openai.html for more information.
  oai = w.serving_endpoints.get_open_ai_client()



=== RFI-108-03798 | Fixture efficacy below spec
Q: The proposed troffer is rated 105 lm/W. Is this acceptable against the specified efficacy?
retrieved: ['08 41 13', '21 13 13', '23 05 93', '05 12 00'] (governing 26 51 00, hit=False)
DRAFT: The supplied specification excerpts (08 41 13, 21 13 13, 23 05 93, and 05 12 00) contain no lighting section and no luminaire efficacy requirement, so we cannot confirm whether a 105 lm/W troffer is acceptable. The specified minimum efficacy (lm/W) must come from the lighting fixture section (e.g., Division 26 luminaire requirements) or the electrical drawings' fixture schedule, neither of which was provided. The design team (Electrical Engineer of Record) must state the required minimum efficacy and confirm whether the proposed 105 lm/W fixture is acceptable or must be substituted. Also note that if the substitute fixture changes ceiling layout and causes sprinkler head relocation, a revised hydraulic calculation stamped by the fire protection engineer would be required (per 21 13 13).

=== RFI-104-02326 | Slab moisture before flooring
Q: The slab at Basement is testing at 82% RH. The flooring installer asks whether they can proceed. What is the maximum allowable moisture?
retrieved: ['09 65 00', '09 29 00', '31 23 00', '03 30 00'] (governing 09 65 00, hit=True)
DRAFT: The maximum allowable substrate moisture is 75% RH per ASTM F2170 before installation (per 09 65 00). The Basement slab at 82% RH exceeds this limit, so the flooring installer may not proceed with the resilient sheet flooring as-is. Installation may proceed only if a manufacturer-approved moisture mitigation system is applied to the slab (per 09 65 00). The excerpts do not identify the specific mitigation product or whether the Basement flooring falls under the 'patient and classroom areas' scope of 09 65 00, so the design team must confirm the applicable flooring type and approve the mitigation system and manufacturer submittal. The other excerpts (09 29 00, 31 23 00, 03 30 00) do not address slab moisture limits.

=== RFI-105-02905 | Door rating at corridor openings
Q: The door schedule lists openings at Level 3 in the 1-hour rated corridor but does not specify a door fire rating. What minimum fire rating is required?
retrieved: ['08 11 13', '21 13 13', '26 51 00', '09 29 00'] (governing 08 11 13, hit=True)
DRAFT: Doors in 1-hour rated corridors must carry a minimum 20-minute fire rating with positive latching (per 08 11 13). The Level 3 corridor openings in the door schedule should therefore be annotated as 20-minute rated, positive-latching hollow metal doors, with 14-gauge frames and reinforcement for all hardware (per 08 11 13). The excerpts do not address whether any specific Level 3 openings are exceptions, such as shaft or stair doors, which would require 90-minute ratings (per 08 11 13). The design team should confirm the schedule revision and verify the rating against the governing code and rated-wall drawings, since none of those were provided.

Retrieval hit@4 (governing section retrieved): 62/65 = 95.4%
Drafts whose citations are all within retrieved context (no hallucinated sections): 64/65
Draft failures: 0/65
```

```python
dbutils.fs.put(f"/Volumes/{catalog}/{schema}/evidence/04_genai.json", json.dumps(ev, indent=2, default=str), overwrite=True)
```

**Output:**

```text
Wrote 8227 bytes.


{"text/plain": "True"}
```
