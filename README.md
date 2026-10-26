# RFI Copilot — answer RFIs before they become delays

**Industry:** Engineering & Construction (general contractors) · **Outcome:** shorter RFI cycle time, fewer SLA breaches, less change-order and liquidated-damages exposure
**Customer (fictional):** Summit Ridge Builders, a general contractor running 8 concurrent projects worth about $1.0B · **All data is synthetic** (generated in `01_generate_raw_data`). No customer data is used.

## The customer problem

A Request for Information (RFI) is how the field asks the design team to clarify a drawing or spec conflict before building it. Large projects
produce hundreds of RFIs (the Navigant Construction Forum study of 1,362 projects found an average of 796 per project and 9.7 days to answer).
Every late answer stalls a crew, and when the activity is on the critical path it pushes the finish date into liquidated damages (LDs).
Project engineers triage RFIs by gut feel, re-type answers that are already in the spec book, and have no portfolio view of which design firm or which
document gap is costing them money.

**What this build does:** it ingests the GC's raw PM-system exports, governs them, predicts which open RFIs will miss their SLA, drafts a cited answer from
the spec book, serves it all to a project-team app from Lakebase in milliseconds, and lets executives ask the portfolio questions in plain English with Genie.

## End-to-end data journey (one integrated chain, one Asset Bundle)

```
raw_landing Volume (JSON RFI exports · CSV firm SLAs · Markdown spec book)        01_generate_raw_data
   │  ① LAKEFLOW  Auto Loader -> bronze -> silver (expectations, dedupe, spec parsing) -> gold   src/pipeline/rfi_lakeflow_pipeline.py
   ▼
gold_rfi_facts · gold_project_kpis · gold_firm_performance · gold_rfi_monthly · silver_spec_chunks
   │  ② UNITY CATALOG  tags, comments, mask_cost() column mask + v_rfi_secure, grants, lineage, DQ evidence    02_unity_catalog_governance
   │  ③ ML   SLA-breach classifier + days-to-answer regressor, MLflow, UC registry @champion -> rfi_risk_scores   03_ml_sla_risk_model
   │  ④ GEN AI  ai_classify root cause (all RFIs) · Vector Search index on spec book · Claude cited drafts -> rfi_ai_drafts   04_genai_rag_and_classify
   ▼
app_rfi_inbox (Delta, CDF)  ──synced table──►  ⑤ LAKEBASE Postgres rfi_ops.rfi_intel.rfi_inbox (≈10 ms reads)     05_lakebase_serving
                                                 ▲   app writes decisions -> ops.rfi_workflow_events (transactional)
                                                 └── 07_workflow_audit_sync -> Delta rfi_workflow_audit (governed)
   ⑥ GENIE space over gold + ML + Gen-AI + audit tables                                                    06_genie_space
   ⑦ DATABRICKS APP  "RFI Copilot" (Dash): Data Journey · Portfolio Insights · RFI Copilot · Approvers (HITL) · Ask Genie   app/app.py
```

| Stage | Databricks capability | Where |
|---|---|---|
| Ingest | Lakeflow Spark Declarative Pipeline (serverless), Auto Loader over JSON/CSV/text, `expect_or_drop` / `expect_or_fail` | `src/pipeline/rfi_lakeflow_pipeline.py` |
| Govern | Unity Catalog: schema/table tags, comments, SQL-UDF column mask + governed view, least-privilege grants to the app service principal, lineage (`system.access.table_lineage`), pipeline event-log DQ | `src/notebooks/02_unity_catalog_governance.py`, `05` (grants) |
| ML | scikit-learn HistGradientBoosting, MLflow tracking, Unity Catalog model registry with `@champion` alias, batch scoring | `src/notebooks/03_ml_sla_risk_model.py` |
| Gen AI | `ai_classify` (AI Functions), Vector Search delta-sync index with managed `gte-large-en` embeddings, Foundation Model API (Claude) RAG with retrieval evaluation | `src/notebooks/04_genai_rag_and_classify.py`, `app/app.py` |
| Operational serving | Lakebase Postgres instance, synced table (Delta → Postgres), transactional write table, app resource binding with OAuth DB credentials | `src/notebooks/05_lakebase_serving.py`, `07_workflow_audit_sync.py` |
| Natural language | AI/BI Genie space (instructions, example SQL, sample questions) + Genie Conversation API embedded in the app | `src/notebooks/06_genie_space.py`, app "Ask Genie" tab |
| Surface | Databricks App (Dash + gunicorn), runs as its own service principal | `app/` |
| Orchestration / IaC | Databricks Asset Bundle: schema, Lakebase instance, pipeline, 2 Lakeflow Jobs, app | `databricks.yml` |

## Evidence the build ran (all text, committed in `evidence/`)

* `evidence/notebooks/*.md` and `*.ipynb`: every notebook task from the final job run, **with cell outputs**, exported from the Databricks job run
  (`scripts/export_evidence.py`). Start with `evidence/notebooks/README.md`.
* `evidence/run_summary.md`: job run ID, task results, pipeline update and expectation results, and the headline metrics.
* `evidence/step_outputs/*.json`: the machine-readable outputs each step wrote to the `evidence` Volume (row counts, DQ, model metrics, RAG samples, Lakebase latency, Genie Q&A with generated SQL).
* `evidence/app_smoke_test.json`: live calls against the deployed app (Lakebase read, warehouse query, Vector Search, live Claude draft, Genie answer, Lakebase write that triggers the audit job).

## Human in the loop

Clicking **Approve & send to design team** runs an approval policy. High ML SLA-risk, AI-assessed Medium/High cost or schedule impact, Urgent priority, or an LD rate of $50K/day or more
sends the response to the **Approvers** tab for a named approver to sign off or return with a comment. Low-risk answers are released directly.
Every button opens a live trace of what is happening: the Lakebase commit time, status change, the audit-sync Lakeflow Job run (polled until it finishes), and confirmation that the event landed in the Delta audit table.
Each RFI also shows a lifecycle stage bar and its full audit trail.

## Business deck

`deck/RFI_Copilot_Business_Case.md` (and the PDF export next to it) leads with the outcome and quantifies impact in the GC's own KPIs for the executive sponsor (COO) and the domain owner (Director of Project Controls).

## Deploy it yourself

```bash
databricks bundle deploy -t dev                      # schema, Lakebase instance, pipeline, jobs, app
databricks bundle run rfi_build -t dev               # raw data -> Lakeflow -> UC -> ML -> Gen AI -> Lakebase -> Genie
databricks bundle run rfi_copilot -t dev             # deploy app code
python3 scripts/export_evidence.py <job_run_id>      # refresh evidence/notebooks from the run
```
Variables (catalog, schema, warehouse, Vector Search endpoint, LLM endpoint, Lakebase instance, app service principal) are in `databricks.yml`.

## Notes and honest limits

* Data is synthetic, but the generator encodes realistic drivers (firm responsiveness, coordination load, missing drawings, priority) so the model learns real structure. The hidden root-cause label is used only to evaluate `ai_classify`.
* Lakebase is registered with Unity Catalog through the synced table (a UC object with lineage to its Delta source). A Lakebase *database catalog* was not created because the build principal lacks `CREATE CATALOG` on this shared metastore; the audit job therefore reads Postgres directly.
* The routing actions (email/ticket) are recorded as workflow events; no external email is sent from this build.
