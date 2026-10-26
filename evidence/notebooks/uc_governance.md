# uc_governance — executed notebook transcript

Job run 111509992172372, task run 462904608483564, result: **SUCCESS**

# 02 · Unity Catalog governance
Everything the pipeline produced is governed in one place: ownership & discoverability (comments, tags),
fine-grained access (a column mask + dynamic view hiding change-order dollars from non-finance users),
least-privilege grants, data-quality evidence from the Lakeflow event log, and automatic lineage.

```python
dbutils.widgets.text("catalog", "gauravsharma_catalog")
dbutils.widgets.text("schema", "rfi_intel")
dbutils.widgets.text("finance_group", "rfi_finance")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema"); fin = dbutils.widgets.get("finance_group")
FQ = f"`{catalog}`.`{schema}`"
spark.sql(f"USE CATALOG `{catalog}`"); spark.sql(f"USE SCHEMA `{schema}`")
import json
evidence = {}

def run(stmt):
    try:
        spark.sql(stmt); print("OK   ", stmt.split("\n")[0][:110]); return True
    except Exception as e:
        print("SKIP ", stmt.split("\n")[0][:110], "->", str(e).split("\n")[0][:260]); return False
```

## 1 · Discoverability: schema + table tags and descriptions

```python
run(f"COMMENT ON SCHEMA {FQ} IS 'RFI Intelligence for general contractors: Lakeflow medallion, ML risk scores, Gen-AI drafts, Lakebase serving. All data synthetic.'")
run(f"ALTER SCHEMA {FQ} SET TAGS ('rfi_industry'='engineering_construction', 'rfi_use_case'='rfi_intelligence', 'rfi_sensitivity'='internal', 'rfi_data_origin'='synthetic')")
TAGS = {
  "bronze_rfis": ("bronze", "raw"), "silver_rfis": ("silver", "conformed"), "gold_rfi_facts": ("gold", "analytics"),
  "gold_project_kpis": ("gold", "analytics"), "gold_firm_performance": ("gold", "analytics"), "gold_rfi_monthly": ("gold", "analytics"),
  "silver_spec_chunks": ("silver", "knowledge_base"),
}
tag_results = {}
for t, (layer, purpose) in TAGS.items():
    ok = run(f"ALTER MATERIALIZED VIEW {FQ}.{t} SET TAGS ('rfi_layer'='{layer}', 'rfi_purpose'='{purpose}', 'rfi_steward'='project_controls')") \
         or run(f"ALTER STREAMING TABLE {FQ}.{t} SET TAGS ('rfi_layer'='{layer}', 'rfi_purpose'='{purpose}', 'rfi_steward'='project_controls')") \
         or run(f"ALTER TABLE {FQ}.{t} SET TAGS ('rfi_layer'='{layer}', 'rfi_purpose'='{purpose}', 'rfi_steward'='project_controls')")
    tag_results[t] = ok
evidence["table_tags_applied"] = tag_results
```

**Output:**

```text
OK    COMMENT ON SCHEMA `gauravsharma_catalog`.`rfi_intel` IS 'RFI Intelligence for general contractors: Lakeflow me
OK    ALTER SCHEMA `gauravsharma_catalog`.`rfi_intel` SET TAGS ('rfi_industry'='engineering_construction', 'rfi_use_
OK    ALTER MATERIALIZED VIEW `gauravsharma_catalog`.`rfi_intel`.bronze_rfis SET TAGS ('rfi_layer'='bronze', 'rfi_pu
OK    ALTER MATERIALIZED VIEW `gauravsharma_catalog`.`rfi_intel`.silver_rfis SET TAGS ('rfi_layer'='silver', 'rfi_pu
OK    ALTER MATERIALIZED VIEW `gauravsharma_catalog`.`rfi_intel`.gold_rfi_facts SET TAGS ('rfi_layer'='gold', 'rfi_p
OK    ALTER MATERIALIZED VIEW `gauravsharma_catalog`.`rfi_intel`.gold_project_kpis SET TAGS ('rfi_layer'='gold', 'rf
OK    ALTER MATERIALIZED VIEW `gauravsharma_catalog`.`rfi_intel`.gold_firm_performance SET TAGS ('rfi_layer'='gold',
OK    ALTER MATERIALIZED VIEW `gauravsharma_catalog`.`rfi_intel`.gold_rfi_monthly SET TAGS ('rfi_layer'='gold', 'rfi
OK    ALTER MATERIALIZED VIEW `gauravsharma_catalog`.`rfi_intel`.silver_spec_chunks SET TAGS ('rfi_layer'='silver',
```

## 2 · Fine-grained access: mask change-order cost for anyone outside the finance group
A SQL UDF column mask (reusable on any Delta table) plus a governed, dynamic view that the app,
Genie and BI read from. Project engineers see the RFI; only finance sees the dollar exposure.

```python
run(f"""CREATE OR REPLACE FUNCTION {FQ}.mask_cost(v DOUBLE) RETURNS DOUBLE
COMMENT 'Column mask: change-order dollars visible only to the finance group or workspace admins'
RETURN CASE WHEN is_account_group_member('{fin}') OR is_account_group_member('admins') THEN v ELSE NULL END""")

run(f"""CREATE OR REPLACE VIEW {FQ}.v_rfi_secure
COMMENT 'Governed view over gold_rfi_facts: cost and LD exposure masked via mask_cost(); used by downstream consumers'
AS SELECT rfi_id, project_id, project_name, sector, discipline, spec_section, subject, priority, status, is_open,
          assigned_firm, submitted_at, answered_at, due_at, sla_days, days_to_answer, days_open, sla_breached,
          became_change_order, {FQ}.mask_cost(cost_impact_usd) AS cost_impact_usd, schedule_impact_days,
          {FQ}.mask_cost(ld_exposure_usd) AS ld_exposure_usd
   FROM {FQ}.gold_rfi_facts""")
display(spark.sql(f"SELECT rfi_id, project_name, became_change_order, cost_impact_usd FROM {FQ}.v_rfi_secure WHERE became_change_order LIMIT 5"))
```

**Output:**

```text
OK    CREATE OR REPLACE FUNCTION `gauravsharma_catalog`.`rfi_intel`.mask_cost(v DOUBLE) RETURNS DOUBLE
OK    CREATE OR REPLACE VIEW `gauravsharma_catalog`.`rfi_intel`.v_rfi_secure


rfi_id | project_name | became_change_order | cost_impact_usd
--- | --- | --- | ---
RFI-101-00006 | Lakeside Regional Hospital Expansion | True | None
RFI-101-00012 | Lakeside Regional Hospital Expansion | True | None
RFI-101-00015 | Lakeside Regional Hospital Expansion | True | None
RFI-101-00016 | Lakeside Regional Hospital Expansion | True | None
RFI-101-00028 | Lakeside Regional Hospital Expansion | True | None
```

## 3 · Data quality evidence from the Lakeflow event log (expectations)

```python
dq = spark.sql(f"""
SELECT 'bronze_rfis' AS layer, count(*) AS rows FROM {FQ}.bronze_rfis
UNION ALL SELECT 'silver_rfis', count(*) FROM {FQ}.silver_rfis
UNION ALL SELECT 'gold_rfi_facts', count(*) FROM {FQ}.gold_rfi_facts
UNION ALL SELECT 'silver_spec_chunks', count(*) FROM {FQ}.silver_spec_chunks""")
display(dq)
counts = {r["layer"]: r["rows"] for r in dq.collect()}
evidence["row_counts"] = counts
print(f"Rows removed between bronze and silver (defects + duplicate re-sends): {counts['bronze_rfis'] - counts['silver_rfis']}")

try:
    ev = spark.sql(f"""
      SELECT row_expectations.dataset, row_expectations.name AS expectation,
             SUM(row_expectations.passed_records) AS passed, SUM(row_expectations.failed_records) AS failed
      FROM (SELECT explode(from_json(details:flow_progress:data_quality:expectations,
                     'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) AS row_expectations
            FROM event_log(TABLE({FQ}.silver_rfis)) WHERE event_type = 'flow_progress')
      GROUP BY 1, 2 ORDER BY 1, 2""")
    display(ev)
    evidence["expectations"] = [r.asDict() for r in ev.collect()]
except Exception as e:
    print("event_log() not available here:", str(e)[:200])
```

**Output:**

```text
layer | rows
--- | ---
silver_rfis | 3795
silver_spec_chunks | 25
bronze_rfis | 3844
gold_rfi_facts | 3795

Rows removed between bronze and silver (defects + duplicate re-sends): 49


dataset | expectation | passed | failed
--- | --- | --- | ---
gauravsharma_catalog.rfi_intel.silver_projects | project_pk | 32 | 0
gauravsharma_catalog.rfi_intel.silver_rfis | answered_after_submitted | 15272 | 0
gauravsharma_catalog.rfi_intel.silver_rfis | known_project | 15212 | 60
gauravsharma_catalog.rfi_intel.silver_rfis | valid_discipline | 15212 | 60
gauravsharma_catalog.rfi_intel.silver_rfis | valid_rfi_id | 15272 | 0
gauravsharma_catalog.rfi_intel.silver_spec_chunks | has_content | 100 | 0
```

## 4 · Least-privilege access model

```python
print("No schema-wide grant to `account users`: access is per-principal. App service-principal grants are applied in 05_lakebase_serving.")
display(spark.sql(f"SHOW GRANTS ON SCHEMA {FQ}"))
```

**Output:**

```text
No schema-wide grant to `account users`: access is per-principal. App service-principal grants are applied in 05_lakebase_serving.


Principal | ActionType | ObjectType | ObjectKey
--- | --- | --- | ---
18dc1d86-01a1-46ad-a7d2-a58667f48569 | USE SCHEMA | SCHEMA | gauravsharma_catalog.rfi_intel
account users | READ METADATA | METASTORE | metastore_aws_us_east_1
other-principal@example.com | ALL PRIVILEGES | CATALOG | gauravsharma_catalog
build-user@example.com | ALL PRIVILEGES | CATALOG | gauravsharma_catalog
build-user@example.com | MANAGE | CATALOG | gauravsharma_catalog
other-principal@example.com | READ METADATA | METASTORE | metastore_aws_us_east_1
```

## 5 · Catalog inventory + lineage

```python
inv = spark.sql(f"""SELECT table_name, table_type, comment FROM `{catalog}`.information_schema.tables
                    WHERE table_schema = '{schema}' AND table_name NOT LIKE '\\_\\_%' ORDER BY table_name""")
display(inv)
evidence["inventory"] = [r.asDict() for r in inv.collect()]
try:
    lin = spark.sql(f"""SELECT DISTINCT source_table_full_name, target_table_full_name
                        FROM system.access.table_lineage
                        WHERE target_table_schema = '{schema}' AND target_table_catalog = '{catalog}'
                          AND source_table_full_name IS NOT NULL AND event_date >= current_date() - INTERVAL 7 DAYS
                        ORDER BY 2, 1""")
    display(lin)
    evidence["lineage_edges"] = [r.asDict() for r in lin.collect()]
except Exception as e:
    print("system.access.table_lineage not accessible:", str(e)[:200])
```

**Output:**

```text
table_name | table_type | comment
--- | --- | ---
app_rfi_inbox | MANAGED | Serving projection for the app: open RFIs enriched with ML SLA-risk, Gen-AI root cause and cited draft. Synced to Lakebase.
bronze_design_firms | STREAMING_TABLE | Raw design-firm contractual SLA sheet (Auto Loader, CSV).
bronze_projects | STREAMING_TABLE | Raw project register (Auto Loader, JSON).
bronze_rfis | STREAMING_TABLE | Raw RFI log exports from the PM system (Auto Loader, JSON). One row per exported record, defects included.
bronze_spec_docs | STREAMING_TABLE | Raw specification book, one Markdown document per CSI division (Auto Loader, whole-text).
event_log_199a997b_3375_412a_8762_e9277d2df980 | MANAGED | None
event_log_25d9d794_e5c9_4523_a23d_bc6967152480 | MANAGED | None
event_log_98b86669_bb57_4052_9cf1_8df3af41b93d | MANAGED | None
gold_firm_performance | MATERIALIZED_VIEW | Design-firm responsiveness by discipline vs contractual SLA (answered RFIs only).
gold_project_kpis | MATERIALIZED_VIEW | Portfolio KPIs per project: RFI volume, response time, SLA breach rate, change-order cost and schedule exposure.
gold_rfi_facts | MATERIALIZED_VIEW | One row per RFI enriched with project, firm SLA, response time and SLA outcome. Primary analytical fact table.
gold_rfi_monthly | MATERIALIZED_VIEW | Monthly RFI trend by discipline.
gold_rfi_root_cause | MANAGED | Gen-AI root cause per RFI (ai_classify over subject + question). Drives preconstruction prevention.
rfi_ai_drafts | MANAGED | Batch RAG drafts (Vector Search + Claude via Foundation Model API) for every open RFI, pending human approval
rfi_inbox | FOREIGN | 
rfi_risk_scores | MANAGED | ML batch scores for open RFIs: SLA-breach probability + expected days to answer (champion models in UC)
rfi_workflow_audit | MANAGED | Governed Delta copy of every human decision taken in the app (sourced from Lakebase ops.rfi_workflow_events)
silver_design_firms | MATERIALIZED_VIEW | Design firm of record per discipline with contractual RFI response SLA (calendar days).
silver_projects | MATERIALIZED_VIEW | Conformed project register.
silver_rfis | MATERIALIZED_VIEW | Cleansed, de-duplicated RFIs. Defective rows are dropped by expectations and counted in the event log.
silver_spec_chunks | MATERIALIZED_VIEW | Specification book parsed into one chunk per CSI section (source for the Vector Search index).
spec_chunks | MANAGED | Specification knowledge base (one chunk per CSI section). Source of the Vector Search index.
spec_chunks_index | FOREIGN | Managed Vector Index with Delta Sync
v_rfi_secure | VIEW | Governed view over gold_rfi_facts: cost and LD exposure masked via mask_cost(); used by downstream consumers

source_table_full_name | target_table_full_name
--- | ---
gauravsharma_catalog.rfi_intel.gold_rfi_facts | gauravsharma_catalog.rfi_intel.app_rfi_inbox
gauravsharma_catalog.rfi_intel.gold_rfi_root_cause | gauravsharma_catalog.rfi_intel.app_rfi_inbox
gauravsharma_catalog.rfi_intel.rfi_ai_drafts | gauravsharma_catalog.rfi_intel.app_rfi_inbox
gauravsharma_catalog.rfi_intel.rfi_risk_scores | gauravsharma_catalog.rfi_intel.app_rfi_inbox
gauravsharma_catalog.rfi_intel.gold_rfi_facts | gauravsharma_catalog.rfi_intel.gold_firm_performance
gauravsharma_catalog.rfi_intel.gold_rfi_facts | gauravsharma_catalog.rfi_intel.gold_project_kpis
gauravsharma_catalog.rfi_intel.silver_design_firms | gauravsharma_catalog.rfi_intel.gold_rfi_facts
gauravsharma_catalog.rfi_intel.silver_projects | gauravsharma_catalog.rfi_intel.gold_rfi_facts
gauravsharma_catalog.rfi_intel.silver_rfis | gauravsharma_catalog.rfi_intel.gold_rfi_facts
gauravsharma_catalog.rfi_intel.gold_rfi_facts | gauravsharma_catalog.rfi_intel.gold_rfi_monthly
gauravsharma_catalog.rfi_intel.gold_rfi_facts | gauravsharma_catalog.rfi_intel.gold_rfi_root_cause
gauravsharma_catalog.rfi_intel.app_rfi_inbox | gauravsharma_catalog.rfi_intel.rfi_inbox
gauravsharma_catalog.rfi_intel.bronze_design_firms | gauravsharma_catalog.rfi_intel.silver_design_firms
gauravsharma_catalog.rfi_intel.bronze_projects | gauravsharma_catalog.rfi_intel.silver_projects
gauravsharma_catalog.rfi_intel.bronze_rfis | gauravsharma_catalog.rfi_intel.silver_rfis
gauravsharma_catalog.rfi_intel.bronze_spec_docs | gauravsharma_catalog.rfi_intel.silver_spec_chunks
gauravsharma_catalog.rfi_intel.silver_spec_chunks | gauravsharma_catalog.rfi_intel.spec_chunks
gauravsharma_catalog.rfi_intel.spec_chunks | gauravsharma_catalog.rfi_intel.spec_chunks_index
gauravsharma_catalog.rfi_intel.gold_rfi_facts | gauravsharma_catalog.rfi_intel.v_rfi_secure
```

```python
dbutils.fs.put(f"/Volumes/{catalog}/{schema}/evidence/02_governance.json", json.dumps(evidence, indent=2, default=str), overwrite=True)
print(json.dumps({k: (v if k != "inventory" else len(v)) for k, v in evidence.items()}, indent=2, default=str))
```

**Output:**

```text
Wrote 9116 bytes.
{
  "table_tags_applied": {
    "bronze_rfis": true,
    "silver_rfis": true,
    "gold_rfi_facts": true,
    "gold_project_kpis": true,
    "gold_firm_performance": true,
    "gold_rfi_monthly": true,
    "silver_spec_chunks": true
  },
  "row_counts": {
    "silver_rfis": 3795,
    "silver_spec_chunks": 25,
    "bronze_rfis": 3844,
    "gold_rfi_facts": 3795
  },
  "expectations": [
    {
      "dataset": "gauravsharma_catalog.rfi_intel.silver_projects",
      "expectation": "project_pk",
      "passed": 32,
      "failed": 0
    },
    {
      "dataset": "gauravsharma_catalog.rfi_intel.silver_rfis",
      "expectation": "answered_after_submitted",
      "passed": 15272,
      "failed": 0
    },
    {
      "dataset": "gauravsharma_catalog.rfi_intel.silver_rfis",
      "expectation": "known_project",
      "passed": 15212,
      "failed": 60
    },
    {
      "dataset": "gauravsharma_catalog.rfi_intel.silver_rfis",
      "expectation": "valid_discipline",
      "passed": 15212,
      "failed": 60
    },
    {
      "dataset": "gauravsharma_catalog.rfi_intel.silver_rfis",
      "expectation": "valid_rfi_id",
      "passed": 15272,
      "failed": 0
    },
    {
      "dataset": "gauravsharma_catalog.rfi_intel.silver_spec_chunks",
      "expectation": "has_content",
      "passed": 100,
      "failed": 0
    }
  ],
  "inventory": 24,
  "lineage_edges": [
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.gold_rfi_facts",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.app_rfi_inbox"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.gold_rfi_root_cause",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.app_rfi_inbox"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.rfi_ai_drafts",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.app_rfi_inbox"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.rfi_risk_scores",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.app_rfi_inbox"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.gold_rfi_facts",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.gold_firm_performance"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.gold_rfi_facts",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.gold_project_kpis"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.silver_design_firms",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.gold_rfi_facts"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.silver_projects",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.gold_rfi_facts"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.silver_rfis",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.gold_rfi_facts"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.gold_rfi_facts",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.gold_rfi_monthly"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.gold_rfi_facts",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.gold_rfi_root_cause"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.app_rfi_inbox",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.rfi_inbox"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.bronze_design_firms",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.silver_design_firms"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.bronze_projects",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.silver_projects"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.bronze_rfis",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.silver_rfis"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.bronze_spec_docs",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.silver_spec_chunks"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.silver_spec_chunks",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.spec_chunks"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.spec_chunks",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.spec_chunks_index"
    },
    {
      "source_table_full_name": "gauravsharma_catalog.rfi_intel.gold_rfi_facts",
      "target_table_full_name": "gauravsharma_catalog.rfi_intel.v_rfi_secure"
    }
  ]
}
```
