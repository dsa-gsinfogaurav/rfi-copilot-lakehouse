# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Unity Catalog governance
# MAGIC Everything the pipeline produced is governed in one place: ownership & discoverability (comments, tags),
# MAGIC fine-grained access (a column mask + dynamic view hiding change-order dollars from non-finance users),
# MAGIC least-privilege grants, data-quality evidence from the Lakeflow event log, and automatic lineage.

# COMMAND ----------
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

# COMMAND ----------
# MAGIC %md ## 1 · Discoverability: schema + table tags and descriptions

# COMMAND ----------
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

# COMMAND ----------
# MAGIC %md ## 2 · Fine-grained access: mask change-order cost for anyone outside the finance group
# MAGIC A SQL UDF column mask (reusable on any Delta table) plus a governed, dynamic view that the app,
# MAGIC Genie and BI read from. Project engineers see the RFI; only finance sees the dollar exposure.

# COMMAND ----------
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

# COMMAND ----------
# MAGIC %md ## 3 · Data quality evidence from the Lakeflow event log (expectations)

# COMMAND ----------
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
            FROM event_log(TABLE({FQ}.silver_rfis))
            WHERE event_type = 'flow_progress'
              AND origin.update_id = (SELECT origin.update_id FROM event_log(TABLE({FQ}.silver_rfis))
                                      WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1))
      GROUP BY 1, 2 ORDER BY 1, 2""")
    display(ev)
    evidence["expectations"] = [r.asDict() for r in ev.collect()]
except Exception as e:
    print("event_log() not available here:", str(e)[:200])

# COMMAND ----------
# MAGIC %md ## 4 · Least-privilege access model

# COMMAND ----------
print("No schema-wide grant to `account users`: access is per-principal. App service-principal grants are applied in 05_lakebase_serving.")
display(spark.sql(f"SHOW GRANTS ON SCHEMA {FQ}"))

# COMMAND ----------
# MAGIC %md ## 5 · Catalog inventory + lineage

# COMMAND ----------
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

# COMMAND ----------
dbutils.fs.put(f"/Volumes/{catalog}/{schema}/evidence/02_governance.json", json.dumps(evidence, indent=2, default=str), overwrite=True)
print(json.dumps({k: (v if k != "inventory" else len(v)) for k, v in evidence.items()}, indent=2, default=str))
