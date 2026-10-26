# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Genie — natural-language access for project executives
# MAGIC Creates (or updates) an AI/BI Genie space over the governed gold tables, ML scores, Gen-AI root causes and the
# MAGIC Lakebase-sourced decision audit, with business instructions and example SQL. Then asks it the questions a
# MAGIC VP of Operations actually asks and records the generated SQL + answers as evidence.

# COMMAND ----------
dbutils.widgets.text("catalog", "gauravsharma_catalog")
dbutils.widgets.text("schema", "rfi_intel")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("app_sp_client_id", "")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema")
WH = dbutils.widgets.get("warehouse_id"); SP = dbutils.widgets.get("app_sp_client_id").strip(); FQ = f"{catalog}.{schema}"
import json, uuid, time
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(); me = w.current_user.me().user_name
TITLE = "RFI Intelligence - Summit Ridge Builders"

# COMMAND ----------
TABLES = ["gold_rfi_facts", "gold_project_kpis", "gold_firm_performance", "gold_rfi_monthly", "gold_rfi_root_cause",
          "rfi_risk_scores", "rfi_workflow_audit"]
INSTRUCTIONS = """You answer questions for a general contractor's operations leadership about RFIs (Requests for Information) across its project portfolio.
- An RFI is 'overdue' / 'late' when sla_breached = true. Open RFIs have is_open = true.
- 'Response time' means days_to_answer (answered RFIs only). Use avg and p90.
- 'Exposure' means change_order_cost_usd (cost) and ld_exposure_usd (liquidated damages: schedule_impact_days x liquidated_damages_per_day, only when on_critical_path = true).
- 'At risk' open RFIs are those in rfi_risk_scores with risk_tier = 'High' (ML predicted probability of missing SLA).
- Root cause comes from gold_rfi_root_cause.root_cause (Gen-AI classification). Join on rfi_id.
- Design firms are in assigned_firm. Always show project_name rather than project_id.
- rfi_workflow_audit holds every human decision from the RFI Copilot app. event_type values: submitted_for_approval (PE approved, waiting for a named approver),
  approver_approved / approver_returned (approver decision, actor = approver), sent_to_design (released to the design firm), approved (low-risk auto-release),
  changes_requested, routed, triage_resolved (closed by GC without design escalation), pco_flagged, closed. An RFI is 'pending approval' when its latest event is submitted_for_approval.
- Format money in USD with no decimals. Default time window is all time unless the user specifies one."""
EXAMPLES = [
 ("Which design firm is slowest to answer RFIs?",
  f"SELECT assigned_firm, sum(answered_rfis) AS answered, round(sum(avg_days_to_answer*answered_rfis)/sum(answered_rfis),1) AS avg_days, round(avg(sla_breach_pct),1) AS sla_breach_pct FROM {FQ}.gold_firm_performance GROUP BY 1 ORDER BY avg_days DESC"),
 ("Which open RFIs are most likely to miss their SLA?",
  f"SELECT f.rfi_id, f.project_name, f.discipline, f.subject, s.breach_probability, s.predicted_days_to_answer, s.top_drivers FROM {FQ}.rfi_risk_scores s JOIN {FQ}.gold_rfi_facts f USING (rfi_id) ORDER BY s.breach_probability DESC LIMIT 10"),
]
QUESTIONS = ["Which projects have the most overdue open RFIs right now?",
             "What is our average RFI response time by discipline, and how has it trended over the last 6 months?",
             "How much change-order cost came from RFIs caused by design conflicts, by project?",
             "Which design firm is slowest to answer RFIs?",
             "Show the 10 open RFIs most likely to miss their SLA with their drivers",
             "What is the total liquidated-damages exposure tied to late RFIs on the Harbor Point Data Center?"]

def hexid(): return uuid.uuid4().hex
space = {"version": 2,
         "config": {"sample_questions": sorted([{"id": hexid(), "question": [q]} for q in QUESTIONS[:5]], key=lambda x: x["id"])},
         "data_sources": {"tables": sorted([{"identifier": f"{FQ}.{t}"} for t in TABLES], key=lambda x: x["identifier"])},
         "instructions": {"text_instructions": [{"id": hexid(), "content": [INSTRUCTIONS]}],
                          "example_question_sqls": sorted([{"id": hexid(), "question": [q], "sql": [s]} for q, s in EXAMPLES], key=lambda x: x["id"])}}

# COMMAND ----------
existing = [s for s in (w.api_client.do("GET", "/api/2.0/genie/spaces").get("spaces") or []) if s.get("title") == TITLE]
body = {"title": TITLE, "description": "Ask about RFI volume, response times, SLA risk, root causes and change-order exposure across the Summit Ridge Builders portfolio (synthetic data).",
        "warehouse_id": WH, "serialized_space": json.dumps(space)}
if existing:
    space_id = existing[0]["space_id"]
    w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{space_id}", body=body); print("updated Genie space", space_id)
else:
    space_id = w.api_client.do("POST", "/api/2.0/genie/spaces", body={**body, "parent_path": f"/Workspace/Users/{me}"})["space_id"]
    print("created Genie space", space_id)
if SP:
    w.api_client.do("PATCH", f"/api/2.0/permissions/genie/{space_id}",
                    body={"access_control_list": [{"service_principal_name": SP, "permission_level": "CAN_RUN"}]})
    print("granted CAN_RUN to app SP", SP)

# COMMAND ----------
# MAGIC %md ## Ask Genie the executive questions (Conversation API) and record SQL + answers

# COMMAND ----------
results = []
for q in QUESTIONS:
    rec = {"question": q}
    try:
        msg = w.genie.start_conversation_and_wait(space_id, q)
        for a in msg.attachments or []:
            if a.text: rec["text"] = a.text.content
            if a.query:
                rec["sql"] = a.query.query; rec["description"] = a.query.description
                qr = w.genie.get_message_attachment_query_result(space_id, msg.conversation_id, msg.id, a.attachment_id)
                sr = qr.statement_response
                cols = [c.name for c in sr.manifest.schema.columns]
                rec["columns"] = cols; rec["rows"] = (sr.result.data_array or [])[:12]
        rec["status"] = str(msg.status)
    except Exception as e:
        rec["error"] = str(e)[:300]
    results.append(rec)
    print(f"\n### Q: {q}\n{rec.get('description') or rec.get('text') or rec.get('error')}\nSQL: {rec.get('sql', '')}")
    if rec.get("rows"):
        print(" | ".join(rec["columns"]))
        for r in rec["rows"][:8]: print(" | ".join(str(x) for x in r))

# COMMAND ----------
host = w.config.host.rstrip("/")
ev = {"space_id": space_id, "url": f"{host}/genie/rooms/{space_id}", "tables": TABLES, "results": results}
dbutils.fs.put(f"/Volumes/{catalog}/{schema}/evidence/06_genie.json", json.dumps(ev, indent=2, default=str), overwrite=True)
dbutils.jobs.taskValues.set("genie_space_id", space_id)
print("Genie space:", ev["url"])
