# genie_space — executed notebook transcript

Job run 111509992172372, task run 568384539121537, result: **SUCCESS**

# 06 · Genie — natural-language access for project executives
Creates (or updates) an AI/BI Genie space over the governed gold tables, ML scores, Gen-AI root causes and the
Lakebase-sourced decision audit, with business instructions and example SQL. Then asks it the questions a
VP of Operations actually asks and records the generated SQL + answers as evidence.

```python
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
```

```python
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
```

```python
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
```

**Output:**

```text
updated Genie space 01f1d15f1b311ebdafa1e2f312eb23b9
granted CAN_RUN to app SP 18dc1d86-01a1-46ad-a7d2-a58667f48569
```

## Ask Genie the executive questions (Conversation API) and record SQL + answers

```python
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
```

**Output:**

```text
### Q: Which projects have the most overdue open RFIs right now?
You want to see a list of projects ranked by the number of their currently open RFIs that are overdue, meaning those RFIs have missed their SLA deadlines.
SQL: WITH project_overdue AS (
  SELECT
    `project_name`,
    COUNT(*) AS `overdue_open_rfis`
  FROM `gauravsharma_catalog`.`rfi_intel`.`gold_rfi_facts`
  WHERE `is_open` = true
    AND `sla_breached` = true
    AND `project_name` IS NOT NULL
  GROUP BY `project_name`
), ranked_projects AS (
  SELECT
    `project_name`,
    `overdue_open_rfis`,
    RANK() OVER (ORDER BY `overdue_open_rfis` DESC) AS `project_rank`
  FROM project_overdue
)
SELECT
  `project_name`,
  `overdue_open_rfis`,
  `project_rank`
FROM ranked_projects
ORDER BY `project_rank`, `overdue_open_rfis` DESC, `project_name`
project_name | overdue_open_rfis | project_rank
Harbor Point Data Center Phase 2 | 15 | 1
Riverbend Wastewater Treatment Upgrade | 11 | 2
Pinecrest Senior Living Community | 7 | 3
Union Station Mixed-Use Tower | 5 | 4
Lakeside Regional Hospital Expansion | 4 | 5
Mesa State University Science Building | 2 | 6

### Q: What is our average RFI response time by discipline, and how has it trended over the last 6 months?
You want to see the average response time for RFIs by discipline overall and how this average has changed month by month over the last six months.
SQL: WITH `overall_by_discipline` AS (
  SELECT
    `discipline`,
    AVG(`days_to_answer`) AS `avg_response_time_days`
  FROM `gauravsharma_catalog`.`rfi_intel`.`gold_rfi_facts`
  WHERE `answered_at` IS NOT NULL
    AND `days_to_answer` IS NOT NULL
    AND `discipline` IS NOT NULL
  GROUP BY `discipline`
),
`monthly_trend` AS (
  SELECT
    DATE_TRUNC('MONTH', `submitted_month`) AS `month`,
    `discipline`,
    AVG(`days_to_answer`) AS `avg_response_time_days`
  FROM `gauravsharma_catalog`.`rfi_intel`.`gold_rfi_facts`
  WHERE `answered_at` IS NOT NULL
    AND `days_to_answer` IS NOT NULL
    AND `discipline` IS NOT NULL
    AND `submitted_month` IS NOT NULL
    AND `submitted_month` BETWEEN ADD_MONTHS(DATE_TRUNC('MONTH', CURRENT_DATE), -5) AND CURRENT_DATE
  GROUP BY DATE_TRUNC('MONTH', `submitted_month`), `discipline`
)
SELECT
  t.`month`,
  t.`discipline`,
  o.`avg_response_time_days` AS `overall_avg_response_time_days`,
  t.`avg_response_time_days` AS `monthly_avg_response_time_days`
FROM `monthly_trend` t
JOIN `overall_by_discipline` o
  ON t.`discipline` = o.`discipline`
ORDER BY t.`month`, t.`discipline`
month | discipline | overall_avg_response_time_days | monthly_avg_response_time_days
2026-05-01T00:00:00.000Z | Architectural | 7.0619582955575675 | 7.327031249999997
2026-05-01T00:00:00.000Z | Civil | 6.803121546961322 | 7.195555555555554
2026-05-01T00:00:00.000Z | Electrical | 9.304741379310345 | 8.618333333333332
2026-05-01T00:00:00.000Z | Fire Protection | 10.07688775510204 | 10.888
2026-05-01T00:00:00.000Z | Mechanical | 8.706142131979693 | 9.144285714285713
2026-05-01T00:00:00.000Z | Plumbing | 10.561120448179274 | 17.321875000000002
2026-05-01T00:00:00.000Z | Structural | 6.065129375951295 | 6.544102564102565
2026-06-01T00:00:00.000Z | Architectural | 7.0619582955575675 | 7.8378181818181805

### Q: How much change-order cost came from RFIs caused by design conflicts, by project?
You want to see the total change-order cost caused by RFIs that have a root cause related to design conflicts, broken down by project name.
SQL: SELECT `f`.`project_name`, SUM(`f`.`cost_impact_usd`) AS `change_order_cost_usd`
FROM `gauravsharma_catalog`.`rfi_intel`.`gold_rfi_facts` AS `f`
INNER JOIN `gauravsharma_catalog`.`rfi_intel`.`gold_rfi_root_cause` AS `rc`
  ON `f`.`rfi_id` = `rc`.`rfi_id`
WHERE `f`.`project_name` IS NOT NULL
  AND `f`.`cost_impact_usd` IS NOT NULL
  AND `rc`.`root_cause` IS NOT NULL
  AND `rc`.`root_cause` ILIKE '%design conflict%'
GROUP BY `f`.`project_name`
ORDER BY `change_order_cost_usd` DESC
project_name | change_order_cost_usd
Lakeside Regional Hospital Expansion | 1154781.0
Harbor Point Data Center Phase 2 | 805506.0
Mesa State University Science Building | 582419.0
Union Station Mixed-Use Tower | 451339.0
Cedar Valley K-12 Campus | 414049.0
Northgate Medical Office Building | 310716.0
Pinecrest Senior Living Community | 288495.0
Riverbend Wastewater Treatment Upgrade | 186111.0

### Q: Which design firm is slowest to answer RFIs?
You want to find the design firm that takes the longest average time to answer RFIs.
SQL: WITH firm_perf AS (
  SELECT
    `assigned_firm`,
    SUM(`answered_rfis`) AS `answered_rfis`,
    try_divide(SUM(`avg_days_to_answer` * `answered_rfis`),SUM(`answered_rfis`)) AS `avg_days_to_answer`,
    AVG(`sla_breach_pct`) AS `sla_breach_pct`
  FROM `gauravsharma_catalog`.`rfi_intel`.`gold_firm_performance`
  WHERE `assigned_firm` IS NOT NULL
  GROUP BY `assigned_firm`
), ranked AS (
  SELECT
    `assigned_firm`,
    `answered_rfis`,
    `avg_days_to_answer`,
    `sla_breach_pct`,
    RANK() OVER (ORDER BY `avg_days_to_answer` DESC) AS `firm_rank`
  FROM firm_perf
)
SELECT
  `assigned_firm`,
  `answered_rfis`,
  ROUND(`avg_days_to_answer`, 1) AS `avg_days_to_answer`,
  ROUND(`sla_breach_pct`, 1) AS `sla_breach_pct`
FROM ranked
WHERE `firm_rank` = 1
assigned_firm | answered_rfis | avg_days_to_answer | sla_breach_pct
Sentinel Fire Protection Engineering | 196 | 10.1 | 73.7

### Q: Show the 10 open RFIs most likely to miss their SLA with their drivers
You want to see the top 10 open RFIs that have the highest predicted probability of missing their SLA, along with details about the project, discipline, subject, predicted days to answer, risk tier, and the main factors driving the risk.
SQL: SELECT
  `f`.`rfi_id`,
  `f`.`project_name`,
  `f`.`discipline`,
  `f`.`subject`,
  `s`.`breach_probability`,
  `s`.`predicted_days_to_answer`,
  `s`.`risk_tier`,
  `s`.`top_drivers`
FROM `gauravsharma_catalog`.`rfi_intel`.`rfi_risk_scores` AS `s`
INNER JOIN `gauravsharma_catalog`.`rfi_intel`.`gold_rfi_facts` AS `f`
  ON `s`.`rfi_id` = `f`.`rfi_id`
WHERE `f`.`is_open` = true
  AND `s`.`breach_probability` IS NOT NULL
ORDER BY `s`.`breach_probability` DESC, `f`.`rfi_id` ASC
LIMIT 10
rfi_id | project_name | discipline | subject | breach_probability | predicted_days_to_answer | risk_tier | top_drivers
RFI-104-02352 | Harbor Point Data Center Phase 2 | Architectural | Storefront deflection criteria | 0.9405 | 6.4 | High | no drawing reference attached; no proposed solution from the field; urgent 3-day SLA
RFI-106-03207 | Riverbend Wastewater Treatment Upgrade | Structural | Slab-on-grade mix strength | 0.9185 | 4.1 | High | no drawing reference attached; no proposed solution from the field; urgent 3-day SLA
RFI-104-02369 | Harbor Point Data Center Phase 2 | Mechanical | Duct offset at structure | 0.8747 | 9.6 | High | Brightline MEP Partners breaches SLA more often than average; no proposed solution from the field
RFI-104-02326 | Harbor Point Data Center Phase 2 | Architectural | Slab moisture before flooring | 0.8567 | 11.9 | High | high project RFI load (29 in last 14d); no drawing reference attached; no proposed solution from the field
RFI-104-02363 | Harbor Point Data Center Phase 2 | Fire Protection | Sprinkler head relocation for duct | 0.783 | 10.5 | High | Sentinel Fire Protection Engineering breaches SLA more often than average; no drawing reference attached
RFI-104-02338 | Harbor Point Data Center Phase 2 | Plumbing | Lavatory flow rate vs local code | 0.7797 | 10.8 | High | Brightline MEP Partners breaches SLA more often than average; no drawing reference attached; no proposed solution from the field
RFI-106-03205 | Riverbend Wastewater Treatment Upgrade | Plumbing | PEX substitution for domestic water | 0.767 | 9.4 | High | Brightline MEP Partners breaches SLA more often than average; no drawing reference attached; no proposed solution from the field
RFI-104-02367 | Harbor Point Data Center Phase 2 | Fire Protection | Sprinkler head relocation for duct | 0.7555 | 10.2 | High | Sentinel Fire Protection Engineering breaches SLA more often than average; no proposed solution from the field

### Q: What is the total liquidated-damages exposure tied to late RFIs on the Harbor Point Data Center?
You want to see the total liquidated damages exposure caused by RFIs that were late (missed their SLA) for the Harbor Point Data Center project.
SQL: SELECT `project_name`, SUM(`ld_exposure_usd`) AS `total_ld_exposure_usd`
FROM `gauravsharma_catalog`.`rfi_intel`.`gold_rfi_facts`
WHERE `project_name` IS NOT NULL
  AND `project_name` ILIKE '%Harbor Point Data Center%'
  AND `sla_breached` = true
GROUP BY `project_name`
project_name | total_ld_exposure_usd
Harbor Point Data Center Phase 2 | 7260000.0
```

```python
host = w.config.host.rstrip("/")
ev = {"space_id": space_id, "url": f"{host}/genie/rooms/{space_id}", "tables": TABLES, "results": results}
dbutils.fs.put(f"/Volumes/{catalog}/{schema}/evidence/06_genie.json", json.dumps(ev, indent=2, default=str), overwrite=True)
dbutils.jobs.taskValues.set("genie_space_id", space_id)
print("Genie space:", ev["url"])
```

**Output:**

```text
Wrote 18728 bytes.
Genie space: https://fevm-gauravsharma.cloud.databricks.com/genie/rooms/01f1d15f1b311ebdafa1e2f312eb23b9
```
