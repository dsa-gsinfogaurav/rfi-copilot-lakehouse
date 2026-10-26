# lakebase_serving — executed notebook transcript

Job run 111509992172372, task run 288708685860787, result: **SUCCESS**

# 05 · Lakebase — operational serving for the app
The analytical Lakehouse is the system of record; the app needs **millisecond** reads and transactional writes.
* `app_rfi_inbox` (Delta, gold + ML scores + Gen-AI drafts) is **synced** into Lakebase Postgres (`rfi_ops.rfi_intel.rfi_inbox`).
* The app writes human decisions (triage, approve, route, PCO, close) to a transactional Postgres table `ops.rfi_workflow_events`.
* The synced table is a **Unity Catalog** object (`<catalog>.rfi_intel.rfi_inbox`, lineage from its Delta source); the Lakebase
  instance is a UC-governed resource. Decisions flow back to Delta (`07_workflow_audit_sync`) for Genie and BI.

```python
%pip install -q -U "databricks-sdk>=0.60" "pg8000==1.31.2" "scramp==1.4.5"
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
dbutils.widgets.text("instance", "rfi-intel-db")
dbutils.widgets.text("app_sp_client_id", "")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema"); INSTANCE = dbutils.widgets.get("instance")
SP = dbutils.widgets.get("app_sp_client_id").strip(); FQ = f"{catalog}.{schema}"
LOGICAL_DB = "rfi_ops"
import json, time, uuid, statistics
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import (DatabaseInstance, SyncedDatabaseTable, SyncedTableSpec,
                                             SyncedTableSchedulingPolicy)
w = WorkspaceClient(); ev = {}
```

## 1 · Build the serving table (open RFIs + ML risk + root cause + AI draft)

```python
spark.sql(f"""CREATE OR REPLACE TABLE {FQ}.app_rfi_inbox
TBLPROPERTIES (delta.enableChangeDataFeed = true)
COMMENT 'Serving projection for the app: open RFIs enriched with ML SLA-risk, Gen-AI root cause and cited draft. Synced to Lakebase.'
AS SELECT f.rfi_id, f.project_id, f.project_name, f.sector, f.discipline, f.spec_section, f.subject, f.question, f.priority,
          f.assigned_firm, f.submitted_by_role, f.submitted_by_company, f.proposed_solution, f.capture_location, f.bim_ref,
          f.has_drawing_ref, f.submitted_at, f.due_at, f.sla_days, f.days_open, f.liquidated_damages_per_day,
          s.breach_probability, s.predicted_days_to_answer, s.risk_tier, s.top_drivers,
          c.root_cause, d.draft_response, d.cited_sections, d.retrieved_sections
   FROM {FQ}.gold_rfi_facts f
   LEFT JOIN {FQ}.rfi_risk_scores s USING (rfi_id)
   LEFT JOIN {FQ}.gold_rfi_root_cause c USING (rfi_id)
   LEFT JOIN {FQ}.rfi_ai_drafts d USING (rfi_id)
   WHERE f.is_open""")
n_inbox = spark.table(f"{FQ}.app_rfi_inbox").count(); print("app_rfi_inbox rows:", n_inbox)
spark.sql(f"""CREATE TABLE IF NOT EXISTS {FQ}.rfi_workflow_audit (event_id STRING, rfi_id STRING, event_type STRING, actor STRING,
              detail STRING, cost_impact STRING, schedule_impact STRING, channel STRING, created_at TIMESTAMP, synced_at TIMESTAMP)
              COMMENT 'Governed Delta copy of every human decision taken in the app (sourced from Lakebase ops.rfi_workflow_events)'""")
```

**Output:**

```text
app_rfi_inbox rows: 65


{"text/plain": "DataFrame[]"}
```

## 2 · Lakebase instance + logical database

```python
names = {i.name for i in w.database.list_database_instances()}
if INSTANCE not in names:
    w.database.create_database_instance(DatabaseInstance(name=INSTANCE, capacity="CU_1"))
    print("creating instance", INSTANCE)
for _ in range(60):
    inst = w.database.get_database_instance(INSTANCE)
    if str(inst.state).endswith("AVAILABLE"): break
    print("instance state:", inst.state); time.sleep(20)
print("Lakebase instance", INSTANCE, inst.state, "host:", inst.read_write_dns, "pg", inst.pg_version)
ev["instance"] = {"name": INSTANCE, "state": str(inst.state), "capacity": inst.capacity, "pg_version": inst.pg_version}
import ssl, pg8000.dbapi  # pure-Python Postgres driver (no native libpq in the serverless process)
me = w.current_user.me().user_name
def connect(db):
    tok = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[INSTANCE]).token
    c = pg8000.dbapi.connect(host=inst.read_write_dns, port=5432, database=db, user=me, password=tok, ssl_context=ssl.create_default_context())
    c.autocommit = True
    return c
c0 = connect("databricks_postgres"); cur0 = c0.cursor()
cur0.execute("SELECT 1 FROM pg_database WHERE datname = %s", (LOGICAL_DB,))
if not cur0.fetchone():
    cur0.execute(f"CREATE DATABASE {LOGICAL_DB}"); print("created database", LOGICAL_DB)
else: print("database exists:", LOGICAL_DB)
c0.close()
```

**Output:**

```text
Lakebase instance rfi-intel-db DatabaseInstanceState.AVAILABLE host: ep-withered-water-d263r3ug.database.us-east-1.cloud.databricks.com pg PG_VERSION_16
database exists: rfi_ops
```

## 3 · Synced table: Delta `app_rfi_inbox` -> Postgres `rfi_ops.rfi_intel.rfi_inbox` (UC object `<catalog>.rfi_intel.rfi_inbox`)

```python
SYNCED = f"{FQ}.rfi_inbox"; PGS = schema
try:
    st = w.database.get_synced_database_table(SYNCED); created = False
except Exception:
    st = w.database.create_synced_database_table(SyncedDatabaseTable(name=SYNCED, database_instance_name=INSTANCE, logical_database_name=LOGICAL_DB, spec=SyncedTableSpec(
        source_table_full_name=f"{FQ}.app_rfi_inbox", primary_key_columns=["rfi_id"],
        scheduling_policy=SyncedTableSchedulingPolicy.SNAPSHOT))); created = True
    print("synced table created:", SYNCED)
pid = None
for _ in range(60):
    st = w.database.get_synced_database_table(SYNCED)
    ds = st.data_synchronization_status
    pid = pid or (ds.pipeline_id if ds else None)
    state = str(ds.detailed_state) if ds else "PENDING"
    if not created and pid:
        w.pipelines.start_update(pipeline_id=pid); created = True; print("snapshot refresh started"); time.sleep(20); continue
    if "ONLINE" in state and "PROVISIONING" not in state and "UPDATING" not in state: break
    print("sync state:", state); time.sleep(20)
print("synced table", SYNCED, "->", state, "| sync pipeline", pid)
ev["synced_table"] = {"name": SYNCED, "source": f"{FQ}.app_rfi_inbox", "state": state, "pipeline_id": pid}
```

**Output:**

```text
snapshot refresh started
sync state: SyncedTableState.SYNCED_TABLE_PROVISIONING_INITIAL_SNAPSHOT
synced table gauravsharma_catalog.rfi_intel.rfi_inbox -> SyncedTableState.SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE | sync pipeline 98b86669-bb57-4052-9cf1-8df3af41b93d
```

## 4 · Transactional tables the app writes to + service-principal role & grants

```python
conn = connect(LOGICAL_DB)
DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS ops.rfi_workflow_events (
  event_id     text PRIMARY KEY,
  rfi_id       text NOT NULL,
  event_type   text NOT NULL ,
  actor        text NOT NULL,
  detail       text,
  cost_impact  text,
  schedule_impact text,
  channel      text,
  created_at   timestamptz NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS ix_wf_rfi ON ops.rfi_workflow_events (rfi_id, created_at DESC);
ALTER TABLE ops.rfi_workflow_events DROP CONSTRAINT IF EXISTS rfi_workflow_events_event_type_check;
ALTER TABLE ops.rfi_workflow_events DROP CONSTRAINT IF EXISTS ck_event_type;
ALTER TABLE ops.rfi_workflow_events ADD CONSTRAINT ck_event_type CHECK (event_type IN ('triage_resolved','approved','changes_requested','routed','pco_flagged','closed','submitted_for_approval','approver_approved','approver_returned','sent_to_design'));
CREATE OR REPLACE VIEW ops.rfi_current_status AS
  SELECT DISTINCT ON (rfi_id) rfi_id, event_type, actor, created_at FROM ops.rfi_workflow_events
  WHERE event_type NOT IN ('pco_flagged') ORDER BY rfi_id, created_at DESC
"""
cur = conn.cursor()
if True:
    for stmt in [x.strip() for x in DDL.split(";") if x.strip()]: cur.execute(stmt)
    if SP:
        cur.execute("CREATE EXTENSION IF NOT EXISTS databricks_auth")
        cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (SP,))
        if not cur.fetchone():
            cur.execute("SELECT databricks_create_role(%s, 'SERVICE_PRINCIPAL')", (SP,))
        for g in [f'GRANT CONNECT ON DATABASE {LOGICAL_DB} TO "{SP}"', f'GRANT USAGE ON SCHEMA {PGS}, ops TO "{SP}"',
                  f'GRANT SELECT ON ALL TABLES IN SCHEMA {PGS} TO "{SP}"',
                  f'ALTER DEFAULT PRIVILEGES IN SCHEMA {PGS} GRANT SELECT ON TABLES TO "{SP}"',
                  f'GRANT SELECT, INSERT ON ops.rfi_workflow_events TO "{SP}"', f'GRANT SELECT ON ops.rfi_current_status TO "{SP}"']:
            cur.execute(g)
        print("Postgres role + grants applied for app SP", SP)
    cur.execute(f"SELECT count(*), count(*) FILTER (WHERE risk_tier = 'High') FROM {PGS}.rfi_inbox")
    total, high = cur.fetchone(); print(f"Postgres {LOGICAL_DB}.{PGS}.rfi_inbox: {total} rows ({high} High risk)")
    cur.execute(f"SELECT rfi_id, project_name, discipline, risk_tier, breach_probability, predicted_days_to_answer FROM {PGS}.rfi_inbox ORDER BY breach_probability DESC NULLS LAST LIMIT 5")
    top = cur.fetchall()
    for r in top: print("  ", r)
    ids = [r[0] for r in top]
    lat = []
    for i in range(100):  # serving latency: indexed point reads, as the app performs when a user opens an RFI
        t = time.perf_counter(); cur.execute(f"SELECT * FROM {PGS}.rfi_inbox WHERE rfi_id = %s", (ids[i % len(ids)],)); cur.fetchall(); lat.append((time.perf_counter() - t) * 1000)
    lat.sort()
print(f"Point-lookup latency over 100 reads: p50 {statistics.median(lat):.1f} ms, p95 {lat[94]:.1f} ms")
ev["postgres"] = {"rfi_inbox_rows": total, "high_risk": high, "top5": [list(map(str, r)) for r in top],
                  "p50_ms": round(statistics.median(lat), 1), "p95_ms": round(lat[94], 1), "delta_rows": n_inbox}
```

**Output:**

```text
Postgres role + grants applied for app SP 18dc1d86-01a1-46ad-a7d2-a58667f48569
Postgres rfi_ops.rfi_intel.rfi_inbox: 65 rows (16 High risk)
   ['RFI-104-02352', 'Harbor Point Data Center Phase 2', 'Architectural', 'High', 0.9405, 6.4]
   ['RFI-106-03207', 'Riverbend Wastewater Treatment Upgrade', 'Structural', 'High', 0.9185, 4.1]
   ['RFI-104-02369', 'Harbor Point Data Center Phase 2', 'Mechanical', 'High', 0.8747, 9.6]
   ['RFI-104-02326', 'Harbor Point Data Center Phase 2', 'Architectural', 'High', 0.8567, 11.9]
   ['RFI-104-02363', 'Harbor Point Data Center Phase 2', 'Fire Protection', 'High', 0.783, 10.5]
Point-lookup latency over 100 reads: p50 5.8 ms, p95 8.1 ms
```

## 5 · Unity Catalog grants for the app service principal (least privilege)

```python
if SP:
    for g in [f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{SP}`", f"GRANT USE SCHEMA ON SCHEMA {FQ} TO `{SP}`",
              *[f"GRANT SELECT ON TABLE {FQ}.{t} TO `{SP}`" for t in ["bronze_rfis", "silver_rfis", "gold_rfi_facts", "gold_project_kpis", "gold_firm_performance",
                 "gold_rfi_monthly", "gold_rfi_root_cause", "rfi_risk_scores", "rfi_ai_drafts", "spec_chunks", "app_rfi_inbox",
                 "rfi_workflow_audit", "v_rfi_secure"]],
              f"GRANT SELECT ON TABLE {FQ}.spec_chunks_index TO `{SP}`", f"GRANT EXECUTE ON FUNCTION {FQ}.mask_cost TO `{SP}`"]:
        try: spark.sql(g); print("OK  ", g)
        except Exception as e: print("FAIL", g, str(e)[:160])
display(spark.sql(f"DESCRIBE TABLE EXTENDED {SYNCED}").filter("col_name IN ('Type','Provider','Owner','Comment')"))
```

**Output:**

```text
OK   GRANT USE CATALOG ON CATALOG `gauravsharma_catalog` TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`
OK   GRANT USE SCHEMA ON SCHEMA gauravsharma_catalog.rfi_intel TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`
OK   GRANT SELECT ON TABLE gauravsharma_catalog.rfi_intel.bronze_rfis TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`
OK   GRANT SELECT ON TABLE gauravsharma_catalog.rfi_intel.silver_rfis TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`
OK   GRANT SELECT ON TABLE gauravsharma_catalog.rfi_intel.gold_rfi_facts TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`
OK   GRANT SELECT ON TABLE gauravsharma_catalog.rfi_intel.gold_project_kpis TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`
OK   GRANT SELECT ON TABLE gauravsharma_catalog.rfi_intel.gold_firm_performance TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`
OK   GRANT SELECT ON TABLE gauravsharma_catalog.rfi_intel.gold_rfi_monthly TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`
OK   GRANT SELECT ON TABLE gauravsharma_catalog.rfi_intel.gold_rfi_root_cause TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`
OK   GRANT SELECT ON TABLE gauravsharma_catalog.rfi_intel.rfi_risk_scores TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`
OK   GRANT SELECT ON TABLE gauravsharma_catalog.rfi_intel.rfi_ai_drafts TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`
OK   GRANT SELECT ON TABLE gauravsharma_catalog.rfi_intel.spec_chunks TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`
OK   GRANT SELECT ON TABLE gauravsharma_catalog.rfi_intel.app_rfi_inbox TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`
OK   GRANT SELECT ON TABLE gauravsharma_catalog.rfi_intel.rfi_workflow_audit TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`
OK   GRANT SELECT ON TABLE gauravsharma_catalog.rfi_intel.v_rfi_secure TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`
OK   GRANT SELECT ON TABLE gauravsharma_catalog.rfi_intel.spec_chunks_index TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`
OK   GRANT EXECUTE ON FUNCTION gauravsharma_catalog.rfi_intel.mask_cost TO `18dc1d86-01a1-46ad-a7d2-a58667f48569`


col_name | data_type | comment
--- | --- | ---
Type | FOREIGN | 
Comment |  | 
Provider | postgresql | 
Owner | build-user@example.com |
```

```python
dbutils.fs.put(f"/Volumes/{catalog}/{schema}/evidence/05_lakebase.json", json.dumps(ev, indent=2, default=str), overwrite=True)
print(json.dumps(ev, indent=2, default=str))
```

**Output:**

```text
Wrote 1374 bytes.
{
  "instance": {
    "name": "rfi-intel-db",
    "state": "DatabaseInstanceState.AVAILABLE",
    "capacity": "CU_1",
    "pg_version": "PG_VERSION_16"
  },
  "synced_table": {
    "name": "gauravsharma_catalog.rfi_intel.rfi_inbox",
    "source": "gauravsharma_catalog.rfi_intel.app_rfi_inbox",
    "state": "SyncedTableState.SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE",
    "pipeline_id": "98b86669-bb57-4052-9cf1-8df3af41b93d"
  },
  "postgres": {
    "rfi_inbox_rows": 65,
    "high_risk": 16,
    "top5": [
      [
        "RFI-104-02352",
        "Harbor Point Data Center Phase 2",
        "Architectural",
        "High",
        "0.9405",
        "6.4"
      ],
      [
        "RFI-106-03207",
        "Riverbend Wastewater Treatment Upgrade",
        "Structural",
        "High",
        "0.9185",
        "4.1"
      ],
      [
        "RFI-104-02369",
        "Harbor Point Data Center Phase 2",
        "Mechanical",
        "High",
        "0.8747",
        "9.6"
      ],
      [
        "RFI-104-02326",
        "Harbor Point Data Center Phase 2",
        "Architectural",
        "High",
        "0.8567",
        "11.9"
      ],
      [
        "RFI-104-02363",
        "Harbor Point Data Center Phase 2",
        "Fire Protection",
        "High",
        "0.783",
        "10.5"
      ]
    ],
    "p50_ms": 5.8,
    "p95_ms": 8.1,
    "delta_rows": 65
  }
}
```
