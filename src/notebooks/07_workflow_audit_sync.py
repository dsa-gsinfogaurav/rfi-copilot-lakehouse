# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Close the loop — Lakebase decisions -> governed Delta audit
# MAGIC Human decisions taken in the app land in Lakebase (`ops.rfi_workflow_events`, transactional, millisecond writes).
# MAGIC This job pulls new events and MERGEs them into the Unity Catalog Delta table `rfi_workflow_audit`, where Genie,
# MAGIC BI and future model retraining can use them. Triggered by the app after each decision and on a 15-minute schedule.

# COMMAND ----------
# MAGIC %pip install -q -U "pg8000==1.31.2" "scramp==1.4.5"

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("catalog", "gauravsharma_catalog")
dbutils.widgets.text("schema", "rfi_intel")
dbutils.widgets.text("instance", "rfi-intel-db")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema"); INSTANCE = dbutils.widgets.get("instance")
FQ = f"{catalog}.{schema}"
import uuid, ssl, pg8000.dbapi
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# COMMAND ----------
hwm = spark.sql(f"SELECT coalesce(max(created_at), timestamp'1970-01-01') AS t FROM {FQ}.rfi_workflow_audit").first().t
inst = w.database.get_database_instance(INSTANCE)
tok = w.database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[INSTANCE]).token
c = pg8000.dbapi.connect(host=inst.read_write_dns, port=5432, database="rfi_ops", user=w.current_user.me().user_name,
                         password=tok, ssl_context=ssl.create_default_context())
cur = c.cursor()
cur.execute("""SELECT event_id, rfi_id, event_type, actor, detail, cost_impact, schedule_impact, channel, created_at
               FROM ops.rfi_workflow_events WHERE created_at >= %s ORDER BY created_at""", (hwm,))
rows = [tuple(r) for r in cur.fetchall()]; c.close()
print(f"high-water mark {hwm} -> {len(rows)} candidate events from Lakebase")

# COMMAND ----------
if rows:
    ddl = "event_id string, rfi_id string, event_type string, actor string, detail string, cost_impact string, schedule_impact string, channel string, created_at timestamp"
    spark.createDataFrame(rows, ddl).createOrReplaceTempView("lakebase_events")
    display(spark.sql(f"""MERGE INTO {FQ}.rfi_workflow_audit t USING lakebase_events s ON t.event_id = s.event_id
        WHEN NOT MATCHED THEN INSERT (event_id, rfi_id, event_type, actor, detail, cost_impact, schedule_impact, channel, created_at, synced_at)
        VALUES (s.event_id, s.rfi_id, s.event_type, s.actor, s.detail, s.cost_impact, s.schedule_impact, s.channel, s.created_at, current_timestamp())"""))
display(spark.sql(f"SELECT event_type, count(*) AS events, max(created_at) AS latest FROM {FQ}.rfi_workflow_audit GROUP BY 1 ORDER BY 2 DESC"))
