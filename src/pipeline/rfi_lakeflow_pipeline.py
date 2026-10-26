# Lakeflow Spark Declarative Pipeline — RFI Intelligence medallion
# Raw landing Volume (JSON / CSV / Markdown) -> bronze (Auto Loader) -> silver (expectations, dedupe, parse) -> gold.
import dlt
from pyspark.sql import functions as F, Window

RAW = spark.conf.get("raw_root")
VALID_DISC = ["Architectural", "Structural", "Mechanical", "Electrical", "Plumbing", "Civil", "Fire Protection"]


def autoload(path, fmt, **opts):
    r = spark.readStream.format("cloudFiles").option("cloudFiles.format", fmt)
    for k, v in opts.items():
        r = r.option(k, v)
    return (r.load(f"{RAW}/{path}")
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp()))


# ── Bronze ──────────────────────────────────────────────────────────────────────
@dlt.table(comment="Raw RFI log exports from the PM system (Auto Loader, JSON). One row per exported record, defects included.")
def bronze_rfis():
    return autoload("rfis", "json", **{"cloudFiles.inferColumnTypes": "true",
                                       "cloudFiles.schemaHints": "cost_impact_usd DOUBLE, schedule_impact_days INT, became_change_order BOOLEAN, on_critical_path BOOLEAN"})


@dlt.table(comment="Raw project register (Auto Loader, JSON).")
def bronze_projects():
    return autoload("projects", "json", **{"cloudFiles.inferColumnTypes": "true"})


@dlt.table(comment="Raw design-firm contractual SLA sheet (Auto Loader, CSV).")
def bronze_design_firms():
    return autoload("design_firms", "csv", header="true", **{"cloudFiles.inferColumnTypes": "true"})


@dlt.table(comment="Raw specification book, one Markdown document per CSI division (Auto Loader, whole-text).")
def bronze_spec_docs():
    return autoload("specs", "text", wholetext="true")


# ── Silver ──────────────────────────────────────────────────────────────────────
@dlt.table(comment="Cleansed, de-duplicated RFIs. Defective rows are dropped by expectations and counted in the event log.",
           table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_rfi_id", "rfi_id IS NOT NULL AND rfi_id LIKE 'RFI-%'")
@dlt.expect_or_drop("known_project", "project_id IS NOT NULL")
@dlt.expect_or_drop("valid_discipline", f"discipline IN ({', '.join(repr(d) for d in VALID_DISC)})")
@dlt.expect("answered_after_submitted", "answered_at IS NULL OR answered_at >= submitted_at")
def silver_rfis():
    b = (dlt.read("bronze_rfis")
         .withColumn("submitted_at", F.to_timestamp("submitted_at"))
         .withColumn("answered_at", F.to_timestamp("answered_at")))
    latest = Window.partitionBy("rfi_id").orderBy(F.col("_ingested_at").desc(), F.col("_source_file").desc())
    return (b.withColumn("_rn", F.row_number().over(latest)).filter("_rn = 1")
            .drop("_rn", "_root_cause_truth", "_rescued_data"))


@dlt.table(comment="Conformed project register.")
@dlt.expect_or_fail("project_pk", "project_id IS NOT NULL")
def silver_projects():
    return (dlt.read("bronze_projects")
            .select("project_id", "project_name", "sector", "general_contractor",
                    F.col("contract_value_usd").cast("double").alias("contract_value_usd"),
                    F.to_date("start_date").alias("start_date"), F.to_date("planned_finish").alias("planned_finish"),
                    F.col("liquidated_damages_per_day").cast("double").alias("liquidated_damages_per_day"))
            .dropDuplicates(["project_id"]))


@dlt.table(comment="Design firm of record per discipline with contractual RFI response SLA (calendar days).")
def silver_design_firms():
    return (dlt.read("bronze_design_firms")
            .select("firm", "discipline", F.col("contract_sla_days").cast("int").alias("contract_sla_days"))
            .dropDuplicates(["discipline"]))


@dlt.table(comment="Specification book parsed into one chunk per CSI section (source for the Vector Search index).")
@dlt.expect_or_drop("has_content", "length(content) > 40")
def silver_spec_chunks():
    docs = dlt.read("bronze_spec_docs").select(
        F.regexp_extract("_source_file", r"([^/]+)\.md$", 1).alias("doc_name"), F.col("value").alias("txt"))
    parts = docs.select("doc_name", F.posexplode(F.split("txt", r"\n## ")).alias("pos", "part")).filter("pos > 0")
    return (parts
            .withColumn("header", F.split("part", "\n")[0])
            .withColumn("section", F.trim(F.regexp_extract("header", r"^(\d\d \d\d \d\d)", 1)))
            .withColumn("title", F.trim(F.regexp_extract("header", r"^\d\d \d\d \d\d - (.+)$", 1)))
            .withColumn("content", F.trim(F.regexp_replace("part", r"^[^\n]*\n", "")))
            .withColumn("division", F.substring("section", 1, 2))
            .withColumn("chunk_id", F.sha2(F.concat_ws("|", "doc_name", "section"), 256).substr(1, 32))
            .select("chunk_id", "doc_name", "division", "section", "title", "content"))


# ── Gold ────────────────────────────────────────────────────────────────────────
@dlt.table(comment="One row per RFI enriched with project, firm SLA, response time and SLA outcome. Primary analytical fact table.",
           table_properties={"quality": "gold"})
def gold_rfi_facts():
    r, p, f = dlt.read("silver_rfis"), dlt.read("silver_projects"), dlt.read("silver_design_firms")
    now = F.current_timestamp()
    sla = F.when(F.col("priority") == "Urgent", F.lit(3)).otherwise(F.col("contract_sla_days"))
    return (r.join(p, "project_id", "left").join(f.select("discipline", "contract_sla_days"), "discipline", "left")
            .withColumn("sla_days", sla)
            .withColumn("due_at", F.expr("submitted_at + make_interval(0,0,0,sla_days)"))
            .withColumn("is_open", F.col("answered_at").isNull())
            .withColumn("days_to_answer", F.round((F.unix_timestamp("answered_at") - F.unix_timestamp("submitted_at")) / 86400, 2))
            .withColumn("days_open", F.round((F.unix_timestamp(now) - F.unix_timestamp("submitted_at")) / 86400, 1))
            .withColumn("sla_breached", F.when(F.col("is_open"), F.col("days_open") > F.col("sla_days"))
                                         .otherwise(F.col("days_to_answer") > F.col("sla_days")))
            .withColumn("submitted_month", F.date_trunc("month", "submitted_at").cast("date"))
            .withColumn("ld_exposure_usd", F.when(F.col("on_critical_path"), F.coalesce("schedule_impact_days", F.lit(0)) * F.col("liquidated_damages_per_day")).otherwise(F.lit(0.0)))
            .select("rfi_id", "project_id", "project_name", "sector", "discipline", "spec_section", "subject", "question",
                    "priority", "status", "is_open", "assigned_firm", "submitted_by_role", "submitted_by_company",
                    "has_drawing_ref", "has_proposed_solution", "proposed_solution", "capture_location", "bim_ref",
                    "submitted_at", "answered_at", "due_at", "sla_days", "days_to_answer", "days_open", "sla_breached",
                    "became_change_order", "cost_impact_usd", "schedule_impact_days", "on_critical_path", "ld_exposure_usd",
                    "liquidated_damages_per_day", "submitted_month"))


@dlt.table(comment="Portfolio KPIs per project: RFI volume, response time, SLA breach rate, change-order cost and schedule exposure.")
def gold_project_kpis():
    g = dlt.read("gold_rfi_facts")
    return (g.groupBy("project_id", "project_name", "sector", "liquidated_damages_per_day").agg(
        F.count("*").alias("total_rfis"),
        F.sum(F.col("is_open").cast("int")).alias("open_rfis"),
        F.sum((F.col("is_open") & F.col("sla_breached")).cast("int")).alias("open_overdue_rfis"),
        F.round(F.avg("days_to_answer"), 2).alias("avg_days_to_answer"),
        F.round(F.percentile_approx("days_to_answer", 0.9), 2).alias("p90_days_to_answer"),
        F.round(100 * F.avg(F.when(~F.col("is_open"), F.col("sla_breached").cast("int"))), 1).alias("sla_breach_pct"),
        F.sum(F.col("became_change_order").cast("int")).alias("change_orders"),
        F.round(F.sum("cost_impact_usd"), 0).alias("change_order_cost_usd"),
        F.sum("schedule_impact_days").alias("schedule_impact_days"),
        F.round(F.sum("ld_exposure_usd"), 0).alias("ld_exposure_usd")))


@dlt.table(comment="Design-firm responsiveness by discipline vs contractual SLA (answered RFIs only).")
def gold_firm_performance():
    g = dlt.read("gold_rfi_facts").filter("NOT is_open")
    return (g.groupBy("assigned_firm", "discipline", "sla_days").agg(
        F.count("*").alias("answered_rfis"),
        F.round(F.avg("days_to_answer"), 2).alias("avg_days_to_answer"),
        F.round(100 * F.avg(F.col("sla_breached").cast("int")), 1).alias("sla_breach_pct"),
        F.round(F.sum("cost_impact_usd"), 0).alias("change_order_cost_usd")))


@dlt.table(comment="Monthly RFI trend by discipline.")
def gold_rfi_monthly():
    g = dlt.read("gold_rfi_facts")
    return (g.groupBy("submitted_month", "discipline").agg(
        F.count("*").alias("rfis"),
        F.round(F.avg("days_to_answer"), 2).alias("avg_days_to_answer"),
        F.sum(F.col("sla_breached").cast("int")).alias("sla_breaches")))
