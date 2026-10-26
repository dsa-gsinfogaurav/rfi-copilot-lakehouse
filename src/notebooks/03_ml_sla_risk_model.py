# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · ML — predict which open RFIs will blow their response SLA
# MAGIC Project engineers triage by gut feel. This model scores every open RFI with the probability the design
# MAGIC team will miss the contractual response SLA, plus an expected days-to-answer, so the GC escalates the right
# MAGIC 10% *before* they become schedule delays. Trained on answered RFIs in `gold_rfi_facts`, tracked in MLflow,
# MAGIC registered in Unity Catalog, and batch-scored into `rfi_risk_scores` (synced to Lakebase for the app).

# COMMAND ----------
dbutils.widgets.text("catalog", "gauravsharma_catalog")
dbutils.widgets.text("schema", "rfi_intel")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema")
FQ = f"{catalog}.{schema}"
import json, numpy as np, pandas as pd, mlflow
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor
from sklearn.metrics import roc_auc_score, brier_score_loss, average_precision_score, mean_absolute_error
from sklearn.inspection import permutation_importance
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------
# MAGIC %md ## Features (all knowable at the moment the RFI is submitted — no leakage)

# COMMAND ----------
FEATURE_SQL = f"""
WITH f AS (
  SELECT g.*,
         length(question) AS question_len,
         month(submitted_at) AS submit_month,
         dayofweek(submitted_at) AS submit_dow,
         substr(spec_section, 1, 2) AS spec_division,
         -- coordination load: RFIs this project submitted in the 14 days before this one
         count(*) OVER (PARTITION BY project_id ORDER BY unix_timestamp(submitted_at)
                        RANGE BETWEEN 1209600 PRECEDING AND 1 PRECEDING) AS project_backlog_14d,
         -- firm load: RFIs routed to the same firm in the prior 14 days (portfolio-wide)
         count(*) OVER (PARTITION BY assigned_firm ORDER BY unix_timestamp(submitted_at)
                        RANGE BETWEEN 1209600 PRECEDING AND 1 PRECEDING) AS firm_backlog_14d
  FROM {FQ}.gold_rfi_facts g)
SELECT * FROM f"""
CAT = ["discipline", "priority", "sector", "assigned_firm", "spec_division", "submitted_by_role"]
NUM = ["question_len", "submit_month", "submit_dow", "project_backlog_14d", "firm_backlog_14d", "sla_days"]
BOOL = ["has_drawing_ref", "has_proposed_solution"]
FEATS = CAT + NUM + BOOL

df = spark.sql(FEATURE_SQL).toPandas()
for b in BOOL: df[b] = df[b].astype(int)
hist = df[~df.is_open].copy(); hist["sla_breached"] = hist["sla_breached"].astype(int)
openr = df[df.is_open].copy()
cut = hist["submitted_at"].quantile(0.8)       # time-based split: train on the past, test on the most recent 20%
train, test = hist[hist.submitted_at <= cut], hist[hist.submitted_at > cut]
print(f"answered={len(hist):,}  train={len(train):,}  test={len(test):,} (after {cut:%Y-%m-%d})  open_to_score={len(openr)}")
print(f"base SLA-breach rate: train {train.sla_breached.mean():.1%} | test {test.sla_breached.mean():.1%}")

# COMMAND ----------
# MAGIC %md ## Train + track in MLflow

# COMMAND ----------
def pipe(est):
    pre = ColumnTransformer([("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), CAT)], remainder="passthrough")
    return Pipeline([("pre", pre), ("model", est)])

user = spark.sql("SELECT current_user()").first()[0]
mlflow.set_experiment(f"/Users/{user}/rfi_intel_sla_risk")
with mlflow.start_run(run_name="sla_breach_classifier") as run:
    clf = pipe(HistGradientBoostingClassifier(max_iter=300, learning_rate=0.06, max_depth=5, l2_regularization=1.0, random_state=7))
    clf.fit(train[FEATS], train.sla_breached)
    p = clf.predict_proba(test[FEATS])[:, 1]
    top = test.assign(p=p).sort_values("p", ascending=False)
    k = max(1, int(len(top) * 0.2))
    metrics = {
        "test_auc": roc_auc_score(test.sla_breached, p),
        "test_avg_precision": average_precision_score(test.sla_breached, p),
        "test_brier": brier_score_loss(test.sla_breached, p),
        "precision_at_top20pct": top.head(k).sla_breached.mean(),
        "base_rate": test.sla_breached.mean(),
        "lift_at_top20pct": top.head(k).sla_breached.mean() / test.sla_breached.mean(),
    }
    mlflow.log_params({"algo": "HistGradientBoostingClassifier", "max_iter": 300, "lr": 0.06, "max_depth": 5,
                       "features": ",".join(FEATS), "train_rows": len(train), "split_cutoff": str(cut)})
    mlflow.log_metrics(metrics)
    imp = permutation_importance(clf, test[FEATS], test.sla_breached, n_repeats=5, random_state=0, scoring="roc_auc")
    importance = pd.Series(imp.importances_mean, index=FEATS).sort_values(ascending=False)
    mlflow.log_dict(importance.round(4).to_dict(), "permutation_importance.json")
    sig = infer_signature(train[FEATS], clf.predict_proba(train[FEATS])[:, 1])
    mlflow.sklearn.log_model(clf, "model", signature=sig, input_example=train[FEATS].head(3),
                             registered_model_name=f"{FQ}.rfi_sla_breach_model")
    clf_run = run.info.run_id
print(json.dumps({k: round(float(v), 4) for k, v in metrics.items()}, indent=2))
print("\nPermutation importance (AUC drop):\n", importance.round(4).to_string())

# COMMAND ----------
with mlflow.start_run(run_name="days_to_answer_regressor") as run:
    reg = pipe(HistGradientBoostingRegressor(max_iter=300, learning_rate=0.06, max_depth=5, loss="absolute_error", random_state=7))
    reg.fit(train[FEATS], train.days_to_answer)
    mae = mean_absolute_error(test.days_to_answer, reg.predict(test[FEATS]))
    naive = mean_absolute_error(test.days_to_answer, np.full(len(test), train.days_to_answer.median()))
    mlflow.log_metrics({"test_mae_days": mae, "naive_median_mae_days": naive})
    mlflow.sklearn.log_model(reg, "model", signature=infer_signature(train[FEATS], reg.predict(train[FEATS])),
                             input_example=train[FEATS].head(3), registered_model_name=f"{FQ}.rfi_days_to_answer_model")
print(f"days-to-answer MAE: {mae:.2f} days (naive median baseline {naive:.2f})")

# COMMAND ----------
# MAGIC %md ## Promote to `champion` alias in Unity Catalog

# COMMAND ----------
from mlflow import MlflowClient
c = MlflowClient()
versions = {}
for m in ["rfi_sla_breach_model", "rfi_days_to_answer_model"]:
    v = max(int(x.version) for x in c.search_model_versions(f"name='{FQ}.{m}'"))
    c.set_registered_model_alias(f"{FQ}.{m}", "champion", v); versions[m] = v
    print(f"{FQ}.{m}  version {v}  -> @champion")

# COMMAND ----------
# MAGIC %md ## Batch-score open RFIs with the registered champion models

# COMMAND ----------
breach = mlflow.sklearn.load_model(f"models:/{FQ}.rfi_sla_breach_model@champion")
days = mlflow.sklearn.load_model(f"models:/{FQ}.rfi_days_to_answer_model@champion")
openr["breach_probability"] = breach.predict_proba(openr[FEATS])[:, 1].round(4)
openr["predicted_days_to_answer"] = days.predict(openr[FEATS]).round(1)
openr["risk_tier"] = pd.cut(openr.breach_probability, [-1, 0.35, 0.65, 2], labels=["Low", "Medium", "High"]).astype(str)

slow_firms = set(hist.groupby("assigned_firm").sla_breached.mean().loc[lambda s: s > hist.sla_breached.mean()].index)
def drivers(r):
    d = []
    if r.assigned_firm in slow_firms: d.append(f"{r.assigned_firm} breaches SLA more often than average")
    if r.project_backlog_14d >= hist.project_backlog_14d.quantile(0.75): d.append(f"high project RFI load ({int(r.project_backlog_14d)} in last 14d)")
    if not r.has_drawing_ref: d.append("no drawing reference attached")
    if not r.has_proposed_solution: d.append("no proposed solution from the field")
    if r.priority == "Urgent": d.append("urgent 3-day SLA")
    return "; ".join(d[:3]) or "no elevated drivers"
openr["top_drivers"] = openr.apply(drivers, axis=1)
scores = openr[["rfi_id", "breach_probability", "predicted_days_to_answer", "risk_tier", "top_drivers"]].copy()
scores["model_version"] = f"breach v{versions['rfi_sla_breach_model']} / days v{versions['rfi_days_to_answer_model']}"
(spark.createDataFrame(scores).selectExpr("*", "current_timestamp() AS scored_at")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.rfi_risk_scores"))
spark.sql(f"COMMENT ON TABLE {FQ}.rfi_risk_scores IS 'ML batch scores for open RFIs: SLA-breach probability + expected days to answer (champion models in UC)'")
print(scores.risk_tier.value_counts().to_string())
display(spark.sql(f"SELECT * FROM {FQ}.rfi_risk_scores ORDER BY breach_probability DESC LIMIT 10"))

# COMMAND ----------
ev = {"classifier_run": clf_run, "metrics": {k: round(float(v), 4) for k, v in metrics.items()},
      "days_mae": round(mae, 2), "days_naive_mae": round(naive, 2), "versions": versions,
      "importance": importance.round(4).to_dict(), "risk_tiers": scores.risk_tier.value_counts().to_dict(),
      "top10": scores.sort_values("breach_probability", ascending=False).head(10).to_dict(orient="records")}
dbutils.fs.put(f"/Volumes/{catalog}/{schema}/evidence/03_ml.json", json.dumps(ev, indent=2, default=str), overwrite=True)
