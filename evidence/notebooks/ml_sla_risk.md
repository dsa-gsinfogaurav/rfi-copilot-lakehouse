# ml_sla_risk — executed notebook transcript

Job run 111509992172372, task run 147429710057638, result: **SUCCESS**

# 03 · ML — predict which open RFIs will blow their response SLA
Project engineers triage by gut feel. This model scores every open RFI with the probability the design
team will miss the contractual response SLA, plus an expected days-to-answer, so the GC escalates the right
10% *before* they become schedule delays. Trained on answered RFIs in `gold_rfi_facts`, tracked in MLflow,
registered in Unity Catalog, and batch-scored into `rfi_risk_scores` (synced to Lakebase for the app).

```python
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
```

## Features (all knowable at the moment the RFI is submitted — no leakage)

```python
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
```

**Output:**

```text
answered=3,730  train=2,984  test=746 (after 2026-05-20)  open_to_score=65
base SLA-breach rate: train 46.6% | test 46.6%
```

## Train + track in MLflow

```python
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
```

**Output:**

```text
2026/10/26 18:07:39 WARNING mlflow.models.model: `artifact_path` is deprecated. Please use `name` instead.
🔗 View Logged Model at: https://fevm-gauravsharma.cloud.databricks.com/ml/experiments/4087986703484191/models/m-03f43df703e74549a08d06738baf956a?o=7474643869233611
Registered model 'gauravsharma_catalog.rfi_intel.rfi_sla_breach_model' already exists. Creating a new version of this model...



🔗 Created version '4' of model 'gauravsharma_catalog.rfi_intel.rfi_sla_breach_model': https://fevm-gauravsharma.cloud.databricks.com/explore/data/models/gauravsharma_catalog/rfi_intel/rfi_sla_breach_model/version/4?o=7474643869233611


{
  "test_auc": 0.7439,
  "test_avg_precision": 0.7065,
  "test_brier": 0.2083,
  "precision_at_top20pct": 0.7651,
  "base_rate": 0.4665,
  "lift_at_top20pct": 1.6401
}

Permutation importance (AUC drop):
 project_backlog_14d      0.0413
sla_days                 0.0336
sector                   0.0319
question_len             0.0187
priority                 0.0185
has_drawing_ref          0.0182
has_proposed_solution    0.0140
discipline               0.0132
spec_division            0.0095
submit_month             0.0068
submit_dow               0.0060
firm_backlog_14d         0.0041
assigned_firm            0.0040
submitted_by_role       -0.0034
```

```python
with mlflow.start_run(run_name="days_to_answer_regressor") as run:
    reg = pipe(HistGradientBoostingRegressor(max_iter=300, learning_rate=0.06, max_depth=5, loss="absolute_error", random_state=7))
    reg.fit(train[FEATS], train.days_to_answer)
    mae = mean_absolute_error(test.days_to_answer, reg.predict(test[FEATS]))
    naive = mean_absolute_error(test.days_to_answer, np.full(len(test), train.days_to_answer.median()))
    mlflow.log_metrics({"test_mae_days": mae, "naive_median_mae_days": naive})
    mlflow.sklearn.log_model(reg, "model", signature=infer_signature(train[FEATS], reg.predict(train[FEATS])),
                             input_example=train[FEATS].head(3), registered_model_name=f"{FQ}.rfi_days_to_answer_model")
print(f"days-to-answer MAE: {mae:.2f} days (naive median baseline {naive:.2f})")
```

**Output:**

```text
2026/10/26 18:07:54 WARNING mlflow.models.model: `artifact_path` is deprecated. Please use `name` instead.
🔗 View Logged Model at: https://fevm-gauravsharma.cloud.databricks.com/ml/experiments/4087986703484191/models/m-e48ef85a88ff4b7c9401224d556f991e?o=7474643869233611
Registered model 'gauravsharma_catalog.rfi_intel.rfi_days_to_answer_model' already exists. Creating a new version of this model...



🔗 Created version '4' of model 'gauravsharma_catalog.rfi_intel.rfi_days_to_answer_model': https://fevm-gauravsharma.cloud.databricks.com/explore/data/models/gauravsharma_catalog/rfi_intel/rfi_days_to_answer_model/version/4?o=7474643869233611


days-to-answer MAE: 3.54 days (naive median baseline 4.09)
```

## Promote to `champion` alias in Unity Catalog

```python
from mlflow import MlflowClient
c = MlflowClient()
versions = {}
for m in ["rfi_sla_breach_model", "rfi_days_to_answer_model"]:
    v = max(int(x.version) for x in c.search_model_versions(f"name='{FQ}.{m}'"))
    c.set_registered_model_alias(f"{FQ}.{m}", "champion", v); versions[m] = v
    print(f"{FQ}.{m}  version {v}  -> @champion")
```

**Output:**

```text
gauravsharma_catalog.rfi_intel.rfi_sla_breach_model  version 4  -> @champion
gauravsharma_catalog.rfi_intel.rfi_days_to_answer_model  version 4  -> @champion
```

## Batch-score open RFIs with the registered champion models

```python
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
```

**Output:**

```text
risk_tier
Low       36
High      16
Medium    13


rfi_id | breach_probability | predicted_days_to_answer | risk_tier | top_drivers | model_version | scored_at
--- | --- | --- | --- | --- | --- | ---
RFI-104-02352 | 0.9405 | 6.4 | High | no drawing reference attached; no proposed solution from the field; urgent 3-day SLA | breach v4 / days v4 | 2026-10-26T18:08:14.789Z
RFI-106-03207 | 0.9185 | 4.1 | High | no drawing reference attached; no proposed solution from the field; urgent 3-day SLA | breach v4 / days v4 | 2026-10-26T18:08:14.789Z
RFI-104-02369 | 0.8747 | 9.6 | High | Brightline MEP Partners breaches SLA more often than average; no proposed solution from the field | breach v4 / days v4 | 2026-10-26T18:08:14.789Z
RFI-104-02326 | 0.8567 | 11.9 | High | high project RFI load (29 in last 14d); no drawing reference attached; no proposed solution from the field | breach v4 / days v4 | 2026-10-26T18:08:14.789Z
RFI-104-02363 | 0.783 | 10.5 | High | Sentinel Fire Protection Engineering breaches SLA more often than average; no drawing reference attached | breach v4 / days v4 | 2026-10-26T18:08:14.789Z
RFI-104-02338 | 0.7797 | 10.8 | High | Brightline MEP Partners breaches SLA more often than average; no drawing reference attached; no proposed solution from the field | breach v4 / days v4 | 2026-10-26T18:08:14.789Z
RFI-106-03205 | 0.767 | 9.4 | High | Brightline MEP Partners breaches SLA more often than average; no drawing reference attached; no proposed solution from the field | breach v4 / days v4 | 2026-10-26T18:08:14.789Z
RFI-104-02367 | 0.7555 | 10.2 | High | Sentinel Fire Protection Engineering breaches SLA more often than average; no proposed solution from the field | breach v4 / days v4 | 2026-10-26T18:08:14.789Z
RFI-104-02347 | 0.7523 | 9.0 | High | Brightline MEP Partners breaches SLA more often than average; no drawing reference attached | breach v4 / days v4 | 2026-10-26T18:08:14.789Z
RFI-104-02360 | 0.7372 | 7.7 | High | Voltaic Electrical Design breaches SLA more often than average; no proposed solution from the field | breach v4 / days v4 | 2026-10-26T18:08:14.789Z
```

```python
ev = {"classifier_run": clf_run, "metrics": {k: round(float(v), 4) for k, v in metrics.items()},
      "days_mae": round(mae, 2), "days_naive_mae": round(naive, 2), "versions": versions,
      "importance": importance.round(4).to_dict(), "risk_tiers": scores.risk_tier.value_counts().to_dict(),
      "top10": scores.sort_values("breach_probability", ascending=False).head(10).to_dict(orient="records")}
dbutils.fs.put(f"/Volumes/{catalog}/{schema}/evidence/03_ml.json", json.dumps(ev, indent=2, default=str), overwrite=True)
```

**Output:**

```text
Wrote 4112 bytes.


{"text/plain": "True"}
```
