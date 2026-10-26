# Build run summary (text evidence)

**Job:** `rfi-intel-end-to-end-build` · **run id** `111509992172372` · started 2026-10-26 18:04:26 UTC · ended 2026-10-26 18:20:41 UTC · **result: SUCCESS**

| task | result | task run id |
|---|---|---|
| generate_raw_data | SUCCESS | 597286035803358 |
| lakeflow_medallion | SUCCESS | 566330290946859 |
| genai_rag_classify | SUCCESS | 163765045862470 |
| ml_sla_risk | SUCCESS | 147429710057638 |
| uc_governance | SUCCESS | 462904608483564 |
| lakebase_serving | SUCCESS | 288708685860787 |
| genie_space | SUCCESS | 568384539121537 |

## ① Lakeflow pipeline `rfi-intel-lakeflow-medallion`
pipeline id `25d9d794-e5c9-4523-a23d-bc6967152480` · latest update `93cd1ce9-0c23-4350-a3a7-d2d7a12c6ddb` · state **COMPLETED** · full refresh: True

Flows completed in this update (from the pipeline event log):

- gauravsharma_catalog.rfi_intel.gold_rfi_monthly: Flow 'gauravsharma_catalog.rfi_intel.gold_rfi_monthly' has COMPLETED.
- gauravsharma_catalog.rfi_intel.gold_project_kpis: Flow 'gauravsharma_catalog.rfi_intel.gold_project_kpis' has COMPLETED.
- gauravsharma_catalog.rfi_intel.gold_firm_performance: Flow 'gauravsharma_catalog.rfi_intel.gold_firm_performance' has COMPLETED.
- gauravsharma_catalog.rfi_intel.gold_rfi_facts: Flow 'gauravsharma_catalog.rfi_intel.gold_rfi_facts' has COMPLETED.
- gauravsharma_catalog.rfi_intel.silver_rfis: Flow 'gauravsharma_catalog.rfi_intel.silver_rfis' has COMPLETED.
- gauravsharma_catalog.rfi_intel.silver_design_firms: Flow 'gauravsharma_catalog.rfi_intel.silver_design_firms' has COMPLETED.
- gauravsharma_catalog.rfi_intel.silver_spec_chunks: Flow 'gauravsharma_catalog.rfi_intel.silver_spec_chunks' has COMPLETED.
- gauravsharma_catalog.rfi_intel.silver_projects: Flow 'gauravsharma_catalog.rfi_intel.silver_projects' has COMPLETED.
- gauravsharma_catalog.rfi_intel.bronze_rfis: Flow 'gauravsharma_catalog.rfi_intel.bronze_rfis' has COMPLETED.
- gauravsharma_catalog.rfi_intel.bronze_design_firms: Flow 'gauravsharma_catalog.rfi_intel.bronze_design_firms' has COMPLETED.
- gauravsharma_catalog.rfi_intel.bronze_spec_docs: Flow 'gauravsharma_catalog.rfi_intel.bronze_spec_docs' has COMPLETED.
- gauravsharma_catalog.rfi_intel.bronze_projects: Flow 'gauravsharma_catalog.rfi_intel.bronze_projects' has COMPLETED.

Row counts: bronze_rfis 3,844 → silver_rfis 3,795 → gold_rfi_facts 3,795; spec chunks 25.
Raw generator: 3,844 RFI records incl. 19 duplicate re-sends and 30 defective rows (15 null project, 15 invalid discipline). Removed in silver: 49.
Note: the expectation pass/fail counts in `02_governance.json` were summed across every pipeline update in the event log (4 full refreshes during development), so they are cumulative. Notebook 02 now filters to the latest update.

Expectation results for **this update only** (pipeline event log):

| dataset | expectation | passed | failed |
|---|---|---|---|
| silver_projects | project_pk | 8 | 0 |
| silver_rfis | answered_after_submitted | 3825 | 0 |
| silver_rfis | known_project | 3810 | 15 |
| silver_rfis | valid_discipline | 3810 | 15 |
| silver_rfis | valid_rfi_id | 3825 | 0 |
| silver_spec_chunks | has_content | 25 | 0 |

## ② Unity Catalog governance
- Tags applied: 7/7 tables; column mask `mask_cost()` + governed view `v_rfi_secure` (cost shows NULL for the build principal, who is not in the finance group, which proves the mask works)
- Lineage edges captured in `system.access.table_lineage`: 19

## ③ ML (MLflow + UC registry)
- SLA-breach classifier: AUC 0.7439, average precision 0.7065, Brier 0.2083; precision in top-20% risk band 0.7651 vs base rate 0.4665 (lift 1.6401)
- Days-to-answer regressor: MAE 3.54 days vs naive 4.09
- Registered versions (@champion): {'rfi_sla_breach_model': 4, 'rfi_days_to_answer_model': 4}; open RFIs scored by tier: {'Low': 36, 'High': 16, 'Medium': 13}

## ④ Gen AI
- Vector Search index `gauravsharma_catalog.rfi_intel.spec_chunks_index`: 25 chunks, embeddings `databricks-gte-large-en`
- `ai_classify` root cause agreement with hidden ground truth: 74.0%; distribution {'Spec ambiguity': 1065, 'Design conflict': 785, 'Field condition': 749, 'Missing information': 721, 'Substitution request': 462, 'Constructability': 13}
- RAG over 65 open RFIs: retrieval hit@4 95.4%; draft failures 0; drafts citing only retrieved sections 64/65
- Sample draft (RFI-108-03798, retrieved 08 41 13, 21 13 13, 23 05 93, 05 12 00): "The supplied specification excerpts (08 41 13, 21 13 13, 23 05 93, and 05 12 00) contain no lighting section and no luminaire efficacy requirement, so we cannot confirm whether a 105 lm/W troffer is acceptable. The specified minimum efficacy (lm/W) must come from the lighting fixture section (e.g., Division 26 luminaire requirements) or the electrical drawings' fixture schedule, neither of which w"

## ⑤ Lakebase
- Instance `rfi-intel-db` (CU_1, PG_VERSION_16); synced table `gauravsharma_catalog.rfi_intel.rfi_inbox` state SyncedTableState.SYNCED_TABLE_ONLINE_NO_PENDING_UPDATE
- Postgres rows 65 (Delta source 65), high risk 16; point-lookup latency p50 5.8 ms / p95 8.1 ms

## ⑥ Genie
- Space `01f1d15f1b311ebdafa1e2f312eb23b9`; questions answered with SQL: 6/6
  - **Which projects have the most overdue open RFIs right now?** → You want to see a list of projects ranked by the number of their currently open RFIs that are overdue, meaning those RFIs have missed their SLA deadli First row: ['Harbor Point Data Center Phase 2', '15', '1']
  - **What is our average RFI response time by discipline, and how has it trended over the last 6 months?** → You want to see the average response time for RFIs by discipline overall and how this average has changed month by month over the last six months. First row: ['2026-05-01T00:00:00.000Z', 'Architectural', '7.0619582955575675', '7.327031249999997']
  - **How much change-order cost came from RFIs caused by design conflicts, by project?** → You want to see the total change-order cost caused by RFIs that have a root cause related to design conflicts, broken down by project name. First row: ['Lakeside Regional Hospital Expansion', '1154781.0']
  - **Which design firm is slowest to answer RFIs?** → You want to find the design firm that takes the longest average time to answer RFIs. First row: ['Sentinel Fire Protection Engineering', '196', '10.1', '73.7']
  - **Show the 10 open RFIs most likely to miss their SLA with their drivers** → You want to see the top 10 open RFIs that have the highest predicted probability of missing their SLA, along with details about the project, disciplin First row: ['RFI-104-02352', 'Harbor Point Data Center Phase 2', 'Architectural', 'Storefront deflection criteria', '0.9405', '6.4', 'High', 'no drawing reference attached; no proposed solution from the field; urgent 3-day SLA']
  - **What is the total liquidated-damages exposure tied to late RFIs on the Harbor Point Data Center?** → You want to see the total liquidated damages exposure caused by RFIs that were late (missed their SLA) for the Harbor Point Data Center project. First row: ['Harbor Point Data Center Phase 2', '7260000.0']

## ⑦ Databricks App
See `app_smoke_test.json`: live calls against the deployed app (Lakebase read, warehouse read, Vector Search, Claude draft, Genie answer, Lakebase write + audit job).
