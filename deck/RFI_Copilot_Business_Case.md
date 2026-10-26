# Answer RFIs before they become delays

**RFI Copilot for Summit Ridge Builders** · Engineering & Construction · General contractor, 8 active and recent projects, $995M contract value

**About $1.6M a year in protected margin.** RFI response time cut from 7.9 to 6 days. RFIs likely to miss their SLA are flagged the day they are submitted, before the delay reaches the critical path.

*Built end to end on the Databricks Data Intelligence Platform. All figures come from the working prototype running on synthetic but realistic portfolio data.*

---

## The outcome, in your KPIs

| KPI (portfolio) | Today | Target with RFI Copilot | Who feels it |
|---|---|---|---|
| Average days to answer an RFI | **7.9 days** (p90: 15 days) | **≤ 6.0 days** | Superintendents, crews |
| RFIs that miss the contractual SLA | **46.6%** | **≤ 30%** | Project Executives |
| Liquidated-damages exposure from late critical-path RFIs | **$11.2M** since 2023 (≈ $3.2M/yr) | **−30%** (≈ $1.0M/yr avoided) | COO, CFO |
| Change-order cost originating in RFIs | **$16.8M** (740 COs) | Root-cause feedback to preconstruction | Preconstruction, Estimating |
| Project-engineer time per RFI | ≈ $1,080 of handling cost per RFI (industry) | −35% with triage + cited AI drafts | Project Engineers |

**Value: about $1.6M per year for this portfolio.** The levers and assumptions are on slide 8. Platform run cost is sized during the pilot (see the appendix).

---

## The problem: RFIs are a hidden schedule risk

- **3,795 RFIs** across 8 projects. **65 are open right now, and 44 of them are already past SLA.**
- Half of all RFIs miss the design team's contractual response window (7 days, or 3 days if urgent).
- When a late RFI sits on a **critical-path** activity, every day it slips becomes a liquidated-damages day: $25K/day at Lakeside Hospital and $60K/day at Harbor Point.
- Project engineers triage by gut feel and re-type answers that already exist in the spec book. Leadership has no portfolio view of which firm, discipline or document gap is driving the cost.

> Industry context: the Navigant Construction Forum study of 1,362 projects found an average of 796 RFIs per project and 9.7 days per response.

---

## Where it hurts most (from the gold tables and Genie)

| Project | RFIs | Open / past SLA | Avg days | SLA breach | CO cost | LD exposure |
|---|---|---|---|---|---|---|
| Harbor Point Data Center Ph 2 | 778 | 24 / 15 | 10.5 | 66% | $3.2M | **$7.6M** |
| Lakeside Regional Hospital | 846 | 8 / 4 | 8.5 | 53% | $4.2M | **$2.6M** |
| Union Station Mixed-Use Tower | 537 | 6 / 5 | 7.3 | 42% | $2.2M | $0.8M |

- **Design firms:** the MEP and fire-protection engineers breach SLA 57–65% of the time, versus 21–36% for civil and structural.
- **Root cause (Gen AI on every RFI):** spec ambiguity is 28% of volume, and field conditions plus design conflicts drive **$10.3M** of change-order cost.
- **Implication:** two projects and two design firms account for most of the exposure. That is where to start.

---

## What changes for the project team (domain owner)

**Today:** an RFI arrives by email or in Procore. The PE reads it, searches the spec PDF, types an answer, forwards it to the architect and chases it in a spreadsheet.

**With RFI Copilot:**
1. **One inbox ranked by risk.** Every open RFI shows the ML probability that the design team will miss the SLA, the expected days to answer, and *why* (for example, "MEP firm breaches more often · 26 RFIs on this project in 14 days · no drawing attached").
2. **Triage gate.** If the spec already answers it, the PE closes it in one click and it never reaches the design team. About 28% of RFIs are spec-ambiguity questions.
3. **A cited draft is waiting.** Vector Search finds the governing spec section, and Claude drafts a response that cites it (for example, "per 21 13 13"). The PE edits and approves it.
4. **Approver sign-off (human in the loop).** High-risk, urgent or cost/schedule-impacting responses stop in an Approvers queue for a named Project Executive, who signs off or returns them with a comment. Low-risk answers go straight to the design firm.
5. **Route, flag a PCO, close.** Every action shows a live trace (Lakebase commit in about 5 ms, then the audit job merging into Delta) and lands in the RFI's audit trail.
6. **Ask Genie.** "Which open RFIs on Harbor Point will miss SLA?" returns a plain-English answer plus the SQL behind it.

---

## How it works: one integrated data journey

1. **Lakeflow:** Auto Loader ingests the nightly PM-system exports (RFI logs as JSON, design-firm SLAs as CSV, the spec book as Markdown) into bronze, silver and gold tables. Expectations removed 49 defective or duplicate records in the latest run.
2. **Unity Catalog:** tags, descriptions, a column mask that hides change-order dollars from anyone outside finance, least-privilege grants to the app, and automatic lineage (19 table-to-table edges captured).
3. **ML:** an SLA-breach classifier and a days-to-answer model, tracked in MLflow, registered in Unity Catalog as `@champion`, and run nightly to score every open RFI.
4. **Gen AI:** `ai_classify` assigns a root cause to every RFI. A Vector Search index over the spec book plus Claude (Foundation Model API) produces cited drafts.
5. **Lakebase:** a synced table serves the inbox to the app in about 6 ms. App decisions are Postgres transactions that a Lakeflow Job merges back into a governed Delta audit table.
6. **Genie + Databricks App:** a Genie space over the gold, ML, root-cause and audit tables, and the RFI Copilot app for the project team.

**Everything is one Asset Bundle:** `databricks bundle deploy && databricks bundle run rfi_build`.

---

## Proof it works (prototype results)

| Capability | Result from the latest end-to-end run |
|---|---|
| Lakeflow data quality | 3,844 raw records → 3,795 clean: 15 unknown-project and 15 invalid-discipline rows dropped by expectations, 19 duplicate re-sends removed |
| ML: who will miss SLA | AUC **0.74** on a time-based holdout; **77% precision in the top-20% risk band vs a 47% base rate (1.64× lift)** |
| ML: days to answer | MAE 3.5 days vs 4.1 days for a naive baseline |
| Gen AI retrieval | The governing spec section was retrieved for **95%** of open RFIs (hit@4) |
| Gen AI drafts | 65 of 65 drafts generated; 64 of 65 cite only spec sections present in the retrieved context |
| Gen AI root cause | `ai_classify` agrees with the ground-truth label for 74% of 3,795 RFIs |
| Lakebase serving | Point reads **p50 5.8 ms / p95 8.1 ms**; app writes commit in about 5 ms |
| Genie | 6 of 6 executive questions answered with generated SQL (overdue by project, firm speed, design-conflict CO cost, LD exposure) |

---

## Value model (annualized for this portfolio)

| Lever | Mechanism | Assumption | Annual value |
|---|---|---|---|
| Avoided liquidated damages | Escalate high-risk critical-path RFIs at submission, days before the SLA lapses | 30% of the ≈ $3.2M/yr LD exposure from late critical-path RFIs avoided | **≈ $0.96M** |
| Project-engineer productivity | Triage gate + cited drafts | ≈ 1,085 RFIs/yr × $1,080 handling cost × 35% saved | **≈ $0.41M** |
| Fewer RFI-driven change orders | Root-cause feedback to preconstruction (design conflicts + missing information) | 10% of ≈ $1.9M/yr | **≈ $0.19M** |
| Crew idle time (not counted) | Faster answers on non-critical work | — | upside |
| **Total** | | | **≈ $1.6M / yr** |

*The history covers about 3.5 years (2023 to Oct 2026), so annual values are total ÷ 3.5. Every assumption is a parameter to validate during the pilot.*

---

## For the executive sponsor (COO / VP Operations)

- **Margin protection you can measure.** Liquidated damages and RFI-driven change orders are tracked per project in governed tables, so there is a weekly number, not an anecdote.
- **Early warning, portfolio-wide.** The risk model flags the RFIs most likely to stall work before the SLA lapses. Genie answers "where are we exposed?" without waiting on an analyst.
- **Design-team accountability.** Firm-level SLA performance (for example, 65% breach for one specialty engineer) gives you leverage at the next fee negotiation or selection.
- **Low-risk platform choice.** One governed platform for data, ML, Gen AI, the app database and BI. There is no new point product to integrate, and it runs on the GC's existing Databricks account.

---

## For the domain owner (Director of Project Controls / Project Executives)

- **Less typing, same control.** AI drafts are always reviewed and approved by a person. Citations show exactly which spec section supports the answer, and gaps are flagged instead of guessed.
- **Fewer RFIs reach the design team.** The triage gate closes the ones the contract documents already answer.
- **One queue, sorted by what will hurt.** Risk tier, expected answer date, liquidated-damages rate and field proposal are all on one screen.
- **Audit trail by default.** Every approve, route, PCO flag or close is a timestamped event in Lakebase, mirrored to a governed Delta table and queryable in Genie.
- **Fits existing tools.** Ingestion reads the PM system's exports (Procore and Autodesk Construction Cloud style), so no new data entry is required.

---

## Trust, governance and controls

- **Unity Catalog** governs every table, model, index and the synced Lakebase table: lineage from raw file to app, tags, and descriptions.
- **Least privilege:** the app runs as its own service principal with SELECT on specific tables, CAN_USE on one warehouse, CAN_RUN on the Genie space, and INSERT on one Postgres table.
- **Sensitive dollars are masked:** change-order cost is NULL for anyone outside the finance group, enforced by a column-mask function. The build's own run shows the masked output.
- **Human in the loop:** AI drafts never go out without a person approving them. A policy gate (ML risk, AI-assessed impact, urgency, LD rate) adds a named approver for high-stakes answers, and sign-off turnaround is tracked. The decision log is append-only and synced to Delta.
- **No customer data:** the prototype uses synthetic data; a pilot would run inside the GC's own workspace.

---

## 90-day pilot plan

| Weeks | Scope | Success criteria |
|---|---|---|
| 1–3 | Connect Procore/ACC RFI exports and the spec book for **Harbor Point** and **Lakeside** | Daily Lakeflow ingest; gold KPIs match the PM system within 1% |
| 4–6 | Risk model retrained on the GC's own history; Copilot rolled out to 6 project engineers | Top-20% risk band precision ≥ 70%; ≥ 60% of drafts approved with light edits |
| 7–10 | Genie space for project executives; weekly risk review | Executives use Genie weekly; high-risk RFIs escalated ≥ 10 days before the SLA |
| 11–13 | Measure and decide | Average response ≤ 6 days on pilot projects; LD-exposure trend down; go/no-go for all 8 projects |

**The ask:** a sponsor (COO), a domain owner (Director of Project Controls), two pilot projects, and read access to RFI exports and spec books.

---

## Appendix: assumptions and sources

- Portfolio, RFIs, firms and spec book are **synthetic**, produced by `01_generate_raw_data`. The generator encodes realistic drivers: firm responsiveness, coordination load, missing drawings, priority, and a 12% critical-path rate.
- Liquidated-damages exposure = schedule-impact days × the project's LD rate, counted **only** for critical-path RFIs.
- RFI handling cost (≈ $1,080 per RFI), 796 RFIs per project and 9.7 days per response: Navigant Construction Forum, "Impact of Requests for Information on Construction Projects" (2013).
- Platform run cost (indicative, to be sized in the pilot): a nightly serverless pipeline and jobs, one small Lakebase instance, one Vector Search endpoint, a serverless SQL warehouse, and pay-per-token model calls.
- Code, executed notebooks with outputs, and app smoke-test results: see the GitHub repository (`evidence/`).
