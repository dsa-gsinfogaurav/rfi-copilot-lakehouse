# Answer RFIs before they become delays

**RFI Copilot for Summit Ridge Builders** · Engineering & Construction · General contractor, 8 active and recent projects, $995M contract value

**About $1.6M a year in protected margin, roughly half a point of operating margin.** RFI response time drops from 7.9 to 6 days. RFIs likely to miss their SLA are flagged the day they are submitted, before the delay reaches the critical path and the bonus scorecard.

*Built end to end on the Databricks Data Intelligence Platform. All figures come from the working prototype running on synthetic but realistic portfolio data.*

---

## Why we are here: the industry is losing margin to late information

| What the industry data shows | Source |
|---|---|
| Large projects typically finish **20% late** and run **up to 80% over budget** | McKinsey Global Institute, *Reinventing Construction* (2017) |
| Construction productivity grew about **1% a year** for two decades, versus 2.8% for the world economy | McKinsey Global Institute (2017) |
| Project teams spend **35% of their time** (over 14 hours a week) searching for information, resolving conflicts and fixing rework | FMI & PlanGrid, *Construction Disconnected* (2018) |
| Bad data cost the industry an estimated **$1.8 trillion** in 2020, and about **14% of rework** traces to bad data | Autodesk & FMI, *Harnessing the Data Advantage* (2021) |
| The average project generates **796 RFIs**, each taking **9.7 days** to answer and costing about **$1,080** to process | Navigant Construction Forum (2013) |

**What we see in Summit Ridge's own data:** 47% of RFIs miss the design team's SLA, 44 of the 65 open RFIs are already late, and late critical-path RFIs carry **$11.2M** of liquidated-damages exposure.

**The RFI is where late information turns into lost margin.** It is the moment a crew stops and waits.

---

## Why invest now, and why build it on your own data

- **Contracts have shifted schedule risk onto the GC.** Harbor Point carries $60K/day in liquidated damages, so schedule certainty now decides both margin and the next bid.
- **AI can now read the contract documents.** In the prototype, retrieval finds the governing spec section for 95% of open RFIs and Claude drafts a cited answer, at pay-per-token cost.
- **The data already exists, just disconnected.** RFI logs, spec books, firm SLAs and schedules sit in Procore or ACC exports and PDFs. Unifying them once in a governed lakehouse also feeds the next use cases: submittals, change orders and schedule risk.
- **Building it beats buying a point tool.** Off-the-shelf RFI tools stop at the workflow. This keeps the data, the risk model, the AI and the audit trail inside the GC's own tenant, with no per-seat lock-in and the GC's history as the training set.
- **Waiting has a price.** Each quarter of the status quo forgoes about **$0.4M** of the $1.6M a year. Harbor Point is in its peak RFI phase right now (24 open, 15 already late).

---

## The outcome, in your KPIs

| KPI (portfolio) | Today | Target with RFI Copilot | Who feels it |
|---|---|---|---|
| Average days to answer an RFI | **7.9 days** (p90: 15 days) | **≤ 6.0 days** | Superintendents, crews |
| RFIs that miss the contractual SLA | **46.6%** | **≤ 30%** | Project Executives |
| Liquidated-damages exposure from late critical-path RFIs | **$11.2M** since 2023 (≈ $3.2M/yr) | **−30%** (≈ $1.0M/yr avoided) | COO, CFO |
| Change-order cost originating in RFIs | **$16.8M** (740 COs) | Root-cause feedback to preconstruction | Preconstruction, Estimating |
| Project-engineer time per RFI | ≈ $1,080 of handling cost per RFI (industry) | −35% with triage + cited AI drafts | Project Engineers |

**Value: about $1.6M per year**, which is roughly **+0.5 points of operating margin** on this portfolio (slide 11).

---

## What this does to the numbers each executive is paid on

| Executive | Typically measured and paid on (confirm in discovery) | What RFI Copilot moves | Impact in their terms |
|---|---|---|---|
| **COO / VP Operations** | Portfolio operating margin vs plan, on-time completion, repeat-owner work | LD days avoided, faster answers, early escalation of high-risk RFIs | **≈ +0.5 pts of margin** on ≈ $285M annual volume, about **+14% net profit** at a 4% net margin |
| **CFO** | Net margin vs forecast (no quarter-end fade), forecast accuracy, cash from approved change orders | PCOs flagged at the RFI; schedule risk visible weeks earlier | $16.8M of RFI-driven change orders priced and submitted while the GC still has leverage |
| **Project Executive** (Harbor Point) | Project fee and gross-profit fade, milestone dates, owner satisfaction | Day-one risk ranking; approver sign-off on high-stakes answers | Each critical-path week avoided = **$420K of LDs, about 3% of a 5% fee** on a $310M job |
| **Director of Project Controls** | EAC forecast accuracy, change-order cycle time, reporting effort | Governed RFI facts, Genie answers, complete audit trail | A weekly risk review on live data instead of spreadsheets |

**Margin fade and missed milestones are the two things that cut these executives' annual incentive.** RFI Copilot attacks both at the RFI, where a large share of the fade begins.

---

## The problem: RFIs are a hidden schedule risk

- **3,795 RFIs** across 8 projects. **65 are open right now, and 44 of them are already past SLA.**
- Half of all RFIs miss the design team's contractual response window (7 days, or 3 days if urgent).
- When a late RFI sits on a **critical-path** activity, every day it slips becomes a liquidated-damages day: $25K/day at Lakeside Hospital and $60K/day at Harbor Point.
- Project engineers triage by gut feel and re-type answers that already exist in the spec book. Leadership has no portfolio view of which firm, discipline or document gap is driving the cost.

---

## Where it hurts most (from the gold tables and Genie)

| Project | RFIs | Open / past SLA | Avg days | SLA breach | CO cost | LD exposure |
|---|---|---|---|---|---|---|
| Harbor Point Data Center Ph 2 | 778 | 24 / 15 | 10.5 | 66% | $3.2M | **$7.6M** |
| Lakeside Regional Hospital | 846 | 8 / 4 | 8.5 | 53% | $4.2M | **$2.6M** |
| Union Station Mixed-Use Tower | 537 | 6 / 5 | 7.3 | 42% | $2.2M | $0.8M |

- **Design firms:** the MEP and fire-protection engineers breach SLA 57–65% of the time, versus 21–36% for civil and structural.
- **Root cause (Gen AI on every RFI):** spec ambiguity is 28% of volume, and field conditions plus design conflicts drive **$10.3M** of change-order cost.
- **Implication:** two projects and two design firms account for most of the exposure, and those two Project Executives carry most of the bonus risk. That is where to start.

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

## Value model (annualized), translated into margin

| Lever | Mechanism | Assumption | Annual value |
|---|---|---|---|
| Avoided liquidated damages | Escalate high-risk critical-path RFIs at submission, days before the SLA lapses | 30% of the ≈ $3.2M/yr LD exposure from late critical-path RFIs avoided | **≈ $0.96M** |
| Project-engineer productivity | Triage gate + cited drafts | ≈ 1,085 RFIs/yr × $1,080 handling cost × 35% saved | **≈ $0.41M** |
| Fewer RFI-driven change orders | Root-cause feedback to preconstruction (design conflicts + missing information) | 10% of ≈ $1.9M/yr | **≈ $0.19M** |
| **Total** | | | **≈ $1.6M / yr** |
| **In margin terms** | ≈ $285M annual construction volume ($995M over ≈ 3.5 years) | 4% net margin ≈ $11.4M profit/yr | **≈ +0.5 pts margin, ≈ +14% profit** |

*Annual values are history total ÷ 3.5 years. Every assumption, including volume and net margin, is a parameter to validate in discovery and the pilot.*

---

## For the executive sponsor (COO / VP Operations)

- **Protects the number on your scorecard.** About half a point of portfolio operating margin, from LD days that never happen and engineering hours that go back into the work, tracked per project in governed tables.
- **No quarter-end surprises.** High-risk RFIs are visible the day they are submitted, not when the fade shows up in the monthly forecast. Genie answers "where are we exposed?" without waiting on an analyst.
- **Leverage with design firms.** Firm-level SLA performance (one specialty engineer breaches 65% of the time) gives you facts for fee negotiations and the next selection.
- **Delivery reputation wins work.** On-time milestones on LD-heavy jobs like Harbor Point are what earn repeat owners and the next negotiated contract.
- **Low-risk platform choice.** One governed platform for data, ML, Gen AI, the app database and BI, running on the GC's existing Databricks account.

---

## For the domain owner (Director of Project Controls / Project Executives)

- **Defends your project's fee.** A week of critical-path slip at Harbor Point costs $420K, about 3% of the fee. The risk-ranked inbox makes sure those RFIs are chased first.
- **Forecasts you can stand behind.** RFI-driven PCOs are flagged at the RFI, so the EAC carries them weeks before close-out.
- **Less typing, same control.** AI drafts are always reviewed and approved by a person. Citations show exactly which spec section supports the answer, and gaps are flagged instead of guessed.
- **Fewer RFIs reach the design team.** The triage gate closes the ones the contract documents already answer.
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

## 90-day pilot, measured on the executive scorecard

| Weeks | Scope | Success criteria |
|---|---|---|
| 1–3 | Connect Procore/ACC RFI exports and the spec book for **Harbor Point** and **Lakeside** | Daily Lakeflow ingest; gold KPIs match the PM system within 1% |
| 4–6 | Risk model retrained on the GC's own history; Copilot rolled out to 6 project engineers | Top-20% risk band precision ≥ 70%; ≥ 60% of drafts approved with light edits |
| 7–10 | Genie space for project executives; weekly risk review | High-risk RFIs escalated ≥ 10 days before the SLA; executives use Genie weekly |
| 11–13 | Measure against the scorecard | Avg response ≤ 6 days; **zero LD days from late RFIs on pilot projects; fee-fade trend flat or improving**; go/no-go for all 8 projects |

**The ask:** a sponsor (COO), a domain owner (Director of Project Controls), two pilot projects, read access to RFI exports and spec books, and **the current incentive scorecard metrics, so the pilot reports in exactly those terms.**

---

## Appendix: assumptions and sources

- Portfolio, RFIs, firms and spec book are **synthetic**, produced by `01_generate_raw_data`. The generator encodes realistic drivers: firm responsiveness, coordination load, missing drawings, priority, and a 12% critical-path rate.
- Liquidated-damages exposure = schedule-impact days × the project's LD rate, counted **only** for critical-path RFIs.
- Margin translation: annual volume ≈ $995M ÷ 3.5 years ≈ $285M; a 4% net margin is an illustrative assumption. Harbor Point fee is assumed at 5% of $310M (≈ $15.5M). Executive incentive metrics are typical of GC compensation plans and **must be confirmed in discovery**.
- Industry sources: McKinsey Global Institute, *Reinventing Construction: A Route to Higher Productivity* (2017); FMI & PlanGrid, *Construction Disconnected* (2018); Autodesk & FMI, *Harnessing the Data Advantage in Construction* (2021); Navigant Construction Forum, *Impact of Requests for Information on Construction Projects* (2013).
- Platform run cost (indicative, to be sized in the pilot): a nightly serverless pipeline and jobs, one small Lakebase instance, one Vector Search endpoint, a serverless SQL warehouse, and pay-per-token model calls.
- Code, executed notebooks with outputs, and app smoke-test results: see the GitHub repository (`evidence/`).
