# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generate raw synthetic source data (landing zone)
# MAGIC
# MAGIC **Customer problem (Engineering & Construction):** a general contractor ("Summit Ridge Builders", fictional)
# MAGIC runs 8 concurrent projects. Each project generates hundreds of RFIs (Requests for Information) to the design
# MAGIC team. Slow RFI answers stall crews, trigger change orders and, on fixed-date contracts, liquidated damages.
# MAGIC
# MAGIC This notebook simulates what the GC's project-management system (Procore / ACC style) exports every night:
# MAGIC raw RFI logs (JSON, batch files), the project register, the design-firm SLA sheet (CSV) and the
# MAGIC project specification book (Markdown, one file per CSI division). **All data is synthetic.**
# MAGIC Deliberate defects (nulls, bad codes, duplicate re-sends) are injected so Lakeflow expectations have work to do.

# COMMAND ----------
dbutils.widgets.text("catalog", "gauravsharma_catalog")
dbutils.widgets.text("schema", "rfi_intel")
catalog = dbutils.widgets.get("catalog"); schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}` COMMENT 'RFI Intelligence - FE build (synthetic construction data)'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.raw_landing COMMENT 'Raw landing zone: nightly PM-system exports'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.evidence COMMENT 'Run evidence written by each build step'")
ROOT = f"/Volumes/{catalog}/{schema}/raw_landing"
for d in ["rfis", "projects", "design_firms", "specs"]:
    dbutils.fs.mkdirs(f"{ROOT}/{d}")
print("Landing zone:", ROOT)

# COMMAND ----------
# MAGIC %md ## Reference data: projects, design firms, specification book

# COMMAND ----------
import json, random, math, datetime as dt
import numpy as np
rng = np.random.default_rng(42); random.seed(42)

PROJECTS = [  # id, name, sector, contract $, start, planned finish, LDs $/day, design firm per discipline group
 ("P-101","Lakeside Regional Hospital Expansion","Healthcare",186_000_000,"2024-09-01","2027-06-30",25_000),
 ("P-102","Northgate Medical Office Building","Healthcare",48_000_000,"2023-06-01","2025-08-30",8_000),
 ("P-103","Cedar Valley K-12 Campus","Education",72_000_000,"2023-04-15","2025-07-31",12_000),
 ("P-104","Harbor Point Data Center Phase 2","Mission Critical",310_000_000,"2025-06-01","2027-09-30",60_000),
 ("P-105","Union Station Mixed-Use Tower","Commercial",142_000_000,"2025-01-15","2027-04-30",18_000),
 ("P-106","Riverbend Wastewater Treatment Upgrade","Water / Civil",95_000_000,"2025-09-01","2027-12-31",10_000),
 ("P-107","Mesa State University Science Building","Education",88_000_000,"2025-04-01","2027-03-31",14_000),
 ("P-108","Pinecrest Senior Living Community","Senior Living",54_000_000,"2025-10-01","2027-08-31",7_500),
]
FIRMS = [  # firm, discipline, contractual SLA days, responsiveness (1.0 = typical; >1 slower)
 ("Halvorsen Architects","Architectural",7,1.05),
 ("Keystone Structural Engineers","Structural",7,0.85),
 ("Brightline MEP Partners","Mechanical",7,1.35),
 ("Brightline MEP Partners","Plumbing",7,1.30),
 ("Voltaic Electrical Design","Electrical",7,1.10),
 ("Corvid Civil Group","Civil",10,0.95),
 ("Sentinel Fire Protection Engineering","Fire Protection",7,1.20),
]
FIRM_BY_DISC = {d:(f,s,r) for f,d,s,r in FIRMS}

# (section, title, division file, discipline, spec text)
SPECS = [
 ("03 20 00","Concrete Reinforcing","Division_03_Concrete","Structural","Reinforcing steel shall be ASTM A615 Grade 60 deformed bars. Minimum concrete cover shall be 3 inches for surfaces cast against earth, 2 inches for surfaces exposed to weather, and 1-1/2 inches for interior beams and columns. Lap splices shall be Class B unless noted otherwise on the structural drawings. Epoxy-coated bars are required in parking structures and exterior slabs exposed to de-icing salts."),
 ("03 30 00","Cast-in-Place Concrete","Division_03_Concrete","Structural","Concrete for structural elements shall attain a minimum 28-day compressive strength of 4,000 psi; slabs-on-grade 3,500 psi. Slump shall not exceed 4 inches unless a water-reducing admixture is used, in which case 8 inches maximum. Cure all formed surfaces for a minimum of 7 days by water or approved curing compound. Cold-weather concreting per ACI 306 is required when ambient temperature is below 40 F."),
 ("04 20 00","Unit Masonry","Division_04_Masonry","Structural","Concrete masonry units shall be ASTM C90, normal weight, with a net-area compressive strength of 2,000 psi. Provide horizontal joint reinforcement at 16 inches on center. Fill cells containing reinforcing with coarse grout. Control joints shall not exceed 25 feet on center."),
 ("05 12 00","Structural Steel Framing","Division_05_Metals","Structural","Structural steel wide-flange shapes shall conform to ASTM A992, Fy = 50 ksi. All welding shall be performed by AWS D1.1 certified welders. High-strength bolts shall be ASTM F3125 Grade A325, pretensioned in slip-critical connections. Field modifications to members, including new penetrations, require written approval of the Structural Engineer of Record."),
 ("05 50 00","Metal Fabrications","Division_05_Metals","Structural","Miscellaneous steel shall be ASTM A36, hot-dip galvanized when exposed to exterior. Stair and guard assemblies shall be designed for a 200 lbf concentrated load at the top rail. Embedded plates shall be furnished to the concrete contractor with setting drawings."),
 ("07 21 00","Thermal Insulation","Division_07_Thermal_Moisture","Architectural","Exterior wall cavity insulation shall achieve a minimum R-value of R-21. Continuous exterior insulation shall be R-7.5 minimum mineral wool. Rigid board insulation at foundation perimeter shall be extruded polystyrene, 25 psi minimum compressive strength. Maintain continuity of the thermal barrier at all penetrations."),
 ("07 54 23","TPO Roofing","Division_07_Thermal_Moisture","Architectural","Roof membrane shall be 60-mil fully adhered TPO over tapered polyiso with minimum 1/4 inch per foot slope to drains. Provide a 20-year NDL manufacturer warranty. Rooftop equipment curbs shall be flashed minimum 8 inches above the finished membrane."),
 ("07 92 00","Joint Sealants","Division_07_Thermal_Moisture","Architectural","Exterior joint sealant shall be a multi-component, non-sag polyurethane. Joint width-to-depth ratio shall be 2:1 for joints up to 1/2 inch. Install backer rod in all joints deeper than 1/2 inch; do not puncture the backer rod. Interior wet-area joints shall use mildew-resistant silicone."),
 ("08 11 13","Hollow Metal Doors and Frames","Division_08_Openings","Architectural","Hollow metal doors shall be 16-gauge face sheets, fully welded seams. Doors in 1-hour rated corridors shall carry a minimum 20-minute rating with positive latching; doors in 2-hour shafts and stairs shall be 90-minute rated. Frames shall be 14-gauge, grouted solid in masonry walls. Provide reinforcement for all hardware."),
 ("08 41 13","Aluminum Storefronts","Division_08_Openings","Architectural","Storefront framing shall be thermally broken aluminum, 2 x 4-1/2 inch, with a maximum U-factor of 0.38. Design for wind loads per ASCE 7 for the project's exposure category. Deflection shall not exceed L/175 of the span."),
 ("08 71 00","Door Hardware","Division_08_Openings","Architectural","Door hardware at accessible openings shall require no more than 5 lbf to operate and use lever handles. Provide closers on all rated doors adjusted to comply with ADA opening-force limits. Exit devices shall be UL listed panic hardware. Electrified hardware shall fail-safe at stair doors."),
 ("08 80 00","Glazing","Division_08_Openings","Architectural","Exterior glazing shall be 1-inch insulating glass units with low-E coating on surface #2. Provide safety glazing (tempered or laminated) at all hazardous locations per IBC 2406. Structural silicone shall be dry-set only, per manufacturer's cure schedule."),
 ("09 29 00","Gypsum Board","Division_09_Finishes","Architectural","Gypsum board at wet areas shall be moisture- and mold-resistant, ASTM C1396. Fire-rated assemblies shall use Type X board with joints finished to Level 4 in occupied spaces and Level 5 where critical lighting occurs. Fasten to framing at 12 inches on center at ceilings."),
 ("09 65 00","Resilient Flooring","Division_09_Finishes","Architectural","Resilient sheet flooring in patient and classroom areas shall be homogeneous vinyl with heat-welded seams. Substrate moisture shall not exceed 75% RH per ASTM F2170 before installation unless a manufacturer-approved moisture mitigation system is applied. Flash-cove base to 6 inches at all wall junctions in wet rooms."),
 ("09 91 00","Painting","Division_09_Finishes","Architectural","Interior gypsum board shall receive one primer coat and two finish coats of low-VOC latex. VOC content shall not exceed 50 g/L for flat and 150 g/L for non-flat coatings. Do not apply when surface or ambient temperature is below 50 F."),
 ("21 13 13","Wet-Pipe Sprinkler Systems","Division_21_Fire_Suppression","Fire Protection","Sprinkler systems shall be designed per NFPA 13 for the occupancy hazard classification shown. Light-hazard areas shall use quick-response sprinklers. Maintain 18 inches clearance below sprinkler deflectors. Any relocation of heads due to ceiling or duct conflicts requires a revised hydraulic calculation stamped by the fire protection engineer."),
 ("22 40 00","Plumbing Fixtures","Division_22_Plumbing","Plumbing","Public lavatories shall be sensor-operated with a maximum flow of 0.5 gpm. Mounting height for accessible lavatories shall not exceed 34 inches to the rim. Provide insulation on hot-water and drain piping below accessible lavatories. Where local amendments are more stringent, the more stringent requirement governs."),
 ("22 11 16","Domestic Water Piping","Division_22_Plumbing","Plumbing","Domestic water piping above grade shall be Type L copper with lead-free solder or press fittings. PEX is not permitted in healthcare occupancies. Provide thermostatic mixing valves to limit hot water at hand-washing fixtures to 110 F. Disinfect all piping per AWWA C651 before turnover."),
 ("23 05 93","Testing, Adjusting and Balancing for HVAC","Division_23_HVAC","Mechanical","All air and hydronic systems shall be balanced to within +/- 10% of design flow by an independent AABC- or NEBB-certified agency; contractor self-performance is not permitted. Submit a certified TAB report before substantial completion. Record final damper and valve positions."),
 ("23 31 13","Metal Ducts","Division_23_HVAC","Mechanical","Ductwork shall be galvanized steel constructed per SMACNA HVAC Duct Construction Standards, pressure class as scheduled. Duct routing shown is diagrammatic; coordinate offsets with structure and sprinkler mains through the BIM coordination process. Changes in duct size require approval of the Mechanical Engineer of Record."),
 ("26 05 19","Low-Voltage Electrical Power Conductors","Division_26_Electrical","Electrical","Branch-circuit conductors shall be copper, THHN/THWN-2, minimum #12 AWG. Aluminum conductors are not permitted for branch circuits; aluminum feeders #1/0 and larger are acceptable with approved compression lugs. Derate conductor ampacity for more than three current-carrying conductors in a raceway."),
 ("26 24 16","Panelboards","Division_26_Electrical","Electrical","Panelboards shall be dead-front, bolt-on circuit breaker type with copper bus. Provide 20% spare breaker capacity and 20% spare space. Maintain NEC 110.26 working clearance; panel locations in conflict with millwork require relocation, not millwork modification."),
 ("26 51 00","Interior Lighting","Division_26_Electrical","Electrical","Interior lighting shall be LED with a minimum efficacy of 120 lumens per watt. Provide occupancy sensors in all enclosed offices, classrooms and restrooms. Emergency egress lighting shall maintain 1 foot-candle average along the path of egress."),
 ("31 23 00","Excavation and Fill","Division_31_Earthwork","Civil","Structural fill shall be compacted to 95% of maximum dry density per ASTM D1557 in 8-inch lifts. Unsuitable soils encountered below footing elevation shall be over-excavated and replaced, with the geotechnical engineer's written observation. Dewatering shall maintain groundwater 2 feet below excavation bottom."),
 ("33 30 00","Sanitary Sewerage","Division_33_Utilities","Civil","Sanitary sewer pipe shall be PVC SDR-35 for depths up to 12 feet and SDR-26 deeper. Minimum slope shall be 1% for 6-inch laterals. Manholes shall be precast concrete with watertight boots. Conflicts with existing utilities shall be potholed and reported before installation."),
]
DIV_TITLES = {f.split("_",2)[1]: f.split("_",2)[2].replace("_"," ") for _,_,f,_,_ in SPECS}

# COMMAND ----------
# MAGIC %md ## RFI question templates (each carries a hidden root-cause label for later evaluation of the Gen-AI classifier)

# COMMAND ----------
LEVELS = ["Level 1","Level 2","Level 3","Level 4","Roof","Basement","Mezzanine"]
GRIDS  = [f"{a}-{n}" for a in "ABCDEFGH" for n in range(1,9)]
SHEETS = {"Structural":"S","Architectural":"A","Mechanical":"M","Plumbing":"P","Electrical":"E","Civil":"C","Fire Protection":"FP"}
T = {  # section -> list of (subject, question template, root_cause)
 "03 20 00":[("Rebar cover at grade beams","Drawing {sheet} shows 2 inch clear cover at grade beams cast directly against earth at grid {grid}. Please confirm the required minimum concrete cover for this condition.","Design conflict"),
             ("Epoxy-coated rebar at loading dock slab","The loading dock slab at {level} is exterior and will see de-icing salts. Drawings call for uncoated bars. Is epoxy-coated reinforcing required?","Spec ambiguity")],
 "03 30 00":[("Slab-on-grade mix strength","The general notes on {sheet} call for 4,000 psi for all concrete, but the slab-on-grade detail shows 3,500 psi. Which strength governs for the slab at {level}?","Design conflict"),
             ("Cold-weather pour at {level}","Forecast lows are 30 F for the pour at {level}, grid {grid}. Please confirm cold-weather protection requirements and whether accelerators are acceptable.","Field condition")],
 "04 20 00":[("CMU control joint spacing","The elevation on {sheet} shows a 40 foot run of CMU at grid {grid} with no control joints. What is the required control joint spacing?","Missing information")],
 "05 12 00":[("New duct penetration through beam","Mechanical duct at {level}, grid {grid} conflicts with a W18 beam. May we core a 10 inch penetration through the beam web?","Design conflict"),
             ("Slip-critical bolt requirement","Connection details on {sheet} do not indicate slip-critical vs snug-tight. Which connections require pretensioned slip-critical bolts?","Missing information")],
 "05 50 00":[("Guardrail design load","The mezzanine guardrail on {sheet} has no design load noted. What concentrated load must the guard assembly resist?","Missing information")],
 "07 21 00":[("Cavity insulation R-value","The wall section on {sheet} shows R-19 batts, but the energy narrative requires more. Confirm the minimum cavity insulation R-value.","Design conflict"),
             ("Substitute closed-cell spray foam","May we substitute closed-cell spray foam for mineral wool continuous insulation at {level} to recover schedule?","Substitution request")],
 "07 54 23":[("Curb flashing height at RTU-3","RTU-3 curb on {sheet} is 6 inches high above finished roof. Is this acceptable or must the curb be raised?","Constructability")],
 "07 92 00":[("Backer rod at deep joints","Expansion joints at grid {grid} are 1 inch deep. Is backer rod required and what sealant depth is expected?","Spec ambiguity")],
 "08 11 13":[("Door rating at corridor openings","The door schedule lists openings at {level} in the 1-hour rated corridor but does not specify a door fire rating. What minimum fire rating is required?","Missing information"),
             ("Frame gauge at stair doors","Stair door frames on {sheet} are called out as 16-gauge. Spec section 08 11 13 appears to require heavier. Please clarify.","Design conflict")],
 "08 41 13":[("Storefront deflection criteria","Storefront supplier needs the deflection limit for the {level} entrance system. Not shown on {sheet}.","Missing information")],
 "08 71 00":[("Closer opening force at rated doors","Hardware submittal sets closers to 6.5 lbf at rated corridor doors. Is this acceptable given accessibility limits?","Spec ambiguity")],
 "08 80 00":[("Safety glazing at sidelites","Sidelites at {level} entrance are within 24 inches of the door edge. Is tempered or laminated glazing required?","Missing information")],
 "09 29 00":[("Level 5 finish locations","Which walls require Level 5 gypsum finish? The finish schedule on {sheet} does not identify critical-lighting walls.","Missing information")],
 "09 65 00":[("Slab moisture before flooring","The slab at {level} is testing at 82% RH. The flooring installer asks whether they can proceed. What is the maximum allowable moisture?","Field condition")],
 "09 91 00":[("Painting below 50 F","Temporary heat at {level} cannot maintain 50 F overnight. May painting proceed during the day?","Field condition")],
 "21 13 13":[("Sprinkler head relocation for duct","Sprinkler heads at {level} grid {grid} conflict with the new duct route. May the installer relocate heads 18 inches without a revised calculation?","Design conflict")],
 "22 40 00":[("Lavatory flow rate vs local code","The fixture schedule calls for 0.5 gpm sensor lavatories but a local amendment allows only 0.35 gpm. Which governs?","Spec ambiguity")],
 "22 11 16":[("PEX substitution for domestic water","Copper lead times are 10 weeks. May we substitute PEX for Type L copper at {level}?","Substitution request")],
 "23 05 93":[("TAB agency requirement","The mechanical sub proposes to self-perform testing and balancing. Is a third-party certified TAB agency required?","Spec ambiguity")],
 "23 31 13":[("Duct offset at structure","The supply main at {level}, grid {grid} does not fit below the beam; a 12 inch offset is needed. Is the offset acceptable or must the duct be resized?","Design conflict"),
             ("Duct pressure class not scheduled","Duct pressure class for the AHU-2 return on {sheet} is not scheduled. Please provide.","Missing information")],
 "26 05 19":[("Aluminum branch conductors","To address copper lead times, may we substitute aluminum conductors for #12 AWG branch circuits at {level}?","Substitution request")],
 "26 24 16":[("Panel conflict with millwork","Panel LP-{n} at {level} conflicts with casework shown on {sheet}. Should the casework be modified or the panel relocated?","Design conflict")],
 "26 51 00":[("Fixture efficacy below spec","The proposed troffer is rated 105 lm/W. Is this acceptable against the specified efficacy?","Substitution request")],
 "31 23 00":[("Unsuitable soil at footing F-{n}","Soft clay encountered 2 feet below footing F-{n} bottom at grid {grid}. Please advise on over-excavation depth and replacement material.","Field condition")],
 "33 30 00":[("Existing utility conflict at sewer run","Potholing found an undocumented 8 inch water main crossing the sewer alignment near grid {grid}. Please advise on vertical separation.","Field condition")],
}
SEC = {s[0]: s for s in SPECS}
ROOT_EFFECT = {"Design conflict":1.25,"Missing information":1.0,"Spec ambiguity":0.9,"Field condition":1.15,"Substitution request":1.35,"Constructability":1.05}
CO_PROB    = {"Design conflict":0.32,"Missing information":0.12,"Spec ambiguity":0.06,"Field condition":0.40,"Substitution request":0.10,"Constructability":0.18}
ROLES = ["Project Engineer","Superintendent","Field Engineer","MEP Coordinator","Subcontractor PM","Foreman"]
SUBS  = ["Granite Concrete Co.","Apex Steel Erectors","Northwind Mechanical","Brightwater Plumbing","Ironclad Electric","Clearview Glass","Summit Drywall","Terraform Earthworks","Redline Fire Sprinkler"]
DISC_SECTIONS = {}
for s in SPECS: DISC_SECTIONS.setdefault(s[3], []).append(s[0])
DISC_W = {"Architectural":0.30,"Structural":0.18,"Mechanical":0.16,"Electrical":0.13,"Plumbing":0.09,"Civil":0.08,"Fire Protection":0.06}

# COMMAND ----------
# MAGIC %md ## Simulate ~3,500 RFIs across the portfolio with realistic drivers of response time

# COMMAND ----------
TODAY = dt.date(2026, 10, 20)
def daterange_weight(start, finish, d):
    # RFI intensity peaks ~40% into the project (coordination-heavy MEP rough-in phase)
    total = (finish - start).days; x = (d - start).days / max(total, 1)
    return max(0.0, math.exp(-((x - 0.4) ** 2) / 0.06))

rows, rfi_n = [], 0
for pid, pname, sector, cv, ps, pf, ld in PROJECTS:
    start, finish = dt.date.fromisoformat(ps), dt.date.fromisoformat(pf)
    scale = cv / 100_000_000
    last = min(finish, TODAY)
    d = start
    while d <= last:
        lam = 9.0 * scale ** 0.6 * daterange_weight(start, finish, d)  # expected RFIs per week
        for _ in range(rng.poisson(lam)):
            rfi_n += 1
            disc = rng.choice(list(DISC_W), p=np.array(list(DISC_W.values())) / sum(DISC_W.values()))
            if sector == "Water / Civil" and rng.random() < 0.35: disc = "Civil"
            sec = rng.choice(DISC_SECTIONS[disc]); subj, q, root = random.choice(T[sec])
            fill = dict(sheet=f"{SHEETS[disc]}-{rng.integers(100, 600)}", grid=random.choice(GRIDS), level=random.choice(LEVELS), n=rng.integers(1, 30))
            submitted = dt.datetime.combine(d, dt.time(0)) + dt.timedelta(days=int(rng.integers(0, 7)), hours=int(rng.integers(6, 18)), minutes=int(rng.integers(0, 60)))
            priority = rng.choice(["Normal", "High", "Urgent"], p=[0.70, 0.22, 0.08])
            has_dwg  = bool(rng.random() < 0.62)
            has_prop = bool(rng.random() < 0.45)
            firm, sla, resp = FIRM_BY_DISC[disc]
            sla_days = 3 if priority == "Urgent" else sla
            backlog = daterange_weight(start, finish, submitted.date()) * scale  # coordination load proxy
            mu = 5.2 * resp * ROOT_EFFECT[root] * (0.55 if priority == "Urgent" else 0.85 if priority == "High" else 1.0) \
                 * (0.82 if has_dwg else 1.0) * (0.80 if has_prop else 1.0) * (1 + 0.35 * backlog)
            days = max(0.3, float(rng.lognormal(math.log(mu), 0.55)))
            answered = submitted + dt.timedelta(days=days)
            co = bool(rng.random() < CO_PROB[root])
            cost = round(float(rng.lognormal(math.log(14_000), 0.9)), 0) if co else 0.0
            sched = int(max(0, round(rng.normal(1.2 * (days / sla_days), 1.0)))) if (co or days > sla_days * 1.5) else 0
            critical = bool(rng.random() < 0.12)  # RFI sits on a critical-path activity -> delay hits the finish date
            is_open = answered.date() > TODAY or ((TODAY - submitted.date()).days <= 18 and rng.random() < 0.55)
            rows.append({
                "rfi_id": f"RFI-{pid[2:]}-{rfi_n:05d}", "project_id": pid, "discipline": disc, "spec_section": sec,
                "subject": subj.format(**fill), "question": q.format(**fill),
                "submitted_by_role": random.choice(ROLES), "submitted_by_company": random.choice(SUBS),
                "assigned_firm": firm, "priority": priority, "has_drawing_ref": has_dwg, "has_proposed_solution": has_prop,
                "proposed_solution": ("Proposed: " + random.choice(["follow the more stringent requirement", "relocate per attached sketch", "provide equal product per attached data sheet", "add offset per field markup"])) if has_prop else None,
                "submitted_at": submitted.isoformat(sep=" ", timespec="seconds"),
                "answered_at": None if is_open else answered.isoformat(sep=" ", timespec="seconds"),
                "status": "Open" if is_open else "Answered",
                "cost_impact_usd": None if is_open else cost, "schedule_impact_days": None if is_open else sched,
                "became_change_order": None if is_open else co, "on_critical_path": critical,
                "capture_location": f"{fill['level']} / grid {fill['grid']}", "bim_ref": f"ACC-{pid}-{rng.integers(1000, 9999)}",
                "_root_cause_truth": root,
            })
        d += dt.timedelta(days=7)

# Data-quality defects a real export contains
bad = rng.choice(len(rows), size=int(len(rows) * 0.008), replace=False)
for i in bad[: len(bad) // 2]: rows[i]["project_id"] = None
for i in bad[len(bad) // 2:]: rows[i]["discipline"] = "TBD"
dups = [dict(rows[i]) for i in rng.choice(len(rows), size=int(len(rows) * 0.005), replace=False)]
rows += dups
print(f"RFIs generated: {len(rows):,} (incl. {len(dups)} duplicate re-sends, {len(bad)} defective rows); open: {sum(r['status']=='Open' for r in rows)}")

# COMMAND ----------
# MAGIC %md ## Land the files (one JSON batch per half-year, like nightly drops accumulating)

# COMMAND ----------
from collections import defaultdict
batches = defaultdict(list)
for r in rows:
    y, m = int(r["submitted_at"][:4]), int(r["submitted_at"][5:7])
    batches[f"{y}H{1 if m <= 6 else 2}"].append(r)
for k, v in sorted(batches.items()):
    dbutils.fs.put(f"{ROOT}/rfis/rfi_export_{k}.json", "\n".join(json.dumps(x) for x in v), overwrite=True)
    print(f"rfis/rfi_export_{k}.json  {len(v):>5} rows")

dbutils.fs.put(f"{ROOT}/projects/project_register.json", "\n".join(json.dumps(dict(project_id=p[0], project_name=p[1], sector=p[2], contract_value_usd=p[3], start_date=p[4], planned_finish=p[5], liquidated_damages_per_day=p[6], general_contractor="Summit Ridge Builders")) for p in PROJECTS), overwrite=True)
dbutils.fs.put(f"{ROOT}/design_firms/design_firm_sla.csv", "firm,discipline,contract_sla_days\n" + "\n".join(f"{f},{d},{s}" for f, d, s, _ in FIRMS), overwrite=True)

by_div = defaultdict(list)
for s in SPECS: by_div[s[2]].append(s)
for f, secs in by_div.items():
    md = f"# Project Specifications - {f.replace('_', ' ')}\n\nSummit Ridge Builders standard project manual (synthetic).\n\n" + \
         "\n\n".join(f"## {s} - {t}\n\n{txt}" for s, t, _, _, txt in secs)
    dbutils.fs.put(f"{ROOT}/specs/{f}.md", md, overwrite=True)
print(f"specs: {len(by_div)} division files, {len(SPECS)} sections")
display(dbutils.fs.ls(f"{ROOT}/rfis"))

# COMMAND ----------
summary = {"rfis": len(rows), "duplicates": len(dups), "defects": int(len(bad)), "open": sum(r['status']=='Open' for r in rows),
           "projects": len(PROJECTS), "spec_sections": len(SPECS), "batches": sorted(batches)}
dbutils.fs.put(f"/Volumes/{catalog}/{schema}/evidence/01_generate_raw_data.json", json.dumps(summary, indent=2), overwrite=True)
print(json.dumps(summary, indent=2))
