"""RFI Copilot - Summit Ridge Builders (synthetic). Databricks App over Lakeflow, Unity Catalog, Lakebase, ML, Gen AI and Genie."""
import os, json, re, uuid, time, threading, traceback
import dash
from dash import html, dcc, Input, Output, State, callback, no_update, ctx, ALL
import plotly.graph_objects as go
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem
import psycopg
from concurrent.futures import ThreadPoolExecutor

CATALOG   = os.environ.get("CATALOG", "gauravsharma_catalog")
SCHEMA    = os.environ.get("SCHEMA", "rfi_intel")
WAREHOUSE = os.environ.get("WAREHOUSE_ID", "")
VS_INDEX  = os.environ.get("VS_INDEX", f"{CATALOG}.{SCHEMA}.spec_chunks_index")
LLM       = os.environ.get("LLM_ENDPOINT", "databricks-claude-sonnet-5-5")
LB_INST   = os.environ.get("LAKEBASE_INSTANCE", "rfi-intel-db")
LB_DB     = os.environ.get("PGDATABASE", "rfi_ops")
GENIE_ID  = os.environ.get("GENIE_SPACE_ID", "")
AUDIT_JOB = os.environ.get("AUDIT_JOB_ID", "")
FQ        = f"{CATALOG}.{SCHEMA}"
APPROVERS = ["Dana Whitfield · Project Executive", "Marcus Lee · Project Manager", "Priya Raman · Director of Project Controls"]
PE = "Jordan Ortiz · GC Project Engineer"

NAVY = "#14202B"; RED = "#C21E2E"; STEEL = "#3E5C76"; GRN = "#1B8A4E"; YEL = "#B8761A"; PUR = "#7A4FB8"; MUT = "#8A97A3"

_w = None
def w():
    global _w
    if _w is None: _w = WorkspaceClient()
    return _w

_cache = {}
_refreshing = set()
def cached(key, fn, ttl=30, stale_ok=False):
    now = time.time(); v = _cache.get(key)
    if v and now - v[0] < ttl: return v[1]
    if v and stale_ok:  # serve the last value instantly and refresh in the background (no cold-warehouse wait)
        if key not in _refreshing:
            _refreshing.add(key)
            def _bg():
                try: _cache[key] = (time.time(), fn())
                except Exception: print(f"refresh {key} failed:\n" + traceback.format_exc(), flush=True)
                finally: _refreshing.discard(key)
            threading.Thread(target=_bg, daemon=True).start()
        return v[1]
    r = fn(); _cache[key] = (now, r); return r
def _invalidate():
    for k in ("rfis", "events"): _cache.pop(k, None)

# ── Lakebase (operational serving) ────────────────────────────────────────────
_pg_lock = threading.Lock(); _pg = {"conn": None, "exp": 0}
def _pg_conn():
    if _pg["conn"] is None or _pg["conn"].closed or time.time() > _pg["exp"]:
        if _pg["conn"] is not None:
            try: _pg["conn"].close()
            except Exception: pass
        tok = w().database.generate_database_credential(request_id=str(uuid.uuid4()), instance_names=[LB_INST]).token
        user = os.environ.get("PGUSER") or w().current_user.me().user_name
        host = os.environ.get("PGHOST") or w().database.get_database_instance(LB_INST).read_write_dns
        _pg["conn"] = psycopg.connect(host=host, port=int(os.environ.get("PGPORT", 5432)), dbname=LB_DB, user=user,
                                      password=tok, sslmode="require", autocommit=True, connect_timeout=15)
        _pg["exp"] = time.time() + 40 * 60
    return _pg["conn"]

def pg(query, params=None, fetch=True):
    for attempt in (1, 2):
        try:
            with _pg_lock:
                with _pg_conn().cursor() as cur:
                    t = time.perf_counter(); cur.execute(query, params or ())
                    ms = (time.perf_counter() - t) * 1000
                    if not fetch: return [], ms
                    cols = [d.name for d in cur.description]
                    return [dict(zip(cols, r)) for r in cur.fetchall()], ms
        except psycopg.OperationalError:
            _pg["conn"] = None
            if attempt == 2: raise

STATUS = {None: "Open", "approved": "Responded", "changes_requested": "Changes requested", "routed": "Routed",
          "triage_resolved": "Resolved internally", "closed": "Closed"}
def list_rfis():
    def q():
        rows, ms = pg(f"""SELECT i.rfi_id, i.project_name, i.discipline, i.spec_section, i.subject, i.question, i.priority,
                   i.assigned_firm, i.submitted_by_role, i.submitted_by_company, i.proposed_solution, i.capture_location, i.bim_ref,
                   i.breach_probability, i.predicted_days_to_answer, i.risk_tier, i.top_drivers, i.root_cause,
                   i.draft_response, i.cited_sections, i.retrieved_sections, i.liquidated_damages_per_day, i.sla_days,
                   to_char(i.submitted_at, 'Mon DD') AS submitted, to_char(i.due_at, 'Mon DD') AS due,
                   round((extract(epoch FROM (i.due_at - now())) / 86400)::numeric, 1) AS days_to_due,
                   s.event_type AS wf, EXISTS (SELECT 1 FROM ops.rfi_workflow_events e WHERE e.rfi_id = i.rfi_id AND e.event_type = 'pco_flagged') AS has_pco
            FROM {SCHEMA}.rfi_inbox i LEFT JOIN ops.rfi_current_status s USING (rfi_id)
            ORDER BY (s.event_type IN ('closed','triage_resolved')) NULLS FIRST, i.breach_probability DESC NULLS LAST""")
        for r in rows:
            r["status"] = STATUS.get(r.pop("wf"), "Open")
            for k in ("breach_probability", "predicted_days_to_answer", "days_to_due", "liquidated_damages_per_day"):
                r[k] = float(r[k]) if r[k] is not None else None
        return rows, ms
    return cached("rfis", q, ttl=15)

def trigger_audit():
    if not AUDIT_JOB: return None
    try: return w().jobs.run_now(job_id=int(AUDIT_JOB)).run_id
    except Exception: print("audit sync trigger failed:\n" + traceback.format_exc(), flush=True)

def log_event(rfi_id, event_type, detail="", cost=None, sched=None, channel=None, actor="GC Project Engineer", trigger=True):
    eid = "EV-" + uuid.uuid4().hex[:10]
    _, ms = pg("""INSERT INTO ops.rfi_workflow_events (event_id, rfi_id, event_type, actor, detail, cost_impact, schedule_impact, channel)
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", (eid, rfi_id, event_type, actor, detail, cost, sched, channel), fetch=False)
    _invalidate()
    return eid, ms, (trigger_audit() if trigger else None)

def recent_events():
    return cached("events", lambda: pg("""SELECT event_id, rfi_id, event_type, channel, to_char(created_at,'Mon DD HH24:MI') AS t
                                          FROM ops.rfi_workflow_events ORDER BY created_at DESC LIMIT 8""")[0])

# ── SQL warehouse (analytics on governed gold tables) ─────────────────────────
def sql(statement, params=None):
    r = w().statement_execution.execute_statement(warehouse_id=WAREHOUSE, statement=statement, wait_timeout="50s",
        parameters=[StatementParameterListItem(name=k, value=str(v)) for k, v in (params or {}).items()] or None)
    while r.status.state.value in ("PENDING", "RUNNING"):
        time.sleep(1.5); r = w().statement_execution.get_statement(r.statement_id)
    if r.status.state.value != "SUCCEEDED": raise RuntimeError(f"SQL {r.status.state.value}: {r.status.error}")
    if not r.result or not r.result.data_array: return []
    cols = [c.name for c in r.manifest.schema.columns]
    return [dict(zip(cols, row)) for row in r.result.data_array]

PORTFOLIO_SQL = {
    "kpi": f"""SELECT sum(total_rfis) total, sum(open_rfis) open, sum(open_overdue_rfis) overdue,
            round(sum(avg_days_to_answer*(total_rfis-open_rfis))/sum(total_rfis-open_rfis),1) avg_days,
            sum(change_order_cost_usd) co_cost, sum(ld_exposure_usd) ld FROM {FQ}.gold_project_kpis""",
    "projects": f"""SELECT project_name, total_rfis, open_rfis, open_overdue_rfis, avg_days_to_answer, sla_breach_pct,
            change_order_cost_usd, ld_exposure_usd FROM {FQ}.gold_project_kpis ORDER BY open_rfis DESC""",
    "firms": f"""SELECT assigned_firm, round(sum(sla_breach_pct*answered_rfis)/sum(answered_rfis),1) breach
            FROM {FQ}.gold_firm_performance GROUP BY 1 ORDER BY 2 DESC""",
    "root": f"SELECT root_cause, count(*) n FROM {FQ}.gold_rfi_root_cause GROUP BY 1 ORDER BY 2 DESC",
    "trend": f"""SELECT cast(submitted_month AS string) m, sum(rfis) rfis, round(sum(avg_days_to_answer*rfis)/sum(rfis),1) d
            FROM {FQ}.gold_rfi_monthly WHERE submitted_month >= add_months(current_date(), -24) GROUP BY 1 ORDER BY 1""",
}
_pool = ThreadPoolExecutor(max_workers=6)

def portfolio():
    def q():
        futs = {k: _pool.submit(sql, v) for k, v in PORTFOLIO_SQL.items()}
        out = {k: f.result() for k, f in futs.items()}
        out["kpi"] = out["kpi"][0]
        return out
    return cached("portfolio", q, ttl=600, stale_ok=True)

def lineage():
    # gold tables only change on the nightly build; the audit count is the only live number
    return cached("lineage", lambda: sql(f"""SELECT (SELECT count(*) FROM {FQ}.bronze_rfis) bronze, (SELECT count(*) FROM {FQ}.silver_rfis) silver,
            (SELECT count(*) FROM {FQ}.gold_rfi_facts) gold, (SELECT count(*) FROM {FQ}.spec_chunks) chunks,
            (SELECT count(*) FROM {FQ}.rfi_risk_scores) scored, (SELECT count(*) FROM {FQ}.rfi_ai_drafts) drafts,
            (SELECT count(*) FROM {FQ}.rfi_workflow_audit) audit""")[0], ttl=600, stale_ok=True)

def _prewarm():
    for fn in (list_rfis, lineage, portfolio, genie_id):
        try: fn()
        except Exception: print(f"prewarm {fn.__name__} failed:\n" + traceback.format_exc(), flush=True)

# ── Gen AI: Vector Search retrieval + cited draft ─────────────────────────────
def retrieve(question, k=4):
    res = w().vector_search_indexes.query_index(index_name=VS_INDEX, columns=["chunk_id", "doc_name", "section", "title", "content"],
                                                query_text=question, num_results=k)
    cols = [c.name for c in res.manifest.columns] if res.manifest else []
    return [dict(zip(cols, row)) for row in ((res.result.data_array if res.result else None) or [])]

SYSTEM = ("You are a senior construction project engineer preparing a formal RFI response for a general contractor. Ground every "
 "statement ONLY in the supplied specification excerpts and cite governing section numbers inline, e.g. (per 03 30 00). Never invent "
 "requirements. Return ONLY a JSON object: {\"response\":\"<3-6 sentence cited response>\",\"clarity_score\":<0-100>,"
 "\"impact\":{\"cost\":\"<None|Low|Medium|High>\",\"schedule\":\"<None|Low|Medium|High>\",\"safety\":\"<None|Low|Medium|High>\"},"
 "\"root_cause\":\"<one sentence: the document gap that caused this RFI>\",\"prevention\":\"<one sentence>\",\"route_to\":[\"<role>\"]}")
def _unesc(x):
    try: return json.loads('"' + x + '"')
    except Exception: return x.replace('\\"', '"').replace("\\n", "\n")

def _text(content):
    if isinstance(content, str): return content
    return "".join(p.get("text", "") if isinstance(p, dict) else (getattr(p, "text", "") or "") for p in (content or []) if (p.get("type") if isinstance(p, dict) else getattr(p, "type", "text")) == "text")

def analyze(rfi, chunks):
    context = "\n\n".join(f"[{c.get('section')} - {c.get('title')}]\n{c.get('content')}" for c in chunks) or "(none)"
    out = w().serving_endpoints.get_open_ai_client().chat.completions.create(model=LLM, max_tokens=1500, messages=[
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": f"RFI {rfi['rfi_id']} - {rfi['discipline']} - {rfi['subject']}\n\nQUESTION:\n{rfi['question']}\n\nSPECIFICATION EXCERPTS:\n{context}"}]
    ).choices[0].message.content
    out = re.sub(r"^```(?:json)?\s*|\s*```$", "", _text(out).strip())
    m = re.search(r"\{.*\}", out, re.S)
    try: return json.loads(m.group(0))
    except Exception:
        r = re.search(r'"response"\s*:\s*"((?:[^"\\]|\\.)*)', out, re.S)
        return {"response": _unesc(r.group(1)) if r else out, "impact": {}, "route_to": []}

# ── Genie (natural-language analytics) ────────────────────────────────────────
def genie_id():
    global GENIE_ID
    if not GENIE_ID:
        title = os.environ.get("GENIE_TITLE", "RFI Intelligence - Summit Ridge Builders")
        spaces = w().api_client.do("GET", "/api/2.0/genie/spaces").get("spaces") or []
        GENIE_ID = next((s["space_id"] for s in spaces if s.get("title") == title), "")
    return GENIE_ID

def ask_genie(question):
    msg = w().genie.start_conversation_and_wait(genie_id(), question)
    out = {"text": None, "sql": None, "desc": None, "cols": [], "rows": []}
    for a in msg.attachments or []:
        if a.text: out["text"] = a.text.content
        if a.query:
            out["sql"] = a.query.query; out["desc"] = a.query.description
            sr = w().genie.get_message_attachment_query_result(genie_id(), msg.conversation_id, msg.id, a.attachment_id).statement_response
            out["cols"] = [c.name for c in sr.manifest.schema.columns]; out["rows"] = (sr.result.data_array or [])[:25]
    return out

# ── UI ────────────────────────────────────────────────────────────────────────
app = dash.Dash(__name__, suppress_callback_exceptions=True, title="RFI Copilot - Summit Ridge Builders")
server = app.server
CSS = open(os.path.join(os.path.dirname(__file__), "style.css")).read()
app.index_string = '<!DOCTYPE html><html><head>{%metas%}<title>{%title%}</title>{%favicon%}{%css%}<style>' + CSS + '</style></head><body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body></html>'

TABS = [("arch", "Data Journey"), ("insights", "Portfolio Insights"), ("copilot", "RFI Copilot"), ("approvals", "Approvers"), ("genie", "Ask Genie")]
def money(v):
    v = float(v or 0)
    return f"${v/1e6:.1f}M" if v >= 1e6 else f"${v/1e3:.0f}K"
def hero():
    return html.Div(className="hero", children=[html.Div(className="l", children=[html.Div("RFI", className="mk"),
        html.Div([html.H1("RFI Copilot"), html.Div("Answer RFIs before they become delays - on the Databricks Data Intelligence Platform", className="sub")])]),
        html.Span("Summit Ridge Builders · 8-project portfolio · synthetic data", className="pill")])
def nav(active):
    return html.Div(className="nav", children=[html.Div(t, id={"type": "nav", "v": v}, n_clicks=0, className="tab" + (" active" if v == active else "")) for v, t in TABS])
def chip(level):
    return html.Span(level or "None", className="chip " + {"none": "c-none", "low": "c-low", "medium": "c-med", "high": "c-high"}.get((level or "None").lower(), "c-none"))
def _style(fig, h=260):
    fig.update_layout(autosize=True, paper_bgcolor="white", plot_bgcolor="white", font=dict(family="DM Sans", color=NAVY, size=12),
                      margin=dict(l=10, r=10, t=10, b=10), height=h, showlegend=False)
    return fig
def hbar(pairs, color=RED, suffix=""):
    pairs = pairs[::-1]
    f = go.Figure(go.Bar(x=[p[1] for p in pairs], y=[p[0] for p in pairs], orientation="h", marker=dict(color=color),
                         text=[f"{p[1]}{suffix}" for p in pairs], textposition="outside", cliponaxis=False))
    f.update_xaxes(visible=False); f.update_yaxes(showgrid=False, tickfont=dict(size=12))
    return _style(f, max(200, len(pairs) * 36 + 40))
def graph(fig): return dcc.Graph(figure=fig, config={"displayModeBar": False, "responsive": True}, style={"width": "100%"})
def err(label): return html.Div([f"{label}:", html.Pre(traceback.format_exc(), style={"whiteSpace": "pre-wrap", "fontSize": ".7rem"})], className="status-msg s-err")

app.layout = html.Div([
    hero(), html.Div(id="nav-bar", children=nav("arch")),
    html.Div(id="view-arch", children=[dcc.Loading(color=RED, children=html.Div(className="page", id="arch-body"))]),
    html.Div(id="view-insights", style={"display": "none"}, children=[dcc.Loading(color=RED, children=html.Div(className="page", id="insights-body"))]),
    html.Div(id="view-copilot", style={"display": "none"}, children=[html.Div(className="wrap", children=[
        html.Div(className="inbox", children=[html.Div(id="inbox-head", className="ih"), html.Div(id="inbox-list")]),
        html.Div(className="main", id="detail", children=[html.Div(className="empty", children=[html.H3("Select an RFI"),
            html.Div("The inbox is served from Lakebase Postgres and ranked by the ML model's probability that the design team misses the SLA. "
                     "Each RFI arrives with a Gen-AI root cause and a cited draft prepared in batch; regenerate it live, approve, route, flag a change order or close. "
                     "Every decision is a transactional write to Lakebase, synced back to a governed Delta audit table.")])])])]),
    html.Div(id="view-approvals", style={"display": "none"}, children=[html.Div(className="page", children=[
        html.P("HUMAN IN THE LOOP · APPROVER WORKBENCH", className="eyebrow"), html.H2("Sign off before anything reaches the design team", className="h2"),
        html.P("High-risk, urgent or cost/schedule-impacting RFI responses stop here for a named approver. Every sign-off or return is a Lakebase "
               "transaction mirrored to the governed Delta audit, with turnaround time tracked.", className="lede"),
        html.Div(style={"display": "flex", "gap": "10px", "alignItems": "center", "marginBottom": "14px"}, children=[html.Span("Signing as", className="meta"),
            dcc.Dropdown(id="appr-who", options=APPROVERS, value=APPROVERS[0], clearable=False, style={"width": "380px"})]),
        dcc.Loading(color=RED, children=html.Div(id="approvals-body"))])]),
    html.Div(id="view-genie", style={"display": "none"}, children=[html.Div(className="page", id="genie-body")]),
    html.Div(id="trace-modal"), dcc.Store(id="store-trace"), dcc.Interval(id="trace-poll", interval=2500, disabled=True),
    dcc.Store(id="store-sel"), dcc.Store(id="store-chunks"), dcc.Store(id="store-view", data="arch"), dcc.Store(id="store-refresh", data=0),
    dcc.Interval(id="boot", interval=300, max_intervals=1),
])

@callback(Output("store-view", "data"), Output("nav-bar", "children"), Output("view-arch", "style"), Output("view-insights", "style"),
          Output("view-copilot", "style"), Output("view-approvals", "style"), Output("view-genie", "style"), Input({"type": "nav", "v": ALL}, "n_clicks"), prevent_initial_call=True)
def switch(_):
    v = ctx.triggered_id["v"] if ctx.triggered_id else "arch"
    return (v, nav(v), *[{"display": "block" if v == k else "none"} for k in ("arch", "insights", "copilot", "approvals", "genie")])

# ── Data Journey tab ──
@callback(Output("arch-body", "children"), Input("boot", "n_intervals"), Input("store-view", "data"))
def render_arch(_, view):
    if view != "arch": return no_update
    try:
        f_l = _pool.submit(lineage); rfis, ms = list_rfis(); L = f_l.result()
    except Exception: return err("Data journey load error")
    def node(k, h, p, tags, cnt=None, cls=""):
        kids = [html.Div([html.P(k, className="k"), html.Span(cnt, className="cnt") if cnt else None], className="nhead"),
                html.H4(h), html.P(p), html.Div([html.Span(t, className="tag") for t in tags])]
        return html.Div(className="node " + cls, children=kids)
    high = sum(1 for r in rfis if r["risk_tier"] == "High")
    return html.Div([
        html.P("END-TO-END DATA JOURNEY", className="eyebrow"), html.H2("From raw PM-system exports to decisions in the field", className="h2"),
        html.P("Every number on this page is live. Raw RFI logs and the spec book land in a Volume, Lakeflow builds the medallion, Unity Catalog governs it, "
               "ML and Gen AI make it intelligent, Lakebase serves it to this app in milliseconds, and Genie answers leadership's questions in plain English.", className="lede"),
        html.Div(className="journey", children=[
            node("① LAKEFLOW", "Ingest + medallion", "Auto Loader over JSON/CSV/Markdown drops; declarative pipeline with expectations drops bad rows and duplicate re-sends",
                 ["Auto Loader", "expectations"], cnt=f"{int(L['bronze']):,} → {int(L['silver']):,}"),
            node("② UNITY CATALOG", "Govern", "Tags, comments, cost column mask for non-finance users, least-privilege grants to this app, lineage",
                 ["mask_cost()", "lineage"], cnt=f"{int(L['gold']):,} gold"),
            node("③ ML", "Predict SLA breach", "Gradient-boosted model in MLflow, registered in UC @champion, batch-scores every open RFI",
                 ["MLflow", "UC models"], cnt=f"{high} high risk", cls="accent"),
            node("④ GEN AI", "Classify + draft", "ai_classify root cause on every RFI; Vector Search over the spec book + Claude drafts cited responses",
                 ["ai_classify", "Vector Search", LLM.replace("databricks-", "")], cnt=f"{int(L['drafts'])} drafts", cls="dark"),
            node("⑤ LAKEBASE", "Operational serving", "Synced table serves the inbox; decisions are Postgres transactions, synced back to a Delta audit",
                 ["synced table", "Postgres"], cnt=f"{ms:.0f} ms read", cls="accent"),
            node("⑥ GENIE + APP", "Ask & act", "Genie space over gold + ML + audit tables; this Databricks App for the project team",
                 ["Genie", "Databricks Apps"], cnt=f"{int(L['audit'])} audited"),
        ]),
        html.Div(className="sec-t", children="What lives where"),
        html.Div(className="cgrid", children=[html.Div(className="comp", children=[html.Span(t, className="pt " + c), html.P(n, className="cn", style={"marginTop": "8px"}),
                 html.P(d, className="role"), html.P(s, className="stat")]) for c, t, n, d, s in [
            ("pt-stor", "LAKEFLOW", f"{FQ}.bronze_rfis → silver_rfis → gold_rfi_facts", "Medallion over raw PM exports", f"{int(L['bronze']) - int(L['silver'])} defective/duplicate rows dropped"),
            ("pt-stor", "UNITY CATALOG", f"{FQ}.v_rfi_secure", "Governed view: change-order $ masked unless finance", "mask_cost() column mask"),
            ("pt-ai", "ML", f"{FQ}.rfi_sla_breach_model@champion", "SLA-breach classifier + days-to-answer regressor", f"{int(L['scored'])} open RFIs scored"),
            ("pt-ai", "GEN AI", VS_INDEX.split(".")[-1], "Delta-sync index, gte-large-en managed embeddings", f"{int(L['chunks'])} spec sections"),
            ("pt-comp", "LAKEBASE", f"{LB_DB}.{SCHEMA}.rfi_inbox · ops.rfi_workflow_events", f"Postgres instance {LB_INST}; synced table governed in UC", f"{len(rfis)} inbox rows · {ms:.0f} ms"),
            ("pt-comp", "GENIE", "RFI Intelligence space", "Gold KPIs, ML scores, root causes, decision audit", "natural-language SQL"),
        ]]),
        html.Div(className="gov", style={"marginTop": "18px"}, children=[html.B("Closed loop: "),
            "decisions taken here are written to Lakebase, merged into the governed Delta table rfi_workflow_audit by a Lakeflow Job, and become queryable in Genie and available as new training labels."]),
    ])

# ── Insights tab ──
@callback(Output("insights-body", "children"), Input("boot", "n_intervals"), Input("store-view", "data"))
def render_insights(_, view):
    if view != "insights": return no_update
    try: P = portfolio()
    except Exception: return err("Insights error")
    k = P["kpi"]
    trend = go.Figure([go.Bar(x=[r["m"] for r in P["trend"]], y=[int(r["rfis"]) for r in P["trend"]], marker_color="#D9DEE3", name="RFIs"),
                       go.Scatter(x=[r["m"] for r in P["trend"]], y=[float(r["d"] or 0) for r in P["trend"]], yaxis="y2", line=dict(color=RED, width=3), name="avg days")])
    trend.update_layout(yaxis2=dict(overlaying="y", side="right", showgrid=False, title="avg days to answer"), yaxis=dict(title="RFIs", showgrid=False))
    return html.Div([
        html.P("PORTFOLIO VIEW · GOLD TABLES VIA SQL WAREHOUSE", className="eyebrow"), html.H2("Where RFIs are costing us time and money", className="h2"),
        html.Div(className="kpis", style={"gridTemplateColumns": "repeat(5,1fr)"}, children=[
            html.Div(className="kpi", children=[html.Div(f"{int(k['open'])}", className="n"), html.Div(f"Open RFIs (of {int(k['total']):,})", className="l")]),
            html.Div(className="kpi", style={"background": RED}, children=[html.Div(f"{int(k['overdue'])}", className="n"), html.Div("Open & past SLA", className="l")]),
            html.Div(className="kpi", children=[html.Div(f"{k['avg_days']}d", className="n"), html.Div("Avg days to answer", className="l")]),
            html.Div(className="kpi", children=[html.Div(money(k["co_cost"]), className="n"), html.Div("Change-order cost from RFIs", className="l")]),
            html.Div(className="kpi", children=[html.Div(money(k["ld"]), className="n"), html.Div("Liquidated-damages exposure", className="l")]),
        ]),
        html.Div(className="panel", style={"marginTop": "16px"}, children=[html.P("RFI VOLUME VS RESPONSE TIME (24 MONTHS)", className="pt"),
            graph(_style(trend, 280))]),
        html.Div(className="grid2", style={"marginTop": "16px"}, children=[
            html.Div(className="panel", children=[html.P("SLA BREACH % BY DESIGN FIRM", className="pt"), graph(hbar([(r["assigned_firm"], float(r["breach"])) for r in P["firms"]], RED, "%"))]),
            html.Div(className="panel", children=[html.P("ROOT CAUSE (GEN AI · ai_classify)", className="pt"), graph(hbar([(r["root_cause"], int(r["n"])) for r in P["root"]], STEEL))]),
        ]),
        html.Div("Projects", className="sec-t"),
        html.Div(className="panel", children=html.Table(className="dtbl", children=[html.Tr([html.Th(h) for h in ["Project", "RFIs", "Open", "Overdue", "Avg days", "SLA breach", "CO cost", "LD exposure"]])] +
            [html.Tr([html.Td(r["project_name"]), html.Td(r["total_rfis"]), html.Td(r["open_rfis"]), html.Td(r["open_overdue_rfis"]),
                      html.Td(r["avg_days_to_answer"]), html.Td(f"{r['sla_breach_pct']}%"), html.Td(money(r["change_order_cost_usd"])), html.Td(money(r["ld_exposure_usd"]))]) for r in P["projects"]])),
    ])

# ── Copilot tab ──
ACTIONABLE = ("Open", "Returned by approver", "Needs design input", "Routed")
STAGES = ["Captured", "Triaged", "AI draft", "PE review", "Approver sign-off", "With design team", "Closed"]
STAGE_AT = {"Open": 3, "Routed": 3, "Needs design input": 5, "Returned by approver": 3, "Pending approval": 4,
            "Sent to design team": 5, "Resolved internally": 6, "Closed": 6}

def approval_policy(r, cost=None, sched=None):
    """Human-in-the-loop gate: which RFI responses need a named approver before release to the design team."""
    why = []
    if r.get("risk_tier") == "High": why.append(f"ML: {r['breach_probability']:.0%} chance the design team misses SLA")
    if (cost or "").lower() in ("medium", "high"): why.append(f"AI-assessed cost impact {cost}")
    if (sched or "").lower() in ("medium", "high"): why.append(f"AI-assessed schedule impact {sched}")
    if r.get("priority") == "Urgent": why.append("Urgent priority (3-day SLA)")
    if (r.get("liquidated_damages_per_day") or 0) >= 50000: why.append(f"LD rate ${r['liquidated_damages_per_day']:,.0f}/day on this project")
    return why

def rfi_events(rfi_id):
    return pg("""SELECT event_id, event_type, actor, detail, channel, cost_impact, schedule_impact,
                        to_char(created_at, 'Mon DD HH24:MI:SS') AS t, created_at
                 FROM ops.rfi_workflow_events WHERE rfi_id = %s ORDER BY created_at""", (rfi_id,))[0]

def approvals_data():
    def q():
        rows, _ = pg("""WITH sub AS (SELECT DISTINCT ON (rfi_id) rfi_id, event_id, actor, detail, cost_impact, schedule_impact, created_at
                                     FROM ops.rfi_workflow_events WHERE event_type = 'submitted_for_approval' ORDER BY rfi_id, created_at DESC)
                        SELECT sub.*, to_char(sub.created_at, 'Mon DD HH24:MI') AS t,
                               round((extract(epoch FROM now() - sub.created_at) / 60)::numeric, 0) AS waiting_min
                        FROM sub JOIN ops.rfi_current_status s USING (rfi_id) WHERE s.event_type = 'submitted_for_approval'
                        ORDER BY sub.created_at""")
        log, _ = pg("""SELECT d.rfi_id, d.event_type, d.actor, d.detail, to_char(d.created_at, 'Mon DD HH24:MI') AS t,
                              round((extract(epoch FROM d.created_at - s.created_at) / 60)::numeric, 1) AS turnaround_min
                       FROM ops.rfi_workflow_events d
                       LEFT JOIN LATERAL (SELECT created_at FROM ops.rfi_workflow_events x WHERE x.rfi_id = d.rfi_id
                                          AND x.event_type = 'submitted_for_approval' AND x.created_at <= d.created_at
                                          ORDER BY x.created_at DESC LIMIT 1) s ON true
                       WHERE d.event_type IN ('approver_approved', 'approver_returned') ORDER BY d.created_at DESC LIMIT 25""")
        return rows, log
    return cached("approvals", q, ttl=10)

def _invalidate():
    for k in ("rfis", "events", "approvals"): _cache.pop(k, None)

STATUS.update({"approved": "Sent to design team", "changes_requested": "Needs design input", "submitted_for_approval": "Pending approval",
               "approver_approved": "Sent to design team", "sent_to_design": "Sent to design team", "approver_returned": "Returned by approver"})

def rfi_row(r, sel):
    st = r["status"]
    bc = {"Open": "b-open", "Needs design input": "b-chg", "Routed": "b-chg", "Pending approval": "b-pend", "Returned by approver": "b-chg"}.get(st, "b-done")
    badges = [html.Span(st, className="badge " + bc)]
    if r["risk_tier"]:
        col = {"High": RED, "Medium": YEL, "Low": GRN}[r["risk_tier"]]
        badges.append(html.Span(f"{r['risk_tier']} risk {r['breach_probability']:.0%}", className="sla-mini", style={"background": col + "22", "color": col}))
    d = r["days_to_due"]
    if st in ACTIONABLE and d is not None and d < 0:
        badges.append(html.Span(f"{abs(d):.0f}d over", className="sla-mini", style={"background": "rgba(194,30,46,.14)", "color": RED}))
    if r["has_pco"]: badges.append(html.Span("PCO", className="sla-mini", style={"background": "rgba(122,79,184,.14)", "color": PUR}))
    return html.Div(className="rfi" + (" sel" if sel == r["rfi_id"] else ""), id={"type": "rfi", "id": r["rfi_id"]}, n_clicks=0, children=[
        html.Div(r["rfi_id"], className="id"), html.Div(r["subject"], className="subj"),
        html.Div(f"{r['project_name']} · {r['discipline']}", className="meta"),
        html.Div(badges, style={"marginTop": "6px", "display": "flex", "gap": "5px", "flexWrap": "wrap"})])

@callback(Output("inbox-list", "children"), Output("inbox-head", "children"), Input("boot", "n_intervals"), Input("store-sel", "data"), Input("store-refresh", "data"))
def load_inbox(_, sel, __):
    try: rfis, ms = list_rfis()
    except Exception: return err("Inbox error"), "RFI INBOX"
    return [rfi_row(r, sel) for r in rfis], f"RFI INBOX · {len(rfis)} open · Lakebase {ms:.0f} ms · ranked by ML risk"

@callback(Output("store-sel", "data"), Input({"type": "rfi", "id": ALL}, "n_clicks"), prevent_initial_call=True)
def select_rfi(clicks):
    if not ctx.triggered_id or not any(clicks or []): return no_update
    return ctx.triggered_id["id"]

def get_rfi(sel): return next((x for x in list_rfis()[0] if x["rfi_id"] == sel), None)

def lifecycle(status):
    cur = STAGE_AT.get(status, 3)
    skipped = status == "Resolved internally"
    return html.Div(className="flow", children=[html.Div(s, className="stg" + (" done" if i < cur or (i == cur and cur == 6) else "") +
        (" current" if i == cur and cur != 6 else "") + (" skip" if skipped and 2 <= i <= 5 else "")) for i, s in enumerate(STAGES)])

EV_LABEL = {"triage_resolved": "Resolved internally by GC", "approved": "PE approved response", "changes_requested": "Design-team input requested",
            "routed": "Routed", "pco_flagged": "Potential Change Order flagged", "closed": "Closed to as-built",
            "submitted_for_approval": "PE approved · submitted for sign-off", "approver_approved": "Approver signed off",
            "approver_returned": "Returned by approver", "sent_to_design": "Transmitted to design team"}
def audit_trail(events):
    if not events: return html.Div("No activity yet. Every action below is written to Lakebase and mirrored to the governed Delta audit.", className="dim")
    return html.Div(className="audit", children=[html.Div(className="step" + (" pending" if e["event_type"] == "submitted_for_approval" else ""), children=[
        html.Div([html.Span(EV_LABEL.get(e["event_type"], e["event_type"]) + (f" · {e['channel']}" if e["channel"] and e["event_type"] in ("routed", "sent_to_design") else ""), className="who"),
                  html.Span(e["actor"], className="role")]),
        html.Div((e["detail"] or "")[:220] + ("…" if len(e["detail"] or "") > 220 else ""), className="note") if e["detail"] else None,
        html.Div(f"{e['t']} · {e['event_id']}", className="t")]) for e in events])

def action_bar(r):
    return html.Div(style={"display": "flex", "gap": "10px", "marginTop": "12px", "flexWrap": "wrap"}, children=[
        html.Button("✓ Approve & send to design team", id="btn-approve", className="btn btn-ok", n_clicks=0),
        html.Button("Request design-team input", id="btn-reject", className="btn btn-ghost", n_clicks=0),
        html.Button("✉ Route: email", id={"type": "route", "ch": "email"}, className="btn btn-ghost", n_clicks=0),
        html.Button("🎫 Route: ticket", id={"type": "route", "ch": "ticket"}, className="btn btn-ghost", n_clicks=0)])

def draft_block(r, text, cites, chunks=None, live=False, meta=None):
    meta = meta or {}; imp = meta.get("impact") or {}
    why = approval_policy(r, imp.get("cost"), imp.get("schedule"))
    kids = [html.Div(("Live draft · Vector Search + " if live else "Batch draft · Vector Search + ") + LLM.replace("databricks-", ""), className="sec-t")]
    if live:
        kids += [html.Div(className="assess", children=[
            html.Div(className="clarity", children=[html.P("SPEC CLARITY", className="lab"), html.Div([str(meta.get("clarity_score", "-")), html.Small(" / 100")], className="score"),
                     html.P(meta.get("root_cause", ""), className="rat")]),
            html.Div(className="impacts", children=[html.P("POTENTIAL IMPACT", className="lab")] + [html.Div(className="imp-row", children=[html.Span(k.title(), className="k"), chip(imp.get(k))]) for k in ("cost", "schedule", "safety")])])]
        kids += [html.Div(className="cite", children=[html.Div(c.get("section"), className="s"), html.Div(c.get("title"), className="t"), html.Div(c.get("content"), className="c")]) for c in (chunks or [])]
    kids += [html.P(f"Cited: {cites or '-'}", className="disp-sub"),
             dcc.Textarea(id="draft-text", value=text or "", className="draft-box"),
             html.Div(className="policy " + ("needs" if why else "clear"), children=[
                 html.B("Approval policy: " + ("named approver sign-off required before release" if why else "auto-release to the design team (low risk)")),
                 html.Div([html.Span(x, className="pchip") for x in why]) if why else None]),
             action_bar(r)]
    return html.Div(kids)

def detail_view(r):
    p = r["breach_probability"] or 0; col = RED if p >= .65 else YEL if p >= .35 else GRN
    st = r["status"]; ld = r["liquidated_damages_per_day"] or 0
    ev = rfi_events(r["rfi_id"])
    last = {e["event_type"]: e for e in ev}
    if st in ACTIONABLE:
        body = [html.Div(className="triage", children=[html.P("GC TRIAGE GATE", className="cap-lab"),
                    html.P("Already answered by the contract documents? Close it here and keep it out of the design team's queue.", className="triage-help"),
                    html.Button("✓ Resolve internally", id="btn-triage", className="btn btn-ghost", n_clicks=0)]),
                html.Div(className="status-msg s-warn", children=[html.B("Returned by approver: "), last["approver_returned"]["detail"] or "see comment"]) if st == "Returned by approver" and "approver_returned" in last else None,
                html.Div(id="draft-area", children=draft_block(r, (last.get("submitted_for_approval") or {}).get("detail") if st == "Returned by approver" else r["draft_response"], r["cited_sections"])),
                html.Button("⚡ Regenerate live (Vector Search + LLM)", id="btn-gen", className="btn btn-primary", n_clicks=0, style={"marginTop": "12px"}),
                dcc.Loading(color=RED, children=html.Div(id="gen-status"))]
    elif st == "Pending approval":
        s = last.get("submitted_for_approval") or {}
        body = [html.Div(className="status-msg s-pend", children=[html.B("Awaiting approver sign-off. "),
                    "Submitted by ", s.get("actor", PE), " at ", s.get("t", ""), ". Open the ", html.B("Approvers"), " tab to sign off or return it."]),
                html.Div(className="policy needs", children=[html.B("Why approval is required"),
                    html.Div([html.Span(x, className="pchip") for x in approval_policy(r, s.get("cost_impact"), s.get("schedule_impact"))])]),
                html.Div("Response pending sign-off", className="sec-t"), html.Div(s.get("detail") or "", className="quote")]
    elif st in ("Sent to design team",):
        body = [html.Div(className="status-msg s-ok", children=[html.B(f"With {r['assigned_firm']}. "), f"Expected answer in {r['predicted_days_to_answer']} days vs the {r['sla_days']}-day SLA."]),
                html.Div(className="pco-box", style={"marginTop": "12px"}, children=[
                    html.P("Does the answer change scope? Flag a Potential Change Order, then close the RFI with an as-built reference.", className="pco-help"),
                    html.Div(style={"display": "flex", "gap": "10px", "flexWrap": "wrap"}, children=[
                        html.Button("⚑ Flag Potential Change Order", id="btn-pco", className="btn btn-primary", n_clicks=0),
                        dcc.Input(id="asbuilt-ref", value=f"Sheet A-301 · pin @ {r['rfi_id']}", className="email-in", style={"flex": "1", "minWidth": "220px", "border": "1px solid #D9DEE3", "borderRadius": "8px", "padding": "9px 12px"}),
                        html.Button("🔗 Close & link to as-built", id="btn-close", className="btn btn-ok", n_clicks=0)])])]
    else:
        body = [html.Div(f"✓ {st}", className="status-msg s-ok")]
    return html.Div([
        html.P(f"REQUEST FOR INFORMATION · {st.upper()}", className="eyebrow"),
        lifecycle(st),
        html.Div(className="q-card", style={"marginTop": "14px"}, children=[html.H2(r["subject"]),
            html.Div(f"{r['rfi_id']} · {r['project_name']} · {r['discipline']} · spec {r['spec_section']} · {r['submitted_by_role']}, {r['submitted_by_company']} · submitted {r['submitted']} · due {r['due']}", className="m"),
            html.P(r["question"])]),
        html.Div(className="assess", children=[
            html.Div(className="clarity", children=[html.P("ML · PROBABILITY DESIGN TEAM MISSES SLA", className="lab"),
                html.Div([f"{p:.0%}"], className="score"), html.Div(className="bar", children=html.Div(className="fill", style={"width": f"{p*100:.0f}%", "background": col})),
                html.P(f"Expected answer in {r['predicted_days_to_answer']} days vs {r['sla_days']}-day SLA ({r['assigned_firm']}). Drivers: {r['top_drivers']}.", className="rat")]),
            html.Div(className="impacts", children=[html.P("CONTEXT", className="lab"),
                html.Div(className="imp-row", children=[html.Span("Root cause (ai_classify)", className="k"), html.Span(r["root_cause"] or "-", className="chip c-med")]),
                html.Div(className="imp-row", children=[html.Span("LD rate if it slips", className="k"), html.Span(f"${ld:,.0f}/day", className="chip c-high")]),
                html.Div(className="imp-row", children=[html.Span("Location / BIM", className="k"), html.Span(f"{r['capture_location']} · {r['bim_ref']}", className="chip c-none")]),
                html.Div(className="imp-row", children=[html.Span("Field proposal", className="k"), html.Span(r["proposed_solution"] or "none", className="chip c-none")])])]),
        *body,
        html.Div("Audit trail · Lakebase ops.rfi_workflow_events", className="sec-t"), audit_trail(ev),
    ])

@callback(Output("detail", "children"), Input("store-sel", "data"), Input("store-refresh", "data"), prevent_initial_call=True)
def show_detail(sel, _):
    r = get_rfi(sel) if sel else None
    if not r: return no_update
    try: return detail_view(r)
    except Exception: return err("Detail error")

# ── live workflow trace ("what is happening right now") ──
def S(step, target=None, detail=None, metric=None, state="done"):
    return {"step": step, "target": target, "detail": detail, "metric": metric, "state": state}

def lakebase_steps(writes):
    return [S("Committed to Lakebase", "ops.rfi_workflow_events", f"INSERT {eid} ({etype})", f"{ms:.0f} ms") for eid, etype, ms in writes]

def tail_steps(run_id, status):
    host = w().config.host.rstrip("/")
    return [S("Inbox status updated", "Lakebase view ops.rfi_current_status", f"RFI now shows: {status}"),
            S("Lakeflow Job: audit sync", f"run {run_id}" if run_id else "not triggered",
              "Merging Lakebase events into Delta rfi_workflow_audit", None, "running" if run_id else "done") | {"url": f"{host}/jobs/{AUDIT_JOB}/runs/{run_id}" if run_id else None},
            S("Governed in Unity Catalog", f"{FQ}.rfi_workflow_audit", "Queryable in Genie and BI; lineage and grants apply", None, "pending" if run_id else "done")]

def write(rfi_id, events, actor, status):
    """Run a sequence of workflow writes, trigger one audit sync, and return trace steps."""
    writes = []
    for etype, kw in events:
        eid, ms, _ = log_event(rfi_id, etype, actor=actor, trigger=False, **kw); writes.append((eid, etype, ms))
    run_id = trigger_audit()
    return lakebase_steps(writes), tail_steps(run_id, status), run_id, writes[0][0]

def trace_payload(title, subtitle, steps, run_id, eid):
    return {"title": title, "subtitle": subtitle, "steps": steps, "run_id": run_id, "event_id": eid, "t0": time.time()}

@callback(Output("store-trace", "data", allow_duplicate=True), Output("store-refresh", "data", allow_duplicate=True),
          Input("btn-triage", "n_clicks"), State("store-sel", "data"), State("store-refresh", "data"), prevent_initial_call=True)
def triage(n, sel, k):
    if not n: return no_update, no_update
    r = get_rfi(sel)
    lb, tail, run, eid = write(sel, [("triage_resolved", {"detail": "Answer already in contract documents; closed by GC without design escalation."})], PE, "Resolved internally")
    steps = [S("Triage decision captured", PE, "Contract documents already answer this RFI"), *lb,
             S("Kept out of the design team's queue", r["assigned_firm"], f"No transmittal issued; saves a {r['sla_days']}-day SLA cycle"), *tail]
    return trace_payload("Resolved internally", f"{sel} · {r['subject']}", steps, run, eid), (k or 0) + 1

@callback(Output("store-trace", "data", allow_duplicate=True), Output("store-refresh", "data", allow_duplicate=True),
          Input("btn-approve", "n_clicks"), Input("btn-reject", "n_clicks"), Input({"type": "route", "ch": ALL}, "n_clicks"),
          State("store-sel", "data"), State("draft-text", "value"), State("store-chunks", "data"), State("store-refresh", "data"), prevent_initial_call=True)
def decide(a, rj, routes, sel, text, meta, k):
    t = ctx.triggered_id
    if not t or not (a or rj or any(routes or [])): return no_update, no_update
    r = get_rfi(sel); imp = (meta or {}).get("impact") or {}
    cites = sorted(set(re.findall(r"\b\d\d \d\d \d\d\b", text or "")))
    if t == "btn-approve":
        why = approval_policy(r, imp.get("cost"), imp.get("schedule"))
        cap = S("PE approved the cited response", PE, f"{len((text or '').split())} words · cites {', '.join(cites) or 'none'}")
        if why:
            lb, tail, run, eid = write(sel, [("submitted_for_approval", {"detail": text, "cost": imp.get("cost"), "sched": imp.get("schedule")})], PE, "Pending approval")
            steps = [cap, S("Approval policy evaluated", "human-in-the-loop gate", "Sign-off required: " + "; ".join(why)), *lb,
                     S("Queued for approver", "Approvers tab", f"Project Executive must sign off before release to {r['assigned_firm']}", None, "pending"), *tail]
            title = "Submitted for approver sign-off"
        else:
            lb, tail, run, eid = write(sel, [("approved", {"detail": text, "cost": imp.get("cost"), "sched": imp.get("schedule")}),
                                             ("sent_to_design", {"detail": f"Transmittal to {r['assigned_firm']} RFI desk", "channel": "transmittal"})], PE, "Sent to design team")
            steps = [cap, S("Approval policy evaluated", "human-in-the-loop gate", "Low risk: cleared for automatic release"), *lb,
                     S("Transmittal issued", r["assigned_firm"], f"RFI package with cited response; SLA {r['sla_days']} days, ML expects {r['predicted_days_to_answer']} days"), *tail]
            title = "Approved and sent to the design team"
    elif t == "btn-reject":
        lb, tail, run, eid = write(sel, [("changes_requested", {"detail": text})], PE, "Needs design input")
        steps = [S("Design-team input requested", PE, "Draft cannot be answered from the specs alone"), *lb,
                 S("Question assigned", r["assigned_firm"], f"Response due in {r['sla_days']} days"), *tail]
        title = "Design-team input requested"
    else:
        ch = t["ch"]
        lb, tail, run, eid = write(sel, [("routed", {"detail": text, "channel": ch})], PE, "Routed")
        steps = [S(f"Routing via {ch}", PE, f"To {r['assigned_firm']} RFI desk"), *lb,
                 S("Notification recorded", ch, "Delivery logged as a workflow event (no external send in this prototype)"), *tail]
        title = f"Routed via {ch}"
    return trace_payload(title, f"{sel} · {r['subject']}", steps, run, eid), (k or 0) + 1

@callback(Output("store-trace", "data", allow_duplicate=True), Output("store-refresh", "data", allow_duplicate=True),
          Input("btn-pco", "n_clicks"), Input("btn-close", "n_clicks"), State("asbuilt-ref", "value"), State("store-sel", "data"), State("store-refresh", "data"), prevent_initial_call=True)
def pco_close(p, c, ref, sel, k):
    if not (p or c): return no_update, no_update
    r = get_rfi(sel)
    if ctx.triggered_id == "btn-pco":
        lb, tail, run, eid = write(sel, [("pco_flagged", {"detail": "RFI response alters scope"})], PE, "Sent to design team")
        steps = [S("Potential Change Order flagged", PE, "Answer changes scope, cost or schedule"), *lb,
                 S("Project controls notified", "PCO log", f"LD rate on this project: ${(r['liquidated_damages_per_day'] or 0):,.0f}/day"), *tail]
        title = "Potential Change Order flagged"
    else:
        lb, tail, run, eid = write(sel, [("closed", {"detail": ref})], PE, "Closed")
        steps = [S("RFI closed", PE, f"As-built reference: {ref}"), *lb, S("Pinned to the drawing set", "as-built", ref), *tail]
        title = "Closed to as-built"
    return trace_payload(title, f"{sel} · {r['subject']}", steps, run, eid), (k or 0) + 1

@callback(Output("draft-area", "children"), Output("store-chunks", "data"), Output("gen-status", "children"), Output("store-trace", "data", allow_duplicate=True),
          Input("btn-gen", "n_clicks"), State("store-sel", "data"), prevent_initial_call=True)
def generate(n, sel):
    if not n: return no_update, no_update, no_update, no_update
    try:
        r = get_rfi(sel)
        t0 = time.perf_counter(); chunks = retrieve(r["question"]); t1 = time.perf_counter(); m = analyze(r, chunks); t2 = time.perf_counter()
    except Exception: return no_update, no_update, err("Generation error"), no_update
    imp = m.get("impact") or {}; why = approval_policy(r, imp.get("cost"), imp.get("schedule"))
    steps = [S("Vector Search retrieval", VS_INDEX.split(".")[-1], "Top sections: " + ", ".join(c["section"] for c in chunks), f"{(t1-t0)*1000:.0f} ms"),
             S("Cited draft generated", LLM, f"Clarity {m.get('clarity_score', '-')}/100 · cost {imp.get('cost', '-')} · schedule {imp.get('schedule', '-')} · safety {imp.get('safety', '-')}", f"{t2-t1:.1f} s"),
             S("Approval policy pre-check", "human-in-the-loop gate", ("Will need sign-off: " + "; ".join(why)) if why else "Low risk: eligible for automatic release"),
             S("Waiting for the project engineer", PE, "Nothing is sent until a person approves", None, "pending")]
    return (draft_block(r, m.get("response"), ", ".join(c["section"] for c in chunks), chunks, live=True, meta=m), m,
            html.Div(f"Generated live in {t2-t0:.1f}s", className="disp-sub"),
            trace_payload("Live draft generated", f"{sel} · {r['subject']}", steps, None, None))

# ── Approvers tab (human in the loop) ──
@callback(Output("approvals-body", "children"), Input("store-view", "data"), Input("store-refresh", "data"), Input("appr-who", "value"))
def render_approvals(view, _, who):
    if view != "approvals": return no_update
    try:
        pending, log = approvals_data(); inbox = {r["rfi_id"]: r for r in list_rfis()[0]}
    except Exception: return err("Approvals error")
    approved = [x for x in log if x["event_type"] == "approver_approved"]; returned = [x for x in log if x["event_type"] == "approver_returned"]
    tat = [float(x["turnaround_min"]) for x in log if x["turnaround_min"] is not None]
    high = sum(1 for p in pending if (inbox.get(p["rfi_id"]) or {}).get("risk_tier") == "High")
    def card(p):
        r = inbox.get(p["rfi_id"]) or {}
        why = approval_policy(r, p["cost_impact"], p["schedule_impact"]) if r else []
        prob = r.get("breach_probability") or 0
        return html.Div(className="appr-card", children=[
            html.Div(className="appr-head", children=[
                html.Div([html.Div(p["rfi_id"], className="id"), html.Div(r.get("subject", ""), className="subj"),
                          html.Div(f"{r.get('project_name', '')} · {r.get('discipline', '')} · to {r.get('assigned_firm', '')}", className="meta")]),
                html.Div([html.Div(f"{prob:.0%}", className="appr-risk", style={"color": RED if prob >= .65 else YEL}), html.Div("SLA-miss risk", className="meta")], style={"textAlign": "right"})]),
            html.Div([html.Span(x, className="pchip") for x in why], style={"margin": "8px 0"}),
            html.Div(f"Submitted by {p['actor']} · {p['t']} · waiting {int(p['waiting_min'] or 0)} min", className="meta"),
            dcc.Textarea(id={"type": "appr-text", "id": p["rfi_id"]}, value=p["detail"] or "", className="draft-box", style={"minHeight": "110px", "marginTop": "8px"}),
            html.Div(style={"display": "flex", "gap": "10px", "marginTop": "10px", "flexWrap": "wrap", "alignItems": "center"}, children=[
                html.Button("✓ Sign off & release to design team", id={"type": "appr-act", "id": p["rfi_id"], "act": "approve"}, className="btn btn-ok", n_clicks=0),
                dcc.Input(id={"type": "appr-note", "id": p["rfi_id"]}, placeholder="Comment (required to return)", className="email-in",
                          style={"flex": "1", "minWidth": "220px", "border": "1px solid #D9DEE3", "borderRadius": "8px", "padding": "9px 12px"}),
                html.Button("↩ Return to PE", id={"type": "appr-act", "id": p["rfi_id"], "act": "return"}, className="btn btn-ghost", n_clicks=0)])])
    return html.Div([
        html.Div(className="kpis", children=[
            html.Div(className="kpi", style={"background": RED if pending else NAVY}, children=[html.Div(str(len(pending)), className="n"), html.Div("Awaiting sign-off", className="l")]),
            html.Div(className="kpi", children=[html.Div(str(high), className="n"), html.Div("High SLA-risk in queue", className="l")]),
            html.Div(className="kpi", children=[html.Div(str(len(approved)), className="n"), html.Div("Signed off (recent)", className="l")]),
            html.Div(className="kpi", children=[html.Div(str(len(returned)), className="n"), html.Div("Returned to PE", className="l")]),
            html.Div(className="kpi", children=[html.Div(f"{sum(tat)/len(tat):.0f}m" if tat else "-", className="n"), html.Div("Avg sign-off time", className="l")])]),
        html.Div(f"Approval queue · signing as {who}", className="sec-t"),
        html.Div([card(p) for p in pending]) if pending else html.Div("Nothing waiting. Approve a high-risk RFI in the RFI Copilot tab to see it arrive here.", className="empty", style={"padding": "24px 4px"}),
        html.Div("Decision log · human-in-the-loop audit", className="sec-t"),
        html.Div(className="panel", children=html.Table(className="dtbl", children=[html.Tr([html.Th(h) for h in ["RFI", "Decision", "Approver", "Comment", "When", "Turnaround"]])] +
            [html.Tr([html.Td(x["rfi_id"]), html.Td("Signed off" if x["event_type"] == "approver_approved" else "Returned"), html.Td(x["actor"]),
                      html.Td((x["detail"] or "")[:80]), html.Td(x["t"]), html.Td(f"{x['turnaround_min']} min" if x["turnaround_min"] is not None else "-")]) for x in log]) if log else
            html.Div("No approver decisions yet.", className="dim")),
    ])

@callback(Output("store-trace", "data", allow_duplicate=True), Output("store-refresh", "data", allow_duplicate=True),
          Input({"type": "appr-act", "id": ALL, "act": ALL}, "n_clicks"), State({"type": "appr-note", "id": ALL}, "value"),
          State({"type": "appr-text", "id": ALL}, "value"), State("appr-who", "value"), State("store-refresh", "data"), prevent_initial_call=True)
def approver_act(clicks, notes, texts, who, k):
    t = ctx.triggered_id
    if not t or not any(clicks or []): return no_update, no_update
    rid = t["id"]
    note = next((v for s, v in zip(ctx.states_list[0], notes) if s["id"]["id"] == rid), "") or ""
    text = next((v for s, v in zip(ctx.states_list[1], texts) if s["id"]["id"] == rid), "") or ""
    r = get_rfi(rid) or {}
    if t["act"] == "approve":
        lb, tail, run, eid = write(rid, [("approver_approved", {"detail": note or "Signed off"}),
                                         ("sent_to_design", {"detail": text, "channel": "transmittal"})], who, "Sent to design team")
        steps = [S("Approver signed off", who, note or "Response reviewed and approved as submitted"), *lb,
                 S("Transmittal released", r.get("assigned_firm"), f"Cited response sent to the design team; SLA {r.get('sla_days')} days"), *tail]
        title = "Signed off and released"
    else:
        if not note.strip():
            return trace_payload("Comment required", rid, [S("Return blocked", who, "Add a comment so the project engineer knows what to change", None, "pending")], None, None), no_update
        lb, tail, run, eid = write(rid, [("approver_returned", {"detail": note})], who, "Returned by approver")
        steps = [S("Returned to project engineer", who, note), *lb, S("Back in the PE's queue", PE, "Shown with the approver's comment in RFI Copilot"), *tail]
        title = "Returned to the project engineer"
    return trace_payload(title, f"{rid} · {r.get('subject', '')}", steps, run, eid), (k or 0) + 1

# ── trace modal render + live job polling ──
@callback(Output("trace-modal", "children"), Output("trace-poll", "disabled"), Input("store-trace", "data"))
def render_trace(tr):
    if not tr: return None, True
    def step(i, s):
        icon = {"done": "✓", "running": "", "pending": "…"}[s["state"]]
        tgt = html.A(s["target"], href=s["url"], target="_blank") if s.get("url") else s.get("target")
        return html.Div(className=f"tstep {s['state']}", style={"animationDelay": f"{i * 0.35}s"}, children=[
            html.Div(icon, className="tdot"),
            html.Div([html.Div([html.Span(s["step"], className="who"), html.Span(tgt, className="role") if tgt else None,
                                html.Span(s["metric"], className="tmetric") if s.get("metric") else None]),
                      html.Div(s["detail"], className="note") if s.get("detail") else None])])
    live = any(s["state"] == "running" for s in tr["steps"])
    return html.Div(className="overlay", children=html.Div(className="trace-card", children=[
        html.Div(className="trace-head", children=[html.Div([html.Div("GOVERNED ACTION · WHAT IS HAPPENING NOW", className="eyebrow"),
            html.H2(tr["title"], style={"margin": "2px 0 0", "fontSize": "1.15rem"}), html.Div(tr.get("subtitle"), className="meta")]),
            html.Button("×", id="trace-x", className="x", n_clicks=0)]),
        html.Div(className="tsteps", children=[step(i, s) for i, s in enumerate(tr["steps"])]),
        html.Div(className="trace-foot", children=["The AI proposes; a named person decides. Nothing is sent automatically. Each action is a Lakebase transaction, "
                 "mirrored to an append-only Delta audit governed by Unity Catalog."]),
        html.Div(style={"textAlign": "right", "marginTop": "12px"}, children=html.Button("Done", id="trace-done", className="btn btn-primary", n_clicks=0))])), not live

@callback(Output("store-trace", "data", allow_duplicate=True), Input("trace-poll", "n_intervals"), State("store-trace", "data"), prevent_initial_call=True)
def poll_trace(_, tr):
    if not tr or not tr.get("run_id"): return no_update
    try: st = w().jobs.get_run(int(tr["run_id"])).state
    except Exception: return no_update
    secs = time.time() - tr["t0"]; lc = st.life_cycle_state.value if st.life_cycle_state else "PENDING"
    steps = tr["steps"]; job = next(s for s in steps if s["step"].startswith("Lakeflow Job")); gov = steps[-1]
    if lc in ("TERMINATED", "INTERNAL_ERROR", "SKIPPED"):
        ok = st.result_state and st.result_state.value == "SUCCESS"
        job.update(state="done" if ok else "pending", metric=f"{secs:.0f} s", detail=("Merged into Delta rfi_workflow_audit" if ok else f"Job ended: {st.result_state}"))
        if ok:
            try: n = sql(f"SELECT count(*) c FROM {FQ}.rfi_workflow_audit WHERE event_id = :e", {"e": tr["event_id"]})[0]["c"]
            except Exception: n = "?"
            gov.update(state="done", detail=f"Event {tr['event_id']} found in Delta ({n} row) · queryable in Genie")
        tr["run_id"] = None
    else:
        job.update(metric=f"{lc.lower()} · {secs:.0f} s")
    return tr

@callback(Output("store-trace", "data", allow_duplicate=True), Input("trace-x", "n_clicks"), Input("trace-done", "n_clicks"), prevent_initial_call=True)
def close_trace(a, b):
    return None if (a or b) else no_update

# ── Genie tab ──
SAMPLES = ["Which projects have the most overdue open RFIs right now?", "Which design firm is slowest to answer RFIs?",
           "How much change-order cost came from RFIs caused by design conflicts, by project?",
           "Show the 10 open RFIs most likely to miss their SLA", "What decisions has the team taken in the RFI Copilot today?"]

@callback(Output("genie-body", "children"), Input("store-view", "data"))
def render_genie(view):
    if view != "genie": return no_update
    host = w().config.host.rstrip("/")
    try: sid = genie_id()
    except Exception: sid = ""
    return html.Div([
        html.P("AI/BI GENIE · NATURAL LANGUAGE OVER GOVERNED DATA", className="eyebrow"), html.H2("Ask the portfolio a question", className="h2"),
        html.P(["Genie writes SQL over the gold tables, ML risk scores, Gen-AI root causes and the decision audit, under Unity Catalog permissions. ",
                html.A("Open the full Genie space ↗", href=f"{host}/genie/rooms/{sid}", target="_blank")], className="lede"),
        html.Div(style={"display": "flex", "gap": "10px"}, children=[
            dcc.Input(id="genie-q", placeholder="e.g. Which discipline has the worst SLA breach rate this year?", debounce=True, className="email-in",
                      style={"flex": "1", "border": "1px solid #D9DEE3", "borderRadius": "8px", "padding": "11px 14px", "background": "#fff"}),
            html.Button("Ask Genie", id="genie-ask", className="btn btn-primary", n_clicks=0)]),
        html.Div(style={"display": "flex", "gap": "8px", "flexWrap": "wrap", "marginTop": "10px"},
                 children=[html.Button(q, id={"type": "gq", "i": i}, className="btn btn-ghost", n_clicks=0, style={"fontSize": ".78rem", "padding": "7px 12px"}) for i, q in enumerate(SAMPLES)]),
        dcc.Loading(color=RED, children=html.Div(id="genie-out", style={"marginTop": "18px"})),
    ])

@callback(Output("genie-out", "children"), Output("genie-q", "value"), Input("genie-ask", "n_clicks"), Input("genie-q", "n_submit"), Input({"type": "gq", "i": ALL}, "n_clicks"),
          State("genie-q", "value"), prevent_initial_call=True)
def genie(n, sub, chips, q):
    t = ctx.triggered_id
    if isinstance(t, dict):
        if not any(chips or []): return no_update, no_update
        q = SAMPLES[t["i"]]
    if not q: return no_update, no_update
    try: g = ask_genie(q)
    except Exception: return err("Genie error"), q
    return html.Div(className="panel", children=[
        html.P(f"Q: {q}", className="pt"),
        html.P(g["desc"] or g["text"] or "", style={"fontSize": ".95rem", "lineHeight": "1.5"}),
        html.Table(className="dtbl", children=[html.Tr([html.Th(c) for c in g["cols"]])] + [html.Tr([html.Td(x) for x in r]) for r in g["rows"]]) if g["rows"] else None,
        html.Details([html.Summary("Generated SQL"), html.Pre(g["sql"] or "", className="disp-pre")]) if g["sql"] else None]), q

@server.route("/api/health")
def health():
    out = {}
    for name, fn in [("lakebase_inbox", lambda: (lambda r: {"rows": len(r[0]), "ms": round(r[1], 1), "high_risk": sum(x["risk_tier"] == "High" for x in r[0])})(list_rfis())),
                     ("warehouse_gold_kpis", lambda: portfolio()["kpi"]),
                     ("vector_search", lambda: [c["section"] for c in retrieve("required concrete cover for grade beams cast against earth", 2)]),
                     ("genie_space", lambda: genie_id())]:
        try: out[name] = fn()
        except Exception as e: out[name] = f"ERROR {type(e).__name__}: {str(e)[:200]}"
    from flask import request
    if request.args.get("deep"):
        r = list_rfis()[0][0]
        for name, fn in [("llm_live_draft", lambda: (lambda ch: analyze(r, ch))(retrieve(r["question"]))),
                         ("genie_answer", lambda: (lambda g: {"desc": g["desc"], "sql": g["sql"], "rows": g["rows"][:5]})(ask_genie("Which project has the most open RFIs?"))),
                         ("lakebase_write", lambda: dict(zip(["event_id", "ms", "audit_run_id"], log_event(r["rfi_id"], "routed", "smoke test", channel="ticket", actor="smoke-test"))))]:
            try: out[name] = fn()
            except Exception as e: out[name] = f"ERROR {type(e).__name__}: {str(e)[:300]}"
    return json.dumps(out, default=str), 200, {"Content-Type": "application/json"}

threading.Thread(target=_prewarm, daemon=True).start()

if __name__ == "__main__":
    app.run(debug=False, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
