#!/usr/bin/env python3

import os, time, threading
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash import Dash, dcc, html, dash_table, Input, Output, ctx, no_update
from dash.exceptions import PreventUpdate
from pyspark.sql import SparkSession, Window, functions as F
from common import geo_rows, ISO3

BASE = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.environ.get("LOG_DIR", os.path.join(BASE, "logs"))
ACCESS, AUTH = os.path.join(LOG_DIR, "access.log"), os.path.join(LOG_DIR, "auth.log")
from pathlib import Path
ACCESS_URI, AUTH_URI = Path(ACCESS).resolve().as_uri(), Path(AUTH).resolve().as_uri()  # file:///... (never HDFS)
MIN_RELOAD = float(os.environ.get("MIN_RELOAD_SECS", "10"))

LOG_RE = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) [^"]*" (\d{3}) (\d+|-) "([^"]*)" "([^"]*)" rt=([0-9.]+)$'
SENS_RE = r"\.env|wp-login|wp-admin|phpmyadmin|\.git|etc/passwd|\.\./|union|xmlrpc|\.aws|backup|server-status|^/admin"
RANGES = {"all": None, "7d": 168, "3d": 72, "24h": 24, "6h": 6}
BUCKETS = {"5 min": 300, "15 min": 900, "1 hour": 3600, "1 day": 86400}
DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

# ------------------------------------------------------------------ Spark / loading
_spark = None
LOCK = threading.RLock()
STATE = {"sig": None, "T": None, "A": None, "P": None, "PA": None, "loaded_at": 0, "version": 0}
RESULTS = {}


def ensure_java():
    """Spark 4 needs Java 17+. If the default `java` is older (e.g. a Java 8 left over from Hadoop),
    look in /usr/lib/jvm for a newer one and point JAVA_HOME / PATH at it."""
    import glob, re, shutil, subprocess

    def major(java):
        try:
            out = subprocess.run([java, "-version"], capture_output=True, text=True, timeout=30).stderr
            m = re.search(r'version "(\d+)(?:\.(\d+))?', out)
            if not m: return 0
            v = int(m.group(1)); return int(m.group(2) or 0) if v == 1 else v
        except Exception:
            return 0

    jh = os.environ.get("JAVA_HOME")
    cur = os.path.join(jh, "bin", "java") if jh else shutil.which("java")
    if cur and major(cur) >= 17:
        return
    best = None
    for d in glob.glob("/usr/lib/jvm/*"):
        j = os.path.join(d, "bin", "java")
        if os.path.exists(j):
            m = major(j)
            if m >= 17 and (best is None or m > best[0]): best = (m, d)
    if not best:
        raise RuntimeError("Java 17+ is required but not found. Install it: sudo apt-get install default-jre-headless")
    os.environ["JAVA_HOME"] = best[1]
    os.environ["PATH"] = os.path.join(best[1], "bin") + os.pathsep + os.environ.get("PATH", "")
    print(f"[dashboard] Using Java {best[0]} at {best[1]}")


def spark():
    global _spark
    if _spark is None:
        ensure_java()
        _spark = (SparkSession.builder.appName("traffic-dashboard").master("local[*]")
                  .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
                  .config("spark.hadoop.fs.defaultFS", "file:///").config("spark.sql.shuffle.partitions", "8").config("spark.ui.enabled", "false")
                  .config("spark.sql.session.timeZone", "UTC").config("spark.sql.ansi.enabled", "false").config("spark.scheduler.mode", "FAIR")
                  .getOrCreate())
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark


def file_sig():
    out = []
    for p in (ACCESS, AUTH):
        out.append((p, os.stat(p).st_mtime_ns, os.stat(p).st_size) if os.path.exists(p) else (p, 0, 0))
    return tuple(out)


def octet(col):
    return F.regexp_extract(col, r"^(\d+)\.", 1).cast("int")


def load_all():
    sp = spark(); t0 = time.time()
    geo = F.broadcast(sp.createDataFrame(geo_rows(), "octet int, country string, iso3 string"))
    # ---- web access log
    raw = sp.read.text(ACCESS_URI)
    g = lambda i: F.regexp_extract("value", LOG_RE, i)
    P = raw.select(g(1).alias("ip"), g(2).alias("ts_raw"), g(3).alias("method"), g(4).alias("path"),
                   g(5).cast("int").alias("status"), g(6).alias("bytes_raw"), g(8).alias("ua"),
                   g(9).cast("double").alias("rt"))
    P = P.withColumn("ts", F.to_timestamp("ts_raw", "dd/MMM/yyyy:HH:mm:ss Z")).cache()
    P_total, P_valid = P.agg(F.count("*"), F.sum((F.col("ts").isNotNull() & (F.col("ip") != "")).cast("int"))).first()
    ual = F.lower("ua")
    T = (P.filter(F.col("ts").isNotNull() & (F.col("ip") != ""))
         .withColumn("epoch", F.unix_timestamp("ts"))
         .withColumn("bytes", F.when(F.col("bytes_raw") == "-", 0).otherwise(F.col("bytes_raw").cast("long")))
         .withColumn("path_noq", F.split("path", r"\?")[0])
         .withColumn("path_clean", F.regexp_replace(F.regexp_replace(F.regexp_replace(
             F.col("path_noq"), r"/\d+", "/{id}"), r"/p\d+\.jpg", "/p{id}.jpg"), r"^/blog/.+", "/blog/{slug}"))
         .withColumn("sens", F.lower("path").rlike(SENS_RE).cast("int"))
         .withColumn("status_class", F.concat((F.col("status") / 100).cast("int").cast("string"), F.lit("xx")))
         .withColumn("ua_type", F.when(ual.rlike("sqlmap|nikto|nmap|masscan|zgrab|dirbuster"), "scanner")
                     .when(ual.rlike("bot|crawler|spider|slurp|curl|wget|python-requests|scrapy|go-http"), "bot")
                     .otherwise("human"))
         .withColumn("device", F.when(ual.rlike("iphone|android|mobile"), "mobile").otherwise("desktop"))
         .withColumn("octet", octet("ip"))
         .join(geo, "octet", "left").fillna({"country": "Unknown"})
         .drop("bytes_raw", "ts_raw", "octet"))
    T = T.cache()
    # ---- auth log
    schema = "ts string, user string, ip string, result string, method string, reason string"
    PA = sp.read.schema(schema).json(AUTH_URI).cache()
    A_total = PA.count()
    A = (PA.withColumn("ts", F.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
         .filter(F.col("ts").isNotNull() & F.col("ip").isNotNull() & F.col("result").isin("success", "failure"))
         .withColumn("epoch", F.unix_timestamp("ts")).withColumn("octet", octet("ip"))
         .join(geo, "octet", "left").fillna({"country": "Unknown"}).drop("octet")).cache()
    A_valid = A.count()
    mm = T.agg(F.min("epoch"), F.max("epoch")).first()
    web_rows = T.count()
    old = (STATE["P"], STATE["T"], STATE["PA"], STATE["A"])
    STATE.update(T=T, A=A, P=P, PA=PA, version=STATE["version"] + 1, loaded_at=time.time(),
                 minepoch=mm[0] or 0, maxepoch=mm[1] or 0,
                 stats=dict(web_total=P_total, web_valid=web_rows, web_bad=P_total - (P_valid or 0),
                            auth_total=A_total, auth_valid=A_valid, auth_bad=A_total - A_valid,
                            load_secs=round(time.time() - t0, 1), dtypes=T.dtypes,
                            spark=sp.version, cores=sp.sparkContext.defaultParallelism))
    RESULTS.clear()
    for df in old:
        if df is not None:
            try: df.unpersist()
            except Exception: pass


def ensure_loaded(force=False):
    """Reload when the log files changed (rate-limited). Returns True if data was reloaded."""
    with LOCK:
        sig = file_sig()
        if not os.path.exists(ACCESS) or not os.path.exists(AUTH):
            raise FileNotFoundError(f"Expected {ACCESS} and {AUTH}. Run generate_logs.py first.")
        if STATE["T"] is not None and not force:
            if sig == STATE["sig"] or time.time() - STATE["loaded_at"] < MIN_RELOAD:
                return False
        STATE["loading"] = True
        try:
            load_all(); STATE["sig"] = sig
        finally:
            STATE["loading"] = False
        return True


# ------------------------------------------------------------------ analysis (all Spark)
def analyse(range_key, types, bucket_s, z_thr, bf_thr):
    key = (STATE["version"], range_key, tuple(sorted(types or [])), bucket_s, z_thr, bf_thr)
    if key in RESULTS:
        return RESULTS[key]
    T, A = STATE["T"], STATE["A"]
    hrs = RANGES[range_key]
    if hrs:
        start = STATE["maxepoch"] - hrs * 3600
        T, A = T.filter(F.col("epoch") >= start), A.filter(F.col("epoch") >= start)
    if types:
        T = T.filter(F.col("ua_type").isin(list(types)))
    bk = lambda: (F.floor(F.col("epoch") / bucket_s) * bucket_s).alias("bucket")
    fail, succ = (F.col("result") == "failure"), (F.col("result") == "success")
    is5, is4 = (F.col("status") >= 500), F.col("status").between(400, 499)
    geo = F.broadcast(spark().createDataFrame(geo_rows(), "octet int, country string, iso3 string"))
    Q = {}

    # --- KPIs
    Q["kpi_web"] = T.agg(F.count("*").alias("req"), F.approx_count_distinct("ip").alias("visitors"),
                         F.sum("bytes").alias("bytes"), F.avg(is5.cast("int")).alias("err5"),
                         F.avg(is4.cast("int")).alias("err4"), F.percentile_approx("rt", 0.95).alias("p95"))
    Q["kpi_auth"] = A.agg(F.count("*").alias("n"), F.avg(fail.cast("int")).alias("fail_rate"),
                          F.approx_count_distinct("user").alias("users"))

    # --- traffic over time + anomaly detection (rolling z-score on previous 24 buckets)
    Q["ts"] = T.groupBy(bk(), "status_class").count()
    base = T.groupBy(bk()).agg(F.count("*").alias("n"))
    w = Window.orderBy("bucket").rowsBetween(-24, -1)
    Q["anom"] = (base.withColumn("mean", F.avg("n").over(w)).withColumn("sd", F.stddev("n").over(w))
                 .withColumn("z", F.when(F.col("sd") > 0, (F.col("n") - F.col("mean")) / F.col("sd")))
                 .withColumn("flag", (F.abs("z") >= z_thr) & (F.abs(F.col("n") - F.col("mean")) / F.col("mean") >= 0.5))
                 .orderBy("bucket"))
    Q["perf"] = T.groupBy(bk()).agg((F.avg(is5.cast("int")) * 100).alias("err5"), F.percentile_approx("rt", 0.95).alias("p95"))

    # --- content / performance
    Q["status"] = T.groupBy("status").count().orderBy("status")
    Q["ua"] = T.groupBy("ua_type", "device").count()
    Q["paths"] = (T.groupBy("path_clean").agg(F.count("*").alias("requests"), F.avg(is5.cast("int") * 100).alias("err5_pct"),
                                              F.sum("bytes").alias("bytes")).orderBy(F.desc("requests")).limit(15))
    Q["rt"] = (T.groupBy("path_clean").agg(F.count("*").alias("n"), F.percentile_approx("rt", 0.5).alias("p50"),
                                           F.percentile_approx("rt", 0.95).alias("p95"), F.percentile_approx("rt", 0.99).alias("p99"))
               .orderBy(F.desc("n")).limit(10))
    Q["heat"] = T.groupBy(F.dayofweek("ts").alias("dow"), F.hour("ts").alias("hr")).count()
    Q["geo"] = T.groupBy("country", "iso3").agg(F.count("*").alias("requests"), F.approx_count_distinct("ip").alias("visitors"),
                                                (F.avg(is4.cast("int")) * 100).alias("err4_pct"))

    # --- login analytics
    Q["login_ts"] = A.groupBy(bk(), "result").count()
    Q["method"] = A.groupBy("method", "result").count()
    Q["targeted"] = A.filter(fail).groupBy("user").count().orderBy(F.desc("count")).limit(10)
    Q["fail_country"] = A.filter(fail).groupBy("country").count().orderBy(F.desc("count"))

    pair = A.groupBy("ip", "user").agg(F.sum(fail.cast("int")).alias("fails"), F.sum(succ.cast("int")).alias("succ"),
                                       F.min(F.when(fail, F.col("epoch"))).alias("first_fail"),
                                       F.max(F.when(succ, F.col("epoch"))).alias("last_succ"))
    auth_ip = pair.groupBy("ip").agg(F.sum("fails").alias("fails"), F.sum("succ").alias("succ"),
                                     F.count("*").alias("users_tried"), F.max("fails").alias("max_per_user"),
                                     F.sum((F.col("fails") > 0).cast("int")).alias("users_failed"))
    web_ip = T.groupBy("ip").agg(F.count("*").alias("req"), F.sum(is4.cast("int")).alias("errs4"),
                                 F.sum("sens").alias("sens"))
    lb = web_ip.join(auth_ip, "ip", "full_outer").fillna(0)
    bf = F.col("max_per_user") >= bf_thr
    stuff = (F.col("users_failed") >= 15) & (F.col("fails") >= 15)
    scan = (F.col("sens") >= 3) | ((F.col("req") >= 30) & (F.col("errs4") / F.col("req") > 0.6))
    comp = (F.col("succ") > 0) & (bf | stuff)
    scr = (F.col("req") >= 2000)
    lb = (lb.withColumn("score", bf.cast("int") * 40 + stuff.cast("int") * 35 + scan.cast("int") * 30
                        + comp.cast("int") * 25 + scr.cast("int") * 10)
          .withColumn("reasons", F.concat_ws(", ", F.when(bf, "Brute force"), F.when(stuff, "Credential stuffing"),
                                              F.when(scan, "Scanning/probing"), F.when(comp, "Login success after attack"),
                                              F.when(scr, "High-volume client")))
          .filter("score > 0").withColumn("octet", octet("ip")).join(geo, "octet", "left")
          .select("ip", F.coalesce("country", F.lit("?")).alias("country"), "score", "reasons", "req", "sens",
                  "fails", "users_failed", "max_per_user", F.col("succ").alias("logins_ok"))
          .orderBy(F.desc("score"), F.desc("fails"), F.desc("req")).limit(20))
    Q["threat"] = lb
    Q["compromised"] = (pair.filter((F.col("fails") >= 5) & (F.col("fails") / (F.col("fails") + F.col("succ")) >= 0.8)
                                    & (F.col("last_succ") > F.col("first_fail")))
                        .select("user", "ip", "fails", F.from_unixtime("last_succ").alias("success_at"))
                        .orderBy(F.desc("fails")).limit(15))
    ws = Window.partitionBy("user").orderBy("epoch")
    S = (A.filter(succ).withColumn("pc", F.lag("country").over(ws)).withColumn("pt", F.lag("epoch").over(ws))
         .withColumn("pip", F.lag("ip").over(ws)))
    Q["travel"] = (S.filter((F.col("country") != F.col("pc")) & ((F.col("epoch") - F.col("pt")) <= 3600))
                   .select("user", F.col("pc").alias("from_country"), F.col("pip").alias("from_ip"),
                           F.col("country").alias("to_country"), F.col("ip").alias("to_ip"),
                           ((F.col("epoch") - F.col("pt")) / 60).cast("int").alias("gap_min"),
                           F.from_unixtime("epoch").alias("at")).orderBy("at"))

    sp = spark()
    def run(item):
        name, df = item
        sp.sparkContext.setLocalProperty("spark.scheduler.pool", None)
        return name, df.toPandas()
    with ThreadPoolExecutor(max_workers=4) as ex:
        out = dict(ex.map(run, Q.items()))
    RESULTS[key] = out
    return out


# ------------------------------------------------------------------ figures
BG = "#161b22"
def style(fig, h=340, legend=True):
    fig.update_layout(template="plotly_dark", paper_bgcolor=BG, plot_bgcolor=BG, height=h,
                      margin=dict(l=45, r=20, t=45, b=40), showlegend=legend,
                      legend=dict(orientation="h", y=-0.25), title_font_size=15)
    return fig


def empty(msg="No data for this selection"):
    f = go.Figure(); f.add_annotation(text=msg, showarrow=False, font=dict(size=14, color="#8b949e"))
    f.update_xaxes(visible=False); f.update_yaxes(visible=False)
    return style(f, 300, False)


def tdt(df, col="bucket"):
    df = df.copy(); df[col] = pd.to_datetime(df[col], unit="s"); return df


def table(df, h=360):
    if df.empty: return html.Div("Nothing flagged in this selection.", className="muted")
    return dash_table.DataTable(
        data=df.to_dict("records"), columns=[{"name": c, "id": c} for c in df.columns], page_size=12,
        style_table={"overflowX": "auto"}, style_header={"backgroundColor": "#21262d", "color": "#e6edf3", "fontWeight": "600"},
        style_cell={"backgroundColor": BG, "color": "#c9d1d9", "border": "1px solid #30363d", "fontSize": 12,
                    "padding": "6px", "textAlign": "left", "maxWidth": 280, "overflow": "hidden", "textOverflow": "ellipsis"})


def human_bytes(n):
    for u in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024: return f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} PB"


def build(d, bucket_name):
    r = {}
    kw, ka = d["kpi_web"].iloc[0], d["kpi_auth"].iloc[0]
    req = int(kw.req or 0)
    threat = d["threat"]
    r["k-req"] = f"{req:,}"; r["k-vis"] = f"{int(kw.visitors or 0):,}"; r["k-bytes"] = human_bytes(float(kw.bytes or 0))
    r["k-err"] = f"{(kw.err5 or 0) * 100:.2f}%"; r["k-p95"] = f"{(kw.p95 or 0) * 1000:.0f} ms"
    r["k-logins"] = f"{int(ka.n or 0):,}"; r["k-fail"] = f"{(ka.fail_rate or 0) * 100:.1f}%"; r["k-flag"] = str(len(threat))

    ts = tdt(d["ts"])
    if ts.empty:
        for k in ["g-traffic", "g-anom", "g-perf", "g-status", "g-ua", "g-paths", "g-rt", "g-heat", "g-geo", "g-geo-bar",
                  "g-login-ts", "g-method", "g-targeted", "g-failc"]:
            r[k] = empty()
    else:
        cmap = {"2xx": "#3fb950", "3xx": "#58a6ff", "4xx": "#d29922", "5xx": "#f85149"}
        f = px.bar(ts.sort_values("bucket"), x="bucket", y="count", color="status_class", color_discrete_map=cmap,
                   title=f"Requests per {bucket_name} by status class", labels={"bucket": "", "count": "requests"})
        r["g-traffic"] = style(f)

        a = tdt(d["anom"]); fl = a[a["flag"] == True]
        f = go.Figure()
        f.add_trace(go.Scatter(x=a.bucket, y=a.n, mode="lines", name="requests", line=dict(color="#58a6ff")))
        f.add_trace(go.Scatter(x=a.bucket, y=a["mean"], mode="lines", name="rolling mean", line=dict(color="#8b949e", dash="dot")))
        f.add_trace(go.Scatter(x=fl.bucket, y=fl.n, mode="markers", name="anomaly",
                               marker=dict(color="#f85149", size=11, symbol="x"),
                               text=[f"z={z:.1f}" for z in fl.z], hovertemplate="%{x}<br>%{y} req<br>%{text}"))
        r["g-anom"] = style(f.update_layout(title=f"Traffic anomalies ({len(fl)} flagged, rolling z-score)"))

        p = tdt(d["perf"]).sort_values("bucket")
        f = make_subplots(specs=[[{"secondary_y": True}]])
        f.add_trace(go.Scatter(x=p.bucket, y=p.err5, name="5xx rate %", line=dict(color="#f85149")), secondary_y=False)
        f.add_trace(go.Scatter(x=p.bucket, y=p.p95 * 1000, name="p95 latency ms", line=dict(color="#d29922")), secondary_y=True)
        r["g-perf"] = style(f.update_layout(title="Error rate and p95 latency (outage detector)"))

        st = d["status"].copy(); st["status"] = st["status"].astype(str)
        r["g-status"] = style(px.bar(st, x="status", y="count", title="Status codes", log_y=True), 300, False)
        r["g-ua"] = style(px.sunburst(d["ua"], path=["ua_type", "device"], values="count", title="Traffic type (human / bot / scanner)"), 300, False)

        pa = d["paths"].sort_values("requests")
        f = px.bar(pa, x="requests", y="path_clean", orientation="h", color="err5_pct", color_continuous_scale="OrRd",
                   title="Top endpoints (colour = 5xx %)", labels={"path_clean": "", "err5_pct": "5xx %"})
        r["g-paths"] = style(f, 420, False)
        rt = d["rt"].melt(id_vars=["path_clean", "n"], value_vars=["p50", "p95", "p99"], var_name="pct", value_name="sec")
        r["g-rt"] = style(px.bar(rt, x="path_clean", y="sec", color="pct", barmode="group",
                                 title="Latency percentiles (s), busiest endpoints", labels={"path_clean": ""}), 420)
        h = d["heat"].copy(); h["day"] = h["dow"].map(lambda x: DAYS[(x - 2) % 7])
        piv = h.pivot_table(index="day", columns="hr", values="count", aggfunc="sum").reindex(DAYS).fillna(0)
        r["g-heat"] = style(px.imshow(piv, aspect="auto", color_continuous_scale="Viridis", title="Requests by weekday and hour (UTC)",
                                      labels=dict(x="hour", y="", color="req")), 340, False)
        g = d["geo"]
        r["g-geo"] = style(px.choropleth(g, locations="iso3", color="requests", hover_name="country", color_continuous_scale="Viridis",
                                         title="Requests by country").update_geos(bgcolor=BG, showframe=False, landcolor="#21262d",
                                                                                   showcountries=True, countrycolor="#30363d"), 420, False)
        r["g-geo-bar"] = style(px.bar(g.sort_values("requests"), x="requests", y="country", orientation="h", color="err4_pct",
                                      color_continuous_scale="YlOrRd", title="Country ranking (colour = 4xx %)"), 420, False)

    lt = tdt(d["login_ts"])
    if lt.empty:
        for k in ["g-login-ts", "g-method", "g-targeted", "g-failc"]: r[k] = empty()
    else:
        r["g-login-ts"] = style(px.bar(lt.sort_values("bucket"), x="bucket", y="count", color="result",
                                       color_discrete_map={"success": "#3fb950", "failure": "#f85149"},
                                       title=f"Login attempts per {bucket_name}", labels={"bucket": ""}))
        r["g-method"] = style(px.bar(d["method"], x="method", y="count", color="result", barmode="group",
                                     color_discrete_map={"success": "#3fb950", "failure": "#f85149"}, title="Auth method outcome"), 320)
        r["g-targeted"] = style(px.bar(d["targeted"].sort_values("count"), x="count", y="user", orientation="h",
                                       title="Most-targeted accounts (failed logins)", labels={"user": ""}), 320, False)
        r["g-failc"] = style(px.bar(d["fail_country"], x="country", y="count", title="Failed logins by source country"), 320, False)
    r["t-threat"] = table(threat); r["t-comp"] = table(d["compromised"]); r["t-travel"] = table(d["travel"])
    return r


# ------------------------------------------------------------------ layout
def card(label, i): return html.Div([html.Div(label, className="kt"), html.Div("-", id=i, className="kv")], className="card")
def graph(i, w=6): return html.Div(dcc.Graph(id=i, config={"displaylogo": False}), className=f"panel w{w}")

FIG_IDS = ["g-traffic", "g-anom", "g-perf", "g-status", "g-ua", "g-paths", "g-rt", "g-heat", "g-geo", "g-geo-bar",
           "g-login-ts", "g-method", "g-targeted", "g-failc"]
KPI_IDS = ["k-req", "k-vis", "k-bytes", "k-err", "k-p95", "k-logins", "k-fail", "k-flag"]
TAB_IDS = ["t-threat", "t-comp", "t-travel"]
OUT_IDS = KPI_IDS + FIG_IDS + TAB_IDS + ["data-info", "status-line"]

app = Dash(__name__, title="Traffic & Login Analytics", suppress_callback_exceptions=True)
server = app.server
app.index_string = """<!DOCTYPE html><html><head>{%metas%}<title>{%title%}</title>{%favicon%}{%css%}
<style>
body{background:#0d1117;color:#e6edf3;font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:0}
.wrap{max-width:1500px;margin:0 auto;padding:16px}
h1{font-size:22px;margin:0 0 4px} .muted{color:#8b949e;font-size:13px}
.controls{display:flex;flex-wrap:wrap;gap:16px;background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px;margin:12px 0;align-items:flex-end}
.controls label{display:block;font-size:12px;color:#8b949e;margin-bottom:4px}.controls .c{min-width:170px;color:#111}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:12px}
.card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:12px}
.kt{font-size:12px;color:#8b949e}.kv{font-size:24px;font-weight:600;margin-top:4px}
.row{display:flex;flex-wrap:wrap;gap:12px;margin-bottom:12px}
.panel{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:8px;box-sizing:border-box}
.w6{flex:1 1 560px}.w12{flex:1 1 100%}
button{background:#238636;color:#fff;border:0;border-radius:6px;padding:8px 14px;cursor:pointer}
.tab{background:#161b22!important;color:#8b949e!important;border-color:#30363d!important}
.tab--selected{background:#0d1117!important;color:#fff!important;border-top:2px solid #58a6ff!important}
h3{font-size:15px;margin:6px 4px}
.hidden{height:0;overflow:hidden;visibility:hidden;margin:0;padding:0}
@media(max-width:700px){.w6{flex:1 1 100%}}
</style></head><body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body></html>"""

def ctl(label, comp): return html.Div([html.Label(label), comp], className="c")

app.layout = html.Div(className="wrap", children=[
    html.H1("Traffic & Login Analytics"),
    html.Div("Starting Spark and loading logs... the first load can take a minute or two.", className="muted", id="status-line"),
    dcc.Store(id="data-ver"),
    html.Div(className="controls", children=[
        ctl("Time range (relative to newest log)", dcc.Dropdown(id="range", clearable=False, value="all",
            options=[{"label": l, "value": v} for l, v in [("All data", "all"), ("Last 7 days", "7d"), ("Last 3 days", "3d"),
                                                            ("Last 24 hours", "24h"), ("Last 6 hours", "6h")]])),
        ctl("Bucket size", dcc.Dropdown(id="bucket", clearable=False, value="15 min", options=list(BUCKETS))),
        ctl("Traffic type", dcc.Dropdown(id="types", multi=True, value=["human", "bot", "scanner"], options=["human", "bot", "scanner"])),
        ctl("Anomaly z-score threshold", dcc.Slider(id="z", min=2, max=8, step=0.5, value=4, marks={2: "2", 4: "4", 6: "6", 8: "8"})),
        ctl("Brute-force: fails on one account", dcc.Input(id="bf", type="number", min=3, max=500, value=20, style={"width": "100%"})),
        html.Div([dcc.Checklist(id="live", options=[{"label": " Live refresh (5s)", "value": "live"}], value=["live"]),
                  html.Button("Reload data now", id="reload", n_clicks=0)]),
    ]),
    dcc.Interval(id="tick", interval=int(os.environ.get("POLL_SECS", "5")) * 1000),
    dcc.Loading(type="dot", color="#58a6ff", children=html.Div(className="cards", children=[
        card("Requests", "k-req"), card("Unique visitors", "k-vis"), card("Bandwidth", "k-bytes"), card("5xx rate", "k-err"),
        card("p95 latency", "k-p95"), card("Login attempts", "k-logins"), card("Login failure rate", "k-fail"),
        card("Flagged IPs", "k-flag")])),
    dcc.Tabs(id="tabs", value="overview", children=[
        dcc.Tab(label=l, value=v, className="tab", selected_className="tab--selected")
        for l, v in [("Overview", "overview"), ("Content & performance", "perf"), ("Geography", "geo"),
                     ("Security & logins", "sec"), ("Pipeline", "data")]]),
    # All panes always exist in the page (so the one big callback can fill every chart);
    # the tab bar only toggles which one is visible.
    html.Div(id="pane-overview", children=html.Div(className="row", children=[
        graph("g-traffic", 12), graph("g-anom"), graph("g-perf"), graph("g-status"), graph("g-ua")])),
    html.Div(id="pane-perf", children=html.Div(className="row", children=[graph("g-paths"), graph("g-rt"), graph("g-heat", 12)])),
    html.Div(id="pane-geo", children=[
        html.Div(className="row", children=[graph("g-geo"), graph("g-geo-bar")]),
        html.Div("Country comes from a fake first-octet lookup (common.py). The map needs the browser to reach cdn.plot.ly.", className="muted")]),
    html.Div(id="pane-sec", children=[
        html.Div(className="row", children=[graph("g-login-ts", 12), graph("g-method"), graph("g-targeted"), graph("g-failc")]),
        html.H3("Threat leaderboard (web + auth signals combined)"), html.Div(id="t-threat", className="panel"),
        html.H3("Possibly compromised accounts (>=5 fails, >=80% failure ratio, then a success, same IP + user)"),
        html.Div(id="t-comp", className="panel"),
        html.H3("Impossible travel (successful logins from 2 countries within 60 min)"), html.Div(id="t-travel", className="panel")]),
    html.Div(id="pane-data", children=html.Div(id="data-info", className="panel", style={"marginTop": "12px"})),
])

PANES = {"overview": "pane-overview", "perf": "pane-perf", "geo": "pane-geo", "sec": "pane-sec", "data": "pane-data"}


@app.callback([Output(p, "className") for p in PANES.values()], Input("tabs", "value"))
def show_pane(tab):
    # hidden panes keep their width (height 0) so Plotly sizes charts correctly when shown
    return ["" if k == tab else "hidden" for k in PANES]


@app.callback(Output("tick", "disabled"), Input("live", "value"))
def toggle_live(v): return "live" not in (v or [])


@app.callback(Output("data-ver", "data"), Input("tick", "n_intervals"))
def poll_files(_n):
    """Cheap timer callback: only compares file signatures, never touches Spark.
    Keeping it separate means the timer can't cancel/supersede the slow data callback."""
    if STATE.get("loading") or STATE.get("sig") is None:
        raise PreventUpdate
    if file_sig() != STATE["sig"] and time.time() - STATE["loaded_at"] >= MIN_RELOAD:
        return time.time()
    raise PreventUpdate


@app.callback([Output(i, "children" if i[0] in "ktds" else "figure") for i in OUT_IDS],
              Input("data-ver", "data"), Input("reload", "n_clicks"), Input("range", "value"), Input("bucket", "value"),
              Input("types", "value"), Input("z", "value"), Input("bf", "value"))
def refresh(_n, _c, rng, bucket, types, z, bf):
    try:
        return _refresh(_n, _c, rng, bucket, types, z, bf)
    except PreventUpdate:
        raise
    except Exception as e:  # show the problem on the page instead of a silent 500
        import traceback; traceback.print_exc()
        msg = str(e).strip().splitlines()[0][:300] if str(e).strip() else repr(e)
        out = [no_update] * len(OUT_IDS)
        out[OUT_IDS.index("status-line")] = html.Div(f"ERROR loading data: {type(e).__name__}: {msg}", style={"color": "#f85149"})
        return out


def _refresh(_ver, _c, rng, bucket, types, z, bf):
    # Heavy callback: runs on filter changes, the reload button, or when poll_files() reports changed files.
    ensure_loaded(force=(ctx.triggered_id in ("reload", "data-ver")))
    d = analyse(rng, types or ["human", "bot", "scanner"], BUCKETS[bucket], float(z or 4), int(bf or 20))
    r = build(d, bucket)
    s = STATE["stats"]
    r["status-line"] = (f"Data loaded {time.strftime('%H:%M:%S', time.localtime(STATE['loaded_at']))} "
                        f"in {s['load_secs']}s · {s['web_valid']:,} web rows · {s['auth_valid']:,} auth events · "
                        f"newest log {pd.to_datetime(STATE['maxepoch'], unit='s'):%Y-%m-%d %H:%M} UTC")
    info = pd.DataFrame([
        ("Spark version / cores", f"{s['spark']} / {s['cores']}"), ("Access log", ACCESS),
        ("Access lines total / parsed / rejected", f"{s['web_total']:,} / {s['web_valid']:,} / {s['web_bad']:,}"),
        ("Auth log", AUTH), ("Auth events total / parsed / rejected", f"{s['auth_total']:,} / {s['auth_valid']:,} / {s['auth_bad']:,}"),
        ("Last load duration", f"{s['load_secs']} s"), ("Parsed schema", ", ".join(f"{a}:{b}" for a, b in s["dtypes"])),
    ], columns=["Item", "Value"])
    r["data-info"] = table(info)
    return [r[i] for i in OUT_IDS]


if __name__ == "__main__":
    app.run(host=os.environ.get("HOST", "0.0.0.0"), port=int(os.environ.get("PORT", "8050")), debug=False, threaded=True)
