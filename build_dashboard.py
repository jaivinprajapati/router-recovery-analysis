"""
Router Recovery Live Dashboard — data refresh + HTML build.

Runs 4 queries against Metabase and bakes results into dashboard.html.
Intended to be run by GitHub Actions on a daily schedule.

Metrics:
  1. MoM recovery rate (ticket creation month, SCORE=1)
  2. Week-on-week: 7-day rolling avg daily recovery rate (resolution date)
  3. Day-on-day daily recovery rate (resolution date)
  4. Open ticket age distribution (TASKS with no TP resolved_time)
"""
import os, json, sys, datetime as dt, requests

sys.stdout.reconfigure(encoding="utf-8")

API_KEY = os.environ.get("METABASE_API_KEY")
if not API_KEY:
    # local run fallback
    key_path = os.path.expanduser("~/.claude/metabase_key.txt")
    API_KEY = open(key_path).read().strip()

URL = "https://metabase.wiom.in/api/dataset"
HEADERS = {"Content-Type": "application/json", "x-api-key": API_KEY}

def run(sql):
    r = requests.post(URL, headers=HEADERS,
        json={"database": 113, "type": "native", "native": {"query": sql}},
        timeout=180)
    d = r.json()
    if "error" in d:
        raise RuntimeError(d["error"])
    cols = [c["name"] for c in d["data"]["cols"]]
    return [dict(zip(cols, row)) for row in d["data"]["rows"]]

# NOTE: Logic aligned with Metabase dashboard #1067 (card 9543, cte1).
# Success = TASKS.STATUS = 2, Failed = TASKS.STATUS = 3, Open = STATUS IN (0,1).
# Base universe deduped by ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CREATED DESC) = 1.

TASKS_CTE = """
WITH cte1 AS (
    SELECT
        ID, CREATED, RSLVD_DATETIME, STATUS,
        ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CREATED DESC) AS rn
    FROM PROD_DB.DYNAMODB_READ.TASKS
    WHERE TYPE = 'ROUTER_PICKUP'
)
"""

# ---------- Q1: MoM creation-basis recovery ----------
# 21-day rule: only include tickets whose recovery window has fully expired
# (CREATED at least 21 days ago). Avoids skew from fresh tickets still in progress.
Q1 = TASKS_CTE + """
SELECT
    TO_CHAR(DATE_TRUNC('month', DATEADD(minute,330,CREATED)),'YYYY-MM') AS month,
    COUNT(*) AS total_created,
    SUM(CASE WHEN STATUS = 2 THEN 1 ELSE 0 END) AS recovered,
    SUM(CASE WHEN STATUS = 3 THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN STATUS IN (0,1) THEN 1 ELSE 0 END) AS open_tickets
FROM cte1
WHERE rn = 1
  AND TO_DATE(DATEADD(minute,330,CREATED)) BETWEEN '2025-11-01' AND DATEADD(day, -21, CURRENT_DATE)
GROUP BY 1
ORDER BY 1
LIMIT 10000
"""

# ---------- Q2: Daily recovery (resolution date from TP, success flag from TASKS) ----------
Q2 = TASKS_CTE + """
SELECT
    TO_CHAR(TO_DATE(DATEADD(minute,330,tp.TASK_RESOLVED_TIME)),'YYYY-MM-DD') AS resolved_date,
    COUNT(*) AS total_resolved,
    SUM(CASE WHEN c.STATUS = 2 THEN 1 ELSE 0 END) AS recovered,
    SUM(CASE WHEN c.STATUS = 3 THEN 1 ELSE 0 END) AS failed
FROM PROD_DB.DYNAMODB.TASK_PERFORMANCE tp
LEFT JOIN cte1 c ON tp.REPORTERID = c.ID AND c.rn = 1
WHERE tp.TASK_TYPE = 'ROUTER_PICKUP'
  AND tp._FIVETRAN_DELETED = false
  AND tp.TASK_RESOLVED_TIME IS NOT NULL
  AND TO_DATE(DATEADD(minute,330,tp.TASK_RESOLVED_TIME)) BETWEEN '2025-11-01' AND CURRENT_DATE
GROUP BY 1
ORDER BY 1
LIMIT 10000
"""

# ---------- Q3: Open ticket age distribution (STATUS IN 0,1) ----------
Q3 = TASKS_CTE + """
SELECT
    CASE
        WHEN DATEDIFF(day, TO_DATE(DATEADD(minute,330,CREATED)), CURRENT_DATE) BETWEEN 0  AND 7  THEN '0-7 days'
        WHEN DATEDIFF(day, TO_DATE(DATEADD(minute,330,CREATED)), CURRENT_DATE) BETWEEN 8  AND 14 THEN '8-14 days'
        WHEN DATEDIFF(day, TO_DATE(DATEADD(minute,330,CREATED)), CURRENT_DATE) BETWEEN 15 AND 21 THEN '15-21 days'
        ELSE '21+ days'
    END AS age_bucket,
    COUNT(*) AS open_tickets
FROM cte1
WHERE rn = 1
  AND STATUS IN (0, 1)
  AND TO_DATE(DATEADD(minute,330,CREATED)) >= '2025-11-01'
GROUP BY 1
ORDER BY
    CASE age_bucket
        WHEN '0-7 days' THEN 1
        WHEN '8-14 days' THEN 2
        WHEN '15-21 days' THEN 3
        ELSE 4
    END
LIMIT 10000
"""

print("Running Q1 (MoM)...")
mom = run(Q1)
print(f"  {len(mom)} months")

print("Running Q2 (daily)...")
daily = run(Q2)
print(f"  {len(daily)} days")

print("Running Q3 (open ages)...")
open_ages = run(Q3)
print(f"  {len(open_ages)} buckets")

# ---------- Post-process ----------
# MoM: compute rate on CLOSED tickets only (recovered + failed)
for m in mom:
    closed = (m["RECOVERED"] or 0) + (m["FAILED"] or 0)
    m["closed"] = closed
    m["recovery_pct"] = round((m["RECOVERED"] or 0) * 100.0 / closed, 1) if closed else None

# Daily: recovery % + 7-day rolling
daily_sorted = sorted(daily, key=lambda x: x["RESOLVED_DATE"])
for i, d in enumerate(daily_sorted):
    d["recovery_pct"] = round((d["RECOVERED"] or 0) * 100.0 / d["TOTAL_RESOLVED"], 1) if d["TOTAL_RESOLVED"] else None
    window = daily_sorted[max(0, i-6): i+1]
    w_rec = sum(x["RECOVERED"] or 0 for x in window)
    w_tot = sum(x["TOTAL_RESOLVED"] or 0 for x in window)
    d["rolling_7d_pct"] = round(w_rec * 100.0 / w_tot, 1) if w_tot else None

last_refresh = dt.datetime.utcnow() + dt.timedelta(hours=5, minutes=30)
payload = {
    "mom": mom,
    "daily": daily_sorted,
    "open_ages": open_ages,
    "last_refresh_ist": last_refresh.strftime("%Y-%m-%d %H:%M IST"),
}

# ---------- Build HTML ----------
HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Router Recovery — Live Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0/dist/chartjs-plugin-datalabels.min.js"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', Arial, sans-serif; background: #f0f2f5; color: #1a1a2e; line-height: 1.5; }
  .header { background: linear-gradient(135deg, #1F4E79, #2E75B6); color: white; padding: 28px 48px; }
  .header h1 { font-size: 24px; font-weight: 700; }
  .header p  { font-size: 12px; opacity: 0.85; margin-top: 4px; }
  .container { max-width: 1200px; margin: 0 auto; padding: 28px 24px; }
  .card { background: white; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.06); padding: 22px 26px; margin-bottom: 22px; }
  .card-head { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; }
  .card h2 { font-size: 15px; font-weight: 700; color: #1F4E79; }
  .card .sub { font-size: 12px; color: #777; margin-bottom: 14px; }
  .src-wrap { position: relative; display: inline-block; }
  .src-icon { display: inline-flex; align-items: center; justify-content: center; width: 18px; height: 18px; border-radius: 50%; background: #2E75B6; color: white; font-size: 11px; font-weight: 700; cursor: help; user-select: none; }
  .src-tip { display: none; position: absolute; top: 24px; left: 0; z-index: 100; min-width: 460px; font-size: 11px; color: #333; background: white; border: 1px solid #BDD7EE; box-shadow: 0 4px 14px rgba(0,0,0,0.12); padding: 10px 14px; border-radius: 5px; font-family: 'Consolas','Courier New',monospace; line-height: 1.55; }
  .src-tip b { color: #1F4E79; font-family: 'Segoe UI', Arial, sans-serif; }
  .src-wrap:hover .src-tip { display: block; }
  .chart-wrap { position: relative; height: 340px; }
  .chart-wrap.tall { height: 380px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 10px; }
  th { background: #1F4E79; color: white; padding: 9px 12px; text-align: center; font-weight: 600; }
  td { padding: 8px 12px; text-align: center; border-bottom: 1px solid #f0f0f0; }
  td.label { text-align: left; font-weight: 500; }
  .two-col { display: grid; grid-template-columns: 1.4fr 1fr; gap: 22px; }
  @media (max-width: 900px) { .two-col { grid-template-columns: 1fr; } }
  .stamp { font-size: 11px; color: #888; text-align: right; padding: 6px 12px 0; }
</style>
</head>
<body>
<div class="header">
  <h1>Router Recovery — Live Dashboard</h1>
  <p>Wiom Analytics &nbsp;|&nbsp; Auto-refreshed daily &nbsp;|&nbsp; Logic aligned with Metabase dashboard #1067 (PICKUP Tickets)</p>
</div>
<div class="container">

  <div class="card">
    <div class="card-head">
      <h2>1. Monthly Success Rate — creation month basis</h2>
      <span class="src-wrap"><span class="src-icon">i</span>
        <div class="src-tip">
          <b>Table:</b> PROD_DB.DYNAMODB_READ.TASKS (deduped by ID, latest row)<br>
          <b>Filter:</b> TYPE = 'ROUTER_PICKUP'<br>
          <b>21-day rule:</b> CREATED (IST) between 2025-11-01 and (today − 21 days). Only tickets whose recovery window has fully expired are included.<br>
          <b>Month:</b> DATE_TRUNC('month', CREATED + 330 min IST)<br>
          <b>Successfully Closed:</b> STATUS = 2<br>
          <b>Unsuccessfully Closed:</b> STATUS = 3<br>
          <b>Open:</b> STATUS IN (0, 1)<br>
          <b>Rate:</b> Success ÷ Total Closed
        </div>
      </span>
    </div>
    <div class="sub">Only tickets created ≥21 days ago (full recovery window expired). Prevents skew from fresh tickets still in progress.</div>
    <div class="chart-wrap"><canvas id="momChart"></canvas></div>
    <table id="momTable"></table>
  </div>

  <div class="card">
    <div class="card-head">
      <h2>2. Week-on-Week — 7-day rolling Success / Total Closed</h2>
      <span class="src-wrap"><span class="src-icon">i</span>
        <div class="src-tip">
          <b>Tables:</b> PROD_DB.DYNAMODB.TASK_PERFORMANCE (tp) LEFT JOIN PROD_DB.DYNAMODB_READ.TASKS (deduped) ON tp.REPORTERID = TASKS.ID<br>
          <b>Filter:</b> tp.TASK_TYPE = 'ROUTER_PICKUP' &nbsp;|&nbsp; tp._FIVETRAN_DELETED = false &nbsp;|&nbsp; tp.TASK_RESOLVED_TIME NOT NULL<br>
          <b>Date:</b> tp.TASK_RESOLVED_TIME (+ 330 min IST), range 2025-11-01 → today<br>
          <b>Successfully Closed:</b> TASKS.STATUS = 2<br>
          <b>Total Closed:</b> all tp rows resolved that day<br>
          <b>Rolling:</b> 7-day trailing sum(Success) ÷ sum(Total Closed)
        </div>
      </span>
    </div>
    <div class="sub">Smoothed daily rate. Resolution-date basis — no cohort lag, shows operational performance in near-real-time.</div>
    <div class="chart-wrap tall"><canvas id="rollingChart"></canvas></div>
  </div>

  <div class="card">
    <div class="card-head">
      <h2>3. Day-on-Day — Success / Total Closed</h2>
      <span class="src-wrap"><span class="src-icon">i</span>
        <div class="src-tip">
          <b>Tables / filter / date:</b> same as View 2 (TASK_PERFORMANCE ⟕ TASKS, resolution date)<br>
          <b>Rate:</b> (rows where TASKS.STATUS = 2) ÷ (all rows resolved that day) — no smoothing
        </div>
      </span>
    </div>
    <div class="sub">Of all tickets closed that day, what % were successfully closed. Noisy but earliest signal.</div>
    <div class="chart-wrap tall"><canvas id="dailyChart"></canvas></div>
  </div>

  <div class="two-col">
    <div class="card">
      <div class="card-head">
        <h2>4. Open Ticket Age Distribution</h2>
        <span class="src-wrap"><span class="src-icon">i</span>
          <div class="src-tip">
            <b>Table:</b> PROD_DB.DYNAMODB_READ.TASKS (deduped by ID, latest row)<br>
            <b>Filter:</b> TYPE = 'ROUTER_PICKUP' &nbsp;|&nbsp; STATUS IN (0, 1) &nbsp;|&nbsp; CREATED (IST) ≥ 2025-11-01<br>
            <b>Age:</b> DATEDIFF(day, CREATED + 330 min IST, CURRENT_DATE)<br>
            <b>Buckets:</b> 0-7, 8-14, 15-21, 21+ days
          </div>
        </span>
      </div>
      <div class="sub">Tickets still open. 21+ day bucket = SLA breach (partners have 21 days).</div>
      <div class="chart-wrap"><canvas id="openChart"></canvas></div>
    </div>
    <div class="card">
      <div class="card-head">
        <h2>Open Tickets — Table</h2>
      </div>
      <div class="sub">Count by age bucket.</div>
      <table id="openTable"></table>
    </div>
  </div>

  <div class="stamp">Last refreshed: __LAST_REFRESH__</div>
</div>

<script>
const DATA = __PAYLOAD__;
// Datalabels plugin loaded but disabled by default; enabled per-chart where needed.
Chart.register(ChartDataLabels);
Chart.defaults.set('plugins.datalabels', { display: false });

// ---------- 1. MoM ----------
(function() {
  const m = DATA.mom;
  const labels = m.map(r => r.MONTH);
  const pct = m.map(r => r.recovery_pct);
  new Chart(document.getElementById('momChart'), {
    type: 'bar',
    data: { labels, datasets: [{
      label: 'Success / Total Closed', data: pct,
      backgroundColor: '#2E75B6', borderRadius: 4
    }]},
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false },
        tooltip: { callbacks: { label: c => c.parsed.y + '%' } } },
      scales: { y: { beginAtZero: true, max: 100, ticks: { callback: v => v+'%' } } }
    }
  });
  // Table
  let html = '<tr><th>Month</th><th>Tickets Created</th><th>Total Closed</th><th>Successfully Closed</th><th>Unsuccessfully Closed</th><th>Open Tickets</th><th>Success / Total Closed</th></tr>';
  m.forEach(r => {
    html += `<tr><td class="label">${r.MONTH}</td><td>${(r.TOTAL_CREATED||0).toLocaleString()}</td><td>${(r.closed||0).toLocaleString()}</td><td>${(r.RECOVERED||0).toLocaleString()}</td><td>${(r.FAILED||0).toLocaleString()}</td><td>${(r.OPEN_TICKETS||0).toLocaleString()}</td><td><b>${r.recovery_pct==null?'—':r.recovery_pct+'%'}</b></td></tr>`;
  });
  document.getElementById('momTable').innerHTML = html;
})();

// ---------- 2. Rolling 7d ----------
(function() {
  const d = DATA.daily;
  new Chart(document.getElementById('rollingChart'), {
    type: 'line',
    data: { labels: d.map(r => r.RESOLVED_DATE), datasets: [{
      label: '7-day rolling avg', data: d.map(r => r.rolling_7d_pct),
      borderColor: '#1F4E79', backgroundColor: 'rgba(46,117,182,0.12)',
      tension: 0.25, fill: true, pointRadius: 2, borderWidth: 2
    }]},
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false },
        tooltip: { callbacks: { label: c => c.parsed.y + '%' } },
        datalabels: {
          display: ctx => {
            const last = ctx.dataset.data.length - 1;
            return ctx.dataIndex === last || ctx.dataIndex % 7 === 0;
          },
          align: 'top', color: '#1F4E79',
          font: { weight: 700, size: 10 },
          formatter: v => v==null?'':v+'%'
        } },
      layout: { padding: { top: 22 } },
      scales: {
        x: { ticks: { maxTicksLimit: 12 } },
        y: { beginAtZero: true, max: 100, ticks: { callback: v => v+'%' } }
      }
    }
  });
})();

// ---------- 3. Daily raw ----------
(function() {
  const d = DATA.daily;
  new Chart(document.getElementById('dailyChart'), {
    type: 'line',
    data: { labels: d.map(r => r.RESOLVED_DATE), datasets: [{
      label: 'Success / Total Closed', data: d.map(r => r.recovery_pct),
      borderColor: '#C55A11', backgroundColor: 'rgba(197,90,17,0.08)',
      tension: 0.15, fill: false, pointRadius: 2, borderWidth: 1.5
    }]},
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false },
        tooltip: { callbacks: { label: c => c.parsed.y + '%' } },
        datalabels: {
          display: ctx => {
            const last = ctx.dataset.data.length - 1;
            return ctx.dataIndex === last || ctx.dataIndex % 7 === 0;
          },
          align: 'top', color: '#C55A11',
          font: { weight: 700, size: 10 },
          formatter: v => v==null?'':v+'%'
        } },
      layout: { padding: { top: 22 } },
      scales: {
        x: { ticks: { maxTicksLimit: 12 } },
        y: { beginAtZero: true, max: 100, ticks: { callback: v => v+'%' } }
      }
    }
  });
})();

// ---------- 4. Open ages ----------
(function() {
  const o = DATA.open_ages;
  const colors = ['#375623','#7f6000','#C55A11','#9c0006'];
  new Chart(document.getElementById('openChart'), {
    type: 'bar',
    data: { labels: o.map(r => r.AGE_BUCKET), datasets: [{
      label: 'Open Tickets', data: o.map(r => r.OPEN_TICKETS),
      backgroundColor: colors, borderRadius: 4
    }]},
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false },
        tooltip: { callbacks: { label: c => c.parsed.y.toLocaleString() } } },
      scales: { y: { beginAtZero: true } }
    }
  });
  let html = '<tr><th>Age Bucket</th><th>Open Tickets</th></tr>';
  const total = o.reduce((s,r) => s + (r.OPEN_TICKETS||0), 0);
  o.forEach(r => { html += `<tr><td class="label">${r.AGE_BUCKET}</td><td>${(r.OPEN_TICKETS||0).toLocaleString()}</td></tr>`; });
  html += `<tr><td class="label"><b>Total</b></td><td><b>${total.toLocaleString()}</b></td></tr>`;
  document.getElementById('openTable').innerHTML = html;
})();
</script>
</body>
</html>
"""

HTML = HTML.replace("__PAYLOAD__", json.dumps(payload))
HTML = HTML.replace("__LAST_REFRESH__", payload["last_refresh_ist"])

out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard.html")
with open(out_path, "w", encoding="utf-8") as f:
    f.write(HTML)
print(f"\nWrote {out_path} ({len(HTML):,} chars)")
