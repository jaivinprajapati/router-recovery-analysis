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

# ---------- Q1: MoM creation-basis recovery ----------
Q1 = """
SELECT
    TO_CHAR(DATE_TRUNC('month', DATEADD(minute,330,t.CREATED)),'YYYY-MM') AS month,
    COUNT(*) AS total_created,
    SUM(CASE WHEN tp.SCORE = 1 THEN 1 ELSE 0 END) AS recovered,
    SUM(CASE WHEN tp.SCORE = 0 THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN tp.REPORTERID IS NULL OR tp.TASK_RESOLVED_TIME IS NULL THEN 1 ELSE 0 END) AS open_tickets
FROM PROD_DB.DYNAMODB_READ.TASKS t
LEFT JOIN PROD_DB.DYNAMODB.TASK_PERFORMANCE tp
    ON t.ID = tp.REPORTERID AND tp._FIVETRAN_DELETED = false
WHERE t.TYPE = 'ROUTER_PICKUP'
  AND TO_DATE(DATEADD(minute,330,t.CREATED)) BETWEEN '2025-11-01' AND CURRENT_DATE
GROUP BY 1
ORDER BY 1
LIMIT 10000
"""

# ---------- Q2: Daily recovery (resolution date) ----------
Q2 = """
SELECT
    TO_CHAR(TO_DATE(DATEADD(minute,330,tp.TASK_RESOLVED_TIME)),'YYYY-MM-DD') AS resolved_date,
    COUNT(*) AS total_resolved,
    SUM(CASE WHEN tp.SCORE = 1 THEN 1 ELSE 0 END) AS recovered,
    SUM(CASE WHEN tp.SCORE = 0 THEN 1 ELSE 0 END) AS failed
FROM PROD_DB.DYNAMODB.TASK_PERFORMANCE tp
WHERE tp.TASK_TYPE = 'ROUTER_PICKUP'
  AND tp._FIVETRAN_DELETED = false
  AND tp.TASK_RESOLVED_TIME IS NOT NULL
  AND TO_DATE(DATEADD(minute,330,tp.TASK_RESOLVED_TIME)) BETWEEN '2025-11-01' AND CURRENT_DATE
GROUP BY 1
ORDER BY 1
LIMIT 10000
"""

# ---------- Q3: Open ticket age distribution ----------
# Open = TASKS with no TP record, OR TP record but no TASK_RESOLVED_TIME
Q3 = """
SELECT
    CASE
        WHEN DATEDIFF(day, TO_DATE(DATEADD(minute,330,t.CREATED)), CURRENT_DATE) BETWEEN 0  AND 7  THEN '0-7 days'
        WHEN DATEDIFF(day, TO_DATE(DATEADD(minute,330,t.CREATED)), CURRENT_DATE) BETWEEN 8  AND 14 THEN '8-14 days'
        WHEN DATEDIFF(day, TO_DATE(DATEADD(minute,330,t.CREATED)), CURRENT_DATE) BETWEEN 15 AND 21 THEN '15-21 days'
        ELSE '21+ days'
    END AS age_bucket,
    COUNT(*) AS open_tickets
FROM PROD_DB.DYNAMODB_READ.TASKS t
LEFT JOIN PROD_DB.DYNAMODB.TASK_PERFORMANCE tp
    ON t.ID = tp.REPORTERID AND tp._FIVETRAN_DELETED = false
WHERE t.TYPE = 'ROUTER_PICKUP'
  AND (tp.REPORTERID IS NULL OR tp.TASK_RESOLVED_TIME IS NULL)
  AND TO_DATE(DATEADD(minute,330,t.CREATED)) >= '2025-11-01'
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
  .card h2 { font-size: 15px; font-weight: 700; color: #1F4E79; margin-bottom: 4px; }
  .card .sub { font-size: 12px; color: #777; margin-bottom: 16px; }
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
  <p>Wiom Analytics &nbsp;|&nbsp; Auto-refreshed daily &nbsp;|&nbsp; Recovery = SCORE=1 in TASK_PERFORMANCE</p>
</div>
<div class="container">

  <div class="card">
    <h2>1. Monthly Recovery Rate — creation month basis</h2>
    <div class="sub">Denominator: closed tickets (recovered + failed) created that month. Latest months have 21-day cohort lag.</div>
    <div class="chart-wrap"><canvas id="momChart"></canvas></div>
    <table id="momTable"></table>
  </div>

  <div class="card">
    <h2>2. Week-on-Week — 7-day rolling avg daily recovery rate</h2>
    <div class="sub">Smoothed daily recovery %. Resolution-date basis. No cohort lag — shows operational performance in near-real-time.</div>
    <div class="chart-wrap tall"><canvas id="rollingChart"></canvas></div>
  </div>

  <div class="card">
    <h2>3. Day-on-Day — raw daily recovery rate</h2>
    <div class="sub">Of all tickets closed that day, what % had SCORE=1. Noisy but earliest signal.</div>
    <div class="chart-wrap tall"><canvas id="dailyChart"></canvas></div>
  </div>

  <div class="two-col">
    <div class="card">
      <h2>4. Open Ticket Age Distribution</h2>
      <div class="sub">Tickets with no resolved_time. 21+ day bucket = SLA breach (partners have 21 days).</div>
      <div class="chart-wrap"><canvas id="openChart"></canvas></div>
    </div>
    <div class="card">
      <h2>Open Tickets — Table</h2>
      <div class="sub">Count by age bucket.</div>
      <table id="openTable"></table>
    </div>
  </div>

  <div class="stamp">Last refreshed: __LAST_REFRESH__</div>
</div>

<script>
const DATA = __PAYLOAD__;
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
      label: 'Recovery %', data: pct,
      backgroundColor: '#2E75B6', borderRadius: 4
    }]},
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false },
        tooltip: { callbacks: { label: c => c.parsed.y + '%' } },
        datalabels: { display: true, anchor: 'end', align: 'top',
          color: '#1F4E79', font: { weight: 700, size: 11 },
          formatter: v => v==null?'':v+'%' } },
      layout: { padding: { top: 22 } },
      scales: { y: { beginAtZero: true, max: 100, ticks: { callback: v => v+'%' } } }
    }
  });
  // Table
  let html = '<tr><th>Month</th><th>Created</th><th>Closed</th><th>Recovered</th><th>Failed</th><th>Open</th><th>Recovery %</th></tr>';
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
        datalabels: { display: ctx => {
            const last = ctx.dataset.data.length - 1;
            return ctx.dataIndex === last || ctx.dataIndex % 7 === 0;
          }, align: 'top', color: '#1F4E79',
          font: { weight: 700, size: 10 },
          formatter: v => v==null?'':v+'%' } },
      layout: { padding: { top: 20 } },
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
      label: 'Daily recovery %', data: d.map(r => r.recovery_pct),
      borderColor: '#C55A11', backgroundColor: 'rgba(197,90,17,0.08)',
      tension: 0.15, fill: false, pointRadius: 2, borderWidth: 1.5
    }]},
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false },
        tooltip: { callbacks: { label: c => c.parsed.y + '%' } },
        datalabels: { display: ctx => {
            const last = ctx.dataset.data.length - 1;
            return ctx.dataIndex === last || ctx.dataIndex % 7 === 0;
          }, align: 'top', color: '#C55A11',
          font: { weight: 700, size: 10 },
          formatter: v => v==null?'':v+'%' } },
      layout: { padding: { top: 20 } },
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
      label: 'Open tickets', data: o.map(r => r.OPEN_TICKETS),
      backgroundColor: colors, borderRadius: 4
    }]},
    options: {
      responsive: true, maintainAspectRatio: false,
      plugins: { legend: { display: false },
        datalabels: { display: true, anchor: 'end', align: 'top',
          color: '#1F4E79', font: { weight: 700, size: 12 },
          formatter: v => v==null?'':v.toLocaleString() } },
      layout: { padding: { top: 22 } },
      scales: { y: { beginAtZero: true } }
    }
  });
  let html = '<tr><th>Age bucket</th><th>Open tickets</th></tr>';
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
