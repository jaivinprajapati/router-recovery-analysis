"""
Router Recovery — 6-Metric Alert Dashboard
Auto-refreshed daily via GitHub Actions.

Metrics:
  1. Churned customers without PUT created
  2. Daily PUT assignment trend
  3. Success / Total Closed (DOD, WOW, MOM)
  4. Open ticket aging
  5. Failed / Total Closed (DOD, WOW, MOM)
  6. RA recovery efficiency — failed tickets aging (device not yet returned)
"""
import os, json, sys, datetime as dt, requests

sys.stdout.reconfigure(encoding="utf-8")

API_KEY = os.environ.get("METABASE_API_KEY")
if not API_KEY:
    key_path = os.path.expanduser("~/.claude/metabase_key.txt")
    API_KEY = open(key_path).read().strip()

URL = "https://metabase.wiom.in/api/dataset"
HEADERS = {"Content-Type": "application/json", "x-api-key": API_KEY}

def run(sql):
    r = requests.post(URL, headers=HEADERS,
        json={"database": 113, "type": "native", "native": {"query": sql}},
        timeout=300)
    d = r.json()
    if "error" in d:
        raise RuntimeError(d["error"])
    cols = [c["name"] for c in d["data"]["cols"]]
    return [dict(zip(cols, row)) for row in d["data"]["rows"]]

TASKS_CTE = """
WITH cte1 AS (
    SELECT ID, REPORTER_ID, CREATED, RSLVD_DATETIME, STATUS, PARTNER_ID,
           ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CREATED DESC) AS rn
    FROM PROD_DB.DYNAMODB_READ.TASKS
    WHERE TYPE = 'ROUTER_PICKUP'
)
"""

# ---- Q1: Churned customers without PUT (simplified from card 10399) ----
Q1 = """
WITH base AS (
    SELECT t.router_nas_id AS router_id, t.mobile,
           DATEADD(minute,330,t.OTP_EXPIRY_TIME) AS expire_ist,
           LEAD(DATEADD(minute,330,t.OTP_ISSUED_TIME))
               OVER (PARTITION BY t.router_nas_id ORDER BY t.OTP_ISSUED_TIME) AS next_recharge
    FROM T_ROUTER_USER_MAPPING t
    WHERE t.otp = 'DONE' AND t.store_group_id = 0 AND t.device_limit > 1
),
eligible AS (
    SELECT router_id, mobile, expire_ist,
           CAST(DATEADD(day,16,expire_ist) AS DATE) AS put_expected_date
    FROM base
    WHERE next_recharge IS NULL
      AND DATEADD(day,15,expire_ist) < CURRENT_DATE
      AND expire_ist >= DATEADD(day,-90,CURRENT_DATE)
),
puts AS (
    SELECT PARSE_JSON(extra_data):nas_id::NUMBER AS nas_id
    FROM PROD_DB.DYNAMODB_READ.TASKS
    WHERE TYPE = 'ROUTER_PICKUP'
    AND CREATED >= DATEADD(day,-120,CURRENT_DATE)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PARSE_JSON(extra_data):nas_id::NUMBER ORDER BY CREATED DESC) = 1
),
audit AS (
    SELECT NAS_ID AS lng_nas_id
    FROM PROD_DB.POSTGRES_RDS_INVENTORY_INVENTORY.T_DEVICE_AUDIT
    WHERE MODIFIED_TIME >= DATEADD(day,-90,CURRENT_DATE)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DEVICE_ID ORDER BY MODIFIED_TIME DESC) = 1
)
SELECT e.put_expected_date AS d, COUNT(*) AS missed_puts
FROM eligible e
LEFT JOIN puts p ON e.router_id = p.nas_id
JOIN audit a ON e.router_id = a.lng_nas_id
WHERE p.nas_id IS NULL
AND e.put_expected_date >= DATEADD(day,-60,CURRENT_DATE)
AND e.put_expected_date <= CURRENT_DATE
GROUP BY 1 ORDER BY 1
LIMIT 10000
"""

# ---- Q2: Daily PUT assignment ----
Q2 = """
SELECT TO_DATE(DATEADD(minute,330,added_time)) AS d, COUNT(*) AS assigned
FROM PROD_DB.PUBLIC.TASK_LOGS
WHERE event_name = 'ASSIGN_TICKET_ROUTER_PICKUP'
AND added_time >= DATEADD(day,-120,CURRENT_DATE)
GROUP BY 1 ORDER BY 1
LIMIT 10000
"""

# ---- Q3/Q5: Daily success/failed rate (resolution date) ----
Q3 = TASKS_CTE + """
SELECT TO_CHAR(TO_DATE(DATEADD(minute,330,tp.TASK_RESOLVED_TIME)),'YYYY-MM-DD') AS d,
       COUNT(*) AS total_closed,
       SUM(CASE WHEN c.STATUS = 2 THEN 1 ELSE 0 END) AS success,
       SUM(CASE WHEN c.STATUS = 3 THEN 1 ELSE 0 END) AS failed
FROM PROD_DB.DYNAMODB.TASK_PERFORMANCE tp
LEFT JOIN cte1 c ON tp.REPORTERID = c.ID AND c.rn = 1
WHERE tp.TASK_TYPE = 'ROUTER_PICKUP' AND tp._FIVETRAN_DELETED = false
  AND tp.TASK_RESOLVED_TIME IS NOT NULL
  AND TO_DATE(DATEADD(minute,330,tp.TASK_RESOLVED_TIME)) BETWEEN DATEADD(day,-120,CURRENT_DATE) AND CURRENT_DATE
GROUP BY 1 ORDER BY 1
LIMIT 10000
"""

# ---- Q3b: MoM success/failed (creation month, 21-day rule) ----
Q3b = TASKS_CTE + """
SELECT TO_CHAR(DATE_TRUNC('month',DATEADD(minute,330,CREATED)),'YYYY-MM') AS month,
       COUNT(*) AS total_created,
       SUM(CASE WHEN STATUS = 2 THEN 1 ELSE 0 END) AS success,
       SUM(CASE WHEN STATUS = 3 THEN 1 ELSE 0 END) AS failed,
       SUM(CASE WHEN STATUS IN (0,1) THEN 1 ELSE 0 END) AS open_tickets
FROM cte1
WHERE rn = 1
  AND TO_DATE(DATEADD(minute,330,CREATED)) BETWEEN '2025-11-01' AND DATEADD(day,-21,CURRENT_DATE)
GROUP BY 1 ORDER BY 1
LIMIT 10000
"""

# ---- Q4: Open ticket aging ----
Q4 = TASKS_CTE + """
SELECT CASE
    WHEN DATEDIFF(day,TO_DATE(DATEADD(minute,330,CREATED)),CURRENT_DATE) BETWEEN 0 AND 7 THEN '0-7 days'
    WHEN DATEDIFF(day,TO_DATE(DATEADD(minute,330,CREATED)),CURRENT_DATE) BETWEEN 8 AND 14 THEN '8-14 days'
    WHEN DATEDIFF(day,TO_DATE(DATEADD(minute,330,CREATED)),CURRENT_DATE) BETWEEN 15 AND 21 THEN '15-21 days'
    ELSE '21+ days' END AS age_bucket,
    COUNT(*) AS open_tickets
FROM cte1
WHERE rn = 1 AND STATUS IN (0,1)
  AND TO_DATE(DATEADD(minute,330,CREATED)) >= '2025-11-01'
GROUP BY 1
LIMIT 10000
"""

# ---- Q6: RA efficiency — failed tickets aging (device not yet returned by RA) ----
Q6 = """
WITH failed_puts AS (
    SELECT tp.CUSTOMER_DEVICE_ID,
           TO_DATE(DATEADD(minute,330,tp.TASK_RESOLVED_TIME)) AS failed_date,
           DATEDIFF('day', TO_DATE(DATEADD(minute,330,tp.TASK_RESOLVED_TIME)), CURRENT_DATE) AS days_since_fail
    FROM PROD_DB.DYNAMODB_READ.TASKS t
    JOIN PROD_DB.DYNAMODB.TASK_PERFORMANCE tp ON t.ID = tp.REPORTERID AND tp._FIVETRAN_DELETED = false
    WHERE t.TYPE = 'ROUTER_PICKUP' AND t.STATUS = 3
    AND tp.CUSTOMER_DEVICE_ID IS NOT NULL AND tp.TASK_RESOLVED_TIME IS NOT NULL
    AND DATEADD(minute,330,tp.TASK_RESOLVED_TIME) >= DATEADD(day,-120,CURRENT_DATE)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY t.ID ORDER BY t.CREATED DESC) = 1
),
ra_returns AS (
    SELECT DEVICE_ID
    FROM PROD_DB.POSTGRES_RDS_INVENTORY_INVENTORY.T_DEVICE_AUDIT
    WHERE PARSE_JSON(extra_data):returnedBy::string = 'RA'
    AND PARSE_JSON(extra_data):router_state::string = 'returned_in_office'
    AND MODIFIED_TIME >= DATEADD(day,-180,CURRENT_DATE)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY DEVICE_ID ORDER BY MODIFIED_TIME DESC) = 1
)
SELECT
    CASE
        WHEN fp.days_since_fail BETWEEN 0 AND 7 THEN '0-7 days'
        WHEN fp.days_since_fail BETWEEN 8 AND 14 THEN '8-14 days'
        WHEN fp.days_since_fail BETWEEN 15 AND 21 THEN '15-21 days'
        WHEN fp.days_since_fail BETWEEN 22 AND 30 THEN '22-30 days'
        ELSE '30+ days'
    END AS age_bucket,
    COUNT(*) AS total_failed,
    SUM(CASE WHEN ra.DEVICE_ID IS NOT NULL THEN 1 ELSE 0 END) AS ra_picked,
    SUM(CASE WHEN ra.DEVICE_ID IS NULL THEN 1 ELSE 0 END) AS still_pending
FROM failed_puts fp
LEFT JOIN ra_returns ra ON fp.CUSTOMER_DEVICE_ID = ra.DEVICE_ID
GROUP BY 1
ORDER BY CASE age_bucket
    WHEN '0-7 days' THEN 1 WHEN '8-14 days' THEN 2 WHEN '15-21 days' THEN 3
    WHEN '22-30 days' THEN 4 ELSE 5 END
LIMIT 10000
"""

# ---- Q6b: Weekly RA returns trend ----
Q6b = """
SELECT TO_CHAR(DATE_TRUNC('week',DATEADD(minute,330,MODIFIED_TIME)),'YYYY-MM-DD') AS week_start,
       COUNT(*) AS ra_returns
FROM PROD_DB.POSTGRES_RDS_INVENTORY_INVENTORY.T_DEVICE_AUDIT
WHERE PARSE_JSON(extra_data):returnedBy::string = 'RA'
AND PARSE_JSON(extra_data):router_state::string = 'returned_in_office'
AND DATEADD(minute,330,MODIFIED_TIME) >= DATEADD(day,-120,CURRENT_DATE)
GROUP BY 1 ORDER BY 1
LIMIT 10000
"""

# ---------- Run all ----------
queries = [
    ("Q1 (Missed PUTs)", Q1),
    ("Q2 (Assignments)", Q2),
    ("Q3 (Daily success/fail)", Q3),
    ("Q3b (MoM)", Q3b),
    ("Q4 (Open aging)", Q4),
    ("Q6 (RA aging)", Q6),
    ("Q6b (RA weekly)", Q6b),
]

results = {}
for label, sql in queries:
    print(f"Running {label}...")
    data = run(sql)
    results[label] = data
    print(f"  {len(data)} rows")

# ---------- Post-process ----------
# Q3: daily + rolling 7d for both success and failed rates
daily = sorted(results["Q3 (Daily success/fail)"], key=lambda x: x["D"])
for i, d in enumerate(daily):
    tc = d["TOTAL_CLOSED"] or 1
    d["success_pct"] = round((d["SUCCESS"] or 0) * 100.0 / tc, 1)
    d["failed_pct"] = round((d["FAILED"] or 0) * 100.0 / tc, 1)
    window = daily[max(0, i-6): i+1]
    w_s = sum(x["SUCCESS"] or 0 for x in window)
    w_f = sum(x["FAILED"] or 0 for x in window)
    w_t = sum(x["TOTAL_CLOSED"] or 0 for x in window)
    d["success_7d"] = round(w_s * 100.0 / w_t, 1) if w_t else None
    d["failed_7d"] = round(w_f * 100.0 / w_t, 1) if w_t else None

# Q3b: MoM rates
mom = results["Q3b (MoM)"]
for m in mom:
    closed = (m["SUCCESS"] or 0) + (m["FAILED"] or 0)
    m["closed"] = closed
    m["success_pct"] = round((m["SUCCESS"] or 0) * 100.0 / closed, 1) if closed else None
    m["failed_pct"] = round((m["FAILED"] or 0) * 100.0 / closed, 1) if closed else None

last_refresh = dt.datetime.now(dt.timezone(dt.timedelta(hours=5, minutes=30)))
payload = {
    "missed_puts": results["Q1 (Missed PUTs)"],
    "assignments": results["Q2 (Assignments)"],
    "daily": daily,
    "mom": mom,
    "open_ages": results["Q4 (Open aging)"],
    "ra_aging": results["Q6 (RA aging)"],
    "ra_weekly": results["Q6b (RA weekly)"],
    "last_refresh_ist": last_refresh.strftime("%Y-%m-%d %H:%M IST"),
}

# ---------- Build HTML ----------
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Router Recovery — Alert Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0/dist/chartjs-plugin-datalabels.min.js"></script>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: 'Segoe UI', Arial, sans-serif; background: #f0f2f5; color: #1a1a2e; line-height: 1.5; }
  .header { background: linear-gradient(135deg, #1F4E79, #2E75B6); color: white; padding: 24px 48px; }
  .header h1 { font-size: 22px; font-weight: 700; }
  .header p  { font-size: 12px; opacity: 0.85; margin-top: 4px; }
  .container { max-width: 1300px; margin: 0 auto; padding: 24px 20px; }

  .card { background: white; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.06); padding: 20px 24px; margin-bottom: 20px; }
  .card-head { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; }
  .card h2 { font-size: 14px; font-weight: 700; color: #1F4E79; }
  .card .sub { font-size: 12px; color: #777; margin-bottom: 6px; }
  .card .why { font-size: 11.5px; color: #555; background: #FFF9F0; border-left: 3px solid #FFC107; padding: 8px 12px; margin-bottom: 14px; border-radius: 3px; line-height: 1.5; }
  .card .why b { color: #7f6000; }

  .src-wrap { position: relative; display: inline-block; }
  .src-icon { display: inline-flex; align-items: center; justify-content: center; width: 16px; height: 16px; border-radius: 50%; background: #2E75B6; color: white; font-size: 10px; font-weight: 700; cursor: help; }
  .src-tip { display: none; position: absolute; top: 22px; left: 0; z-index: 100; min-width: 440px; font-size: 11px; color: #333; background: white; border: 1px solid #BDD7EE; box-shadow: 0 4px 14px rgba(0,0,0,0.12); padding: 10px 14px; border-radius: 5px; font-family: 'Consolas','Courier New',monospace; line-height: 1.55; }
  .src-tip b { color: #1F4E79; font-family: 'Segoe UI', Arial, sans-serif; }
  .src-wrap:hover .src-tip { display: block; }

  .chart-wrap { position: relative; height: 300px; }
  .chart-wrap.tall { height: 340px; }
  table { width: 100%; border-collapse: collapse; font-size: 12.5px; margin-top: 8px; }
  th { background: #1F4E79; color: white; padding: 7px 10px; text-align: center; font-weight: 600; font-size: 11.5px; }
  td { padding: 6px 10px; text-align: center; border-bottom: 1px solid #f0f0f0; }
  td.label { text-align: left; font-weight: 500; }

  .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
  .three-col { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 20px; }
  @media (max-width: 900px) { .two-col, .three-col { grid-template-columns: 1fr; } }

  .tabs { display: flex; gap: 0; margin-bottom: 12px; }
  .tab { padding: 6px 16px; font-size: 12px; font-weight: 600; color: #666; background: #f0f2f5; border: 1px solid #ddd; cursor: pointer; }
  .tab:first-child { border-radius: 6px 0 0 6px; }
  .tab:last-child { border-radius: 0 6px 6px 0; }
  .tab.active { background: #1F4E79; color: white; border-color: #1F4E79; }
  .tab-pane { display: none; }
  .tab-pane.active { display: block; }

  .stamp { font-size: 11px; color: #888; text-align: right; padding: 6px 12px 0; }
  .metric-num { font-size: 11px; font-weight: 700; color: #1F4E79; }
</style>
</head>
<body>
<div class="header">
  <h1>Router Recovery — Alert Dashboard</h1>
  <p>Wiom Analytics &nbsp;|&nbsp; Auto-refreshed daily 09:00 IST &nbsp;|&nbsp; 6 metrics across the PUT lifecycle</p>
</div>
<div class="container">

  <!-- ============ METRIC 1 ============ -->
  <div class="card">
    <div class="card-head">
      <h2>1. Churned Customers Without PUT Created</h2>
      <span class="src-wrap"><span class="src-icon">i</span>
        <div class="src-tip">
          <b>Source:</b> T_ROUTER_USER_MAPPING (recharge expiry) + DYNAMODB_READ.TASKS (PUT tickets) + T_DEVICE_AUDIT (device still at same NAS)<br>
          <b>Logic:</b> Customer recharge expired 15+ days ago, no subsequent recharge, router still at same customer location, but no PUT ticket created<br>
          <b>Date:</b> Expected PUT creation date = expiry + 16 days<br>
          <b>Aligned with:</b> Metabase dashboard #1143, card 10399
        </div>
      </span>
    </div>
    <div class="sub">Daily count of customers who churned but no PUT ticket was raised.</div>
    <div class="why"><b>Why this matters:</b> This is the entry point of the entire recovery funnel. If a customer churns and no PUT is created, the device is silently lost — no partner action, no RA fallback. Every missed PUT = a device Wiom has zero chance of recovering.</div>
    <div class="chart-wrap"><canvas id="chart1"></canvas></div>
  </div>

  <!-- ============ METRIC 2 ============ -->
  <div class="card">
    <div class="card-head">
      <h2>2. Daily PUT Tickets Assigned</h2>
      <span class="src-wrap"><span class="src-icon">i</span>
        <div class="src-tip">
          <b>Source:</b> PROD_DB.PUBLIC.TASK_LOGS<br>
          <b>Filter:</b> event_name = 'ASSIGN_TICKET_ROUTER_PICKUP'<br>
          <b>Date:</b> DATEADD(minute,330,added_time) — IST<br>
          <b>Benchmark:</b> Nov-Dec avg ~67% assigned within 24hrs, median 11h. Assignment rate has been declining (80% → 66% Nov→Mar).
        </div>
      </span>
    </div>
    <div class="sub">Count of PUT tickets assigned to a partner/agent each day.</div>
    <div class="why"><b>Why this matters:</b> Assignment is the handoff from system to field. Tickets created but never assigned = dead tickets. A drop in assignments while creation stays flat signals a dispatch bottleneck — tickets pile up unactioned.</div>
    <div class="chart-wrap"><canvas id="chart2"></canvas></div>
  </div>

  <!-- ============ METRIC 3 ============ -->
  <div class="card">
    <div class="card-head">
      <h2>3. Success / Total Closed</h2>
      <span class="src-wrap"><span class="src-icon">i</span>
        <div class="src-tip">
          <b>DOD/WOW:</b> TASK_PERFORMANCE (resolution date) LEFT JOIN TASKS (deduped). Successfully Closed = TASKS.STATUS=2. Denominator = all resolved that day.<br>
          <b>MOM:</b> TASKS only (deduped, rn=1). Creation month basis. 21-day rule applied (only tickets created ≥21 days ago).<br>
          <b>WOW:</b> 7-day trailing sum(success) / sum(total closed).
        </div>
      </span>
    </div>
    <div class="sub">Of all PUT tickets closed, what % were successfully closed (STATUS=2).</div>
    <div class="why"><b>Why this matters:</b> The core health metric. Tells you whether the field is actually recovering devices. A sustained drop means something fundamental is failing — partner effort, customer cooperation, or process.</div>
    <div class="tabs" data-group="success">
      <div class="tab active" onclick="switchTab(this,'success','dod')">Day-on-Day</div>
      <div class="tab" onclick="switchTab(this,'success','wow')">Week-on-Week</div>
      <div class="tab" onclick="switchTab(this,'success','mom')">Month-on-Month</div>
    </div>
    <div class="tab-pane active" data-group="success" data-tab="dod"><div class="chart-wrap tall"><canvas id="chart3dod"></canvas></div></div>
    <div class="tab-pane" data-group="success" data-tab="wow"><div class="chart-wrap tall"><canvas id="chart3wow"></canvas></div></div>
    <div class="tab-pane" data-group="success" data-tab="mom"><div class="chart-wrap"><canvas id="chart3mom"></canvas></div><table id="table3mom"></table></div>
  </div>

  <!-- ============ METRIC 4 ============ -->
  <div class="two-col">
    <div class="card">
      <div class="card-head">
        <h2>4. Open Ticket Aging</h2>
        <span class="src-wrap"><span class="src-icon">i</span>
          <div class="src-tip">
            <b>Table:</b> DYNAMODB_READ.TASKS (deduped, rn=1)<br>
            <b>Filter:</b> TYPE='ROUTER_PICKUP', STATUS IN (0,1), CREATED ≥ 2025-11-01<br>
            <b>Buckets:</b> 0-7d (healthy), 8-14d (aging), 15-21d (danger), 21+d (SLA breach)
          </div>
        </span>
      </div>
      <div class="sub">Currently open PUT tickets bucketed by age.</div>
      <div class="why"><b>Why this matters:</b> Open tickets are future failures. A ticket at 18 days old will almost certainly fail. Rising 15-21d count = failure spike incoming next week.</div>
      <div class="chart-wrap"><canvas id="chart4"></canvas></div>
    </div>

  <!-- ============ METRIC 5 ============ -->
    <div class="card">
      <div class="card-head">
        <h2>5. Failed / Total Closed</h2>
        <span class="src-wrap"><span class="src-icon">i</span>
          <div class="src-tip">
            <b>Same source as Metric 3</b> (complement). Failed = TASKS.STATUS=3 (send_to_wiom).<br>
            <b>This is the top-of-funnel for the RA team.</b>
          </div>
        </span>
      </div>
      <div class="sub">Of all PUT tickets closed, what % were unsuccessfully closed (STATUS=3).</div>
      <div class="why"><b>Why this matters:</b> Every failed ticket becomes RA's problem. A spike here means RA team needs to brace for volume. Also flags if partners are bulk-closing tickets as failed without recovery attempt.</div>
      <div class="tabs" data-group="fail">
        <div class="tab active" onclick="switchTab(this,'fail','dod')">DOD</div>
        <div class="tab" onclick="switchTab(this,'fail','wow')">WOW</div>
      </div>
      <div class="tab-pane active" data-group="fail" data-tab="dod"><div class="chart-wrap"><canvas id="chart5dod"></canvas></div></div>
      <div class="tab-pane" data-group="fail" data-tab="wow"><div class="chart-wrap"><canvas id="chart5wow"></canvas></div></div>
    </div>
  </div>

  <!-- ============ METRIC 6 ============ -->
  <div class="card">
    <div class="card-head">
      <h2>6. RA Recovery Efficiency</h2>
      <span class="src-wrap"><span class="src-icon">i</span>
        <div class="src-tip">
          <b>Failed tickets:</b> TASKS (STATUS=3) + TASK_PERFORMANCE (CUSTOMER_DEVICE_ID, resolved time)<br>
          <b>RA returns:</b> T_DEVICE_AUDIT where extra_data.returnedBy='RA' and router_state='returned_in_office'<br>
          <b>Join:</b> CUSTOMER_DEVICE_ID = DEVICE_ID<br>
          <b>Aging:</b> days since PUT failed, bucketed. "Still pending" = device not yet returned by RA.
        </div>
      </span>
    </div>
    <div class="sub">After partner fails a PUT ticket, is RA picking up the device? Aging view of failed tickets by RA return status.</div>
    <div class="why"><b>Why this matters:</b> RA is the last line of defense. When partners fail, RA picks up the device. A failed PUT with no subsequent RA return = device permanently lost. Rising "still pending" count in older buckets = RA falling behind or devices slipping through.</div>
    <div class="two-col">
      <div><div class="chart-wrap"><canvas id="chart6"></canvas></div></div>
      <div><table id="table6"></table>
        <div style="margin-top:12px;"><div class="chart-wrap"><canvas id="chart6b"></canvas></div></div>
      </div>
    </div>
  </div>

  <div class="stamp">Last refreshed: __LAST_REFRESH__</div>
</div>

<script>
const DATA = __PAYLOAD__;
Chart.register(ChartDataLabels);
Chart.defaults.set('plugins.datalabels', { display: false });

function switchTab(el, group, tab) {
  document.querySelectorAll('.tab[onclick*="'+group+'"]').forEach(t => t.classList.remove('active'));
  el.classList.add('active');
  document.querySelectorAll('.tab-pane[data-group="'+group+'"]').forEach(p => p.classList.remove('active'));
  document.querySelector('.tab-pane[data-group="'+group+'"][data-tab="'+tab+'"]').classList.add('active');
}

const sparse = ctx => { const l=ctx.dataset.data.length-1; return ctx.dataIndex===l||ctx.dataIndex%7===0; };
const pctFmt = v => v==null?'':v+'%';
const pctTip = { callbacks: { label: c => c.parsed.y+'%' } };
const yPct = { beginAtZero:true, max:100, ticks:{callback:v=>v+'%'} };

// ---- 1. Missed PUTs ----
(function(){
  const d = DATA.missed_puts;
  new Chart(document.getElementById('chart1'), {
    type:'bar',
    data:{labels:d.map(r=>r.D?.substring(0,10)||r.D), datasets:[{
      label:'Missed PUTs', data:d.map(r=>r.MISSED_PUTS),
      backgroundColor:'#C00000', borderRadius:3
    }]},
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},tooltip:{}},
      scales:{x:{ticks:{maxTicksLimit:15}},y:{beginAtZero:true}}}
  });
})();

// ---- 2. Assignments ----
(function(){
  const d = DATA.assignments;
  new Chart(document.getElementById('chart2'), {
    type:'line',
    data:{labels:d.map(r=>r.D?.substring(0,10)||r.D), datasets:[{
      label:'PUT Assigned', data:d.map(r=>r.ASSIGNED),
      borderColor:'#2E75B6', backgroundColor:'rgba(46,117,182,0.1)',
      tension:0.2, fill:true, pointRadius:1.5, borderWidth:2
    }]},
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},tooltip:{},
        datalabels:{display:sparse, align:'top', color:'#1F4E79',
          font:{weight:700,size:10}, formatter:v=>v==null?'':v.toLocaleString()}},
      layout:{padding:{top:18}},
      scales:{x:{ticks:{maxTicksLimit:15}},y:{beginAtZero:true}}}
  });
})();

// ---- 3. Success DOD ----
(function(){
  const d = DATA.daily;
  new Chart(document.getElementById('chart3dod'), {
    type:'line',
    data:{labels:d.map(r=>r.D), datasets:[{
      label:'Success / Total Closed', data:d.map(r=>r.success_pct),
      borderColor:'#C55A11', tension:0.15, pointRadius:1.5, borderWidth:1.5, fill:false
    }]},
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},tooltip:pctTip,
        datalabels:{display:sparse,align:'top',color:'#C55A11',font:{weight:700,size:10},formatter:pctFmt}},
      layout:{padding:{top:20}},
      scales:{x:{ticks:{maxTicksLimit:12}},y:yPct}}
  });
})();

// ---- 3. Success WOW ----
(function(){
  const d = DATA.daily;
  new Chart(document.getElementById('chart3wow'), {
    type:'line',
    data:{labels:d.map(r=>r.D), datasets:[{
      label:'7-day rolling avg', data:d.map(r=>r.success_7d),
      borderColor:'#1F4E79', backgroundColor:'rgba(46,117,182,0.1)',
      tension:0.25, fill:true, pointRadius:1.5, borderWidth:2
    }]},
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},tooltip:pctTip,
        datalabels:{display:sparse,align:'top',color:'#1F4E79',font:{weight:700,size:10},formatter:pctFmt}},
      layout:{padding:{top:20}},
      scales:{x:{ticks:{maxTicksLimit:12}},y:yPct}}
  });
})();

// ---- 3. Success MOM ----
(function(){
  const m = DATA.mom;
  new Chart(document.getElementById('chart3mom'), {
    type:'bar',
    data:{labels:m.map(r=>r.MONTH), datasets:[{
      label:'Success / Total Closed', data:m.map(r=>r.success_pct),
      backgroundColor:'#2E75B6', borderRadius:4
    }]},
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},tooltip:pctTip,
        datalabels:{display:true,anchor:'end',align:'top',color:'#1F4E79',font:{weight:700,size:11},formatter:pctFmt}},
      layout:{padding:{top:20}},
      scales:{y:yPct}}
  });
  let html='<tr><th>Month</th><th>Tickets Created</th><th>Total Closed</th><th>Successfully Closed</th><th>Unsuccessfully Closed</th><th>Open</th><th>Success %</th></tr>';
  m.forEach(r=>{html+=`<tr><td class="label">${r.MONTH}</td><td>${(r.TOTAL_CREATED||0).toLocaleString()}</td><td>${(r.closed||0).toLocaleString()}</td><td>${(r.SUCCESS||0).toLocaleString()}</td><td>${(r.FAILED||0).toLocaleString()}</td><td>${(r.OPEN_TICKETS||0).toLocaleString()}</td><td><b>${r.success_pct==null?'-':r.success_pct+'%'}</b></td></tr>`;});
  document.getElementById('table3mom').innerHTML=html;
})();

// ---- 4. Open aging ----
(function(){
  const o = DATA.open_ages;
  const colors = ['#375623','#7f6000','#C55A11','#9c0006'];
  const bucketOrder = ['0-7 days','8-14 days','15-21 days','21+ days'];
  const sorted = bucketOrder.map(b => o.find(r=>r.AGE_BUCKET===b)||{AGE_BUCKET:b,OPEN_TICKETS:0});
  new Chart(document.getElementById('chart4'), {
    type:'bar',
    data:{labels:sorted.map(r=>r.AGE_BUCKET), datasets:[{
      label:'Open Tickets', data:sorted.map(r=>r.OPEN_TICKETS),
      backgroundColor:colors, borderRadius:4
    }]},
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},
        tooltip:{callbacks:{label:c=>c.parsed.y.toLocaleString()}},
        datalabels:{display:true,anchor:'end',align:'top',color:'#1F4E79',font:{weight:700,size:12},formatter:v=>v.toLocaleString()}},
      layout:{padding:{top:20}},
      scales:{y:{beginAtZero:true}}}
  });
})();

// ---- 5. Failed DOD ----
(function(){
  const d = DATA.daily;
  new Chart(document.getElementById('chart5dod'), {
    type:'line',
    data:{labels:d.map(r=>r.D), datasets:[{
      label:'Failed / Total Closed', data:d.map(r=>r.failed_pct),
      borderColor:'#9c0006', tension:0.15, pointRadius:1.5, borderWidth:1.5, fill:false
    }]},
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},tooltip:pctTip,
        datalabels:{display:sparse,align:'top',color:'#9c0006',font:{weight:700,size:10},formatter:pctFmt}},
      layout:{padding:{top:20}},
      scales:{x:{ticks:{maxTicksLimit:12}},y:yPct}}
  });
})();

// ---- 5. Failed WOW ----
(function(){
  const d = DATA.daily;
  new Chart(document.getElementById('chart5wow'), {
    type:'line',
    data:{labels:d.map(r=>r.D), datasets:[{
      label:'7-day rolling avg', data:d.map(r=>r.failed_7d),
      borderColor:'#9c0006', backgroundColor:'rgba(156,0,6,0.08)',
      tension:0.25, fill:true, pointRadius:1.5, borderWidth:2
    }]},
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},tooltip:pctTip,
        datalabels:{display:sparse,align:'top',color:'#9c0006',font:{weight:700,size:10},formatter:pctFmt}},
      layout:{padding:{top:20}},
      scales:{x:{ticks:{maxTicksLimit:12}},y:yPct}}
  });
})();

// ---- 6. RA aging ----
(function(){
  const o = DATA.ra_aging;
  const bucketOrder = ['0-7 days','8-14 days','15-21 days','22-30 days','30+ days'];
  const sorted = bucketOrder.map(b => o.find(r=>r.AGE_BUCKET===b)||{AGE_BUCKET:b,TOTAL_FAILED:0,RA_PICKED:0,STILL_PENDING:0});
  new Chart(document.getElementById('chart6'), {
    type:'bar',
    data:{labels:sorted.map(r=>r.AGE_BUCKET), datasets:[
      {label:'RA Picked Up', data:sorted.map(r=>r.RA_PICKED), backgroundColor:'#375623', borderRadius:3},
      {label:'Still Pending', data:sorted.map(r=>r.STILL_PENDING), backgroundColor:'#C00000', borderRadius:3}
    ]},
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{position:'bottom',labels:{font:{size:11}}},
        datalabels:{display:true,anchor:'end',align:'top',color:'#333',font:{weight:700,size:10},formatter:v=>v?v.toLocaleString():''}},
      layout:{padding:{top:20}},
      scales:{x:{stacked:true},y:{stacked:true,beginAtZero:true}}}
  });

  let html='<tr><th>Age Bucket</th><th>Total Failed</th><th>RA Picked</th><th>Still Pending</th><th>RA Pickup %</th></tr>';
  sorted.forEach(r=>{
    const pct = r.TOTAL_FAILED? Math.round(r.RA_PICKED*100/r.TOTAL_FAILED)+'%' : '-';
    html+=`<tr><td class="label">${r.AGE_BUCKET}</td><td>${(r.TOTAL_FAILED||0).toLocaleString()}</td><td>${(r.RA_PICKED||0).toLocaleString()}</td><td style="color:#C00000;font-weight:700">${(r.STILL_PENDING||0).toLocaleString()}</td><td>${pct}</td></tr>`;
  });
  const totF=sorted.reduce((s,r)=>s+(r.TOTAL_FAILED||0),0);
  const totP=sorted.reduce((s,r)=>s+(r.RA_PICKED||0),0);
  const totS=sorted.reduce((s,r)=>s+(r.STILL_PENDING||0),0);
  html+=`<tr><td class="label"><b>Total</b></td><td><b>${totF.toLocaleString()}</b></td><td><b>${totP.toLocaleString()}</b></td><td style="color:#C00000;font-weight:700"><b>${totS.toLocaleString()}</b></td><td><b>${totF?Math.round(totP*100/totF)+'%':'-'}</b></td></tr>`;
  document.getElementById('table6').innerHTML=html;

  // Weekly RA returns
  const w = DATA.ra_weekly;
  new Chart(document.getElementById('chart6b'), {
    type:'bar',
    data:{labels:w.map(r=>r.WEEK_START?.substring(0,10)||r.WEEK_START), datasets:[{
      label:'RA Returns (weekly)', data:w.map(r=>r.RA_RETURNS),
      backgroundColor:'#375623', borderRadius:3
    }]},
    options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{display:false},
        datalabels:{display:true,anchor:'end',align:'top',color:'#375623',font:{weight:700,size:10},formatter:v=>v?v.toLocaleString():''}},
      layout:{padding:{top:20}},
      scales:{y:{beginAtZero:true}}}
  });
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
