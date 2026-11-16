//! Dashboard HTML/JS embedded into the broker binary.

pub const DASHBOARD: &str = r##"<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>mqueue 运维控制台</title>
<style>
  :root { --bg:#0f1420; --card:#171e2e; --line:#243047; --txt:#dbe4f3; --dim:#8494ad;
          --accent:#4f8cff; --green:#37c997; --amber:#f0b34f; --red:#ef6a6a; }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--txt);
         font:14px/1.5 -apple-system,"PingFang SC","Microsoft YaHei",sans-serif; }
  header { padding:18px 28px; border-bottom:1px solid var(--line);
           display:flex; align-items:center; gap:16px; }
  header h1 { font-size:18px; margin:0; letter-spacing:.5px; }
  header .dot { width:9px; height:9px; border-radius:50%; background:var(--green);
                box-shadow:0 0 8px var(--green); }
  main { padding:22px 28px; display:grid; gap:18px; }
  .grid { display:grid; gap:14px; }
  .grid.cols4 { grid-template-columns:repeat(4,1fr); }
  .grid.cols2 { grid-template-columns:1fr 1fr; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:16px; }
  .card h2 { font-size:13px; margin:0 0 12px; color:var(--dim); font-weight:600;
             text-transform:uppercase; letter-spacing:1px; }
  .kpi .v { font-size:26px; font-weight:700; }
  .kpi .s { color:var(--dim); font-size:12px; margin-top:4px; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th,td { text-align:left; padding:8px 10px; border-bottom:1px solid var(--line); }
  th { color:var(--dim); font-weight:600; }
  tr:last-child td { border-bottom:none; }
  .tag { display:inline-block; padding:1px 8px; border-radius:999px; font-size:12px;
         background:#1d2b45; color:var(--accent); }
  .tag.active { background:#13352a; color:var(--green); }
  .tag.warn { background:#3a2f16; color:var(--amber); }
  .bar { height:6px; border-radius:3px; background:#22314c; overflow:hidden; margin-top:6px; }
  .bar i { display:block; height:100%; background:var(--accent); }
  svg { width:100%; height:180px; display:block; }
  .legend { display:flex; gap:16px; font-size:12px; color:var(--dim); margin-top:6px; }
  .legend i { display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:5px; }
  form { display:flex; gap:8px; flex-wrap:wrap; }
  input { background:#0d1320; border:1px solid var(--line); color:var(--txt);
          border-radius:8px; padding:8px 10px; width:130px; }
  button { background:var(--accent); color:#fff; border:none; border-radius:8px;
           padding:8px 16px; cursor:pointer; font-weight:600; }
  button:hover { filter:brightness(1.1); }
  .muted { color:var(--dim); }
  .lag { color:var(--amber); font-weight:600; }
  .lag.zero { color:var(--green); font-weight:400; }
  code { background:#0d1320; padding:1px 6px; border-radius:5px; }
  .scroll { max-height:340px; overflow:auto; }
</style>
</head>
<body>
<header>
  <span class="dot"></span><h1>mqueue 运维控制台</h1>
  <span class="muted" id="uptime"></span>
  <span style="flex:1"></span>
  <form id="newtopic">
    <input name="name" placeholder="主题名" required>
    <input name="partitions" type="number" min="1" value="4" placeholder="分区数">
    <input name="max_age_ms" type="number" placeholder="保留时长ms(可选)">
    <button type="submit">创建主题</button>
  </form>
</header>
<main>
  <section class="grid cols4" id="kpis"></section>

  <section class="card">
    <h2>生产 / 消费速率（条/秒，近 5 分钟）</h2>
    <svg id="rateChart" viewBox="0 0 1000 180" preserveAspectRatio="none"></svg>
    <div class="legend">
      <span><i style="background:#4f8cff"></i>生产速率</span>
      <span><i style="background:#37c997"></i>消费速率</span>
    </div>
  </section>

  <section class="grid cols2">
    <div class="card">
      <h2>主题与分区 / 存储段</h2>
      <div class="scroll" id="topics"></div>
    </div>
    <div class="card">
      <h2>消费组 / 积压</h2>
      <div class="scroll" id="groups"></div>
    </div>
  </section>
</main>

<script>
const fmt = n => n == null ? '-' : n.toLocaleString('en-US');
const bytes = n => {
  if (n < 1024) return n + ' B';
  const u = ['KB','MB','GB','TB']; let i = -1;
  do { n /= 1024; i++; } while (n >= 1024 && i < u.length-1);
  return n.toFixed(1) + ' ' + u[i];
};
const up = s => `${Math.floor(s/3600)}h ${Math.floor(s%3600/60)}m ${s%60}s`;

async function getJson(u) {
  const r = await fetch(u);
  if (!r.ok) throw new Error(u + ' -> ' + r.status);
  return r.json();
}

function kpi(v, s) {
  return `<div class="card kpi"><div class="v">${v}</div><div class="s">${s}</div></div>`;
}

function lineChart(svg, series) {
  const W = 1000, H = 180, P = 8;
  if (!series.length) { svg.innerHTML = ''; return; }
  const t0 = series[0].t_sec, t1 = series[series.length-1].t_sec || t0+1;
  const maxV = Math.max(1, ...series.map(p => Math.max(p.produced, p.fetched)));
  const x = t => P + (W - 2*P) * ((t - t0) / Math.max(1, t1 - t0));
  const y = v => H - P - (H - 2*P) * (v / maxV);
  const path = key => series.map((p,i) =>
    (i ? 'L' : 'M') + x(p.t_sec).toFixed(1) + ' ' + y(p[key]).toFixed(1)).join(' ');
  svg.innerHTML = `
    <line x1="${P}" y1="${H-P}" x2="${W-P}" y2="${H-P}" stroke="#243047"/>
    <path d="${path('produced')}" fill="none" stroke="#4f8cff" stroke-width="2"/>
    <path d="${path('fetched')}" fill="none" stroke="#37c997" stroke-width="2"/>
    <text x="${P}" y="14" fill="#8494ad" font-size="11">峰值 ${fmt(maxV)}/s</text>`;
}

async function refresh() {
  try {
    const [ov, ts, gs, m] = await Promise.all([
      getJson('/api/overview'), getJson('/api/topics'),
      getJson('/api/groups'), getJson('/api/metrics?seconds=300'),
    ]);
    document.getElementById('uptime').textContent = '运行时长 ' + up(ov.uptime_secs);
    document.getElementById('kpis').innerHTML =
      kpi(fmt(ov.topics), '主题数') +
      kpi(fmt(ov.partitions), '分区总数') +
      kpi(fmt(ov.total_produced), '累计生产（去重命中 ' + fmt(ov.dedup_hits) + '）') +
      kpi(ov.rates.produce_mps_1m.toFixed(0) + ' / ' + ov.rates.fetch_mps_1m.toFixed(0),
          '生产 / 消费 条每秒（1 分钟均值）');
    lineChart(document.getElementById('rateChart'), m.series);

    document.getElementById('topics').innerHTML = ts.topics.map(t => `
      <table style="margin-bottom:14px">
        <thead><tr><th colspan="5">主题 <code>${t.name}</code>
          <span class="tag">${t.partition_count} 分区</span></th></tr>
        <tr><th>分区</th><th>起始位点</th><th>最新位点</th><th>数据量</th><th>存储段</th></tr></thead>
        <tbody>${t.partitions.map(p => `
          <tr>
            <td>${p.partition}</td>
            <td>${fmt(p.log_start_offset)}</td>
            <td>${fmt(p.log_end_offset)}</td>
            <td>${bytes(p.total_bytes)}</td>
            <td>${p.segments.map(s => `
              <span class="tag ${s.active ? 'active':''}">
                ${fmt(s.base_offset)}–${fmt(s.end_offset)} · ${bytes(s.size_bytes)}
                ${s.active ? ' · 活动段' : ''}
              </span>`).join(' ')}
              <div class="bar"><i style="width:${Math.min(100, p.total_bytes/ (256*1024*1024) *100)}%"></i></div>
            </td>
          </tr>`).join('')}
        </tbody></table>`).join('') || '<p class="muted">暂无主题</p>';

    document.getElementById('groups').innerHTML = gs.groups.length ? gs.groups.map(g => `
      <table style="margin-bottom:14px">
        <thead><tr><th colspan="3">消费组 <code>${g.snapshot.name}</code>
          <span class="tag">generation ${g.snapshot.generation}</span>
          <span class="tag ${g.total_lag ? 'warn':'active'}">积压 ${fmt(g.total_lag)}</span>
        </th></tr><tr><th>消费者</th><th>分配分区</th><th>已提交位点</th></tr></thead>
        <tbody>${g.snapshot.members.map(mem => `
          <tr><td>${mem.consumer_id}</td>
          <td>${mem.assignments.map(a => `<span class="tag">P${a}</span>`).join(' ') || '<span class="muted">无</span>'}</td>
          <td>${mem.assignments.map(a => fmt(g.snapshot.committed[a]) ?? '-').join(', ')}</td>
          </tr>`).join('')}
        </tbody></table>`).join('') : '<p class="muted">暂无消费组</p>';
  } catch (e) {
    console.error(e);
  }
}

document.getElementById('newtopic').addEventListener('submit', async e => {
  e.preventDefault();
  const f = e.target;
  const body = {
    name: f.name.value,
    partitions: Number(f.partitions.value),
  };
  if (f.max_age_ms.value) body.max_age_ms = Number(f.max_age_ms.value);
  const r = await fetch('/api/topics', {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify(body),
  });
  const j = await r.json();
  if (!r.ok) alert(j.error);
  f.reset(); f.partitions.value = 4;
  refresh();
});

refresh();
setInterval(refresh, 2000);
</script>
</body>
</html>"##;
