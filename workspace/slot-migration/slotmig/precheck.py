"""Read-only resource precheck (只读资源预检) for candidate plan specs.

Before a migration plan is submitted, the operator runs this precheck with
the candidate spec (source slot, target slot, consumer group, table set) and
gets back availability, conflicting objects (with the conflicting plans'
states), schema versions and the target tables that would need cleanup.

Guarantees:

* strictly read-only — never creates a plan, never touches slots, fences or
  consumer watermarks;
* deterministic — the same input over unchanged state always produces the
  same report (no timestamps, no generated ids);
* clear errors — unknown cluster/slot (NOT_FOUND, 404), empty table set
  (BAD_REQUEST, 400) and non-empty target tables (TARGET_NOT_EMPTY, 409,
  with the cleanup list in details) each fail with a stable error code;
* the formal flow is unaffected — precheck only mirrors the checks that
  create_plan/freeze/start would perform, it never performs them.
"""
from __future__ import annotations

import html

from .common import BAD_REQUEST, NOT_FOUND, TARGET_NOT_EMPTY, MigrationError
from .reconcile import CSS

# Stable blocking-reason codes reported in the precheck report.
ACTIVE_PLAN_CONFLICT = "ACTIVE_PLAN_CONFLICT"
SOURCE_SLOT_NOT_ACTIVE = "SOURCE_SLOT_NOT_ACTIVE"
GROUP_BOUND_ELSEWHERE = "GROUP_BOUND_ELSEWHERE"


def run_precheck(clusters, store, spec: dict) -> dict:
    """Evaluate a candidate plan spec WITHOUT changing anything.

    Raises the clear error cases; otherwise returns the availability report.
    """
    if not isinstance(spec, dict):
        raise MigrationError(BAD_REQUEST, "precheck spec must be a JSON object")
    for key in ("source_cluster", "source_slot", "target_cluster",
                "target_slot", "tables"):
        if not spec.get(key):
            raise MigrationError(BAD_REQUEST, f"missing field: {key}")
    for c in (spec["source_cluster"], spec["target_cluster"]):
        if c not in clusters:
            raise MigrationError(NOT_FOUND, f"unknown cluster '{c}'",
                                 http_status=404)
    tables = sorted({t for t in spec["tables"] if isinstance(t, str) and t})
    if not tables:
        raise MigrationError(BAD_REQUEST, "tables must be a non-empty list")
    # Same defaulting rule as create_plan, so the conflict check covers
    # exactly the group the real plan would use.
    group_id = spec.get("consumer_group") or f"grp_{spec['source_slot']}"

    source = clusters[spec["source_cluster"]]
    target = clusters[spec["target_cluster"]]
    source_slot = source.get_slot(spec["source_slot"])  # NOT_FOUND if unknown
    target_slot = target.get_slot(spec["target_slot"])  # NOT_FOUND if unknown

    # Non-empty target tables are a hard error — say exactly what to clean.
    non_empty = [{"table": t, "rows": target.table_count(t)}
                 for t in tables if target.table_count(t) > 0]
    if non_empty:
        raise MigrationError(
            TARGET_NOT_EMPTY,
            "target tables are not empty; clean them up before submitting "
            "the plan",
            {"tables_to_cleanup": non_empty}, http_status=409)

    group = store.find_group(group_id)
    plans = store.find_active_conflicts(
        source_cluster=spec["source_cluster"], source_slot=spec["source_slot"],
        target_cluster=spec["target_cluster"], target_slot=spec["target_slot"],
        consumer_group=group_id)

    conflicts, blocking_reasons, conflicting_plans = [], [], []
    for p in plans:
        covers = []
        if (p.source_cluster, p.source_slot) == (spec["source_cluster"],
                                                 spec["source_slot"]):
            covers.append("source_slot")
        if (p.target_cluster, p.target_slot) == (spec["target_cluster"],
                                                 spec["target_slot"]):
            covers.append("target_slot")
        if p.consumer_group == group_id:
            covers.append("consumer_group")
        conflicting_plans.append({"plan_id": p.plan_id, "state": p.state,
                                  "covers": covers})
        conflicts.append({
            "type": "active_plan", "plan_id": p.plan_id, "state": p.state,
            "covers": covers,
            "detail": f"active plan {p.plan_id} ({p.state}) already covers "
                      f"{', '.join(covers)}"})
    if conflicting_plans:
        blocking_reasons.append(ACTIVE_PLAN_CONFLICT)

    if source_slot["status"] != "ACTIVE":
        blocking_reasons.append(SOURCE_SLOT_NOT_ACTIVE)
        conflicts.append({
            "type": "source_slot", "status": source_slot["status"],
            "detail": f"source slot is {source_slot['status']}; "
                      f"freeze requires ACTIVE"})

    binding_ok = True
    if group is not None and (group["cluster"], group["slot"]) != (
            spec["source_cluster"], spec["source_slot"]):
        binding_ok = False
        blocking_reasons.append(GROUP_BOUND_ELSEWHERE)
        conflicts.append({
            "type": "consumer_group", "group_id": group_id,
            "bound_to": {"cluster": group["cluster"], "slot": group["slot"]},
            "detail": f"consumer group is bound to "
                      f"{group['cluster']}/{group['slot']}, "
                      f"not the plan's source slot"})

    group_info = {"group_id": group_id, "exists": group is not None,
                  "binding_ok": binding_ok}
    if group is not None:
        group_info.update({"cluster": group["cluster"], "slot": group["slot"],
                           "epoch": group["epoch"],
                           "watermark": group["watermark"]})

    return {
        "available": not blocking_reasons,
        "blocking_reasons": blocking_reasons,
        "spec": {
            "source_cluster": spec["source_cluster"],
            "source_slot": spec["source_slot"],
            "target_cluster": spec["target_cluster"],
            "target_slot": spec["target_slot"],
            "consumer_group": group_id,
            "tables": tables,
        },
        "schema_versions": source.schema_versions(tables),
        "tables_to_cleanup": [],
        "conflicts": conflicts,
        "conflicting_plans": conflicting_plans,
        "source": {
            "cluster": spec["source_cluster"],
            "slot": source_slot,
            "current_lsn": source.current_lsn(),
        },
        "target": {
            "cluster": spec["target_cluster"],
            "slot": target_slot,
            "tables": [{"table": t, "rows": target.table_count(t),
                        "empty": True} for t in tables],
        },
        "consumer_group": group_info,
    }


# --------------------------------------------------------------------- HTML
_PAGE_CSS = CSS + """
input{width:100%;box-sizing:border-box;padding:6px 8px;border:1px solid #ced4da;
border-radius:6px;font-size:13px}
button{background:#1971c2;color:#fff;border:0;border-radius:6px;padding:8px 18px;
font-size:13.5px;font-weight:600;cursor:pointer}
button:hover{background:#1864ab}
.errbox{background:#fff5f5;border:1px solid #ffc9c9;border-radius:8px;
padding:12px 16px}
"""


def render_page() -> str:
    """The planning page with the precheck area (计划页预检区): a form for the
    candidate spec that calls POST /precheck and renders the report (or the
    error) client-side."""
    return f"""<!DOCTYPE html>
<html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>迁移预检 · 提交计划前的只读资源检查</title><style>{_PAGE_CSS}</style></head>
<body><header>
<h1>迁移预检 <span class="pill st-FROZEN">只读</span></h1>
<div class="sub">提交迁移计划前的资源预检 —— 不创建计划、不修改槽 / 围栏 /
消费者水位；同一输入再次请求结果一致 · JSON 接口 <code>POST /precheck</code></div>
</header><main>
<section><h2>候选计划规格</h2>
<form id="f" onsubmit="return run()">
<table>
<tr><th>源集群</th><td><input id="source_cluster" value="source"></td>
    <th>源槽</th><td><input id="source_slot" value="app_slot"></td></tr>
<tr><th>目标集群</th><td><input id="target_cluster" value="target"></td>
    <th>目标槽</th><td><input id="target_slot" value="app_slot_replica"></td></tr>
<tr><th>消费者组</th><td><input id="consumer_group"
    placeholder="留空默认为 grp_&lt;源槽&gt;"></td>
    <th>表集合（逗号分隔）</th><td><input id="tables" value="orders"></td></tr>
</table>
<p><button type="submit">运行预检</button>
<span class="muted">结果仅来自只读检查，不产生任何状态变更</span></p>
</form></section>
<section id="result" style="display:none"><h2>预检结果</h2>
<div id="out"></div></section>
</main>
<script>
const $ = id => document.getElementById(id);
const esc = s => String(s ?? '—').replace(/[&<>"]/g,
  c => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}}[c]));

async function run() {{
  const spec = {{
    source_cluster: $('source_cluster').value.trim(),
    source_slot: $('source_slot').value.trim(),
    target_cluster: $('target_cluster').value.trim(),
    target_slot: $('target_slot').value.trim(),
    tables: $('tables').value.split(',').map(s => s.trim()).filter(Boolean),
  }};
  const g = $('consumer_group').value.trim();
  if (g) spec.consumer_group = g;
  const resp = await fetch('/precheck', {{method: 'POST',
    headers: {{'Content-Type': 'application/json'}},
    body: JSON.stringify(spec)}});
  const data = await resp.json();
  $('result').style.display = 'block';
  $('out').innerHTML = resp.ok ? report(data) : error(data);
  return false;
}}

function error(e) {{
  let rows = '';
  const cleanup = (e.details || {{}}).tables_to_cleanup || [];
  if (cleanup.length) {{
    rows = '<table style="margin-top:8px"><tr><th>需要清理的目标表</th>'
      + '<th class="num">行数</th></tr>'
      + cleanup.map(t => `<tr><td><code>${{esc(t.table)}}</code></td>`
        + `<td class="num">${{t.rows}}</td></tr>`).join('') + '</table>';
  }}
  return `<div class="errbox"><p><span class="badge-warn">预检未通过</span>　`
    + `<b>${{esc(e.error)}}</b>：${{esc(e.message)}}</p>${{rows}}</div>`;
}}

function report(r) {{
  const badge = r.available
    ? '<span class="badge-ok">可提交</span>'
    : '<span class="badge-warn">暂不可提交</span>';
  const reasons = r.blocking_reasons.length
    ? '<ul>' + r.blocking_reasons.map(x => `<li><b>${{esc(x)}}</b></li>`).join('')
      + '</ul>' : '<span class="muted">无阻止条件</span>';
  const conflicts = r.conflicts.length
    ? '<table><tr><th>冲突对象</th><th>说明</th></tr>' + r.conflicts.map(c =>
        `<tr><td><code>${{esc(c.type)}}</code></td><td>${{esc(c.detail)}}</td></tr>`
      ).join('') + '</table>'
    : '<span class="muted">无冲突对象</span>';
  const plans = r.conflicting_plans.length
    ? '<table><tr><th>计划</th><th>状态</th><th>占用</th></tr>'
      + r.conflicting_plans.map(p =>
        `<tr><td><code>${{esc(p.plan_id)}}</code></td>`
        + `<td><span class="pill st-${{p.state}}">${{p.state}}</span></td>`
        + `<td>${{esc(p.covers.join(', '))}}</td></tr>`).join('') + '</table>'
    : '<span class="muted">无冲突计划</span>';
  const schemas = Object.entries(r.schema_versions).map(([t, v]) =>
    `<tr><td><code>${{esc(t)}}</code></td><td class="num">${{v}}</td></tr>`
  ).join('');
  const tables = r.target.tables.map(t =>
    `<tr><td><code>${{esc(t.table)}}</code></td><td class="num">${{t.rows}}</td>`
    + '<td class="ok">空</td></tr>').join('');
  const grp = r.consumer_group;
  const grpTxt = grp.exists
    ? `已存在，绑定 <code>${{esc(grp.cluster)}}/${{esc(grp.slot)}}</code>，`
      + `epoch=${{grp.epoch}}，水位=${{grp.watermark}}，`
      + (grp.binding_ok ? '<span class="ok">绑定匹配</span>'
                        : '<span class="bad">绑定到其他槽</span>')
    : '尚不存在（freeze 时自动创建）';
  return `<p>${{badge}}　${{reasons}}</p>`
    + `<h2>冲突对象</h2>${{conflicts}}`
    + `<h2>冲突计划（含状态）</h2>${{plans}}`
    + `<h2>模式版本（源）</h2><table><tr><th>表</th>`
    + `<th class="num">版本</th></tr>${{schemas}}</table>`
    + `<h2>目标表</h2><table><tr><th>表</th><th class="num">行数</th>`
    + `<th>结论</th></tr>${{tables}}</table>`
    + `<p class="muted">需要清理的目标表：`
    + `${{r.tables_to_cleanup.length ? esc(JSON.stringify(r.tables_to_cleanup)) : '无'}}`
    + `　·　消费者组：${{grpTxt}}</p>`
    + `<p class="muted">源位点 ${{r.source.current_lsn}} · 源槽 `
    + `${{esc(r.source.slot.status)}} · 目标槽 ${{esc(r.target.slot.status)}}</p>`;
}}
</script>
</body></html>"""
