"""Self-contained interactive HTML report for a bug found by detangle."""

from __future__ import annotations

import html
import json
from typing import TYPE_CHECKING

from ._version import __version__

if TYPE_CHECKING:
    from .explore import BugReport

_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>detangle report</title>
<style>
:root {
  --bg: #f7f7f5; --panel: #ffffff; --ink: #1d1d1f; --muted: #6b6b70; --line: #e3e3e0;
  --accent: #5b4bdb; --bad: #c62828; --bad-bg: #fdecec; --dev: #b25e00; --dev-bg: #fff4e0;
  --net: #0b7285; --ok: #2b8a3e; --code-bg: #f1f1ee; --on-accent: #ffffff;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #141416; --panel: #1c1c1f; --ink: #ececef; --muted: #9a9aa2; --line: #2e2e33;
    --accent: #9d92ff; --bad: #ff6b6b; --bad-bg: #3a1d1d; --dev: #ffb454; --dev-bg: #3a2c14;
    --net: #66d9e8; --ok: #69db7c; --code-bg: #26262a; --on-accent: #141416;
  }
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--ink);
  font: 14px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
main { max-width: 1400px; margin: 0 auto; padding: 24px 16px 64px; }
h1 { font-size: 20px; margin: 0 0 4px; }
h2 { font-size: 15px; margin: 28px 0 10px; text-transform: uppercase; letter-spacing: .06em; color: var(--muted); }
.sub { color: var(--muted); }
.card { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 16px; margin-top: 16px; }
.headline { color: var(--bad); font-weight: 600; font-size: 16px; white-space: pre-wrap; word-break: break-word; }
pre, code { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 12.5px; }
pre { background: var(--code-bg); padding: 12px; border-radius: 8px; overflow-x: auto; margin: 10px 0 0; }
.facts { display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px; margin-top: 14px; }
.fact { border: 1px solid var(--line); border-radius: 8px; padding: 8px 10px; }
.fact b { display: block; font-size: 18px; }
.fact span { color: var(--muted); font-size: 12px; }
.token { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; margin-top: 12px; }
.token code { background: var(--code-bg); padding: 6px 8px; border-radius: 6px; word-break: break-all; }
button { background: var(--accent); color: var(--on-accent); border: 0; border-radius: 6px; padding: 6px 10px; cursor: pointer; font: inherit; }
.controls { display: flex; gap: 14px; flex-wrap: wrap; align-items: center; margin-bottom: 10px; }
.controls input[type=search] { padding: 6px 8px; border-radius: 6px; border: 1px solid var(--line); background: var(--panel); color: var(--ink); min-width: 220px; }
.scroll { overflow-x: auto; border: 1px solid var(--line); border-radius: 10px; background: var(--panel); }
table { border-collapse: collapse; width: max-content; min-width: 100%; }
th, td { border-bottom: 1px solid var(--line); padding: 5px 8px; vertical-align: top; text-align: left; }
th { position: sticky; top: 0; background: var(--panel); font-size: 12px; color: var(--muted); z-index: 1; }
td.t, td.i { color: var(--muted); font-variant-numeric: tabular-nums; white-space: nowrap; }
td.lane { min-width: 160px; max-width: 360px; }
tr.dev td { background: var(--dev-bg); }
tr.bad td { background: var(--bad-bg); }
.ev { font-size: 12.5px; }
.verb { display: inline-block; font-size: 11px; font-weight: 600; border-radius: 4px; padding: 0 5px; margin-right: 4px;
  border: 1px solid var(--line); text-transform: uppercase; letter-spacing: .03em; }
.verb.raise, .verb.error { color: var(--bad); border-color: var(--bad); }
.verb.net { color: var(--net); border-color: var(--net); }
.verb.fault { color: var(--dev); border-color: var(--dev); }
.verb.return { color: var(--ok); border-color: var(--ok); }
.loc { color: var(--muted); font-size: 11.5px; display: block; }
.src { display: block; font-family: ui-monospace, Menlo, Consolas, monospace; font-size: 11.5px; white-space: pre-wrap; word-break: break-word; }
.ahead { color: var(--dev); font-size: 11.5px; display: block; }
footer { margin-top: 30px; color: var(--muted); font-size: 12px; }
</style>
</head>
<body>
<main>
  <h1>detangle found a bug</h1>
  <div class="sub" id="name"></div>
  <div class="card">
    <div class="headline" id="headline"></div>
    <div class="sub" id="location"></div>
    <pre id="details" hidden></pre>
    <div class="facts" id="facts"></div>
    <div class="token"><span>Replay token</span><code id="token"></code><button id="copy">Copy</button></div>
  </div>
  <h2>Interleaving</h2>
  <div class="controls">
    <label><input type="checkbox" id="shownet" checked> network events</label>
    <label><input type="checkbox" id="showcb" checked> callbacks</label>
    <label><input type="checkbox" id="onlydev"> deviations &amp; faults only</label>
    <input type="search" id="q" placeholder="filter (task, file, text)">
  </div>
  <div class="scroll"><table id="grid"></table></div>
  <footer>Generated by detangle __VERSION__. Rows highlighted in orange are points where the scheduler deviated from asyncio's default FIFO order, or where a fault was injected.</footer>
</main>
<script id="data" type="application/json">__DATA__</script>
<script>
(function () {
  const data = JSON.parse(document.getElementById('data').textContent);
  const $ = (id) => document.getElementById(id);
  const esc = (s) => String(s == null ? '' : s).replace(/[&<>"]/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
  $('name').textContent = data.name;
  $('headline').textContent = data.headline;
  $('location').textContent = data.location ? 'at ' + data.location : '';
  if (data.deadlock) { $('details').hidden = false; $('details').textContent = data.deadlock; }
  $('token').textContent = data.token;
  $('copy').onclick = () => navigator.clipboard && navigator.clipboard.writeText(data.token);
  const facts = [
    [data.kind, 'failure kind'], [data.deviations, "deviations from asyncio's default"],
    [data.runs, 'runs before failure'], [data.strategy, 'strategy'],
  ];
  $('facts').innerHTML = facts.map(([v, l]) => `<div class="fact"><b>${esc(v)}</b><span>${esc(l)}</span></div>`).join('');
  const trace = data.trace || {actors: [], events: []};
  const actors = trace.actors;
  function fmtTime(t) {
    if (t === 0) return '0';
    if (Math.abs(t) < 1) return (t * 1000).toFixed(3).replace(/\\.?0+$/, '') + 'ms';
    return t.toFixed(4).replace(/\\.?0+$/, '') + 's';
  }
  function render() {
    const showNet = $('shownet').checked, showCb = $('showcb').checked, onlyDev = $('onlydev').checked;
    const q = $('q').value.toLowerCase();
    const cols = actors.filter((a) => (showNet || a !== 'net') && (showCb || a !== 'callbacks'));
    let out = '<thead><tr><th>#</th><th>time</th>' + cols.map((a) => `<th>${esc(a)}</th>`).join('') + '</tr></thead><tbody>';
    for (const e of trace.events) {
      if (!showNet && e.actor === 'net') continue;
      if (!showCb && e.actor === 'callbacks') continue;
      if (onlyDev && !e.deviation.length && e.verb !== 'fault' && !/fragmented|dropped|partition|crash|cut|reset/.test(e.text)) continue;
      const loc = e.location ? `${e.location.file.split(/[\\\\/]/).slice(-2).join('/')}:${e.location.line}` : '';
      const hay = (e.actor + ' ' + e.text + ' ' + loc + ' ' + e.source).toLowerCase();
      if (q && !hay.includes(q)) continue;
      const cls = (e.deviation.length || e.verb === 'fault') ? 'dev' : (e.verb === 'raise' || e.verb === 'error') ? 'bad' : '';
      let cells = '';
      for (const a of cols) {
        if (a !== e.actor) { cells += '<td class="lane"></td>'; continue; }
        cells += `<td class="lane"><div class="ev"><span class="verb ${esc(e.verb)}">${esc(e.verb)}</span>${esc(e.text)}` +
          (loc ? `<span class="loc">${esc(loc)}${e.location.function ? ' in ' + esc(e.location.function) : ''}</span>` : '') +
          (e.source ? `<span class="src">${esc(e.source)}</span>` : '') +
          (e.deviation.length ? `<span class="ahead">ran ahead of ${esc(e.deviation.join(', '))}</span>` : '') +
          '</div></td>';
      }
      out += `<tr class="${cls}"><td class="i">${e.index}</td><td class="t">${fmtTime(e.time)}</td>${cells}</tr>`;
    }
    $('grid').innerHTML = out + '</tbody>';
  }
  for (const id of ['shownet', 'showcb', 'onlydev', 'q']) $(id).addEventListener('input', render);
  render();
})();
</script>
</body>
</html>
"""


def render_html(report: BugReport) -> str:
    payload = json.dumps(report.to_dict(), default=str)
    # Make the JSON safe to embed inside a <script> element.
    payload = payload.replace("</", "<\\/").replace("<!--", "<\\!--")
    return _TEMPLATE.replace("__VERSION__", html.escape(__version__)).replace("__DATA__", payload)
