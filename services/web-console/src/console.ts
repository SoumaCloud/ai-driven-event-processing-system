// console.ts — polls /api/overview every 2 s, /api/history every 5 s, and wires the demo.

import type {
  ArchiveListing,
  ConsoleConfig,
  CounterName,
  DiagnoseResult,
  ForecastResult,
  ForecasterInfo,
  GraphContext,
  GraphDependency,
  HistoryResponse,
  HistorySample,
  IncidentsInfo,
  Overview,
  Playbook,
  PredictiveAlert,
  ProduceResult,
  ScoreResult,
  ServiceCheck,
  Severity,
  SimulateResult,
  TailMessage,
  ThresholdResult,
  TopicInfo,
} from "./types.js";
import { $, $$, SVG_NS, api, byId, compact, errorMessage, esc, fmt, hhmmss, kb } from "./util.js";

function log(msg: string, bad = false): void {
  const li = document.createElement("li");
  if (bad) li.className = "bad";
  const t = document.createElement("time");
  t.textContent = hhmmss();
  const s = document.createElement("span");
  s.textContent = msg;
  li.append(t, s);
  $("#log").prepend(li);
}

// ── theme ───────────────────────────────────────────────────────────
$("#theme").addEventListener("click", () => {
  const root = document.documentElement;
  const dark = root.dataset.theme ? root.dataset.theme === "dark" : matchMedia("(prefers-color-scheme: dark)").matches;
  const next = dark ? "light" : "dark";
  root.dataset.theme = next;
  try {
    localStorage.setItem("ep-theme", next);
  } catch {
    /* private mode */
  }
  drawChart();
});

// ── nav: scroll-spy + breadcrumb ────────────────────────────────────
const crumbName: Record<string, string | undefined> = {
  overview: "Overview",
  flow: "Event flow",
  kafka: "Kafka",
  ai: "GraphRAG & TFT",
  services: "Containers",
  demo: "Guided demo",
};
const markNav = (id: string): void => {
  $$<HTMLAnchorElement>(".nav a[href^='#']").forEach((a) => a.classList.toggle("on", a.getAttribute("href") === "#" + id));
  $("#crumb").textContent = crumbName[id] ?? "Overview";
};
const spy = new IntersectionObserver(
  (entries) => {
    for (const e of entries) if (e.isIntersecting) markNav(e.target.id);
  },
  { rootMargin: "-40% 0px -55% 0px" },
);
Object.keys(crumbName).forEach((id) => {
  const el = document.getElementById(id);
  if (el) spy.observe(el);
});
window.addEventListener("scroll", () => { if (window.scrollY < 120) markNav("overview"); }, { passive: true });

// ── links ───────────────────────────────────────────────────────────
let links: ConsoleConfig["links"] = {};
async function loadConfig(): Promise<void> {
  const cfg = await api<ConsoleConfig>("/api/config");
  links = cfg.links;
  $$<HTMLAnchorElement>("[data-link]").forEach((a) => {
    a.href = (links[a.dataset.link ?? ""] || "#") + (a.dataset.suffix ?? "");
  });
  $("#v-bucket").textContent = `${cfg.s3 === "aws" ? "S3" : "MinIO"} · ${cfg.bucket}`;
}

// ── overview polling ────────────────────────────────────────────────
type CounterValues = Record<CounterName, number>;
/** An overview with null counters read as 0 (a fresh Prometheus counter has no samples yet). */
type Snapshot = Omit<Overview, "counters"> & { counters: CounterValues };

let prev: Snapshot | null = null;
let produced = 0;
const deltas: Record<string, { v: number; until: number } | undefined> = {};
const liveUntil: Record<string, number | undefined> = {};
const TOPIC_COLOR: Record<string, string | undefined> = { "raw-events": "--s1", "raw-events.DLT": "--s2", "normalized-events": "--s3" };
const TOPICS = ["raw-events", "normalized-events", "raw-events.DLT", "anomaly-audit", "incident-playbooks"];

// Writes a schematic number; a recent "+N" badge is re-appended so it survives polls.
function setText(id: string, value: string): void {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = value;
  const d = deltas[id];
  if (d && d.until > Date.now()) {
    const t = document.createElementNS(SVG_NS, "tspan");
    t.setAttribute("class", "n-delta");
    t.setAttribute("dx", "6");
    t.textContent = `+${compact(d.v)}`;
    el.appendChild(t);
  }
}
const flash = (id: string, delta: number): void => {
  if (delta > 0) deltas[id] = { v: delta, until: Date.now() + 5000 };
};
const topicMap = (ov: Pick<Overview, "kafka">): Record<string, TopicInfo | undefined> =>
  Object.fromEntries(ov.kafka.topics.map((t) => [t.name, t]));
const messages = (t: TopicInfo | undefined): number | null => (t?.exists ? t.messages : null);
const kpi = (id: string, value: string): void => {
  $(`#${id} .k-v span`).textContent = value;
};

type IconName = "kafka" | "db" | "ai" | "svc" | "chart";
const ICON: Record<IconName, string> = {
  kafka: '<circle cx="8" cy="3" r="1.6"/><circle cx="8" cy="13" r="1.6"/><circle cx="3" cy="8" r="1.6"/><circle cx="13" cy="8" r="1.6"/><path d="M8 4.6v6.8M4.6 8h6.8"/>',
  db: '<ellipse cx="8" cy="4" rx="5" ry="2"/><path d="M3 4v8c0 1.1 2.2 2 5 2s5-.9 5-2V4"/>',
  ai: '<circle cx="4" cy="4" r="2"/><circle cx="12" cy="5" r="2"/><circle cx="7" cy="12" r="2"/><path d="M6 4.3l4 .5M5 6l1.4 4M11 7l-3 3.5"/>',
  svc: '<rect x="2" y="3" width="12" height="10" rx="2"/><path d="M5 7h6M5 10h4"/>',
  chart: '<path d="M2 13h12M4 11V7M8 11V4M12 11V8"/>',
};
const SVC_ICON: Record<string, IconName | undefined> = {
  kafka: "kafka", zookeeper: "db", minio: "db", neo4j: "ai", ollama: "ai",
  forecasting: "ai", diagnostic: "ai", prometheus: "chart", grafana: "chart",
};
// Schematic node for each service whose health is drawn on the flow diagram.
const NODE_OF: Record<string, string> = {
  ingestion: "n-ingestion", diagnostic: "n-diagnostic", archiver: "n-archiver", minio: "n-minio",
  grafana: "n-grafana", prometheus: "n-prometheus", forecasting: "n-forecasting",
};

function render(ov: Overview): void {
  const svcs: Record<string, ServiceCheck | undefined> = Object.fromEntries(ov.services.map((s) => [s.id, s]));
  const up = ov.services.filter((s) => s.ok).length;
  const down = ov.services.length - up;
  $("#status").className = "status " + (down ? "bad" : "ok");
  $("#status-t").textContent = down ? `${down} container${down > 1 ? "s" : ""} down` : "All systems operational";
  $("#env-up").textContent = `${up}/${ov.services.length} healthy`;
  const kafkaOk = ov.kafka.ok && !!svcs.kafka?.ok;
  const tm = topicMap(ov);
  const before = prev ? topicMap(prev) : {};
  const dt = prev ? Math.max(ov.time - prev.time, 0.5) : null;
  const c = Object.fromEntries(Object.entries(ov.counters).map(([k, v]) => [k, v ?? 0])) as CounterValues;
  const pc = prev?.counters;
  const fc: ForecasterInfo = ov.forecaster ?? {};

  $("#sub").textContent = `${ov.kafka.topics.filter((t) => t.exists).length} Kafka topics · ${new Set(ov.kafka.groups.map((g) => g.group)).size} consumer groups · ${ov.services.length} containers · forecaster: ${fc.forecaster || "–"}`;

  // containers
  const maxMs = Math.max(1, ...ov.services.map((s) => s.ms));
  $("#svc-table tbody").innerHTML = ov.services.map((s) => {
    const link = links[s.id];
    const port = s.port == null ? "–" : link ? `<a href="${esc(link)}" target="_blank" rel="noopener">${s.port}</a>` : String(s.port);
    return `<tr>
      <td><span class="svc-name"><span class="svc-ic"><svg viewBox="0 0 16 16">${ICON[SVC_ICON[s.id] ?? "svc"]}</svg></span>${esc(s.name)}</span></td>
      <td class="dim">${esc(s.role)}</td>
      <td><span class="pill ${s.ok ? "" : "bad"}">${s.ok ? "Healthy" : "Down"}</span></td>
      <td class="r">${port}</td>
      <td class="r"><span class="ms-bar" style="width:${Math.round((s.ms / maxMs) * 40)}px"></span>${s.ms.toFixed(0)} ms</td>
      <td class="dim mono">${esc(s.detail)}</td></tr>`;
  }).join("");

  // topics + groups
  $("#topic-table tbody").innerHTML = ov.kafka.topics.map((t) => {
    if (!t.exists) return `<tr><td><span class="topic-name"><i class="none"></i>${esc(t.name)}</span></td><td colspan="3" class="dim">not created</td></tr>`;
    const b = messages(before[t.name]);
    const rate = b != null && dt ? (t.messages - b) / dt : null;
    const col = TOPIC_COLOR[t.name];
    const parts = Array.from({ length: Math.min(t.partitions, 12) }, () => "<i></i>").join("");
    return `<tr><td><span class="topic-name"><i class="${col ? "" : "none"}" style="${col ? `background:var(${col})` : ""}"></i>${esc(t.name)}</span></td>
      <td><span class="parts">${parts}</span><span>${t.partitions}</span></td>
      <td class="r">${fmt(t.messages)}</td><td class="r">${rate == null ? "–" : fmt(rate)}</td></tr>`;
  }).join("") || `<tr><td colspan="4" class="dim">${esc(ov.kafka.error || "broker unreachable")}</td></tr>`;
  const maxLag = Math.max(1, ...ov.kafka.groups.map((g) => g.lag ?? 0));
  $("#group-table tbody").innerHTML = ov.kafka.groups.map((g) => `<tr>
    <td class="mono">${esc(g.group)}</td><td class="dim mono">${esc(g.topic || "not joined yet")}</td>
    <td class="r">${g.lag == null ? "–" : `<span class="lagbar" style="width:${Math.round((g.lag / maxLag) * 48)}px"></span>${fmt(g.lag)}`}</td></tr>`).join("");

  // flow numbers
  for (const name of TOPICS) {
    const now = messages(tm[name]);
    const was = messages(before[name]);
    if (now != null && was != null) flash(`v-${name}`, now - was);
    setText(`v-${name}`, now != null ? `${compact(now)} msgs` : "not created");
  }
  const ingested = c.ingest_valid + c.ingest_invalid;
  if (pc) flash("v-ingest", ingested - (pc.ingest_valid + pc.ingest_invalid));
  setText("v-ingest", compact(ingested));
  setText("v-ingest-sub", `${compact(c.ingest_valid)} ok · ${compact(c.ingest_invalid)} to DLT`);
  if (pc) flash("v-scored", c.scored - pc.scored);
  setText("v-scored", compact(c.scored));
  setText("v-scored-sub", `scored · ${fmt(c.anomalies)} anomalies`);
  if (pc) flash("v-archived", c.archived_records - pc.archived_records);
  setText("v-archived", compact(c.archived_records));
  setText("v-archived-sub", `records · ${fmt(c.archived_objects)} files`);
  setText("v-objects", `${fmt(c.archived_objects)} files`);
  setText("v-produced", fmt(produced));
  setText("v-alerts", ov.alerts ? `${ov.alerts.active} open` : "–");
  setText("v-playbooks", `${fmt(c.playbooks)} made`);
  const ollama = svcs.ollama;
  setText("v-model", ollama?.ok ? (ollama.detail.split(",")[0] ?? "").slice(0, 13) : "offline");
  setText("v-llm", c.diagnoses_ok || c.diagnoses_fallback ? `${fmt(c.diagnoses_ok)} by LLM · ${fmt(c.diagnoses_fallback)} by rules` : "");
  const mv = fc.model_version || fc.forecaster || "–";
  setText("v-forecaster", mv.length > 21 ? mv.slice(0, 20) + "…" : mv);

  // health dots
  for (const [id, node] of Object.entries(NODE_OF)) {
    const ok = !!svcs[id]?.ok;
    const g = byId<SVGGElement>(node);
    g.classList.toggle("down", !ok);
    g.querySelector(".hdot")?.classList.toggle("bad", !ok);
  }
  $("#h-neo4j").classList.toggle("bad", !svcs.neo4j?.ok);
  $("#h-ollama").classList.toggle("bad", !svcs.ollama?.ok);
  for (const name of TOPICS) byId(`n-${name}`).classList.toggle("down", !kafkaOk);

  // animate links whose topic moved recently (kept on for 6 s so a burst is visible)
  const now = Date.now();
  for (const t of ov.kafka.topics) {
    const was = messages(before[t.name]);
    if (t.exists && was != null && t.messages > was) liveUntil[t.name] = now + 6000;
  }
  if (pc && c.archived_records > pc.archived_records) liveUntil.archive = now + 6000;
  if (ov.alerts && prev?.alerts && ov.alerts.active > prev.alerts.active) liveUntil.alerts = now + 6000;
  $$<SVGElement>(".link[data-topic]").forEach((p) => p.classList.toggle("live", (liveUntil[p.dataset.topic ?? ""] ?? 0) > now));

  // KPIs
  const total = ov.kafka.topics.reduce((a, t) => a + (messages(t) ?? 0), 0);
  kpi("k-msgs", compact(total));
  kpi("k-dlt", ingested ? ((100 * c.ingest_invalid) / ingested).toFixed(1) : "–");
  kpi("k-anom", fmt(c.anomalies));
  kpi("k-pb", fmt(c.playbooks));
  const lag = ov.kafka.groups.reduce((a, g) => a + (g.lag ?? 0), 0);
  kpi("k-lag", fmt(lag));
  $("#k-lag").classList.toggle("alert", lag > 10000);

  const active = ov.alerts?.active;
  const sev: Partial<Record<Severity, number>> = ov.alerts?.by_severity ?? {};
  $("#tft-facts").innerHTML = `
    <div><dt>Forecaster</dt><dd>${esc(fc.forecaster || "–")}</dd></div>
    <div><dt>Model version</dt><dd title="${esc(fc.model_version || "")}">${esc(fc.model_version || "–")}</dd></div>
    <div><dt>Open alerts</dt><dd class="${active ? "bad" : "ok"}">${active ?? "–"} <small>${active ? `${(sev.critical ?? 0) + (sev.high ?? 0)} high or critical` : ""}</small></dd></div>`;
  prev = { ...ov, counters: c };
}

async function refresh(): Promise<void> {
  try {
    const t0 = performance.now();
    render(await api<Overview>("/api/overview"));
    $("#live").textContent = `live · ${Math.round(performance.now() - t0)} ms`;
  } catch {
    $("#status").className = "status bad";
    $("#status-t").textContent = "Console API unreachable";
  }
}
async function poll(): Promise<void> {
  await refresh();
  setTimeout(poll, 2000);
}

// ── history: sparklines + throughput chart ──────────────────────────
type Point = [t: number, v: number];
type Picker = (s: HistorySample) => number | null | undefined;

let samples: HistorySample[] = [];

function rateSeries(list: HistorySample[], pick: Picker): Point[] {
  const out: Point[] = [];
  for (let i = 1; i < list.length; i++) {
    const s0 = list[i - 1]!, s1 = list[i]!;
    const a = pick(s0), b = pick(s1);
    const dt = s1.t - s0.t;
    if (a == null || b == null || dt <= 0) continue;
    out.push([s1.t, Math.max(0, (b - a) / dt)]);
  }
  return out;
}
const levelSeries = (list: HistorySample[], pick: Picker): Point[] =>
  list.flatMap((s): Point[] => {
    const v = pick(s);
    return v == null ? [] : [[s.t, v]];
  });
const sumTopics = (s: HistorySample): number => Object.values(s.topics).reduce<number>((a, b) => a + (b ?? 0), 0);

function spark(id: string, pts: Point[], note: string | null): void {
  const svg = $<SVGSVGElement>(`#${id} .spark`);
  if (note != null) $(`#${id} .k-d`).innerHTML = note;
  const tail = pts.slice(-36);
  if (tail.length < 2) {
    svg.innerHTML = "";
    return;
  }
  const W = 120, H = 32;
  const ys = tail.map((p) => p[1]);
  const lo = Math.min(...ys), hi = Math.max(...ys);
  const span = hi - lo || 1;
  const x = (i: number): number => (i / (tail.length - 1)) * W;
  const y = (v: number): number => H - 3 - ((v - lo) / span) * (H - 8);
  const path = tail.map((p, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(p[1]).toFixed(1)}`).join("");
  svg.innerHTML = `<path class="sa" d="${path}L${W},${H}L0,${H}Z"/><path class="sl" d="${path}"/><circle class="se" cx="${W}" cy="${y(ys[ys.length - 1]!).toFixed(1)}" r="3"/>`;
}

async function loadHistory(): Promise<void> {
  try {
    samples = (await api<HistoryResponse>("/api/history")).samples ?? [];
  } catch {
    samples = [];
  }
  const tp = rateSeries(samples, (s) => s.topics["raw-events"]);
  const lastTp = tp.at(-1)?.[1];
  kpi("k-tp", lastTp == null ? "–" : lastTp >= 10 ? fmt(lastTp) : lastTp.toFixed(1));
  const first = samples.at(0), last = samples.at(-1);
  const mins = first && last ? Math.max(1, Math.round((last.t - first.t) / 60)) : 15;
  const since = (pick: Picker): number | null => {
    const a = first && pick(first), b = last && pick(last);
    return a != null && b != null ? b - a : null;
  };
  const plus = (v: number | null): string | null =>
    v == null ? null : v > 0 ? `<span class="up">+${compact(v)}</span> in ${mins} min` : `no change in ${mins} min`;
  spark("k-tp", tp, `raw-events · peak ${tp.length ? fmt(Math.max(...tp.map((p) => p[1]))) : "–"}/s`);
  spark("k-msgs", levelSeries(samples, sumTopics), plus(since(sumTopics)));
  spark("k-dlt", rateSeries(samples, (s) => s.topics["raw-events.DLT"]), null);
  spark("k-anom", levelSeries(samples, (s) => s.anomalies), plus(since((s) => s.anomalies)));
  spark("k-pb", levelSeries(samples, (s) => s.playbooks), plus(since((s) => s.playbooks)));
  spark("k-lag", levelSeries(samples, (s) => s.lag), null);
  drawChart();
}

// Throughput chart: one axis (msg/s), three topics, crosshair + tooltip, table twin.
interface Series {
  key: string;
  label: string;
  color: string;
}
interface Plotted extends Series {
  pts: Point[];
}
const SERIES: Series[] = [
  { key: "raw-events", label: "raw-events", color: "--s1" },
  { key: "raw-events.DLT", label: "raw-events.DLT", color: "--s2" },
  { key: "normalized-events", label: "normalized-events", color: "--s3" },
];
let chartGeom: { data: Plotted[]; x: (t: number) => number; y: (v: number) => number } | null = null;

const plotted = (): Plotted[] => SERIES.map((s) => ({ ...s, pts: rateSeries(samples, (h) => h.topics[s.key]) }));

function niceMax(v: number): number {
  if (v <= 0) return 1;
  const p = 10 ** Math.floor(Math.log10(v));
  return [1, 2, 2.5, 5, 10].map((m) => m * p).find((m) => m >= v) ?? v;
}

function drawChart(): void {
  const svg = $<SVGSVGElement>("#tp-svg");
  const box = $("#tp-chart").getBoundingClientRect();
  if (!box.width) return;
  const W = box.width, H = box.height;
  const data = plotted();
  const base = data[0]!.pts;
  const n = base.length;
  $("#tp-empty").hidden = n >= 3;
  if (n < 3) {
    svg.innerHTML = "";
    chartGeom = null;
    return;
  }
  const L = 44, R = 8, T = 8, B = 22;
  const t0 = base[0]![0], t1 = base[n - 1]![0];
  const max = niceMax(Math.max(...data.flatMap((d) => d.pts.map((p) => p[1]))));
  const x = (t: number): number => L + ((t - t0) / Math.max(1, t1 - t0)) * (W - L - R);
  const y = (v: number): number => T + (1 - v / max) * (H - T - B);
  const line = (pts: Point[]): string => pts.map((p, i) => `${i ? "L" : "M"}${x(p[0]).toFixed(1)},${y(p[1]).toFixed(1)}`).join("");
  svg.setAttribute("viewBox", `0 0 ${W} ${H}`);
  let out = "";
  for (let i = 0; i <= 4; i++) {
    const v = (max / 4) * i;
    out += `<line class="grid" x1="${L}" x2="${W - R}" y1="${y(v)}" y2="${y(v)}"/><text class="axis" x="${L - 8}" y="${y(v) + 4}" text-anchor="end">${max < 10 ? +v.toFixed(2) : compact(v)}</text>`;
  }
  for (let i = 0; i <= 4; i++) {
    const t = t0 + ((t1 - t0) / 4) * i;
    out += `<text class="axis" x="${x(t)}" y="${H - 4}" text-anchor="${i === 0 ? "start" : i === 4 ? "end" : "middle"}">${new Date(t * 1000).toISOString().slice(11, 16)}</text>`;
  }
  out += `<path class="wash" fill="var(${data[0]!.color})" d="${line(base)}L${x(t1)},${y(0)}L${x(t0)},${y(0)}Z"/>`;
  for (const d of data) out += `<path class="ln" stroke="var(${d.color})" d="${line(d.pts)}"/>`;
  out += `<g id="xh" visibility="hidden"><line class="xh" y1="${T}" y2="${H - B}"/>${data.map((d) => `<circle class="dot" r="4" fill="var(${d.color})"/>`).join("")}</g>`;
  out += `<rect id="hit" x="${L}" y="${T}" width="${W - L - R}" height="${H - T - B}" fill="transparent"/>`;
  svg.innerHTML = out;
  chartGeom = { data, x, y };
  const hit = $<SVGRectElement>("#hit");
  hit.addEventListener("pointermove", onHover);
  hit.addEventListener("pointerleave", () => {
    $<SVGGElement>("#xh").setAttribute("visibility", "hidden");
    $("#tp-tip").hidden = true;
  });
}

function onHover(ev: PointerEvent): void {
  if (!chartGeom) return;
  const { data, x, y } = chartGeom;
  const base = data[0]!.pts;
  const rect = $<SVGSVGElement>("#tp-svg").getBoundingClientRect();
  const px = ev.clientX - rect.left;
  let best = 0, bestD = Infinity;
  base.forEach((p, i) => {
    const d = Math.abs(x(p[0]) - px);
    if (d < bestD) {
      bestD = d;
      best = i;
    }
  });
  const t = base[best]![0];
  const xh = $<SVGGElement>("#xh");
  xh.setAttribute("visibility", "visible");
  const cross = $<SVGLineElement>("line", xh);
  cross.setAttribute("x1", String(x(t)));
  cross.setAttribute("x2", String(x(t)));
  $$<SVGCircleElement>("circle", xh).forEach((c, i) => {
    const p = data[i]?.pts[best];
    c.setAttribute("cx", String(x(t)));
    c.setAttribute("cy", String(p ? y(p[1]) : -10));
  });
  const tip = $("#tp-tip");
  tip.replaceChildren();
  const tt = document.createElement("div");
  tt.className = "tt";
  tt.textContent = `${new Date(t * 1000).toISOString().slice(11, 19)} UTC`;
  tip.append(tt);
  for (const d of data) {
    const row = document.createElement("div");
    row.className = "tr";
    const key = document.createElement("i");
    key.style.background = `var(${d.color})`;
    const val = document.createElement("b");
    const p = d.pts[best];
    val.textContent = p ? `${p[1] >= 10 ? fmt(p[1]) : p[1].toFixed(1)}/s` : "–";
    const name = document.createElement("span");
    name.textContent = d.label;
    row.append(key, val, name);
    tip.append(row);
  }
  tip.hidden = false;
  const right = x(t) + 12 + tip.offsetWidth > rect.width;
  tip.style.left = `${right ? x(t) - tip.offsetWidth - 12 : x(t) + 12}px`;
  tip.style.top = "6px";
}

function renderTable(): void {
  const data = plotted();
  const base = data[0]!.pts;
  if (!base.length) {
    $("#tp-table").innerHTML = '<p class="empty">No samples yet.</p>';
    return;
  }
  const rows = base
    .map((p, i) => `<tr><td class="num">${new Date(p[0] * 1000).toISOString().slice(11, 19)}</td>${data.map((d) => `<td class="r">${d.pts[i] ? d.pts[i][1].toFixed(1) : "–"}</td>`).join("")}</tr>`)
    .reverse()
    .join("");
  $("#tp-table").innerHTML = `<table class="tbl"><thead><tr><th>UTC</th>${data.map((d) => `<th class="r">${esc(d.label)} /s</th>`).join("")}</tr></thead><tbody>${rows}</tbody></table>`;
}
$<HTMLButtonElement>("#tp-table-btn").addEventListener("click", (e) => {
  const btn = e.currentTarget as HTMLButtonElement;
  const on = btn.getAttribute("aria-pressed") !== "true";
  btn.setAttribute("aria-pressed", String(on));
  $("#tp-table").hidden = !on;
  $("#tp-chart").hidden = on;
  if (on) renderTable();
  else drawChart();
});

// ── GraphRAG playbooks + predictive alerts panels ───────────────────
type PlaybookView = Pick<Playbook, "component_id" | "summary" | "generated_by" | "generated_at" | "root_cause_hypothesis" | "steps" | "alert_id">;

function playbookHtml(pb: PlaybookView, open = false): string {
  const llm = !String(pb.generated_by).startsWith("fallback");
  const steps = (pb.steps ?? []).map((s) => `<li><b>${esc(s.title)}</b>${s.command ? `<code>${esc(s.command)}</code>` : ""}<div class="exp">${esc(s.expected_outcome)}</div></li>`).join("");
  return `<details ${open ? "open" : ""}><summary>
      <span class="pb-top"><span class="pb-comp">${esc(pb.component_id)}</span><span class="tag ${llm ? "llm" : ""}">${esc(llm ? pb.generated_by : "rule-based fallback")}</span>${pb.alert_id ? `<span class="tag">${esc(pb.alert_id)}</span>` : ""}</span>
      <span class="pb-time">${esc((pb.generated_at || "").slice(11, 19))} UTC</span>
      <span class="pb-sum">${esc(pb.summary)}</span></summary>
    <p class="hyp"><b>Root cause:</b> ${esc(pb.root_cause_hypothesis)}</p><ol class="steps-ol">${steps}</ol></details>`;
}
async function loadPlaybooks(): Promise<void> {
  try {
    const [pbs, inc] = await Promise.all([api<Playbook[]>("/api/playbooks?limit=6"), api<IncidentsInfo>("/api/incidents")]);
    const openIds = new Set($$<HTMLDetailsElement>("#pb-list details[open]").map((d) => d.dataset.id));
    $("#pb-list").innerHTML = pbs.length
      ? pbs.map((p) => `<li>${playbookHtml(p, openIds.has(p.playbook_id)).replace("<details", `<details data-id="${esc(p.playbook_id)}"`)}</li>`).join("")
      : '<li class="empty">No playbooks yet. Publish some events, or run step 4 of the demo.</li>';
    $("#rag-sub").textContent = `Graph: ${inc.graph_backend} · LLM: ${inc.llm} · automatic on anomalies: ${inc.auto_diagnose ? "on" : "off"}`;
  } catch (e) {
    $("#pb-list").innerHTML = `<li class="err">${esc(errorMessage(e))}</li>`;
  }
}
async function renderAlerts(): Promise<void> {
  const out = $("#alert-list");
  try {
    const list = await api<PredictiveAlert[]>("/api/alerts");
    if (!list.length) {
      out.innerHTML = '<p class="empty">None.</p>';
      return;
    }
    out.innerHTML = list.slice(0, 6).map((a) => {
      const mins = Math.round((new Date(a.predicted_breach_time).getTime() - Date.now()) / 60000);
      return `<div class="alert-row"><span class="sev ${esc(a.severity)}">${esc(a.severity)}</span>
        <div><b>${esc(a.component_id)}</b> · ${esc(a.metric_name)} → ${Number(a.predicted_value).toFixed(3)} (threshold ${a.threshold})
          <div class="small">${esc(a.alert_id)} · breach ${mins >= 0 ? `in ${mins} min` : `${-mins} min ago`} · ${esc(a.model_version)} · ${a.playbook_id ? `playbook ${esc(a.playbook_id)}` : "playbook pending"}</div></div>
        <div>${a.status === "open" ? `<button class="mini" data-ack="${esc(a.alert_id)}">Ack</button>` : ""}<button class="mini" data-resolve="${esc(a.alert_id)}">Resolve</button></div></div>`;
    }).join("");
  } catch (e) {
    out.innerHTML = `<p class="err">${esc(errorMessage(e))}</p>`;
  }
}
$("#alert-list").addEventListener("click", async (e) => {
  const b = e.target instanceof Element ? e.target.closest<HTMLButtonElement>("button") : null;
  if (!b) return;
  const [action, id] = b.dataset.ack ? ["ack", b.dataset.ack] : ["resolve", b.dataset.resolve ?? ""];
  try {
    await api(`/api/alerts/${id}/${action}`, { method: "POST" });
    log(`${action} ${id}`);
  } catch (err) {
    log(`${action} ${id} failed: ${errorMessage(err)}`, true);
  }
  void renderAlerts();
});

// ── guided demo ─────────────────────────────────────────────────────
function showStep(n: string): void {
  $$("#steps button").forEach((b) => b.classList.toggle("on", b.dataset.step === n));
  $$(".pane article").forEach((a) => a.classList.toggle("on", a.dataset.pane === n));
  if (n === "4") showGraph().catch(() => {});
}
$$("#steps button").forEach((b) => b.addEventListener("click", () => showStep(b.dataset.step ?? "1")));
$$("[data-goto]").forEach((a) => a.addEventListener("click", () => showStep(a.dataset.goto ?? "1")));
const doneStep = (n: number): void => $(`#steps button[data-step="${n}"]`).classList.add("done");

// Segmented controls: `seg[name]` holds the picked value; a "pick" CustomEvent fires on change.
const seg: Record<string, string | undefined> = {};
$$(".seg").forEach((s) => {
  const name = s.dataset.name ?? "";
  const on = s.querySelector<HTMLButtonElement>("button.on");
  if (on) seg[name] = on.dataset.v;
  s.addEventListener("click", (e) => {
    const b = e.target instanceof Element ? e.target.closest<HTMLButtonElement>("button") : null;
    if (!b) return;
    $$("button", s).forEach((x) => x.classList.toggle("on", x === b));
    seg[name] = b.dataset.v;
    s.dispatchEvent(new CustomEvent<string>("pick", { detail: b.dataset.v ?? "" }));
  });
});
const onPick = (selector: string, fn: (value: string) => void): void =>
  $(selector).addEventListener("pick", (e) => fn((e as CustomEvent<string>).detail));

/** Wires a button that runs `fn`, disabled while it runs. */
function action(selector: string, fn: () => Promise<void>): void {
  const btn = $<HTMLButtonElement>(selector);
  btn.addEventListener("click", async () => {
    btn.disabled = true;
    try {
      await fn();
    } finally {
      btn.disabled = false;
    }
  });
}
const errHtml = (e: unknown): string => `<p class="err">${esc(errorMessage(e))}</p>`;
/** Values are HTML; escape anything that came from the API before passing it in. */
const facts = (pairs: [label: string, html: string, cls?: string][]): string =>
  `<dl class="facts" style="grid-template-columns:repeat(${pairs.length},minmax(0,1fr))">${pairs.map(([k, v, cls = ""]) => `<div><dt>${esc(k)}</dt><dd class="${cls}">${v}</dd></div>`).join("")}</dl>`;

// 1 · send traffic
async function produce(count: number, invalid: number): Promise<ProduceResult> {
  const r = await api<ProduceResult>("/api/produce", { method: "POST", body: { count, invalid_ratio: invalid } });
  produced += r.delivered;
  setTimeout(refresh, 300); // show the counters move without waiting for the next poll
  log(`published ${r.sent} events (${r.invalid} broken) to raw-events, run ${r.run_id}`);
  return r;
}
action("#quick-produce", async () => {
  try {
    await produce(300, 0.1);
  } catch (err) {
    log(`publish failed: ${errorMessage(err)}`, true);
  }
});
action("#btn-produce", async () => {
  const out = $("#out-produce");
  try {
    const r = await produce(Number(seg.count), Number(seg.invalid));
    out.innerHTML = facts([["Sent", fmt(r.sent)], ["Valid", fmt(r.sent - r.invalid)], ["Broken", fmt(r.invalid), r.invalid ? "bad" : ""], ["Broker acks", fmt(r.delivered), r.failed ? "bad" : "ok"], ["Took", `${r.seconds}s`]])
      + (r.sample ? `<p class="hint" style="margin:12px 0 6px">One of the valid events, as sent:</p><pre class="json">${esc(JSON.stringify(r.sample, null, 2))}</pre>` : "");
    doneStep(1);
  } catch (err) {
    out.innerHTML = errHtml(err);
    log(`publish failed: ${errorMessage(err)}`, true);
  }
});

// 2 · inside Kafka
action("#btn-tail", async () => {
  const out = $("#out-tail");
  const topic = seg.topic ?? "raw-events";
  try {
    const msgs = await api<TailMessage[]>(`/api/topics/${encodeURIComponent(topic)}/tail?n=8`);
    if (!msgs.length) {
      out.innerHTML = `<p class="hint">${esc(topic)} is empty.</p>`;
      return;
    }
    out.innerHTML = `<ul class="msgs">${msgs.map((m) => {
      const why = m.headers["x-error-reason"] || m.headers["kafka_dlt-exception-message"];
      const headers = Object.keys(m.headers).length;
      return `<li><div class="meta"><span>partition ${m.partition}</span><span>offset ${fmt(m.offset)}</span><span>${new Date(m.timestamp).toISOString().replace("T", " ").slice(0, 19)} UTC</span>${headers ? `<span>${headers} headers</span>` : ""}</div>
        ${why ? `<div class="why">reason: ${esc(why)}</div>` : ""}<pre class="json">${esc(typeof m.value === "string" ? m.value : JSON.stringify(m.value, null, 2))}</pre></li>`;
    }).join("")}</ul>`;
    log(`read ${msgs.length} messages from ${topic}`);
    doneStep(2);
  } catch (err) {
    out.innerHTML = errHtml(err);
    log(`read ${topic} failed: ${errorMessage(err)}`, true);
  }
});

// 3 · anomaly
/** The last anomalous score, handed to GraphRAG in step 4 as evidence. */
interface AnomalyEvidence {
  kind: "event_anomaly";
  metric_name: string;
  observed_value: number;
  score: number;
  threshold: number;
  severity: Severity;
  baseline_ms: string;
}
let lastAnomaly: AnomalyEvidence | null = null;
const lat = $<HTMLInputElement>("#lat");
const syncLat = (): void => {
  $("#lat-val").textContent = `${lat.value} ms`;
};
lat.addEventListener("input", () => {
  syncLat();
  $$('.seg[data-name="preset"] button').forEach((b) => b.classList.remove("on"));
});
onPick('.seg[data-name="preset"]', (v) => {
  lat.value = v;
  syncLat();
});
action("#btn-score", async () => {
  const out = $("#out-score");
  try {
    const r = await api<ScoreResult>("/api/anomaly/score", { method: "POST", body: { latency_ms: Number(lat.value) } });
    const pct = (v: number): string => `${(v * 100).toFixed(1)}%`;
    if (r.anomalous) {
      lastAnomaly = {
        kind: "event_anomaly", metric_name: r.reason || "latency_ms", observed_value: r.latency_ms, score: r.score, threshold: r.threshold,
        severity: r.score >= 0.99 ? "critical" : r.score >= 0.95 ? "high" : "medium", baseline_ms: `${r.baseline.mean} ± ${r.baseline.stdev}`,
      };
    }
    out.innerHTML = facts([["Latency", `${fmt(r.latency_ms)} ms`], ["Baseline", `${r.baseline.mean} ± ${r.baseline.stdev} ms`], ["Score", r.score.toFixed(3), r.anomalous ? "bad" : "ok"], ["Verdict", r.anomalous ? "Anomaly" : "Normal", r.anomalous ? "bad" : "ok"], ["Audit topic", r.audited ? "published" : "–"]])
      + `<div class="gauge"><div class="fill ${r.anomalous ? "hot" : ""}" style="width:${pct(r.score)}"></div><div class="thr" style="left:${pct(r.threshold)}" data-l="threshold ${r.threshold}"></div></div>
      <div class="gauge-scale"><span>0 · normal</span><span>1 · certainly anomalous</span></div>
      <p class="hint" style="margin-top:12px">Model <code>${esc(r.model_version)}</code>${r.reason ? `, driven by <code>${esc(r.reason)}</code>` : ""}.${r.anomalous ? " Go to step 4 and GraphRAG will explain it, with this anomaly as evidence." : ""}</p>`;
    log(`scored ${r.latency_ms} ms → ${r.score.toFixed(3)} (${r.anomalous ? "anomaly" : "normal"})`);
    doneStep(3);
  } catch (err) {
    out.innerHTML = errHtml(err);
    log(`scoring failed: ${errorMessage(err)}`, true);
  }
});
const markThreshold = (v: number): void =>
  $$("#seg-threshold button").forEach((b) => b.classList.toggle("on", Math.abs(Number(b.dataset.v) - v) < 1e-9));
onPick("#seg-threshold", async (v) => {
  try {
    const r = await api<ThresholdResult>("/api/anomaly/threshold", { method: "PUT", body: { threshold: Number(v) } });
    markThreshold(r.threshold);
    log(`anomaly threshold set to ${r.threshold}, live`);
  } catch (err) {
    log(`threshold change failed: ${errorMessage(err)}`, true);
  }
});

// 4 · GraphRAG. Component ids match diagnostic-service/data/dependency_graph.json.
const GRAPH_COMPONENTS = ["checkout-api", "payments-api", "auth-service", "inventory-api", "ingestion-service", "diagnostic-service",
  "forecasting-service", "event-archiver", "kafka", "zookeeper", "prometheus", "grafana", "neo4j", "ollama", "s3-archive"];
const ragComp = $<HTMLSelectElement>("#rag-comp");
ragComp.innerHTML = GRAPH_COMPONENTS.map((c) => `<option>${c}</option>`).join("");

function drawGraph(ctx: GraphContext): void {
  const svg = $<SVGSVGElement>("#graph-svg");
  const W = 440, H = 340, cx = W / 2, cy = H / 2;
  const deps = ctx.dependencies ?? [];
  const pos: Record<string, [number, number] | undefined> = { [ctx.component.node_id]: [cx, cy] };
  const at = (id: string): [number, number] => pos[id] ?? [cx, cy];
  const d1 = deps.filter((d) => d.depth === 1);
  d1.forEach((d, i) => {
    const a = (i / Math.max(1, d1.length)) * 2 * Math.PI - Math.PI / 2;
    pos[d.node_id] = [cx + 100 * Math.cos(a), cy + 80 * Math.sin(a)];
  });
  const byVia: Record<string, GraphDependency[]> = {};
  deps.filter((d) => d.depth === 2).forEach((d) => (byVia[d.via] ??= []).push(d));
  for (const [via, list] of Object.entries(byVia)) {
    const [vx, vy] = at(via);
    const base = Math.atan2(vy - cy, vx - cx);
    list.forEach((d, i) => {
      const a = base + (i - (list.length - 1) / 2) * 0.38;
      pos[d.node_id] = [cx + 175 * Math.cos(a), cy + 145 * Math.sin(a)];
    });
  }
  const w = (id: string): number => Math.max(60, id.length * 6.3 + 24);
  const clampX = (id: string, x: number): number => Math.min(Math.max(x, w(id) / 2 + 2), W - w(id) / 2 - 2);
  const clampY = (y: number): number => Math.min(Math.max(y, 14), H - 14);
  let out = "";
  for (const d of deps) {
    const via = pos[d.via] ? d.via : ctx.component.node_id;
    out += `<line class="ge ${d.direction === "upstream" ? "up" : ""}" x1="${clampX(via, at(via)[0]).toFixed(1)}" y1="${clampY(at(via)[1]).toFixed(1)}" x2="${clampX(d.node_id, at(d.node_id)[0]).toFixed(1)}" y2="${clampY(at(d.node_id)[1]).toFixed(1)}"/>`;
  }
  const node = (id: string, type: string, health: string, depth: number): string => {
    const ww = w(id);
    const x = clampX(id, at(id)[0]), y = clampY(at(id)[1]);
    const bad = health === "down" || health === "degraded";
    return `<g transform="translate(${x.toFixed(1)},${y.toFixed(1)})"><rect class="gn ${depth === 0 ? "center" : depth === 2 ? "d2" : ""} ${esc(type)}" x="${-ww / 2}" y="-12" width="${ww}" height="24" rx="${type === "broker" ? 12 : 5}"/>
      <circle class="gh ${bad ? "bad" : ""}" cx="${-ww / 2 + 10}" cy="0" r="3"/>
      <text class="gt ${depth === 0 ? "center" : ""}" x="${-ww / 2 + 18}" y="4"${depth === 0 ? ' style="fill:#fff"' : ""}>${esc(id)}</text></g>`;
  };
  for (const d of deps) out += node(d.node_id, d.node_type, d.health_status, d.depth);
  out += node(ctx.component.node_id, ctx.component.node_type, ctx.component.health_status, 0);
  svg.innerHTML = out;
  const ups = deps.filter((d) => d.direction === "upstream").length;
  $("#graph-cap").textContent = `${deps.length} components within two hops, read from ${ctx.backend}: ${deps.length - ups} it depends on (solid), ${ups} that depend on it (dashed). Owner: ${ctx.component.metadata.owner_team}, ${ctx.component.metadata.sla_tier}.`;
}
async function showGraph(): Promise<GraphContext> {
  const ctx = await api<GraphContext>(`/api/graph/${encodeURIComponent(ragComp.value)}`);
  drawGraph(ctx);
  return ctx;
}
ragComp.addEventListener("change", () => {
  showGraph().catch((e: unknown) => {
    $("#graph-cap").textContent = errorMessage(e);
  });
});
action("#btn-graph", async () => {
  try {
    const ctx = await showGraph();
    log(`graph context for ${ctx.component.node_id}: ${ctx.dependencies.length} components`);
  } catch (err) {
    $("#graph-cap").textContent = errorMessage(err);
  }
});
action("#btn-diagnose", async () => {
  const out = $("#out-diagnose");
  const comp = ragComp.value;
  const t0 = performance.now();
  out.innerHTML = `<p class="busy">Retrieving ${esc(comp)}'s neighbourhood from Neo4j and asking the LLM. This takes up to a minute or two on CPU…</p>`;
  showGraph().catch(() => {});
  try {
    const r = await api<DiagnoseResult>("/api/diagnose", { method: "POST", body: { component_id: comp, anomalies: lastAnomaly ? [lastAnomaly] : [] } });
    const d = r.details;
    const secs = ((performance.now() - t0) / 1000).toFixed(1);
    out.innerHTML = facts([["Written by", esc(String(d.generated_by).startsWith("fallback") ? "rule-based fallback" : d.generated_by)], ["Graph", `${esc(d.graph_backend)} · ${d.dependency_count} nodes`], ["Took", `${secs}s`]])
      + (d.llm_error ? `<p class="hint" style="margin:10px 0">The LLM didn't answer usefully (${esc(String(d.llm_error).slice(0, 120))}), so the rule-based fallback wrote this one.</p>` : "")
      + `<ul class="pb-list" style="margin-top:12px"><li>${playbookHtml({ ...d, component_id: comp, summary: r.summary, generated_at: new Date().toISOString() }, true)}</li></ul>`;
    log(`GraphRAG playbook ${r.playbook_id} for ${comp} by ${d.generated_by} in ${secs}s`);
    doneStep(4);
    void loadPlaybooks();
  } catch (err) {
    out.innerHTML = errHtml(err);
    log(`diagnose failed: ${errorMessage(err)}`, true);
  }
});

// 5 · TFT forecast
function forecastTable(res: ForecastResult): string {
  const det = res.details;
  const v = (x: number | null | undefined): string => (x == null ? "–" : x < 1 ? x.toFixed(4) : x.toFixed(2));
  const rows = Object.entries(det.metrics).map(([name, m]) => {
    const outc = m.outcome || m.status;
    const pill = /alert|dedup/.test(outc) ? "bad" : outc === "insufficient_data" ? "warn" : "";
    return `<tr><td class="mono">${esc(name)}</td><td class="r">${v(m.last_value)}</td><td class="r">${v(m.max_upper_90)}</td><td class="r">${v(m.threshold)}</td>
      <td class="r">${m.confidence == null ? "–" : (m.confidence * 100).toFixed(0) + "%"}</td><td><span class="pill ${pill}">${esc(outc.replace(/_/g, " "))}</span></td></tr>`;
  }).join("");
  return `<p class="hint" style="margin:12px 0 8px">${esc(res.component_id)} · <b>${esc(det.forecaster)}</b> <code>${esc(det.model_version)}</code> · ${det.forecast_window_minutes}-min horizon · ${(det.latency_seconds * 1000).toFixed(0)} ms</p>
    <table class="tbl"><thead><tr><th>Metric</th><th class="r">Now</th><th class="r">P90 peak</th><th class="r">Threshold</th><th class="r">Confidence</th><th>Outcome</th></tr></thead><tbody>${rows}</tbody></table>`;
}
const forecastComponent = (): string => $<HTMLSelectElement>("#component").value;
action("#btn-forecast", async () => {
  const out = $("#out-forecast");
  try {
    const r = await api<ForecastResult>("/api/forecast", { method: "POST", body: { component_id: forecastComponent() } });
    out.innerHTML = forecastTable(r);
    log(`forecast ${r.component_id} with ${r.details.forecaster}: alert_emitted=${r.alert_emitted}`);
    doneStep(5);
    void renderAlerts();
  } catch (err) {
    out.innerHTML = errHtml(err);
    log(`forecast failed: ${errorMessage(err)}`, true);
  }
});
action("#btn-simulate", async () => {
  const out = $("#out-forecast");
  try {
    const s = await api<SimulateResult>("/api/forecast/simulate", { method: "POST", body: { component_id: forecastComponent() } });
    const m = s.result.details.metrics[s.metric];
    const a = s.result.alert;
    const outcome = m?.outcome ?? "unknown";
    out.innerHTML = facts([["Metric", esc(s.metric)], ["Now", m?.last_value == null ? "–" : m.last_value.toFixed(4)], ["Threshold", `${s.lowered_to} <small>(was ${s.restored_to})</small>`], ["Result", a ? "New alert" : outcome === "deduplicated" ? "Already alerted" : esc(outcome), a || m?.alert_id ? "bad" : ""]])
      + (a ? `<p class="hint" style="margin-top:10px">Alert <code>${esc(a.alert_id)}</code>: ${esc(a.severity)}, breach predicted ${new Date(a.predicted_breach_time).toISOString().slice(11, 16)} UTC. It has been handed to GraphRAG; the playbook shows up under “GraphRAG root-cause playbooks” when the LLM finishes.</p>` : "")
      + forecastTable(s.result);
    log(`simulated breach on ${s.metric}: ${a ? a.alert_id : outcome}`);
    doneStep(5);
    void renderAlerts();
  } catch (err) {
    out.innerHTML = errHtml(err);
    log(`simulate failed: ${errorMessage(err)}`, true);
  }
});

// 6 · archive
action("#btn-archive", async () => {
  const out = $("#out-archive");
  try {
    const r = await api<ArchiveListing>("/api/archive");
    out.innerHTML = facts([["Bucket", esc(r.bucket)], ["Files", fmt(r.count)], ["Stored", kb(r.bytes)]])
      + (r.latest.length ? `<table class="tbl" style="margin-top:12px"><thead><tr><th>Newest objects</th><th class="r">Size</th><th class="r">Written (UTC)</th></tr></thead><tbody>${r.latest.map((o) => `<tr><td class="mono">${esc(o.key)}</td><td class="r">${kb(o.size)}</td><td class="r">${o.modified.slice(11, 19)}</td></tr>`).join("")}</tbody></table>` : '<p class="hint">Nothing archived yet.</p>');
    log(`archive: ${r.count} files, ${kb(r.bytes)}`);
    doneStep(6);
  } catch (err) {
    out.innerHTML = errHtml(err);
    log(`archive listing failed: ${errorMessage(err)}`, true);
  }
});

// ── start ───────────────────────────────────────────────────────────
$("#tp-legend").innerHTML = SERIES.map((s) => `<span><i style="background:var(${s.color})"></i>${s.label}</span>`).join("");
new ResizeObserver(() => drawChart()).observe($("#tp-chart"));
loadConfig().catch(() => {});
void poll();
setInterval(() => {
  $("#clock").textContent = `${hhmmss()} UTC`;
}, 1000);
void loadHistory();
setInterval(loadHistory, 5000);
void loadPlaybooks();
void renderAlerts();
setInterval(() => {
  void loadPlaybooks();
  void renderAlerts();
}, 10000);
api<Overview>("/api/overview")
  .then((ov) => {
    if (ov.counters.threshold != null) markThreshold(ov.counters.threshold);
  })
  .catch(() => {});
