// DOM, formatting and fetch helpers shared by the console.

export const SVG_NS = "http://www.w3.org/2000/svg";

/** querySelector that throws when nothing matches, so a stale selector fails loudly. */
export function $<E extends Element = HTMLElement>(selector: string, root: ParentNode = document): E {
  const el = root.querySelector<E>(selector);
  if (!el) throw new Error(`console: nothing matches ${selector}`);
  return el;
}

export const $$ = <E extends Element = HTMLElement>(selector: string, root: ParentNode = document): E[] => [
  ...root.querySelectorAll<E>(selector),
];

/** getElementById for ids that aren't valid CSS selectors (topic ids such as `n-raw-events.DLT`). */
export function byId<E extends Element = HTMLElement>(id: string): E {
  const el = document.getElementById(id);
  if (!el) throw new Error(`console: no element with id ${id}`);
  return el as unknown as E;
}

export const fmt = (n: number | null | undefined): string =>
  n == null || Number.isNaN(n) ? "–" : Math.round(n).toLocaleString("en-US");

export function compact(n: number | null | undefined): string {
  if (n == null) return "–";
  const a = Math.abs(n);
  if (a >= 1e6) return (n / 1e6).toFixed(a >= 1e7 ? 0 : 1) + "M";
  if (a >= 1e4) return (n / 1e3).toFixed(a >= 1e5 ? 0 : 1) + "K";
  return fmt(n);
}

export const kb = (bytes: number): string =>
  bytes >= 1048576 ? `${(bytes / 1048576).toFixed(1)} MB` : `${(bytes / 1024).toFixed(1)} KB`;

const ENTITIES: Record<string, string> = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };
export const esc = (s: unknown): string => String(s ?? "").replace(/[&<>"']/g, (c) => ENTITIES[c] ?? c);

export const hhmmss = (d: Date = new Date()): string => d.toISOString().slice(11, 19);

export const errorMessage = (err: unknown): string => (err instanceof Error ? err.message : String(err));

export interface ApiOptions {
  method?: "GET" | "POST" | "PUT";
  body?: unknown;
}

/** Calls the console API; a non-2xx response throws with FastAPI's `detail` when there is one. */
export async function api<T>(path: string, opts: ApiOptions = {}): Promise<T> {
  const res = await fetch(path, {
    method: opts.method ?? "GET",
    headers: { "content-type": "application/json" },
    body: opts.body === undefined ? null : JSON.stringify(opts.body),
  });
  const text = await res.text();
  let data: unknown;
  try {
    data = JSON.parse(text);
  } catch {
    data = text;
  }
  if (!res.ok) {
    const detail = typeof data === "object" && data !== null && "detail" in data ? data.detail : null;
    throw new Error(detail == null ? `HTTP ${res.status}` : typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return data as T;
}
