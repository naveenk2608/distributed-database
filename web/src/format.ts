// Formatting only: every value comes from the console's JSON unchanged.

export function bytes(n: number): string {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let v = n;
  let u = 0;
  while (v >= 1024 && u < units.length - 1) {
    v /= 1024;
    u++;
  }
  return `${u === 0 ? v : v.toFixed(1)} ${units[u]}`;
}

// rateValue is rate without its unit, for figures that set the unit apart.
export function rateValue(n: number): string {
  return n < 10 ? n.toFixed(1) : Math.round(n).toLocaleString("en-US");
}

export function rate(n: number): string {
  return `${rateValue(n)}/s`;
}

// msValue is ms without its unit; null (no requests measured) is "–".
export function msValue(n: number | null): string {
  if (n === null) return "–";
  return n < 10 ? n.toFixed(2) : n.toFixed(0);
}

export function ms(n: number | null): string {
  if (n === null) return "–";
  return `${msValue(n)} ms`;
}

export function count(n: number): string {
  return n.toLocaleString("en-US");
}

export function shardName(s: number | null): string {
  if (s === null) return "–";
  return s === -1 ? "meta" : String(s);
}

export function shortID(id: string): string {
  return id.length > 8 ? id.slice(0, 8) : id;
}

export function clock(iso: string): string {
  const d = new Date(iso);
  return `${d.toISOString().slice(11, 23)}Z`;
}

// ago is the time from iso to now (ms since the epoch), coarsened: "just
// now", "42s ago", "5m ago", "3h ago", "2d ago". A time slightly ahead of
// this browser's clock (skew) reads as "just now".
export function ago(iso: string, now: number): string {
  const s = Math.floor((now - Date.parse(iso)) / 1000);
  if (Number.isNaN(s)) return "–";
  if (s < 5) return "just now";
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 48) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}

// kindLabel turns an event kind into words: "leader_elected" -> "Leader elected".
export function kindLabel(kind: string): string {
  const words = kind.replace(/_/g, " ");
  return words.charAt(0).toUpperCase() + words.slice(1);
}
