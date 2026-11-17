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

export function rate(n: number): string {
  return `${n < 10 ? n.toFixed(1) : Math.round(n).toLocaleString("en-US")}/s`;
}

export function ms(n: number | null): string {
  if (n === null) return "–";
  return `${n < 10 ? n.toFixed(2) : n.toFixed(0)} ms`;
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
