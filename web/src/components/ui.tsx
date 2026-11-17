import type { ReactNode } from "react";

export function Card({ title, children, testid }: { title?: string; children: ReactNode; testid?: string }) {
  return (
    <section className="rounded-lg border border-slate-200 bg-white p-4 shadow-sm" data-testid={testid}>
      {title && <h2 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">{title}</h2>}
      {children}
    </section>
  );
}

export function Stat({ label, value, testid }: { label: string; value: string; testid?: string }) {
  return (
    <div className="rounded-md bg-slate-50 px-3 py-2">
      <div className="text-xs text-slate-500">{label}</div>
      <div className="text-lg font-semibold tabular-nums" data-testid={testid}>
        {value}
      </div>
    </div>
  );
}

export function UpBadge({ up }: { up: boolean }) {
  return (
    <span
      className={`rounded px-2 py-0.5 text-xs font-semibold ${up ? "bg-emerald-100 text-emerald-800" : "bg-red-100 text-red-800"}`}
    >
      {up ? "up" : "down"}
    </span>
  );
}

export function RoleBadge({ role }: { role: string }) {
  const style =
    role === "leader"
      ? "bg-indigo-100 text-indigo-800"
      : role === "follower"
        ? "bg-slate-100 text-slate-700"
        : "bg-amber-100 text-amber-800";
  return <span className={`rounded px-2 py-0.5 text-xs ${style}`}>{role}</span>;
}

export function Table({ head, children, testid }: { head: string[]; children: ReactNode; testid?: string }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left text-sm" data-testid={testid}>
        <thead>
          <tr className="border-b border-slate-200 text-xs uppercase text-slate-500">
            {head.map((h) => (
              <th key={h} className="px-2 py-1 font-medium">
                {h}
              </th>
            ))}
          </tr>
        </thead>
        <tbody className="tabular-nums">{children}</tbody>
      </table>
    </div>
  );
}

export function Loading({ error }: { error: unknown }) {
  if (error) return <p className="text-red-700">The console did not answer: {String(error)}</p>;
  return <p className="text-slate-500">Loading…</p>;
}
