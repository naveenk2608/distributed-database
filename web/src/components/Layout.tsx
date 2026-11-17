import { NavLink, Outlet } from "react-router-dom";
import { useOverview, useStream } from "../api";

const pages = [
  { to: "/", label: "Overview" },
  { to: "/shards", label: "Shards" },
  { to: "/nodes", label: "Nodes" },
  { to: "/events", label: "Events" },
];

export function Layout() {
  const stream = useStream();
  const { data } = useOverview();
  return (
    <div className="mx-auto max-w-7xl p-4">
      <header className="mb-4 flex flex-wrap items-center gap-4">
        <h1 className="text-xl font-bold">dkv</h1>
        <nav className="flex gap-1">
          {pages.map((p) => (
            <NavLink
              key={p.to}
              to={p.to}
              end
              className={({ isActive }) =>
                `rounded px-3 py-1 text-sm ${isActive ? "bg-slate-900 text-white" : "text-slate-700 hover:bg-slate-200"}`
              }
            >
              {p.label}
            </NavLink>
          ))}
        </nav>
        <span className="ml-auto text-xs text-slate-500" data-testid="stream-state">
          {stream === "live" ? "live" : stream}
          {data ? ` · polling every ${data.poll_interval_ms} ms` : ""}
        </span>
      </header>
      <main>
        <Outlet />
      </main>
    </div>
  );
}
