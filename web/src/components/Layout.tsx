import type { ComponentType } from "react";
import { NavLink, Outlet, useLocation } from "react-router-dom";
import { useOverview, useStream, type StreamState } from "../api";
import { ago, clock } from "../format";
import { useNow, useTheme } from "../theme";
import { AlertIcon, EventsIcon, LogoMark, MoonIcon, NodesIcon, OverviewIcon, ShardsIcon, SunIcon } from "./icons";
import { cx, Dot, type Tone } from "./ui";

const pages: { to: string; label: string; about: string; icon: ComponentType<{ className?: string }> }[] = [
  { to: "/", label: "Overview", about: "Cluster health and throughput across every node.", icon: OverviewIcon },
  { to: "/shards", label: "Shards", about: "Raft groups: placement, leaders and replication lag.", icon: ShardsIcon },
  { to: "/nodes", label: "Nodes", about: "Each node's liveness, leadership and storage engine.", icon: NodesIcon },
  { to: "/events", label: "Events", about: "Elections, failures and membership changes, newest first.", icon: EventsIcon },
];

const streamTone: Record<StreamState, Tone> = { live: "ok", connecting: "neutral", reconnecting: "bad" };

function StreamStatus({ stream, pollMs }: { stream: StreamState; pollMs?: number }) {
  return (
    <div
      data-testid="stream-state"
      className="inline-flex items-center gap-2 whitespace-nowrap rounded-full border border-line bg-surface px-3 py-1 text-xs text-muted"
    >
      <span className="relative flex h-1.5 w-1.5">
        {stream === "live" && <span aria-hidden="true" className="absolute inline-flex h-full w-full animate-ping rounded-full bg-ok opacity-60" />}
        <Dot tone={streamTone[stream]} className="relative" />
      </span>
      <span>
        <span className="font-medium capitalize text-fg">{stream}</span>
        {pollMs !== undefined && ` · polling every ${pollMs} ms`}
      </span>
    </div>
  );
}

// StreamBanner: the live stream is gone, so what the page shows stops
// changing; say so, and since when.
function StreamBanner({ generatedAt }: { generatedAt?: string }) {
  const now = useNow(1000);
  return (
    <div
      role="status"
      data-testid="stream-banner"
      className="mb-6 flex items-start gap-3 rounded-xl border border-bad/30 bg-bad/10 px-4 py-3"
    >
      <AlertIcon className="mt-0.5 h-4 w-4 shrink-0 text-bad" />
      <div className="min-w-0 text-sm">
        <p className="font-medium text-fg">Live stream disconnected</p>
        <p className="mt-0.5 text-muted">
          Reconnecting to the console…{" "}
          {generatedAt
            ? `The figures below are from the last update at ${clock(generatedAt)} (${ago(generatedAt, now)}) and will not change until it is back.`
            : "Nothing has arrived from it yet."}
        </p>
      </div>
    </div>
  );
}

export function Layout() {
  const stream = useStream();
  const { data } = useOverview();
  const [theme, toggleTheme] = useTheme();
  const { pathname } = useLocation();
  const page = pages.find((p) => p.to === pathname) ?? pages[0];
  const nextTheme = theme === "dark" ? "light" : "dark";
  return (
    <div className="min-h-screen">
      <aside className="border-b border-line bg-bg lg:fixed lg:inset-y-0 lg:left-0 lg:z-20 lg:w-60 lg:border-b-0 lg:border-r">
        <div className="flex flex-wrap items-center gap-x-4 gap-y-2 px-4 pt-4 lg:h-full lg:flex-col lg:flex-nowrap lg:items-stretch lg:gap-0 lg:px-4 lg:py-6">
          <div className="flex items-center gap-2 lg:mb-6 lg:px-2">
            <LogoMark />
            <span className="text-base font-semibold tracking-tight text-fg">dkv</span>
            <span className="text-xs text-muted">console</span>
          </div>
          <button
            type="button"
            onClick={toggleTheme}
            aria-label={`Switch to ${nextTheme} theme`}
            className="ml-auto inline-flex items-center gap-2 rounded-lg px-2 py-1.5 text-sm text-muted hover:bg-raised hover:text-fg lg:order-last lg:ml-0 lg:mt-auto"
          >
            {theme === "dark" ? <SunIcon /> : <MoonIcon />}
            <span className="hidden lg:inline">{theme === "dark" ? "Light theme" : "Dark theme"}</span>
          </button>
          <nav aria-label="Pages" className="-mx-4 w-[calc(100%+2rem)] overflow-x-auto px-4 lg:mx-0 lg:w-auto lg:overflow-visible lg:px-0">
            <ul className="flex gap-1 pb-2 lg:flex-col lg:pb-0">
              {pages.map((p) => (
                <li key={p.to}>
                  <NavLink
                    to={p.to}
                    end
                    className={({ isActive }) =>
                      cx(
                        "flex items-center gap-2 whitespace-nowrap rounded-lg px-3 py-2 text-sm lg:px-2",
                        isActive ? "bg-raised font-medium text-fg" : "text-muted hover:bg-raised/60 hover:text-fg",
                      )
                    }
                  >
                    {({ isActive }) => (
                      <>
                        <p.icon className={cx("hidden h-4 w-4 sm:block", isActive ? "text-accent-fg" : "")} />
                        {p.label}
                      </>
                    )}
                  </NavLink>
                </li>
              ))}
            </ul>
          </nav>
        </div>
      </aside>
      <div className="lg:pl-60">
        <main className="mx-auto max-w-6xl px-4 py-6 sm:px-6 lg:px-8 lg:py-8">
          <header className="mb-6 flex flex-wrap items-start justify-between gap-4">
            <div className="min-w-0">
              <h1 className="text-title text-fg">{page.label}</h1>
              <p className="mt-1 text-sm text-muted">{page.about}</p>
            </div>
            <StreamStatus stream={stream} pollMs={data?.poll_interval_ms} />
          </header>
          {stream === "reconnecting" && <StreamBanner generatedAt={data?.generated_at} />}
          <Outlet />
        </main>
      </div>
    </div>
  );
}
