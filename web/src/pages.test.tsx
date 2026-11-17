// The pages render the console's JSON as captured from a real cluster (a
// 4-node cluster just after one node was killed; regenerate with
// DKV_DASHBOARD_FIXTURE=$PWD/web/src/fixtures go test ./test/dashboard).
// Every value they show must be the fixture's.
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import type { ReactNode } from "react";
import { applyOverview, eventsKey, historyKey, overviewKey, type EventView, type Overview, type Sample } from "./api";
import { bytes, count, shortID } from "./format";
import { EventsPage } from "./pages/Events";
import { NodesPage } from "./pages/Nodes";
import { OverviewPage } from "./pages/Overview";
import { ShardsPage } from "./pages/Shards";
import overviewJSON from "./fixtures/overview.json";
import eventsJSON from "./fixtures/events.json";
import historyJSON from "./fixtures/history.json";

const overview = overviewJSON as Overview;
const events = eventsJSON as EventView[];
const history = historyJSON as Sample[];

// ResizeObserver is a browser API the charts need and jsdom lacks.
globalThis.ResizeObserver ??= class {
  observe() {}
  unobserve() {}
  disconnect() {}
};

function client(): QueryClient {
  const qc = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  qc.setQueryData(overviewKey, overview);
  qc.setQueryData(eventsKey, events);
  for (const n of overview.nodes) qc.setQueryData(historyKey(n.addr), n.addr === history[0]?.addr ? history : []);
  return qc;
}

function show(page: ReactNode, qc = client()) {
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter>{page}</MemoryRouter>
    </QueryClientProvider>,
  );
}

const down = overview.nodes.filter((n) => !n.up);
const up = overview.nodes.filter((n) => n.up);

test("the fixture is a cluster with a killed node", () => {
  expect(down).toHaveLength(1);
  expect(up.length).toBeGreaterThan(0);
  expect(events.some((e) => e.kind === "node_dead")).toBe(true);
});

test("overview: nodes up and down, groups led, totals", () => {
  show(<OverviewPage />);
  expect(screen.getByTestId("nodes-up")).toHaveTextContent(`${up.length} / ${overview.nodes.length}`);
  const led = overview.shards.filter((s) => s.leader !== "").length;
  expect(screen.getByTestId("groups-led")).toHaveTextContent(`${led} / ${overview.shards.length}`);
  for (const n of overview.nodes) {
    const card = screen.getByTestId(`node-${n.addr}`);
    expect(within(card).getByText(n.up ? "up" : "down")).toBeInTheDocument();
    if (n.up) {
      expect(card).toHaveTextContent(`leads${n.leaders}`);
      expect(card).toHaveTextContent(`replicas${n.replicas.length}`);
    } else {
      expect(card).toHaveTextContent(n.error ?? "");
    }
  }
});

test("shards: each group's leader, term and members", () => {
  show(<ShardsPage />);
  for (const s of overview.shards) {
    const row = screen.getByTestId(`shard-${s.shard}`);
    expect(row).toHaveTextContent(s.leader ? shortID(s.leader) : "none");
    if (s.leader) expect(row).toHaveTextContent(count(s.term));
    for (const m of s.members) expect(row).toHaveTextContent(`${shortID(m.node_id)}${m.voter ? "" : " (learner)"} · lag ${count(m.lag)}`);
  }
});

test("nodes: each live node's storage, nothing for the down one", () => {
  show(<NodesPage />);
  for (const n of up) {
    const detail = screen.getByTestId(`node-detail-${n.addr}`);
    expect(within(detail).getByTestId("memtable-bytes")).toHaveTextContent(bytes(n.storage.memtable_bytes));
    expect(within(detail).getByTestId("table-bytes")).toHaveTextContent(`${count(n.storage.tables)} · ${bytes(n.storage.table_bytes)}`);
  }
  for (const n of down) {
    const detail = screen.getByTestId(`node-detail-${n.addr}`);
    expect(detail).toHaveTextContent("Not answering");
    expect(within(detail).queryByTestId("table-bytes")).toBeNull();
  }
});

test("events: newest first, filtered by kind", () => {
  show(<EventsPage />);
  const rows = within(screen.getByTestId("events")).getAllByRole("row").slice(1);
  expect(rows).toHaveLength(events.length);
  expect(rows[0]).toHaveAttribute("data-kind", events[events.length - 1].kind);
  fireEvent.change(screen.getByRole("combobox"), { target: { value: "node_dead" } });
  const dead = within(screen.getByTestId("events")).getAllByRole("row").slice(1);
  expect(dead).toHaveLength(events.filter((e) => e.kind === "node_dead").length);
  expect(dead.every((r) => r.getAttribute("data-kind") === "node_dead")).toBe(true);
});

test("a streamed overview updates the overview, each node's history and the events", () => {
  const qc = client();
  const next: Overview = {
    ...overview,
    generated_at: "2099-01-01T00:00:00Z",
    events: [...overview.events, { ...overview.events[0], seq: 999999, time: "2099-01-01T00:00:00Z", kind: "peer_up" }],
  };
  applyOverview(qc, next);
  applyOverview(qc, next); // the same overview twice adds its events once
  expect(qc.getQueryData(overviewKey)).toEqual(next);
  const h = qc.getQueryData<Sample[]>(historyKey(history[0].addr))!;
  expect(h.at(-1)?.time).toBe("2099-01-01T00:00:00Z");
  const evs = qc.getQueryData<EventView[]>(eventsKey)!;
  expect(evs.filter((e) => e.seq === 999999)).toHaveLength(1);
});
