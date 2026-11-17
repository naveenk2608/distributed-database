import { Link } from "react-router-dom";
import { useHistories, useOverview, type Sample } from "../api";
import { Card, Loading, Stat, UpBadge } from "../components/ui";
import { RateChart, type Point } from "../components/RateChart";
import { clock, ms, rate, shardName, shortID } from "../format";

// clusterSeries adds up the nodes' samples of each poll round (they share
// the round's time) into cluster-wide rates.
function clusterSeries(histories: Sample[][]): Point[] {
  const byTime = new Map<string, Point>();
  for (const h of histories) {
    for (const s of h) {
      const p = byTime.get(s.time) ?? { t: s.time, reads: 0, writes: 0, errors: 0 };
      p.reads = (p.reads as number) + s.rates.reads_per_sec;
      p.writes = (p.writes as number) + s.rates.writes_per_sec;
      p.errors = (p.errors as number) + s.rates.errors_per_sec;
      byTime.set(s.time, p);
    }
  }
  return [...byTime.values()].sort((a, b) => a.t.localeCompare(b.t));
}

export function OverviewPage() {
  const { data, error } = useOverview();
  const histories = useHistories(data?.nodes.map((n) => n.addr) ?? []);
  if (!data) return <Loading error={error} />;
  const up = data.nodes.filter((n) => n.up).length;
  const led = data.shards.filter((s) => s.leader !== "").length;
  const points = clusterSeries(histories.map((h) => h.data ?? []));
  return (
    <div className="grid gap-4">
      <Card title="Cluster">
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-4 lg:grid-cols-7">
          <Stat label="Nodes up" value={`${up} / ${data.nodes.length}`} testid="nodes-up" />
          <Stat label="Groups with a leader" value={`${led} / ${data.shards.length}`} testid="groups-led" />
          <Stat label="Reads" value={rate(data.totals.reads_per_sec)} testid="total-reads" />
          <Stat label="Writes" value={rate(data.totals.writes_per_sec)} testid="total-writes" />
          <Stat label="Errors" value={rate(data.totals.errors_per_sec)} testid="total-errors" />
          <Stat label="Latency p50" value={ms(data.totals.p50_ms)} testid="total-p50" />
          <Stat label="Latency p99" value={ms(data.totals.p99_ms)} testid="total-p99" />
        </div>
      </Card>
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        {data.nodes.map((n) => (
          <Card key={n.addr} testid={`node-${n.addr}`}>
            <div className="mb-2 flex items-center justify-between gap-2">
              <span className="font-mono text-sm" title={n.node_id}>
                {n.node_id ? shortID(n.node_id) : n.addr}
              </span>
              <UpBadge up={n.up} />
            </div>
            <dl className="grid grid-cols-2 gap-x-2 text-sm">
              <dt className="text-slate-500">address</dt>
              <dd className="truncate font-mono text-xs">{n.addr}</dd>
              <dt className="text-slate-500">state</dt>
              <dd>
                {n.state}
                {n.drained ? " (drained)" : ""}
                {n.suspect ? " (suspect)" : ""}
              </dd>
              <dt className="text-slate-500">leads</dt>
              <dd>{n.up ? n.leaders : "–"}</dd>
              <dt className="text-slate-500">replicas</dt>
              <dd>{n.up ? n.replicas.length : "–"}</dd>
              <dt className="text-slate-500">ops</dt>
              <dd>{n.up ? rate(n.rates.reads_per_sec + n.rates.writes_per_sec) : "–"}</dd>
            </dl>
            {!n.up && n.error && <p className="mt-2 truncate text-xs text-red-700" title={n.error}>{n.error}</p>}
          </Card>
        ))}
      </div>
      <Card title="Throughput">
        <RateChart points={points} series={["reads", "writes", "errors"]} unit="/s" />
      </Card>
      <Card title="Recent events">
        <ul className="text-sm">
          {data.events.slice(-10).reverse().map((e) => (
            <li key={`${e.reporter}-${e.seq}-${e.time}`} className="font-mono text-xs">
              {clock(e.time)} {e.kind} shard {shardName(e.shard)} {e.node ? shortID(e.node) : ""}
            </li>
          ))}
        </ul>
        <Link to="/events" className="mt-2 inline-block text-sm text-indigo-700 hover:underline">
          All events
        </Link>
      </Card>
    </div>
  );
}
