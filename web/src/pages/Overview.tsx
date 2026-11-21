import { Link } from "react-router-dom";
import { useHistories, useOverview, type EventView, type NodeView, type Sample } from "../api";
import { ChevronIcon } from "../components/icons";
import { ChartLegend, RateChart, type Point, type Series } from "../components/RateChart";
import { Ago, Card, CardHeader, Dot, EmptyState, EventBadge, Kpi, Label, NodeState, PageState, Skeleton, StatusPill, Unit } from "../components/ui";
import { count, msValue, rate, rateValue, shardName, shortID } from "../format";

// clusterSeries adds up the nodes' samples of each poll round (they share
// the round's time) into cluster-wide rates.
function clusterSeries(histories: Sample[][]): Point[] {
  const byTime = new Map<string, Point>();
  for (const h of histories) {
    for (const s of h) {
      const p = byTime.get(s.time) ?? { t: s.time, reads: 0, writes: 0 };
      p.reads = (p.reads as number) + s.rates.reads_per_sec;
      p.writes = (p.writes as number) + s.rates.writes_per_sec;
      byTime.set(s.time, p);
    }
  }
  return [...byTime.values()].sort((a, b) => a.t.localeCompare(b.t));
}

// tickNumber labels the rate axis: whole steps as integers, fractional
// steps (a nearly idle cluster) with one decimal.
const tickNumber = (n: number) => (Number.isInteger(n) ? count(n) : n.toFixed(1));

const throughput: Series[] = [
  { key: "reads", label: "Reads", color: "var(--series-1)" },
  { key: "writes", label: "Writes", color: "var(--series-2)" },
];

function NodeCard({ n }: { n: NodeView }) {
  return (
    <Card testid={`node-${n.addr}`} className="p-4">
      <div className="flex items-center justify-between gap-2">
        <span className="truncate font-mono text-sm font-medium text-fg" title={n.node_id}>
          {n.node_id ? shortID(n.node_id) : n.addr}
        </span>
        <StatusPill up={n.up} />
      </div>
      <div className="mt-1 flex flex-wrap items-center justify-between gap-2">
        <span className="truncate font-mono text-xs text-muted">{n.addr}</span>
        <NodeState state={n.state} suspect={n.suspect} drained={n.drained} />
      </div>
      <dl className="mt-4 grid grid-cols-3 gap-2 border-t border-line pt-4">
        <div>
          <Label as="dt">leads</Label>
          <dd className="mt-1 text-base font-medium text-fg">{n.up ? n.leaders : "–"}</dd>
        </div>
        <div>
          <Label as="dt">replicas</Label>
          <dd className="mt-1 text-base font-medium text-fg">{n.up ? n.replicas.length : "–"}</dd>
        </div>
        <div>
          <Label as="dt">ops</Label>
          <dd className="mt-1 text-base font-medium text-fg">{n.up ? rate(n.rates.reads_per_sec + n.rates.writes_per_sec) : "–"}</dd>
        </div>
      </dl>
      {!n.up && n.error && (
        <p className="mt-4 line-clamp-2 break-all rounded-lg bg-bad/10 px-2 py-1 font-mono text-xs text-bad" title={n.error}>
          {n.error}
        </p>
      )}
    </Card>
  );
}

function RecentEvents({ events }: { events: EventView[] }) {
  const recent = events.slice(-8).reverse();
  return (
    <Card className="p-6">
      <CardHeader label="Recent events">
        <Link to="/events" className="inline-flex items-center gap-1 text-sm text-accent-fg hover:underline">
          All events
          <ChevronIcon className="h-3.5 w-3.5" />
        </Link>
      </CardHeader>
      {recent.length === 0 ? (
        <EmptyState title="No events yet">Elections, failures and membership changes will show here.</EmptyState>
      ) : (
        <ul className="mt-4 divide-y divide-line">
          {recent.map((e) => (
            <li key={`${e.reporter}-${e.seq}-${e.time}`} className="flex flex-wrap items-center gap-x-4 gap-y-1 py-2">
              <EventBadge kind={e.kind} />
              <span className="min-w-0 flex-1 truncate text-sm text-muted">
                {e.shard !== null && <>shard {shardName(e.shard)}</>}
                {e.node && (
                  <>
                    {e.shard !== null ? " · " : ""}
                    <span className="font-mono text-fg" title={e.node}>
                      {shortID(e.node)}
                    </span>
                  </>
                )}
              </span>
              <Ago iso={e.time} className="whitespace-nowrap text-xs text-muted" />
            </li>
          ))}
        </ul>
      )}
    </Card>
  );
}

function OverviewSkeleton() {
  return (
    <div className="grid gap-6">
      <div className="grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-5">
        {Array.from({ length: 5 }, (_, i) => (
          <Card key={i} className="p-4">
            <Skeleton className="h-3 w-20" />
            <Skeleton className="mt-4 h-8 w-24" />
            <Skeleton className="mt-2 h-3 w-28" />
          </Card>
        ))}
      </div>
      <Card className="p-6">
        <Skeleton className="h-3 w-24" />
        <Skeleton className="mt-6 h-64 w-full" />
      </Card>
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {Array.from({ length: 3 }, (_, i) => (
          <Card key={i} className="p-4">
            <Skeleton className="h-4 w-24" />
            <Skeleton className="mt-2 h-3 w-16" />
            <Skeleton className="mt-6 h-8 w-full" />
          </Card>
        ))}
      </div>
    </div>
  );
}

export function OverviewPage() {
  const { data, error } = useOverview();
  const histories = useHistories(data?.nodes.map((n) => n.addr) ?? []);
  if (!data) return <PageState error={error} skeleton={<OverviewSkeleton />} />;
  const up = data.nodes.filter((n) => n.up).length;
  const led = data.shards.filter((s) => s.leader !== "").length;
  const down = data.nodes.length - up;
  const leaderless = data.shards.length - led;
  const t = data.totals;
  const points = clusterSeries(histories.map((h) => h.data ?? []));
  const historyLoading = histories.some((h) => h.isPending);
  return (
    <div className="grid gap-6">
      <div className="grid grid-cols-2 gap-4 md:grid-cols-3 xl:grid-cols-5">
        <Kpi
          label="Nodes up"
          testid="nodes-up"
          sub={
            data.nodes.length === 0 ? (
              "no nodes configured"
            ) : (
              <span className="inline-flex items-center gap-1.5">
                <Dot tone={down ? "bad" : "ok"} />
                {down ? `${down} down` : "all answering"}
              </span>
            )
          }
        >
          {up}
          <span className="text-muted"> / {data.nodes.length}</span>
        </Kpi>
        <Kpi
          label="Groups with a leader"
          testid="groups-led"
          sub={
            data.shards.length === 0 ? (
              "no groups reported by a live node"
            ) : (
              <span className="inline-flex items-center gap-1.5">
                <Dot tone={leaderless ? "bad" : "ok"} />
                {leaderless ? `${leaderless} without a leader` : "every group led"}
              </span>
            )
          }
        >
          {led}
          <span className="text-muted"> / {data.shards.length}</span>
        </Kpi>
        <Kpi
          label="Ops/s"
          sub={
            <>
              reads <span data-testid="total-reads" className="text-fg">{rate(t.reads_per_sec)}</span> · writes{" "}
              <span data-testid="total-writes" className="text-fg">{rate(t.writes_per_sec)}</span> · errors{" "}
              <span data-testid="total-errors" className={t.errors_per_sec > 0 ? "text-bad" : "text-fg"}>
                {rate(t.errors_per_sec)}
              </span>
            </>
          }
        >
          {rateValue(t.reads_per_sec + t.writes_per_sec)}
        </Kpi>
        <Kpi label="Latency p50" sub="median request, all nodes">
          <span data-testid="total-p50">
            {msValue(t.p50_ms)}
            {t.p50_ms !== null && <Unit> ms</Unit>}
          </span>
        </Kpi>
        <Kpi label="Latency p99" sub="99th percentile, all nodes">
          <span data-testid="total-p99">
            {msValue(t.p99_ms)}
            {t.p99_ms !== null && <Unit> ms</Unit>}
          </span>
        </Kpi>
      </div>

      <Card className="p-6" testid="throughput-chart">
        <CardHeader label="Throughput" sub={`Operations per second, summed over all nodes, sampled every ${count(data.poll_interval_ms)} ms`}>
          <ChartLegend series={throughput} />
        </CardHeader>
        <div className="mt-6">
          {historyLoading && points.length === 0 ? (
            <Skeleton className="h-64 w-full" />
          ) : (
            <RateChart points={points} series={throughput} format={rate} formatTick={tickNumber} />
          )}
        </div>
      </Card>

      <section aria-labelledby="overview-nodes" className="grid gap-4">
        <Label as="h2" className="flex items-center justify-between">
          <span id="overview-nodes">Nodes</span>
          <Link to="/nodes" className="inline-flex items-center gap-1 text-sm normal-case tracking-normal text-accent-fg hover:underline">
            Details
            <ChevronIcon className="h-3.5 w-3.5" />
          </Link>
        </Label>
        {data.nodes.length === 0 ? (
          <Card>
            <EmptyState title="No nodes configured">The console has no node addresses to poll.</EmptyState>
          </Card>
        ) : (
          <div className="grid grid-cols-[repeat(auto-fit,minmax(240px,1fr))] gap-4">
            {data.nodes.map((n) => (
              <NodeCard key={n.addr} n={n} />
            ))}
          </div>
        )}
      </section>

      <RecentEvents events={data.events} />
    </div>
  );
}
