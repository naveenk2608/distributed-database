import type { ReactNode } from "react";
import { useHistory, useOverview, type NodeView } from "../api";
import { ChevronIcon } from "../components/icons";
import { ChartLegend, RateChart, type Series } from "../components/RateChart";
import { Ago, Card, CopyID, cx, EmptyState, Label, NodeState, PageState, RoleBadge, Skeleton, StatusPill } from "../components/ui";
import { bytes, count, rate, shardName, shortID } from "../format";

const storageSeries: Series[] = [
  { key: "memtables", label: "Memtables", color: "var(--series-1)" },
  { key: "sstables", label: "SSTables", color: "var(--series-2)" },
];

function Field({ label, children, testid }: { label: string; children: ReactNode; testid?: string }) {
  return (
    <div className="min-w-0">
      <Label as="dt">{label}</Label>
      <dd className="mt-1 truncate text-sm font-medium text-fg" data-testid={testid}>
        {children}
      </dd>
    </div>
  );
}

// Meter: how full the block cache is, drawn from the two figures beside it.
function Meter({ used, capacity }: { used: number; capacity: number }) {
  if (capacity <= 0) return null;
  const pct = Math.min(100, (100 * used) / capacity);
  return (
    <div aria-hidden="true" className="mt-2 h-1 w-full overflow-hidden rounded-full bg-raised">
      <div className="h-full rounded-full bg-accent" style={{ width: `${pct}%` }} />
    </div>
  );
}

const th = "sticky top-0 z-10 border-b border-line bg-surface px-3 py-2 text-xs font-medium uppercase tracking-wider text-muted";
const td = "border-b border-line px-3 py-1.5 font-mono text-xs";

function Replicas({ node }: { node: NodeView }) {
  return (
    <details className="group mt-6 border-t border-line pt-4">
      <summary className="flex cursor-pointer list-none items-center gap-2 text-sm text-fg [&::-webkit-details-marker]:hidden">
        <ChevronIcon className="h-4 w-4 text-muted transition-transform group-open:rotate-90" />
        Replicas
        <span className="rounded-full bg-raised px-2 text-xs text-muted">{count(node.replicas.length)}</span>
      </summary>
      <div className="mt-4 max-h-96 overflow-auto rounded-lg border border-line">
        <table className="w-full border-separate border-spacing-0 text-left">
          <thead>
            <tr>
              <th scope="col" className={th}>Shard</th>
              <th scope="col" className={th}>Role</th>
              <th scope="col" className={cx(th, "text-right")}>Term</th>
              <th scope="col" className={cx(th, "text-right")}>Commit</th>
              <th scope="col" className={cx(th, "text-right")}>Applied</th>
              <th scope="col" className={cx(th, "text-right")}>Log</th>
              <th scope="col" className={cx(th, "text-right")}>SSTables</th>
              <th scope="col" className={cx(th, "text-right")}>Snapshots in / out</th>
            </tr>
          </thead>
          <tbody>
            {node.replicas.map((r) => (
              <tr key={r.shard} className="hover:bg-raised/60">
                <td className={cx(td, "font-sans text-sm font-medium text-fg")}>{shardName(r.shard)}</td>
                <td className={cx(td, "font-sans")}>
                  <RoleBadge role={r.role} />
                </td>
                <td className={cx(td, "text-right text-fg")}>{count(r.term)}</td>
                <td className={cx(td, "text-right text-fg")}>{count(r.commit)}</td>
                <td className={cx(td, "text-right text-fg")}>{count(r.applied)}</td>
                <td className={cx(td, "whitespace-nowrap text-right text-muted")}>
                  {count(r.first_index)}–{count(r.last_index)}
                </td>
                <td className={cx(td, "whitespace-nowrap text-right text-muted")}>
                  {count(r.tables)} · {bytes(r.table_bytes)}
                </td>
                <td className={cx(td, "text-right text-muted")}>
                  {count(r.snapshots_installed)} / {count(r.snapshots_sent)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </details>
  );
}

function NodeCard({ node }: { node: NodeView }) {
  const history = useHistory(node.addr);
  const s = node.storage;
  const lookups = s.cache_hits + s.cache_misses;
  return (
    <Card testid={`node-detail-${node.addr}`} className="p-6">
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2">
        {node.node_id ? (
          <CopyID id={node.node_id} short={shortID(node.node_id)} />
        ) : (
          <span className="font-mono text-sm font-medium text-fg">{node.addr}</span>
        )}
        <StatusPill up={node.up} />
        <span className="ml-auto">
          <NodeState state={node.state} suspect={node.suspect} drained={node.drained} />
        </span>
      </div>
      <p className="mt-1 flex flex-wrap gap-x-2 text-xs text-muted">
        <span className="font-mono">{node.addr}</span>
        {node.up && node.started_at && (
          <span>
            · started <Ago iso={node.started_at} />
          </span>
        )}
        {!node.up && node.last_seen && (
          <span>
            · last answered <Ago iso={node.last_seen} />
          </span>
        )}
      </p>

      {node.up ? (
        <>
          <dl className="mt-6 grid grid-cols-3 gap-4 rounded-lg bg-raised/60 px-4 py-4">
            <div>
              <Label as="dt">Leaders</Label>
              <dd className="mt-1 text-xl font-semibold text-fg">{count(node.leaders)}</dd>
            </div>
            <div>
              <Label as="dt">Replicas</Label>
              <dd className="mt-1 text-xl font-semibold text-fg">{count(node.replicas.length)}</dd>
            </div>
            <div>
              <Label as="dt">Ops/s</Label>
              <dd className="mt-1 text-xl font-semibold text-fg">{rate(node.rates.reads_per_sec + node.rates.writes_per_sec)}</dd>
            </div>
          </dl>

          <h3 className="mt-6 text-sm font-medium text-fg">Storage engine</h3>
          <dl className="mt-4 grid grid-cols-2 gap-x-6 gap-y-4">
            <Field label="Memtables" testid="memtable-bytes">
              {bytes(s.memtable_bytes)}
            </Field>
            <Field label="SSTables" testid="table-bytes">
              {`${count(s.tables)} · ${bytes(s.table_bytes)}`}
            </Field>
            <Field label="Flushes">{count(s.flushes)}</Field>
            <Field label="Compactions">{count(s.compactions)}</Field>
            <Field label="Compaction read / written">{`${bytes(s.compaction_bytes_read)} / ${bytes(s.compaction_bytes_written)}`}</Field>
            <Field label="Write stalls">{count(s.write_stalls)}</Field>
            <div className="min-w-0">
              <Label as="dt">Block cache</Label>
              <dd className="mt-1 truncate text-sm font-medium text-fg">
                {`${bytes(s.cache_used_bytes)} / ${bytes(s.cache_capacity_bytes)}`}
                <Meter used={s.cache_used_bytes} capacity={s.cache_capacity_bytes} />
              </dd>
            </div>
            <Field label="Cache hit rate">{lookups ? `${((100 * s.cache_hits) / lookups).toFixed(1)}%` : "–"}</Field>
          </dl>

          <div className="mt-6 flex flex-wrap items-center justify-between gap-2">
            <h3 className="text-sm font-medium text-fg">Storage over time</h3>
            <ChartLegend series={storageSeries} />
          </div>
          <div className="mt-4">
            {history.isPending ? (
              <Skeleton className="h-40 w-full" />
            ) : (
              <RateChart
                points={(history.data ?? []).map((h) => ({ t: h.time, memtables: h.memtable_bytes, sstables: h.table_bytes }))}
                series={storageSeries}
                format={bytes}
                height={160}
                yWidth={72}
              />
            )}
          </div>

          <Replicas node={node} />
        </>
      ) : (
        <div className="mt-6 rounded-lg border border-bad/30 bg-bad/10 px-4 py-3 text-sm">
          <p className="font-medium text-bad">Not answering</p>
          <p className="mt-1 break-all font-mono text-xs text-muted">{node.error ?? "no answer"}</p>
        </div>
      )}
    </Card>
  );
}

function NodesSkeleton() {
  return (
    <div className="grid gap-6 xl:grid-cols-2">
      {Array.from({ length: 2 }, (_, i) => (
        <Card key={i} className="p-6">
          <div className="flex items-center gap-4">
            <Skeleton className="h-5 w-24" />
            <Skeleton className="h-5 w-12 rounded-full" />
          </div>
          <Skeleton className="mt-2 h-3 w-40" />
          <Skeleton className="mt-6 h-16 w-full" />
          <div className="mt-6 grid grid-cols-2 gap-4">
            {Array.from({ length: 6 }, (_, j) => (
              <Skeleton key={j} className="h-8" />
            ))}
          </div>
          <Skeleton className="mt-6 h-40 w-full" />
        </Card>
      ))}
    </div>
  );
}

export function NodesPage() {
  const { data, error } = useOverview();
  if (!data) return <PageState error={error} skeleton={<NodesSkeleton />} />;
  if (data.nodes.length === 0) {
    return (
      <Card>
        <EmptyState title="No nodes configured">The console has no node addresses to poll.</EmptyState>
      </Card>
    );
  }
  return (
    <div className="grid gap-6 xl:grid-cols-2">
      {data.nodes.map((n) => (
        <NodeCard key={n.addr} node={n} />
      ))}
    </div>
  );
}
