import { useHistory, useOverview, type NodeView } from "../api";
import { Card, Loading, RoleBadge, Stat, Table, UpBadge } from "../components/ui";
import { RateChart } from "../components/RateChart";
import { bytes, count, shardName } from "../format";

function NodeStorage({ node }: { node: NodeView }) {
  const history = useHistory(node.addr);
  const s = node.storage;
  const lookups = s.cache_hits + s.cache_misses;
  return (
    <Card testid={`node-detail-${node.addr}`}>
      <div className="mb-3 flex items-center gap-3">
        <h2 className="font-mono text-sm">{node.node_id || node.addr}</h2>
        <UpBadge up={node.up} />
        <span className="text-xs text-slate-500">{node.addr}</span>
      </div>
      {node.up ? (
        <>
          <div className="mb-3 grid grid-cols-2 gap-2 sm:grid-cols-4 lg:grid-cols-6">
            <Stat label="Memtables" value={bytes(s.memtable_bytes)} testid="memtable-bytes" />
            <Stat label="SSTables" value={`${count(s.tables)} · ${bytes(s.table_bytes)}`} testid="table-bytes" />
            <Stat label="Flushes" value={count(s.flushes)} />
            <Stat label="Compactions" value={count(s.compactions)} />
            <Stat
              label="Compaction read / written"
              value={`${bytes(s.compaction_bytes_read)} / ${bytes(s.compaction_bytes_written)}`}
            />
            <Stat label="Write stalls" value={count(s.write_stalls)} />
            <Stat label="Block cache" value={`${bytes(s.cache_used_bytes)} / ${bytes(s.cache_capacity_bytes)}`} />
            <Stat
              label="Cache hit rate"
              value={lookups ? `${((100 * s.cache_hits) / lookups).toFixed(1)}%` : "–"}
            />
          </div>
          <Table head={["Shard", "Role", "Term", "Commit", "Applied", "Log", "SSTables", "Snapshots in / out"]}>
            {node.replicas.map((r) => (
              <tr key={r.shard} className="border-b border-slate-100">
                <td className="px-2 py-1">{shardName(r.shard)}</td>
                <td className="px-2 py-1">
                  <RoleBadge role={r.role} />
                </td>
                <td className="px-2 py-1">{count(r.term)}</td>
                <td className="px-2 py-1">{count(r.commit)}</td>
                <td className="px-2 py-1">{count(r.applied)}</td>
                <td className="px-2 py-1">
                  {count(r.first_index)}–{count(r.last_index)}
                </td>
                <td className="px-2 py-1">
                  {count(r.tables)} · {bytes(r.table_bytes)}
                </td>
                <td className="px-2 py-1">
                  {count(r.snapshots_installed)} / {count(r.snapshots_sent)}
                </td>
              </tr>
            ))}
          </Table>
          <div className="mt-3">
            <RateChart
              points={(history.data ?? []).map((h) => ({ t: h.time, memtables: h.memtable_bytes, sstables: h.table_bytes }))}
              series={["memtables", "sstables"]}
              unit=" B"
            />
          </div>
        </>
      ) : (
        <p className="text-sm text-red-700">Not answering: {node.error ?? "no answer"}</p>
      )}
    </Card>
  );
}

export function NodesPage() {
  const { data, error } = useOverview();
  if (!data) return <Loading error={error} />;
  return (
    <div className="grid gap-4">
      {data.nodes.map((n) => (
        <NodeStorage key={n.addr} node={n} />
      ))}
    </div>
  );
}
