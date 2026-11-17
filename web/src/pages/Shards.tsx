import { useOverview } from "../api";
import { Card, Loading, Table } from "../components/ui";
import { count, shardName, shortID } from "../format";

export function ShardsPage() {
  const { data, error } = useOverview();
  if (!data) return <Loading error={error} />;
  return (
    <Card title="Shards: placement, leaders and replication lag">
      <Table head={["Shard", "Leader", "Term", "Commit", "Members (lag in entries · applied)"]} testid="shards">
        {data.shards.map((s) => (
          <tr key={s.shard} className="border-b border-slate-100 align-top" data-testid={`shard-${s.shard}`}>
            <td className="px-2 py-1 font-semibold">{shardName(s.shard)}</td>
            <td className="px-2 py-1 font-mono" title={s.leader}>
              {s.leader ? shortID(s.leader) : "none"}
            </td>
            <td className="px-2 py-1">{s.leader ? count(s.term) : "–"}</td>
            <td className="px-2 py-1">{s.leader ? count(s.commit) : "–"}</td>
            <td className="px-2 py-1">
              <ul className="flex flex-wrap gap-2">
                {s.members.map((m) => (
                  <li key={m.node_id} className="rounded bg-slate-100 px-2 py-0.5 font-mono text-xs" title={m.node_id}>
                    {shortID(m.node_id)}
                    {m.voter ? "" : " (learner)"} · lag {count(m.lag)} · applied {m.applied === null ? "?" : count(m.applied)}
                  </li>
                ))}
              </ul>
            </td>
          </tr>
        ))}
      </Table>
    </Card>
  );
}
