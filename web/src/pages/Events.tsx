import { useState } from "react";
import { useEvents } from "../api";
import { Card, Loading, Table } from "../components/ui";
import { clock, count, shardName, shortID } from "../format";

export function EventsPage() {
  const { data, error } = useEvents();
  const [kind, setKind] = useState("");
  if (!data) return <Loading error={error} />;
  const kinds = [...new Set(data.map((e) => e.kind))].sort();
  const shown = data.filter((e) => kind === "" || e.kind === kind).slice().reverse();
  return (
    <Card title="Events: elections, failures, membership changes">
      <label className="mb-3 block text-sm">
        Kind{" "}
        <select className="rounded border border-slate-300 px-2 py-1" value={kind} onChange={(e) => setKind(e.target.value)}>
          <option value="">all</option>
          {kinds.map((k) => (
            <option key={k}>{k}</option>
          ))}
        </select>
      </label>
      <Table head={["Time (UTC)", "Event", "Shard", "Node", "Term", "Index", "Reported by", "Detail"]} testid="events">
        {shown.map((e) => (
          <tr key={`${e.reporter}-${e.seq}-${e.time}-${e.kind}`} className="border-b border-slate-100" data-kind={e.kind}>
            <td className="px-2 py-1 font-mono text-xs">{clock(e.time)}</td>
            <td className="px-2 py-1">{e.kind}</td>
            <td className="px-2 py-1">{shardName(e.shard)}</td>
            <td className="px-2 py-1 font-mono text-xs" title={e.node}>
              {e.node ? shortID(e.node) : "–"}
            </td>
            <td className="px-2 py-1">{e.term ? count(e.term) : "–"}</td>
            <td className="px-2 py-1">{e.index ? count(e.index) : "–"}</td>
            <td className="px-2 py-1 font-mono text-xs" title={e.reporter}>
              {shortID(e.reporter)}
            </td>
            <td className="px-2 py-1 text-xs text-slate-600">{e.detail ?? ""}</td>
          </tr>
        ))}
      </Table>
    </Card>
  );
}
