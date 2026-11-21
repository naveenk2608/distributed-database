import { useState } from "react";
import { useOverview, type Member, type Overview, type ShardView } from "../api";
import { SearchIcon } from "../components/icons";
import { Card, cx, EmptyState, Label, PageState, Pill, Skeleton } from "../components/ui";
import { count, shardName, shortID } from "../format";

// A member chip reads "<short id>[ (learner)] · lag N · applied N"; the
// leader's carries a "leader" tag in front.
function MemberChip({ m, leader }: { m: Member; leader: boolean }) {
  const learner = !m.voter;
  return (
    <li
      title={`${m.node_id} (${leader ? "leader" : learner ? "learner" : "voter"})`}
      className={cx(
        "inline-flex items-center gap-1.5 whitespace-nowrap rounded-md border px-1.5 py-0.5 font-mono text-xs",
        leader ? "border-ok/40 bg-ok/10" : learner ? "border-dashed border-warn/70" : "border-line bg-raised",
      )}
    >
      {leader && <span className="font-sans text-[10px] font-semibold uppercase tracking-wider text-ok">{"leader "}</span>}
      <span className="text-fg">{shortID(m.node_id)}</span>
      {learner && <span className="font-sans text-warn">{" (learner)"}</span>}
      <span className={m.lag > 0 ? "font-medium text-fg" : "text-muted"}>{` · lag ${count(m.lag)}`}</span>
      <span className="text-muted">{` · applied ${m.applied === null ? "?" : count(m.applied)}`}</span>
    </li>
  );
}

// matches: the filter is a shard name ("12", "meta"), or the start of a
// node ID or address of the leader or any member.
function matches(s: ShardView, needle: string, addrOf: Map<string, string>): boolean {
  if (shardName(s.shard) === needle) return true;
  const ids = [s.leader, ...s.members.map((m) => m.node_id)].filter(Boolean);
  return ids.some((id) => id.toLowerCase().startsWith(needle) || (addrOf.get(id) ?? "").toLowerCase().startsWith(needle));
}

const th = "sticky top-0 z-10 border-b border-line bg-surface px-3 py-2 text-xs font-medium uppercase tracking-wider text-muted first:pl-4";
const td = "border-b border-line px-3 py-2 align-middle first:pl-4 group-last:border-b-0";

// ShardTable: one row per group. The leader is the highlighted, labelled
// chip among the replicas (nodes keep their order, so leadership reads down
// the columns); a group without one says so in red.
function ShardTable({ shards }: { shards: ShardView[] }) {
  return (
    <div className="overflow-x-auto lg:overflow-visible">
      <table data-testid="shards" className="w-full min-w-[56rem] border-separate border-spacing-0 text-left text-sm lg:min-w-0">
        <thead>
          <tr>
            <th scope="col" className={cx(th, "w-16")}>Shard</th>
            <th scope="col" className={cx(th, "w-16 text-right")}>Term</th>
            <th scope="col" className={cx(th, "w-20 text-right")}>Commit</th>
            <th scope="col" className={th}>Replicas</th>
          </tr>
        </thead>
        <tbody>
          {shards.map((s) => (
            <tr key={s.shard} className="group hover:bg-raised/60" data-testid={`shard-${s.shard}`}>
              <td className={cx(td, "font-medium text-fg")}>{shardName(s.shard)}</td>
              <td className={cx(td, "text-right font-mono text-xs text-fg")}>{s.leader ? count(s.term) : "–"}</td>
              <td className={cx(td, "text-right font-mono text-xs text-muted")}>{s.leader ? count(s.commit) : "–"}</td>
              <td className={td}>
                <ul className="flex flex-wrap items-center gap-1.5">
                  {!s.leader && (
                    <li>
                      <Pill tone="bad" dot title="No member leads this group">
                        leader: none
                      </Pill>
                    </li>
                  )}
                  {s.members.map((m) => (
                    <MemberChip key={m.node_id} m={m} leader={m.node_id === s.leader} />
                  ))}
                </ul>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Legend() {
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-2 border-t border-line px-4 py-3 text-xs text-muted">
      <span className="inline-flex items-center gap-2">
        <span className="rounded-md border border-ok/40 bg-ok/10 px-1.5 font-sans text-[10px] font-semibold uppercase tracking-wider text-ok">leader</span>
        leads the group
      </span>
      <span className="inline-flex items-center gap-2">
        <span className="h-4 w-6 rounded-md border border-line bg-raised" aria-hidden="true" />
        voter
      </span>
      <span className="inline-flex items-center gap-2">
        <span className="h-4 w-6 rounded-md border border-dashed border-warn/70" aria-hidden="true" />
        learner (not yet voting)
      </span>
      <span>lag in log entries · applied index</span>
    </div>
  );
}

function Toolbar({ data, shown, filter, setFilter }: { data: Overview; shown: number; filter: string; setFilter: (v: string) => void }) {
  const total = data.shards.length;
  const leaderless = data.shards.filter((s) => s.leader === "").length;
  return (
    <div className="flex flex-wrap items-center justify-between gap-4 border-b border-line p-4">
      <div>
        <Label as="h2">Raft groups</Label>
        <p className="mt-1 text-sm text-muted">
          {filter.trim() ? `${count(shown)} of ${count(total)} groups` : `${count(total)} groups`} across {count(data.nodes.length)} nodes
          {leaderless > 0 && <span className="text-bad"> · {count(leaderless)} without a leader</span>}
        </p>
      </div>
      <label className="relative w-full sm:w-72">
        <span className="sr-only">Filter shards</span>
        <SearchIcon className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted" />
        <input
          type="search"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          placeholder="Shard, node ID or address"
          className="w-full rounded-lg border border-line bg-bg py-2 pl-9 pr-3 text-sm text-fg placeholder:text-muted focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
        />
      </label>
    </div>
  );
}

function ShardsSkeleton() {
  return (
    <Card>
      <div className="flex items-center justify-between gap-4 border-b border-line p-4">
        <div>
          <Skeleton className="h-3 w-24" />
          <Skeleton className="mt-2 h-4 w-40" />
        </div>
        <Skeleton className="h-9 w-72" />
      </div>
      <div className="grid gap-4 p-4">
        {Array.from({ length: 10 }, (_, i) => (
          <div key={i} className="flex items-center gap-6">
            <Skeleton className="h-4 w-10" />
            <Skeleton className="h-4 w-20" />
            <Skeleton className="h-4 w-12" />
            <Skeleton className="h-4 flex-1" />
          </div>
        ))}
      </div>
    </Card>
  );
}

export function ShardsPage() {
  const { data, error } = useOverview();
  const [filter, setFilter] = useState("");
  if (!data) return <PageState error={error} skeleton={<ShardsSkeleton />} />;
  const needle = filter.trim().toLowerCase();
  const addrOf = new Map(data.nodes.map((n) => [n.node_id, n.addr]));
  const shown = needle ? data.shards.filter((s) => matches(s, needle, addrOf)) : data.shards;
  return (
    <Card>
      <Toolbar data={data} shown={shown.length} filter={filter} setFilter={setFilter} />
      {data.shards.length === 0 ? (
        <EmptyState title="No Raft groups reported yet">Groups show up once a node answers the console's poll.</EmptyState>
      ) : shown.length === 0 ? (
        <EmptyState
          title={`No groups match “${filter.trim()}”`}
          action={
            <button type="button" onClick={() => setFilter("")} className="rounded-lg border border-line px-3 py-1.5 text-sm text-fg hover:bg-raised">
              Clear filter
            </button>
          }
        >
          Try a shard number, “meta”, or the start of a node ID or address.
        </EmptyState>
      ) : (
        <>
          <ShardTable shards={shown} />
          <Legend />
        </>
      )}
    </Card>
  );
}
