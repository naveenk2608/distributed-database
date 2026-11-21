import { memo, useState, type ReactNode } from "react";
import { useEvents, type EventView } from "../api";
import { Card, cx, dotTone, EmptyState, EventBadge, eventTone, Label, PageState, Skeleton } from "../components/ui";
import { ago, clock, count, kindLabel, shardName, shortID } from "../format";
import { useNow } from "../theme";

// Facts: what the event concerns, as "shard 3 · node 1a2b3c4d · term 7 · index 1,204".
function Facts({ e }: { e: EventView }) {
  const parts: ReactNode[] = [];
  if (e.shard !== null)
    parts.push(
      <span key="shard">
        shard <span className="font-medium text-fg">{shardName(e.shard)}</span>
      </span>,
    );
  if (e.node)
    parts.push(
      <span key="node">
        node{" "}
        <span className="font-mono text-fg" title={e.node}>
          {shortID(e.node)}
        </span>
      </span>,
    );
  if (e.term)
    parts.push(
      <span key="term">
        term <span className="font-mono text-fg">{count(e.term)}</span>
      </span>,
    );
  if (e.index)
    parts.push(
      <span key="index">
        index <span className="font-mono text-fg">{count(e.index)}</span>
      </span>,
    );
  return (
    <span className="text-sm text-muted">
      {parts.map((p, i) => (
        <span key={i}>
          {i > 0 && " · "}
          {p}
        </span>
      ))}
    </span>
  );
}

// EventRow is one step of the timeline: a rail with a dot in the event's
// tone, what happened, who reported it and when. Rows re-render only when
// their relative time changes.
const EventRow = memo(function EventRow({ e, when }: { e: EventView; when: string }) {
  return (
    <tr data-kind={e.kind} className="group align-top hover:bg-raised/40">
      <td className="relative w-10 p-0">
        <span
          aria-hidden="true"
          className="absolute bottom-0 left-5 top-0 w-px bg-line-strong group-first:top-[22px] group-last:bottom-auto group-last:h-[22px]"
        />
        <span
          aria-hidden="true"
          className={cx("absolute left-5 top-[22px] h-2.5 w-2.5 -translate-x-1/2 -translate-y-1/2 rounded-full ring-4 ring-surface", dotTone[eventTone(e.kind)])}
        />
      </td>
      <td className="py-3 pr-4">
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
          <EventBadge kind={e.kind} />
          <Facts e={e} />
        </div>
        <p className="mt-1 break-words text-xs text-muted">
          reported by{" "}
          <span className="font-mono" title={e.reporter}>
            {shortID(e.reporter)}
          </span>
          {e.detail && ` · ${e.detail}`}
        </p>
      </td>
      <td className="whitespace-nowrap py-3 pr-6 text-right">
        <time dateTime={e.time} title={e.time} className="block text-sm leading-5 text-fg">
          {when}
        </time>
        <span className="font-mono text-xs text-muted">{clock(e.time)}</span>
      </td>
    </tr>
  );
});

function EventsSkeleton() {
  return (
    <Card>
      <div className="flex items-center justify-between gap-4 border-b border-line p-4">
        <div>
          <Skeleton className="h-3 w-20" />
          <Skeleton className="mt-2 h-4 w-28" />
        </div>
        <Skeleton className="h-9 w-48" />
      </div>
      <div className="grid gap-6 p-6">
        {Array.from({ length: 8 }, (_, i) => (
          <div key={i} className="flex items-center gap-4">
            <Skeleton className="h-2.5 w-2.5 rounded-full" />
            <Skeleton className="h-5 w-28 rounded-full" />
            <Skeleton className="h-4 flex-1" />
            <Skeleton className="h-4 w-16" />
          </div>
        ))}
      </div>
    </Card>
  );
}

export function EventsPage() {
  const { data, error } = useEvents();
  const [kind, setKind] = useState("");
  const now = useNow(1000);
  if (!data) return <PageState error={error} skeleton={<EventsSkeleton />} />;
  const perKind = new Map<string, number>();
  for (const e of data) perKind.set(e.kind, (perKind.get(e.kind) ?? 0) + 1);
  const kinds = [...perKind.keys()].sort();
  const shown = data.filter((e) => kind === "" || e.kind === kind).slice().reverse();
  return (
    <Card>
      <div className="flex flex-wrap items-center justify-between gap-4 border-b border-line p-4">
        <div>
          <Label as="h2">Timeline</Label>
          <p className="mt-1 text-sm text-muted">
            {kind ? `${count(shown.length)} of ${count(data.length)} events` : `${count(data.length)} events`}, times in UTC
          </p>
        </div>
        <label className="flex items-center gap-2 text-sm text-muted">
          Kind
          <select
            className="rounded-lg border border-line bg-bg px-3 py-2 text-sm text-fg focus:border-accent focus:outline-none focus:ring-1 focus:ring-accent"
            value={kind}
            onChange={(e) => setKind(e.target.value)}
          >
            <option value="">All kinds</option>
            {kinds.map((k) => (
              <option key={k} value={k}>
                {kindLabel(k)} ({count(perKind.get(k) ?? 0)})
              </option>
            ))}
          </select>
        </label>
      </div>
      {data.length === 0 ? (
        <EmptyState title="No events yet">Elections, failures and membership changes will show here as the nodes report them.</EmptyState>
      ) : (
        <div className="overflow-x-auto px-2 py-2">
          <table data-testid="events" className="w-full border-separate border-spacing-0 text-left">
            <thead className="sr-only">
              <tr>
                <th scope="col">Tone</th>
                <th scope="col">Event</th>
                <th scope="col">Time</th>
              </tr>
            </thead>
            <tbody>
              {shown.map((e) => (
                <EventRow key={`${e.reporter}-${e.seq}-${e.time}-${e.kind}`} e={e} when={ago(e.time, now)} />
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
}
