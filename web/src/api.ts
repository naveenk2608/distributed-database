// Types mirroring dkv-console's JSON (internal/console/model.go), and the
// hooks the pages read them through. The dashboard computes nothing of its
// own: every number it shows is a field of these, which the console
// derives from the nodes' metrics.
import { useEffect, useState } from "react";
import { QueryClient, useQueries, useQuery, useQueryClient } from "@tanstack/react-query";

export interface Rates {
  reads_per_sec: number;
  writes_per_sec: number;
  errors_per_sec: number;
  p50_ms: number | null;
  p99_ms: number | null;
}

export interface StorageView {
  memtable_bytes: number;
  table_bytes: number;
  tables: number;
  flushes: number;
  compactions: number;
  compaction_bytes_read: number;
  compaction_bytes_written: number;
  write_stalls: number;
  cache_used_bytes: number;
  cache_capacity_bytes: number;
  cache_hits: number;
  cache_misses: number;
}

export interface Replica {
  shard: number;
  role: string;
  term: number;
  leader: string;
  commit: number;
  applied: number;
  first_index: number;
  last_index: number;
  snapshots_installed: number;
  snapshots_sent: number;
  memtable_bytes: number;
  table_bytes: number;
  tables: number;
  compactions: number;
}

export interface NodeView {
  addr: string;
  node_id: string;
  up: boolean;
  error?: string;
  last_seen?: string;
  state: string;
  drained: boolean;
  suspect: boolean;
  started_at?: string;
  rates: Rates;
  leaders: number;
  storage: StorageView;
  replicas: Replica[];
}

export interface Member {
  node_id: string;
  voter: boolean;
  match: number;
  lag: number;
  applied: number | null;
}

export interface ShardView {
  shard: number;
  leader: string;
  term: number;
  commit: number;
  voters: string[];
  learners: string[];
  members: Member[];
}

export interface EventView {
  time: string;
  kind: string;
  reporter: string;
  shard: number | null;
  node?: string;
  term?: number;
  index?: number;
  detail?: string;
  seq: number;
}

export interface Overview {
  generated_at: string;
  poll_interval_ms: number;
  nodes: NodeView[];
  shards: ShardView[];
  totals: Rates;
  events: EventView[];
}

export interface Sample {
  time: string;
  addr: string;
  node_id: string;
  up: boolean;
  rates: Rates;
  memtable_bytes: number;
  table_bytes: number;
  leaders: number;
}

// historyLength bounds the samples kept per node in the browser.
export const historyLength = 600;

async function getJSON<T>(path: string): Promise<T> {
  const resp = await fetch(path);
  if (!resp.ok) throw new Error(`${path}: ${resp.status} ${resp.statusText}`);
  return (await resp.json()) as T;
}

export const overviewKey = ["overview"] as const;
export const historyKey = (addr: string) => ["history", addr] as const;
export const eventsKey = ["events"] as const;

export function useOverview() {
  return useQuery({ queryKey: overviewKey, queryFn: () => getJSON<Overview>("/api/overview") });
}

export function useHistory(addr: string) {
  return useQuery({
    queryKey: historyKey(addr),
    queryFn: () => getJSON<Sample[]>(`/api/history?addr=${encodeURIComponent(addr)}`),
    staleTime: Infinity, // kept current by the stream
  });
}

export function useHistories(addrs: string[]) {
  return useQueries({
    queries: addrs.map((addr) => ({
      queryKey: historyKey(addr),
      queryFn: () => getJSON<Sample[]>(`/api/history?addr=${encodeURIComponent(addr)}`),
      staleTime: Infinity,
    })),
  });
}

export function useEvents() {
  return useQuery({ queryKey: eventsKey, queryFn: () => getJSON<EventView[]>("/api/events?limit=5000") });
}

// applyOverview puts a streamed overview into the cache: the overview
// itself, one history sample per node (the same sample the console keeps),
// and its events.
export function applyOverview(qc: QueryClient, ov: Overview) {
  qc.setQueryData(overviewKey, ov);
  for (const n of ov.nodes) {
    const s: Sample = {
      time: ov.generated_at,
      addr: n.addr,
      node_id: n.node_id,
      up: n.up,
      rates: n.rates,
      memtable_bytes: n.storage.memtable_bytes,
      table_bytes: n.storage.table_bytes,
      leaders: n.leaders,
    };
    qc.setQueryData<Sample[]>(historyKey(n.addr), (old) => (old ? [...old, s].slice(-historyLength) : old));
  }
  qc.setQueryData<EventView[]>(eventsKey, (old) => {
    if (!old) return old;
    const seen = new Set(old.map(eventKey));
    const fresh = ov.events.filter((e) => !seen.has(eventKey(e)));
    return fresh.length ? [...old, ...fresh] : old;
  });
}

function eventKey(e: EventView): string {
  return `${e.kind}|${e.reporter}|${e.seq}|${e.time}|${e.shard}|${e.index}`;
}

export type StreamState = "connecting" | "live" | "reconnecting";

// useStream follows the console's server-sent overviews; EventSource
// reconnects by itself.
export function useStream(): StreamState {
  const qc = useQueryClient();
  const [state, setState] = useState<StreamState>("connecting");
  useEffect(() => {
    const es = new EventSource("/api/stream");
    es.addEventListener("overview", (msg) => {
      applyOverview(qc, JSON.parse((msg as MessageEvent<string>).data) as Overview);
      setState("live");
    });
    es.onerror = () => setState("reconnecting");
    return () => es.close();
  }, [qc]);
  return state;
}
