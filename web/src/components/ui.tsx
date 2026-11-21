import { useEffect, useRef, useState, type ReactNode } from "react";
import { ago, clock, kindLabel } from "../format";
import { useNow } from "../theme";
import { AlertIcon, CheckIcon, CopyIcon, InboxIcon } from "./icons";

// Ago: a relative time that keeps itself current; the exact UTC time is
// its tooltip. For a few on a page; long lists share one clock instead.
export function Ago({ iso, className }: { iso: string; className?: string }) {
  const now = useNow(1000);
  return (
    <time dateTime={iso} title={`${clock(iso)} UTC`} className={className}>
      {ago(iso, now)}
    </time>
  );
}

export function cx(...classes: (string | false | null | undefined)[]): string {
  return classes.filter(Boolean).join(" ");
}

export function Card({
  children,
  className,
  testid,
}: {
  children: ReactNode;
  className?: string;
  testid?: string;
}) {
  return (
    <section className={cx("min-w-0 rounded-xl border border-line bg-surface", className)} data-testid={testid}>
      {children}
    </section>
  );
}

// Label is the small uppercase caption of a card, stat or field.
export function Label({ children, className, as: As = "div" }: { children: ReactNode; className?: string; as?: "div" | "dt" | "h2" | "span" }) {
  return <As className={cx("text-xs font-medium uppercase tracking-wider text-muted", className)}>{children}</As>;
}

// CardHeader: a card's label, an optional line under it, and actions on the right.
export function CardHeader({ label, sub, children }: { label: ReactNode; sub?: ReactNode; children?: ReactNode }) {
  return (
    <div className="flex flex-wrap items-start justify-between gap-4">
      <div className="min-w-0">
        <Label as="h2">{label}</Label>
        {sub && <p className="mt-1 text-sm text-muted">{sub}</p>}
      </div>
      {children}
    </div>
  );
}

// Tone: ok, warn and bad are state colors only (up/alive/leader,
// suspect/learner, down/dead); accent and neutral are for everything else.
export type Tone = "ok" | "warn" | "bad" | "accent" | "neutral";

const pillTone: Record<Tone, string> = {
  ok: "bg-ok/10 text-ok ring-ok/25",
  warn: "bg-warn/10 text-warn ring-warn/25",
  bad: "bg-bad/10 text-bad ring-bad/25",
  accent: "bg-accent/10 text-accent-fg ring-accent/25",
  neutral: "bg-raised text-muted ring-line-strong/60",
};

export const dotTone: Record<Tone, string> = {
  ok: "bg-ok",
  warn: "bg-warn",
  bad: "bg-bad",
  accent: "bg-accent",
  neutral: "bg-muted",
};

export function Dot({ tone, className }: { tone: Tone; className?: string }) {
  return <span aria-hidden="true" className={cx("inline-block h-1.5 w-1.5 shrink-0 rounded-full", dotTone[tone], className)} />;
}

// Pill: a short label on a tinted background; the text always says what the
// color does.
export function Pill({ tone, children, dot, title, className }: { tone: Tone; children: ReactNode; dot?: boolean; title?: string; className?: string }) {
  return (
    <span
      title={title}
      className={cx(
        "inline-flex items-center gap-1.5 whitespace-nowrap rounded-full px-2 py-0.5 text-xs font-medium ring-1 ring-inset",
        pillTone[tone],
        className,
      )}
    >
      {dot && <Dot tone={tone} />}
      {children}
    </span>
  );
}

// StatusPill: whether the node answered the console's last poll.
export function StatusPill({ up }: { up: boolean }) {
  return (
    <Pill tone={up ? "ok" : "bad"} dot className="uppercase tracking-wide">
      {up ? "up" : "down"}
    </Pill>
  );
}

// NodeState: the meta group's record of a node, with its suspect and
// drained flags.
export function NodeState({ state, suspect, drained }: { state: string; suspect: boolean; drained: boolean }) {
  const tone: Tone = state === "alive" ? "ok" : state === "dead" ? "bad" : "neutral";
  return (
    <span className="inline-flex flex-wrap items-center gap-2 text-xs">
      <span className="inline-flex items-center gap-1.5 text-fg">
        <Dot tone={tone} />
        {state}
      </span>
      {suspect && <Pill tone="warn">suspect</Pill>}
      {drained && <Pill tone="neutral">drained</Pill>}
    </span>
  );
}

export function RoleBadge({ role }: { role: string }) {
  const tone: Tone = role === "leader" ? "ok" : role === "learner" ? "warn" : "neutral";
  return <Pill tone={tone}>{role}</Pill>;
}

// eventTone colors an event by the state it reports: leadership won and
// nodes coming back are ok, nodes lost are bad, membership changes and
// transfers are accent, the rest neutral.
export function eventTone(kind: string): Tone {
  switch (kind) {
    case "leader_elected":
    case "peer_up":
    case "node_alive":
      return "ok";
    case "peer_down":
    case "node_dead":
      return "bad";
    case "replica_added":
    case "replica_promoted":
    case "replica_removed":
    case "leader_transfer":
      return "accent";
    default:
      return "neutral";
  }
}

export function EventBadge({ kind }: { kind: string }) {
  return (
    <Pill tone={eventTone(kind)} title={kind}>
      {kindLabel(kind)}
    </Pill>
  );
}

export function Skeleton({ className }: { className?: string }) {
  return <div aria-hidden="true" className={cx("skeleton", className)} />;
}

export function EmptyState({ title, children, action }: { title: string; children?: ReactNode; action?: ReactNode }) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 px-6 py-12 text-center">
      <span className="grid h-10 w-10 place-items-center rounded-full bg-raised text-muted">
        <InboxIcon className="h-5 w-5" />
      </span>
      <p className="mt-2 text-sm font-medium text-fg">{title}</p>
      {children && <p className="max-w-sm text-sm text-muted">{children}</p>}
      {action && <div className="mt-2">{action}</div>}
    </div>
  );
}

// PageState stands in for a page until its first answer: a skeleton of the
// page while loading, or why there is nothing to show.
export function PageState({ error, skeleton }: { error: unknown; skeleton: ReactNode }) {
  if (error) {
    return (
      <Card className="p-6">
        <div role="alert" className="flex items-start gap-4">
          <span className="grid h-8 w-8 shrink-0 place-items-center rounded-full bg-bad/10 text-bad">
            <AlertIcon />
          </span>
          <div className="min-w-0">
            <p className="font-medium text-fg">The console did not answer</p>
            <p className="mt-1 break-words font-mono text-xs text-muted">{String(error)}</p>
          </div>
        </div>
      </Card>
    );
  }
  return (
    <div role="status" aria-label="Loading">
      {skeleton}
    </div>
  );
}

async function copyText(text: string): Promise<void> {
  if (navigator.clipboard && window.isSecureContext) {
    await navigator.clipboard.writeText(text);
    return;
  }
  // Plain-HTTP consoles have no Clipboard API.
  const ta = document.createElement("textarea");
  ta.value = text;
  ta.setAttribute("readonly", "");
  ta.style.position = "fixed";
  ta.style.opacity = "0";
  document.body.appendChild(ta);
  ta.select();
  const ok = document.execCommand("copy");
  ta.remove();
  if (!ok) throw new Error("copy refused");
}

// CopyID shows a node ID's short form; a click copies the whole ID.
export function CopyID({ id, short }: { id: string; short: string }) {
  const [state, setState] = useState<"idle" | "copied" | "failed">("idle");
  const timer = useRef<ReturnType<typeof setTimeout>>();
  useEffect(() => () => clearTimeout(timer.current), []);
  const copy = () => {
    copyText(id).then(
      () => setState("copied"),
      () => setState("failed"),
    );
    clearTimeout(timer.current);
    timer.current = setTimeout(() => setState("idle"), 1500);
  };
  return (
    <button
      type="button"
      onClick={copy}
      title={`${id} (click to copy)`}
      aria-label={`Copy node ID ${id}`}
      className="group inline-flex items-center gap-1.5 rounded-md px-1 py-0.5 -mx-1 font-mono text-sm font-medium text-fg hover:bg-raised"
    >
      {short}
      <span className="text-muted group-hover:text-fg" aria-hidden="true">
        {state === "copied" ? <CheckIcon className="h-3.5 w-3.5 text-ok" /> : <CopyIcon className="h-3.5 w-3.5" />}
      </span>
      <span className="sr-only" aria-live="polite">
        {state === "copied" ? "Copied" : state === "failed" ? "Copy failed" : ""}
      </span>
      {state !== "idle" && (
        <span aria-hidden="true" className={cx("font-sans text-xs font-normal", state === "copied" ? "text-muted" : "text-bad")}>
          {state === "copied" ? "Copied" : "Copy failed"}
        </span>
      )}
    </button>
  );
}

// Kpi: one headline figure. value carries the testid; unit and sub are
// set apart but stay in the figure's text.
export function Kpi({ label, children, sub, testid }: { label: string; children: ReactNode; sub?: ReactNode; testid?: string }) {
  return (
    <Card className="p-4">
      <Label>{label}</Label>
      <div className="mt-2 truncate text-kpi tracking-tight text-fg" data-testid={testid}>
        {children}
      </div>
      {sub && <div className="mt-1 text-xs leading-5 text-muted">{sub}</div>}
    </Card>
  );
}

// Unit: a figure's unit, smaller and muted.
export function Unit({ children }: { children: ReactNode }) {
  return <span className="ml-0.5 text-base font-normal text-muted">{children}</span>;
}
