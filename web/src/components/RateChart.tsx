import { CartesianGrid, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

export interface Point {
  t: string;
  [series: string]: number | string | null;
}

// Series: a line of the chart. color is a CSS value (var(--series-n)).
export interface Series {
  key: string;
  label: string;
  color: string;
}

const tick = { fill: "rgb(var(--muted))", fontSize: 11 };

function time(t: string): string {
  return typeof t === "string" ? t.slice(11, 19) : String(t);
}

// ChartLegend keys each series with a short stroke of its color, as the
// lines are drawn.
export function ChartLegend({ series }: { series: Series[] }) {
  return (
    <ul className="flex flex-wrap items-center gap-4 text-xs text-muted">
      {series.map((s) => (
        <li key={s.key} className="inline-flex items-center gap-2">
          <span aria-hidden="true" className="h-0.5 w-3 rounded-full" style={{ background: s.color }} />
          {s.label}
        </li>
      ))}
    </ul>
  );
}

interface TooltipEntry {
  dataKey?: string | number;
  value?: number | string | null;
}

function ChartTooltip({
  active,
  payload,
  label,
  series,
  format,
}: {
  active?: boolean;
  payload?: TooltipEntry[];
  label?: string;
  series: Series[];
  format: (n: number) => string;
}) {
  if (!active || !payload?.length) return null;
  return (
    <div className="min-w-40 rounded-lg border border-line bg-surface px-3 py-2 text-xs shadow-lg shadow-black/20">
      <div className="mb-1 font-mono text-muted">{label ? `${time(label)} UTC` : ""}</div>
      <ul className="grid gap-1">
        {series.map((s) => {
          const v = payload.find((p) => p.dataKey === s.key)?.value;
          return (
            <li key={s.key} className="flex items-center gap-2">
              <span aria-hidden="true" className="h-0.5 w-3 rounded-full" style={{ background: s.color }} />
              <span className="font-semibold text-fg">{typeof v === "number" ? format(v) : "–"}</span>
              <span className="text-muted">{s.label}</span>
            </li>
          );
        })}
      </ul>
    </div>
  );
}

// RateChart draws series over time; points are taken from the console's
// samples as they are.
export function RateChart({
  points,
  series,
  format,
  formatTick = format,
  height = 256,
  yWidth = 48,
}: {
  points: Point[];
  series: Series[];
  format: (n: number) => string;
  formatTick?: (n: number) => string;
  height?: number;
  yWidth?: number;
}) {
  if (points.length === 0) {
    return (
      <div style={{ height }} className="grid place-items-center rounded-lg border border-dashed border-line text-sm text-muted">
        Waiting for the first samples…
      </div>
    );
  }
  return (
    <div style={{ height }} className="w-full min-w-0">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={points} margin={{ left: 0, right: 8, top: 8, bottom: 0 }}>
          <CartesianGrid vertical={false} stroke="var(--grid)" />
          <XAxis
            dataKey="t"
            tickFormatter={time}
            minTickGap={64}
            tick={tick}
            tickLine={false}
            axisLine={{ stroke: "rgb(var(--line))" }}
            tickMargin={8}
          />
          <YAxis tickFormatter={formatTick} tick={tick} tickLine={false} axisLine={false} width={yWidth} tickMargin={4} />
          <Tooltip
            content={(p) => (
              <ChartTooltip
                active={p.active}
                payload={p.payload as TooltipEntry[] | undefined}
                label={p.label as string | undefined}
                series={series}
                format={format}
              />
            )}
            cursor={{ stroke: "rgb(var(--line-strong))", strokeWidth: 1 }}
            isAnimationActive={false}
          />
          {series.map((s) => (
            <Line
              key={s.key}
              type="monotone"
              dataKey={s.key}
              name={s.label}
              stroke={s.color}
              strokeWidth={2}
              strokeLinecap="round"
              strokeLinejoin="round"
              dot={false}
              activeDot={{ r: 4, strokeWidth: 2, stroke: "rgb(var(--surface))", fill: s.color }}
              isAnimationActive={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
