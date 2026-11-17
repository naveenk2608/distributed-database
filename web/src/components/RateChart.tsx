import { CartesianGrid, Legend, Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

export interface Point {
  t: string;
  [series: string]: number | string | null;
}

// RateChart draws series over time; points are taken from the console's
// samples as they are.
export function RateChart({ points, series, unit }: { points: Point[]; series: string[]; unit: string }) {
  const colors = ["#4f46e5", "#059669", "#dc2626", "#d97706", "#0891b2"];
  return (
    <div className="h-56">
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={points} margin={{ left: 8, right: 8, top: 8, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
          <XAxis dataKey="t" tickFormatter={(t: string) => t.slice(11, 19)} minTickGap={40} fontSize={11} />
          <YAxis fontSize={11} unit={unit} width={64} />
          <Tooltip />
          <Legend />
          {series.map((s, i) => (
            <Line key={s} type="monotone" dataKey={s} stroke={colors[i % colors.length]} dot={false} isAnimationActive={false} />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
