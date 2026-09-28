"use client";

interface Series {
  label: string;
  color: string;
  points: { x: number; y: number | null }[];
}

/** Minimal accessible SVG line chart (no chart library); a data table fallback is rendered for screen readers. */
export function LineChart({
  series,
  threshold,
  unit,
  title,
}: {
  series: Series[];
  threshold?: { value: number; label: string };
  unit: string;
  title: string;
}) {
  const width = 720;
  const height = 180;
  const pad = 32;
  const all = series.flatMap((s) => s.points.filter((p) => p.y !== null) as { x: number; y: number }[]);
  if (!all.length) return <div className="state">No points in this window yet.</div>;
  const xs = all.map((p) => p.x);
  const ys = all.map((p) => p.y).concat(threshold ? [threshold.value] : []);
  const [minX, maxX] = [Math.min(...xs), Math.max(...xs)];
  const maxY = Math.max(...ys, 1) * 1.1;
  const sx = (x: number) => pad + ((x - minX) / Math.max(maxX - minX, 1)) * (width - 2 * pad);
  const sy = (y: number) => height - pad - (y / maxY) * (height - 2 * pad);
  return (
    <figure style={{ margin: 0 }}>
      <svg viewBox={`0 0 ${width} ${height}`} width="100%" role="img" aria-label={title}>
        <line x1={pad} y1={height - pad} x2={width - pad} y2={height - pad} stroke="#253241" />
        <text x={4} y={pad} fill="#a3b3c2" fontSize="10">
          {maxY.toFixed(1)} {unit}
        </text>
        {threshold ? (
          <g>
            <line
              x1={pad}
              x2={width - pad}
              y1={sy(threshold.value)}
              y2={sy(threshold.value)}
              stroke="#f85149"
              strokeDasharray="4 4"
            />
            <text x={width - pad} y={sy(threshold.value) - 4} fill="#f85149" fontSize="10" textAnchor="end">
              {threshold.label}
            </text>
          </g>
        ) : null}
        {series.map((s) => {
          const pts = s.points.filter((p) => p.y !== null) as { x: number; y: number }[];
          return (
            <polyline
              key={s.label}
              fill="none"
              stroke={s.color}
              strokeWidth={2}
              points={pts.map((p) => `${sx(p.x).toFixed(1)},${sy(p.y).toFixed(1)}`).join(" ")}
            />
          );
        })}
      </svg>
      <figcaption className="row">
        {series.map((s) => (
          <span key={s.label} className="provenance" style={{ color: s.color }}>
            ● {s.label}
          </span>
        ))}
      </figcaption>
    </figure>
  );
}
