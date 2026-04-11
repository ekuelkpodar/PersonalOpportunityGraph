import { RadarChart, Radar, PolarGrid, PolarAngleAxis, ResponsiveContainer, Tooltip } from 'recharts';
import type { ScoreBreakdown } from '../types';

interface Props {
  breakdown: ScoreBreakdown;
  size?: number;
}

export default function ScoreRadar({ breakdown, size = 220 }: Props) {
  const data = [
    { subject: 'Relevance',      value: Math.round(breakdown.relevance * 100) },
    { subject: 'Reachability',   value: Math.round(breakdown.reachability * 100) },
    { subject: 'Influence',      value: Math.round(breakdown.influence * 100) },
    { subject: 'Responsiveness', value: Math.round(breakdown.responsiveness * 100) },
    { subject: 'Confidence',     value: Math.round(breakdown.confidence * 100) },
    { subject: 'Novelty',        value: Math.round(breakdown.novelty * 100) },
  ];

  return (
    <ResponsiveContainer width="100%" height={size}>
      <RadarChart data={data} margin={{ top: 10, right: 30, bottom: 10, left: 30 }}>
        <PolarGrid stroke="#2a2d3a" />
        <PolarAngleAxis
          dataKey="subject"
          tick={{ fill: '#64748b', fontSize: 10 }}
        />
        <Tooltip
          contentStyle={{ background: '#1a1d27', border: '1px solid #2a2d3a', borderRadius: 8 }}
          labelStyle={{ color: '#e2e8f0' }}
          formatter={((val: number | string | undefined) => [`${val ?? 0}%`, '']) as any}
        />
        <Radar
          name="Score"
          dataKey="value"
          stroke="#14b8a6"
          fill="#14b8a6"
          fillOpacity={0.2}
          strokeWidth={2}
        />
      </RadarChart>
    </ResponsiveContainer>
  );
}
