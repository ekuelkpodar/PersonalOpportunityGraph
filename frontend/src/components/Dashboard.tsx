import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { getDashboardStats } from '../lib/api';
import { scoreColor, fmtScore, nodeTypeColor, VENTURE_LABELS } from '../lib/utils';
import type { NodeSummary, VentureContext } from '../types';
import {
  PieChart, Pie, Cell, Tooltip, BarChart, Bar, XAxis, YAxis,
  ResponsiveContainer, Legend,
} from 'recharts';

const VENTURES: VentureContext[] = ['applied_insights', 'aegis_t2a', 'rgn_trucking', 'job_search'];

export default function Dashboard() {
  const [activeVenture, setActiveVenture] = useState<VentureContext>('applied_insights');

  const { data: stats, isLoading } = useQuery({
    queryKey: ['dashboard'],
    queryFn: getDashboardStats,
    staleTime: 60_000,
  });

  if (isLoading) {
    return (
      <div className="flex items-center justify-center h-full">
        <div className="w-8 h-8 border-2 border-[#14b8a6] border-t-transparent rounded-full animate-spin" />
      </div>
    );
  }

  const nodeTypeData = stats
    ? Object.entries(stats.node_type_distribution).map(([name, value]) => ({ name, value }))
    : [];

  const warmthData = stats
    ? [
        { name: 'Hot', value: stats.warmth_distribution['hot'] || 0, fill: '#f87171' },
        { name: 'Warm', value: stats.warmth_distribution['warm'] || 0, fill: '#fbbf24' },
        { name: 'Cool', value: stats.warmth_distribution['cool'] || 0, fill: '#60a5fa' },
        { name: 'Cold', value: stats.warmth_distribution['cold'] || 0, fill: '#2a2d3a' },
      ]
    : [];

  const confData = stats
    ? [
        { name: 'High', value: stats.confidence_distribution['high'] || 0 },
        { name: 'Med',  value: stats.confidence_distribution['medium'] || 0 },
        { name: 'Low',  value: stats.confidence_distribution['low'] || 0 },
      ]
    : [];

  const topOpps = stats?.top_opportunities[activeVenture] || [];

  return (
    <div className="h-full overflow-y-auto bg-[#0f1117] p-6 space-y-6">
      {/* Corpus Stats */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        {[
          { label: 'Total Nodes', value: stats?.total_nodes ?? 0, color: '#14b8a6' },
          { label: 'Total Edges', value: stats?.total_edges ?? 0, color: '#a78bfa' },
          { label: 'Unexpected Opps', value: stats?.unexpected_opps_count ?? 0, color: '#f87171' },
          { label: 'Trending', value: stats?.trending_nodes.length ?? 0, color: '#f59e0b' },
        ].map(stat => (
          <div key={stat.label} className="bg-[#1a1d27] border border-[#2a2d3a] rounded-xl p-4">
            <p className="text-xs text-[#64748b] uppercase tracking-widest mb-1">{stat.label}</p>
            <p className="text-2xl font-bold" style={{ color: stat.color }}>{stat.value.toLocaleString()}</p>
          </div>
        ))}
      </div>

      {/* Node type + warmth charts row */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
        {/* Node type donut */}
        <div className="bg-[#1a1d27] border border-[#2a2d3a] rounded-xl p-4">
          <p className="text-xs text-[#64748b] uppercase tracking-widest mb-3">Node Types</p>
          <ResponsiveContainer width="100%" height={160}>
            <PieChart>
              <Pie data={nodeTypeData} dataKey="value" nameKey="name" cx="50%" cy="50%" outerRadius={60} innerRadius={35}>
                {nodeTypeData.map((entry) => (
                  <Cell key={entry.name} fill={nodeTypeColor(entry.name)} />
                ))}
              </Pie>
              <Tooltip
                contentStyle={{ background: '#1a1d27', border: '1px solid #2a2d3a', borderRadius: 8, fontSize: 12 }}
              />
              <Legend iconSize={8} wrapperStyle={{ fontSize: 11 }} />
            </PieChart>
          </ResponsiveContainer>
        </div>

        {/* Warmth distribution */}
        <div className="bg-[#1a1d27] border border-[#2a2d3a] rounded-xl p-4">
          <p className="text-xs text-[#64748b] uppercase tracking-widest mb-3">Warmth Tiers</p>
          <ResponsiveContainer width="100%" height={160}>
            <BarChart data={warmthData} barSize={24}>
              <XAxis dataKey="name" tick={{ fill: '#64748b', fontSize: 11 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false} />
              <Tooltip
                contentStyle={{ background: '#1a1d27', border: '1px solid #2a2d3a', borderRadius: 8 }}
              />
              <Bar dataKey="value" radius={[4, 4, 0, 0]}>
                {warmthData.map((entry) => (
                  <Cell key={entry.name} fill={entry.fill} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Confidence distribution */}
        <div className="bg-[#1a1d27] border border-[#2a2d3a] rounded-xl p-4">
          <p className="text-xs text-[#64748b] uppercase tracking-widest mb-3">Data Confidence</p>
          <ResponsiveContainer width="100%" height={160}>
            <BarChart data={confData} barSize={32}>
              <XAxis dataKey="name" tick={{ fill: '#64748b', fontSize: 11 }} axisLine={false} tickLine={false} />
              <YAxis tick={{ fill: '#64748b', fontSize: 10 }} axisLine={false} tickLine={false} />
              <Tooltip contentStyle={{ background: '#1a1d27', border: '1px solid #2a2d3a', borderRadius: 8 }} />
              <Bar dataKey="value" fill="#a78bfa" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Top Opportunities by venture */}
      <div className="bg-[#1a1d27] border border-[#2a2d3a] rounded-xl p-4">
        <div className="flex items-center justify-between mb-4 flex-wrap gap-2">
          <p className="text-xs text-[#64748b] uppercase tracking-widest">Top Opportunities</p>
          <div className="flex rounded-lg overflow-hidden border border-[#2a2d3a]">
            {VENTURES.map(v => (
              <button
                key={v}
                onClick={() => setActiveVenture(v)}
                className={`px-3 py-1 text-xs font-medium transition-colors ${
                  activeVenture === v ? 'bg-[#14b8a6] text-black' : 'text-[#64748b] hover:text-[#e2e8f0]'
                }`}
              >
                {VENTURE_LABELS[v]}
              </button>
            ))}
          </div>
        </div>
        <div className="space-y-2">
          {topOpps.map((node: NodeSummary, i: number) => (
            <div key={node.id} className="flex items-center gap-3">
              <span className="text-xs text-[#64748b] w-4">{i + 1}</span>
              <div className="w-2 h-2 rounded-full flex-shrink-0" style={{ background: nodeTypeColor(node.node_type) }} />
              <span className="text-sm text-[#e2e8f0] flex-1 truncate">{node.name}</span>
              <div className="w-24 h-1.5 bg-[#2a2d3a] rounded-full overflow-hidden">
                <div className="h-full rounded-full" style={{
                  width: `${node.opportunity_score * 100}%`,
                  background: scoreColor(node.opportunity_score)
                }} />
              </div>
              <span className="text-xs font-bold w-10 text-right" style={{ color: scoreColor(node.opportunity_score) }}>
                {fmtScore(node.opportunity_score)}
              </span>
            </div>
          ))}
          {topOpps.length === 0 && (
            <p className="text-sm text-[#64748b] text-center py-4">
              Run the pipeline and scoring job to see opportunities
            </p>
          )}
        </div>
      </div>

      {/* Trending nodes */}
      {stats?.trending_nodes && stats.trending_nodes.length > 0 && (
        <div className="bg-[#1a1d27] border border-[#2a2d3a] rounded-xl p-4">
          <p className="text-xs text-[#64748b] uppercase tracking-widest mb-3">🔥 Trending This Week</p>
          <div className="flex flex-wrap gap-2">
            {stats.trending_nodes.slice(0, 10).map(node => (
              <span key={node.id} className="text-xs px-3 py-1.5 rounded-full bg-[#f59e0b]/10 text-[#f59e0b] border border-[#f59e0b]/20">
                {node.name}
              </span>
            ))}
          </div>
        </div>
      )}

      {/* Interaction summary */}
      {stats?.interaction_summary && Object.keys(stats.interaction_summary).length > 0 && (
        <div className="bg-[#1a1d27] border border-[#2a2d3a] rounded-xl p-4">
          <p className="text-xs text-[#64748b] uppercase tracking-widest mb-3">Outreach Outcomes</p>
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            {['replied', 'meeting', 'converted', 'not_relevant'].map(outcome => {
              const count = typeof stats.interaction_summary === 'object'
                ? Object.values(stats.interaction_summary).reduce(
                    (acc: number, v: any) => acc + (v[outcome] || 0), 0
                  )
                : 0;
              return (
                <div key={outcome} className="text-center">
                  <p className="text-xl font-bold text-[#e2e8f0]">{count}</p>
                  <p className="text-xs text-[#64748b] capitalize">{outcome.replace(/_/g, ' ')}</p>
                </div>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
