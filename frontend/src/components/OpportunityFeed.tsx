import { useState } from 'react';
import { useInfiniteQuery, useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getOpportunityFeed, getUnexpectedOpps, logInteraction } from '../lib/api';
import { nodeTypeColor, scoreColor, fmtScore, warmthLabel, warmthColor, VENTURE_LABELS, INTENT_LABELS } from '../lib/utils';
import type { NodeSummary, VentureContext, IntentMode, UnexpectedOpportunity } from '../types';
import ActionDrawer from './ActionDrawer';
import ScoreRadar from './ScoreRadar';

const VENTURES: VentureContext[] = ['applied_insights', 'aegis_t2a', 'rgn_trucking', 'job_search'];
const INTENTS: IntentMode[] = ['Exploit', 'Explore', 'Bridge', 'Recruit', 'Sell'];
const NODE_TYPES = ['Person', 'Company', 'Publisher', 'Community'];
const WARMTH_TIERS = ['hot', 'warm', 'cool', 'cold'];

interface Filters {
  node_types: string[];
  warmth_tiers: string[];
  min_score: number;
  location: string;
  topic_cluster: string;
}

export default function OpportunityFeed() {
  const [venture, setVenture] = useState<VentureContext>('applied_insights');
  const [intent, setIntent] = useState<IntentMode>('Exploit');
  const [filters, setFilters] = useState<Filters>({ node_types: [], warmth_tiers: ['hot', 'warm', 'cool', 'cold'], min_score: 0, location: '', topic_cluster: '' });
  const [selectedNode, setSelectedNode] = useState<NodeSummary | null>(null);
  const [actionNode, setActionNode] = useState<NodeSummary | null>(null);
  const qc = useQueryClient();

  const {
    data,
    fetchNextPage,
    hasNextPage,
    isFetchingNextPage,
    isLoading,
  } = useInfiniteQuery({
    queryKey: ['feed', venture, intent, filters],
    queryFn: ({ pageParam = 0 }) =>
      getOpportunityFeed({
        venture_context: venture,
        intent_mode: intent,
        node_types: filters.node_types,
        warmth_tiers: filters.warmth_tiers,
        min_score: filters.min_score,
        location: filters.location || undefined,
        page: pageParam as number,
        page_size: 20,
      }),
    getNextPageParam: (lastPage) => {
      const nextPage = lastPage.page + 1;
      return nextPage * lastPage.page_size < lastPage.total ? nextPage : undefined;
    },
    initialPageParam: 0,
  });

  const { data: unexpectedOpps } = useQuery({
    queryKey: ['unexpected', venture],
    queryFn: () => getUnexpectedOpps(venture, 10),
  });

  const contactMutation = useMutation({
    mutationFn: (payload: { node_id: string; outcome: string }) =>
      logInteraction({
        node_id: payload.node_id,
        venture_context: venture,
        intent_mode: intent,
        action_taken: 'manual_contact',
        action_type: 'DM',
        channel_used: 'Manual',
        outcome: payload.outcome as any,
        notes: '',
      }),
    onSuccess: () => qc.invalidateQueries({ queryKey: ['feed'] }),
  });

  const allItems = data?.pages.flatMap(p => p.items) ?? [];
  const total = data?.pages[0]?.total ?? 0;

  const toggleFilter = (key: keyof Filters, value: string) => {
    setFilters(prev => {
      const arr = prev[key] as string[];
      return { ...prev, [key]: arr.includes(value) ? arr.filter(x => x !== value) : [...arr, value] };
    });
  };

  return (
    <div className="flex h-full min-h-screen bg-[#0f1117]">
      {/* Sidebar filters */}
      <aside className="w-60 flex-shrink-0 border-r border-[#2a2d3a] p-4 space-y-5 overflow-y-auto">
        <div>
          <p className="text-xs font-semibold text-[#64748b] uppercase tracking-widest mb-2">Node Type</p>
          {NODE_TYPES.map(t => (
            <label key={t} className="flex items-center gap-2 cursor-pointer mb-1">
              <input
                type="checkbox"
                checked={filters.node_types.includes(t)}
                onChange={() => toggleFilter('node_types', t)}
                className="accent-[#14b8a6]"
              />
              <span className="text-sm text-[#e2e8f0]" style={{ color: nodeTypeColor(t) }}>{t}</span>
            </label>
          ))}
        </div>
        <div>
          <p className="text-xs font-semibold text-[#64748b] uppercase tracking-widest mb-2">Warmth</p>
          {WARMTH_TIERS.map(t => (
            <label key={t} className="flex items-center gap-2 cursor-pointer mb-1">
              <input
                type="checkbox"
                checked={filters.warmth_tiers.includes(t)}
                onChange={() => toggleFilter('warmth_tiers', t)}
                className="accent-[#fbbf24]"
              />
              <span className="text-sm capitalize text-[#e2e8f0]">{t}</span>
            </label>
          ))}
        </div>
        <div>
          <p className="text-xs font-semibold text-[#64748b] uppercase tracking-widest mb-2">Min Score</p>
          <input
            type="range" min={0} max={100} step={5}
            value={filters.min_score * 100}
            onChange={e => setFilters(prev => ({ ...prev, min_score: Number(e.target.value) / 100 }))}
            className="w-full accent-[#14b8a6]"
          />
          <p className="text-xs text-[#64748b] text-right">{Math.round(filters.min_score * 100)}%</p>
        </div>
        <div>
          <p className="text-xs font-semibold text-[#64748b] uppercase tracking-widest mb-2">Location</p>
          <input
            type="text"
            value={filters.location}
            onChange={e => setFilters(prev => ({ ...prev, location: e.target.value }))}
            placeholder="e.g. Atlanta"
            className="w-full bg-[#1a1d27] border border-[#2a2d3a] rounded px-2 py-1 text-sm text-[#e2e8f0] placeholder-[#64748b] outline-none"
          />
        </div>
      </aside>

      {/* Main content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Header: venture tabs + intent selector */}
        <header className="border-b border-[#2a2d3a] px-6 py-3 flex items-center gap-4 flex-wrap">
          <div className="flex rounded-lg overflow-hidden border border-[#2a2d3a]">
            {VENTURES.map(v => (
              <button
                key={v}
                onClick={() => setVenture(v)}
                className={`px-3 py-1.5 text-sm font-medium transition-colors ${
                  venture === v
                    ? 'bg-[#14b8a6] text-black'
                    : 'text-[#64748b] hover:text-[#e2e8f0] hover:bg-[#1a1d27]'
                }`}
              >
                {VENTURE_LABELS[v]}
              </button>
            ))}
          </div>
          <select
            value={intent}
            onChange={e => setIntent(e.target.value as IntentMode)}
            className="bg-[#1a1d27] border border-[#2a2d3a] rounded px-3 py-1.5 text-sm text-[#e2e8f0] outline-none"
          >
            {INTENTS.map(i => (
              <option key={i} value={i}>{INTENT_LABELS[i]}</option>
            ))}
          </select>
          <span className="text-sm text-[#64748b] ml-auto">{total.toLocaleString()} results</span>
        </header>

        <div className="flex-1 overflow-y-auto p-6 space-y-3">
          {isLoading && (
            <div className="flex items-center justify-center h-40">
              <div className="w-6 h-6 border-2 border-[#14b8a6] border-t-transparent rounded-full animate-spin" />
            </div>
          )}

          {/* Main feed */}
          {allItems.map(node => (
            <OpportunityCard
              key={node.id}
              node={node}
              onOpenAction={() => setActionNode(node)}
              onOpenDetail={() => setSelectedNode(node)}
              onMarkContacted={() => contactMutation.mutate({ node_id: node.id, outcome: 'replied' })}
            />
          ))}

          {hasNextPage && (
            <button
              onClick={() => fetchNextPage()}
              disabled={isFetchingNextPage}
              className="w-full py-3 rounded-lg border border-[#2a2d3a] text-sm text-[#64748b] hover:text-[#e2e8f0] hover:border-[#14b8a6] transition-colors"
            >
              {isFetchingNextPage ? 'Loading...' : 'Load more'}
            </button>
          )}

          {/* Unexpected Opportunities */}
          {unexpectedOpps && unexpectedOpps.length > 0 && (
            <div className="mt-8">
              <div className="flex items-center gap-2 mb-4">
                <div className="w-2 h-2 rounded-full bg-[#f87171] animate-pulse" />
                <h2 className="text-sm font-semibold text-[#e2e8f0] uppercase tracking-widest">
                  Unexpected Opportunities
                </h2>
                <span className="text-xs text-[#64748b]">({unexpectedOpps.length} bridge nodes)</span>
              </div>
              {unexpectedOpps.map(opp => (
                <UnexpectedCard
                  key={opp.node.id}
                  opp={opp}
                  onOpenAction={() => setActionNode(opp.node)}
                />
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Node Detail Panel */}
      {selectedNode && (
        <NodeDetailPanel
          node={selectedNode}
          onClose={() => setSelectedNode(null)}
          onOpenAction={() => { setActionNode(selectedNode); setSelectedNode(null); }}
        />
      )}

      {/* Action Drawer */}
      {actionNode && (
        <ActionDrawer
          node={actionNode}
          venture={venture}
          onClose={() => setActionNode(null)}
          onLogged={() => {
            setActionNode(null);
            qc.invalidateQueries({ queryKey: ['feed'] });
          }}
        />
      )}
    </div>
  );
}

function OpportunityCard({
  node, onOpenAction, onOpenDetail, onMarkContacted,
}: {
  node: NodeSummary;
  onOpenAction: () => void;
  onOpenDetail: () => void;
  onMarkContacted: () => void;
}) {
  const score = node.opportunity_score;
  const topFactors = getTopFactors(node.score_breakdown);

  return (
    <div
      className="bg-[#1a1d27] border border-[#2a2d3a] rounded-xl p-4 hover:border-[#14b8a6]/40 transition-colors cursor-pointer group"
      onClick={onOpenDetail}
    >
      <div className="flex items-start justify-between gap-4">
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap mb-1">
            <span
              className="text-xs font-medium px-2 py-0.5 rounded-full"
              style={{ background: nodeTypeColor(node.node_type) + '22', color: nodeTypeColor(node.node_type) }}
            >
              {node.node_type}
            </span>
            {node.source.map(s => (
              <span key={s} className="text-xs px-1.5 py-0.5 rounded bg-[#2a2d3a] text-[#64748b]">{s}</span>
            ))}
            {node.is_trending && (
              <span className="text-xs px-2 py-0.5 rounded-full bg-[#f59e0b22] text-[#f59e0b] font-medium">🔥 Trending</span>
            )}
            {node.new_to_network && (
              <span className="text-xs px-2 py-0.5 rounded-full bg-[#22c55e22] text-[#22c55e] font-medium">✨ New</span>
            )}
            {node.confidence_score < 0.3 && (
              <span className="text-xs px-2 py-0.5 rounded-full bg-[#ef444422] text-[#ef4444]">⚠ Low confidence</span>
            )}
          </div>
          <h3 className="font-semibold text-[#e2e8f0] truncate">{node.name}</h3>
          {node.location && <p className="text-xs text-[#64748b] mt-0.5">{node.location}</p>}
          {node.topic_cluster && (
            <p className="text-xs text-[#a78bfa] mt-1">#{node.topic_cluster.replace(/_/g, ' ')}</p>
          )}
          {topFactors.length > 0 && (
            <p className="text-xs text-[#64748b] mt-1">{topFactors.join(' · ')}</p>
          )}
        </div>

        <div className="flex flex-col items-end gap-2 flex-shrink-0">
          {/* Score bar */}
          <div className="flex items-center gap-2">
            <div className="w-20 h-1.5 bg-[#2a2d3a] rounded-full overflow-hidden">
              <div
                className="h-full rounded-full transition-all"
                style={{ width: `${score * 100}%`, background: scoreColor(score) }}
              />
            </div>
            <span className="text-sm font-bold" style={{ color: scoreColor(score) }}>
              {fmtScore(score)}
            </span>
          </div>
          {/* Warmth */}
          <span
            className="text-xs px-2 py-0.5 rounded-full"
            style={{ background: warmthColor(node.warmth_score) + '22', color: warmthColor(node.warmth_score) }}
          >
            {warmthLabel(node.warmth_score)}
          </span>
        </div>
      </div>

      {/* Actions row */}
      <div className="flex gap-2 mt-3" onClick={e => e.stopPropagation()}>
        <button
          onClick={onOpenAction}
          className="flex-1 py-1.5 rounded-lg bg-[#14b8a6] text-black text-xs font-semibold hover:bg-[#0d9488] transition-colors"
        >
          Next Action →
        </button>
        <button
          onClick={onMarkContacted}
          className="px-3 py-1.5 rounded-lg border border-[#2a2d3a] text-xs text-[#64748b] hover:border-[#14b8a6] hover:text-[#14b8a6] transition-colors"
        >
          ✓ Contacted
        </button>
      </div>
    </div>
  );
}

function UnexpectedCard({ opp, onOpenAction }: { opp: UnexpectedOpportunity; onOpenAction: () => void }) {
  return (
    <div className="bg-[#1a1d27] border border-[#f87171]/30 rounded-xl p-4 mb-3">
      <div className="flex items-start justify-between">
        <div>
          <div className="flex items-center gap-2 mb-1">
            <div className="w-2 h-2 rounded-full bg-[#f87171]" />
            <span
              className="text-xs font-medium px-2 py-0.5 rounded-full"
              style={{ background: nodeTypeColor(opp.node.node_type) + '22', color: nodeTypeColor(opp.node.node_type) }}
            >
              {opp.node.node_type}
            </span>
          </div>
          <h3 className="font-semibold text-[#e2e8f0]">{opp.node.name}</h3>
          <p className="text-xs text-[#64748b] mt-1">
            Bridges: {opp.bridged_clusters.slice(0, 3).join(' → ')}
          </p>
          <p className="text-xs text-[#f87171] mt-1">
            {opp.path_length} hops · Betweenness: {fmtScore(opp.betweenness)}
          </p>
        </div>
        <button
          onClick={onOpenAction}
          className="px-3 py-1.5 rounded-lg bg-[#f87171]/20 text-[#f87171] text-xs font-semibold hover:bg-[#f87171]/30 transition-colors"
        >
          Explore →
        </button>
      </div>
    </div>
  );
}

function NodeDetailPanel({
  node, onClose, onOpenAction,
}: {
  node: NodeSummary;
  onClose: () => void;
  onOpenAction: () => void;
}) {
  return (
    <div className="w-96 border-l border-[#2a2d3a] bg-[#1a1d27] overflow-y-auto flex flex-col">
      <div className="flex items-center justify-between p-4 border-b border-[#2a2d3a]">
        <h2 className="font-semibold text-[#e2e8f0] truncate">{node.name}</h2>
        <button onClick={onClose} className="text-[#64748b] hover:text-[#e2e8f0] text-lg">✕</button>
      </div>
      <div className="p-4 space-y-4 flex-1">
        <div className="flex gap-2 flex-wrap">
          <span className="text-xs px-2 py-1 rounded-full" style={{ background: nodeTypeColor(node.node_type) + '22', color: nodeTypeColor(node.node_type) }}>
            {node.node_type}
          </span>
          <span className="text-sm font-bold" style={{ color: scoreColor(node.opportunity_score) }}>
            Score: {fmtScore(node.opportunity_score)}
          </span>
        </div>
        {node.score_breakdown && (
          <div>
            <p className="text-xs text-[#64748b] mb-2">Score Breakdown</p>
            <ScoreRadar breakdown={node.score_breakdown} />
          </div>
        )}
        <button
          onClick={onOpenAction}
          className="w-full py-2 rounded-lg bg-[#14b8a6] text-black text-sm font-semibold hover:bg-[#0d9488] transition-colors"
        >
          View Next Best Action
        </button>
      </div>
    </div>
  );
}

function getTopFactors(breakdown?: NodeSummary['score_breakdown']): string[] {
  if (!breakdown) return [];
  const factors: Record<string, number> = {
    'High reachability': breakdown.reachability,
    'Strong relevance':  breakdown.relevance,
    'High influence':    breakdown.influence,
    'Warm contact':      breakdown.responsiveness,
  };
  return Object.entries(factors)
    .filter(([, v]) => v > 0.6)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 2)
    .map(([k]) => k);
}
