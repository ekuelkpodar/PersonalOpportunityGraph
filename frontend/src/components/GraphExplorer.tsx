import { useEffect, useRef, useState, useCallback } from 'react';
import { useQuery } from '@tanstack/react-query';
import cytoscape from 'cytoscape';
import type { Core, NodeSingular } from 'cytoscape';
import { getEgoGraph, getNodeGraph } from '../lib/api';
import { nodeTypeColor, scoreColor, fmtScore, VENTURE_LABELS } from '../lib/utils';
import type { GraphData, VentureContext, IntentMode } from '../types';

const VENTURES: VentureContext[] = ['applied_insights', 'aegis_t2a', 'rgn_trucking', 'job_search'];
const INTENTS: IntentMode[] = ['Exploit', 'Explore', 'Bridge', 'Recruit', 'Sell'];

export default function GraphExplorer() {
  const cyRef  = useRef<Core | null>(null);
  const divRef = useRef<HTMLDivElement>(null);

  const [venture, setVenture]   = useState<VentureContext>('applied_insights');
  const [intent, setIntent]     = useState<IntentMode>('Exploit');
  const [minScore, setMinScore] = useState(0);
  const [topN, setTopN]         = useState(50);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [louvainColors, setLouvainColors] = useState(false);
  const [weakTiesOnly, setWeakTiesOnly]   = useState(false);
  const [nodeInfo, setNodeInfo] = useState<Record<string, any> | null>(null);

  const { data: graphData, isLoading } = useQuery({
    queryKey: ['ego-graph', venture, intent, topN, minScore],
    queryFn: () => getEgoGraph({ venture_context: venture, intent_mode: intent, top_n: topN, min_score: minScore }),
    staleTime: 30_000,
  });

  // Initialize Cytoscape
  useEffect(() => {
    if (!divRef.current) return;
    cyRef.current = cytoscape({
      container: divRef.current,
      style: _cytoscapeStyle() as any,
      layout: { name: 'cose', animate: false, randomize: true, nodeRepulsion: 8000 } as any,
      wheelSensitivity: 0.3,
    });

    cyRef.current.on('tap', 'node', (evt) => {
      const nodeId = evt.target.id();
      setSelectedId(nodeId);
      setNodeInfo(evt.target.data());
    });

    return () => {
      cyRef.current?.destroy();
      cyRef.current = null;
    };
  }, []);

  // Update graph data
  useEffect(() => {
    if (!cyRef.current || !graphData) return;
    const cy = cyRef.current;
    cy.elements().remove();

    const nodes = graphData.nodes
      .filter(n => !weakTiesOnly || n.is_weak_tie || n.node_type === 'Ego')
      .map(n => ({
        data: {
          id: n.id,
          label: n.name.length > 20 ? n.name.slice(0, 18) + '…' : n.name,
          nodeType: n.node_type,
          score: n.opportunity_score,
          warmth: n.warmth_score,
          confidence: n.confidence_score,
          isWeak: n.is_weak_tie,
          isTrending: n.is_trending,
          louvain: n.louvain_community,
          topic_cluster: n.topic_cluster,
          size: n.node_type === 'Ego' ? 40 : 16 + (n.opportunity_score * 24),
          color: louvainColors && n.louvain_community !== undefined
            ? _louvainColor(n.louvain_community)
            : nodeTypeColor(n.node_type),
          borderColor: n.is_weak_tie ? '#f87171' : (n.node_type === 'Ego' ? '#f59e0b' : 'transparent'),
        },
      }));

    const visibleIds = new Set(nodes.map(n => n.data.id));
    const edges = graphData.edges
      .filter(e => visibleIds.has(e.source) && visibleIds.has(e.target))
      .map(e => ({
        data: {
          id: `${e.source}_${e.target}_${e.rel_type}`,
          source: e.source,
          target: e.target,
          weight: e.weight,
          relType: e.rel_type,
          lineWidth: 0.5 + (e.weight * 2),
        },
      }));

    cy.add([...nodes, ...edges]);
    cy.layout({ name: 'cose', animate: false, randomize: true, nodeRepulsion: 6000 } as any).run();
    cy.fit(cy.elements(), 30);
  }, [graphData, louvainColors, weakTiesOnly]);

  const handleExpandNode = useCallback(async () => {
    if (!selectedId || !cyRef.current) return;
    try {
      const subgraph: GraphData = await getNodeGraph(selectedId);
      const cy = cyRef.current;
      const existingIds = new Set(cy.nodes().map((n: NodeSingular) => n.id()));

      const newNodes = subgraph.nodes
        .filter(n => !existingIds.has(n.id))
        .map(n => ({
          data: {
            id: n.id,
            label: n.name.length > 20 ? n.name.slice(0, 18) + '…' : n.name,
            nodeType: n.node_type,
            score: n.opportunity_score,
            warmth: n.warmth_score,
            topic_cluster: n.topic_cluster,
            isWeak: n.is_weak_tie,
            isTrending: n.is_trending,
            size: 16 + (n.opportunity_score * 24),
            color: nodeTypeColor(n.node_type),
            borderColor: 'transparent',
          },
        }));

      const existingEdgeIds = new Set(cy.edges().map((e: any) => e.id()));
      const newEdges = subgraph.edges
        .filter(e => {
          const id = `${e.source}_${e.target}_${e.rel_type}`;
          return !existingEdgeIds.has(id);
        })
        .map(e => ({
          data: {
            id: `${e.source}_${e.target}_${e.rel_type}`,
            source: e.source,
            target: e.target,
            weight: e.weight,
            relType: e.rel_type,
            lineWidth: 0.5 + (e.weight * 2),
          },
        }));

      if (newNodes.length > 0 || newEdges.length > 0) {
        cy.add([...newNodes, ...newEdges]);
        cy.layout({ name: 'cose', animate: true, fit: false, randomize: false } as any).run();
      }
    } catch (e) {
      console.error('expand node failed', e);
    }
  }, [selectedId]);

  return (
    <div className="flex h-screen bg-[#0f1117] overflow-hidden">
      {/* Controls sidebar */}
      <aside className="w-56 flex-shrink-0 border-r border-[#2a2d3a] p-4 space-y-4 overflow-y-auto">
        <div>
          <p className="text-xs text-[#64748b] uppercase tracking-widest mb-2">Venture</p>
          <select
            value={venture}
            onChange={e => setVenture(e.target.value as VentureContext)}
            className="w-full bg-[#1a1d27] border border-[#2a2d3a] rounded px-2 py-1.5 text-sm text-[#e2e8f0] outline-none"
          >
            {VENTURES.map(v => <option key={v} value={v}>{VENTURE_LABELS[v]}</option>)}
          </select>
        </div>
        <div>
          <p className="text-xs text-[#64748b] uppercase tracking-widest mb-2">Intent Mode</p>
          <select
            value={intent}
            onChange={e => setIntent(e.target.value as IntentMode)}
            className="w-full bg-[#1a1d27] border border-[#2a2d3a] rounded px-2 py-1.5 text-sm text-[#e2e8f0] outline-none"
          >
            {INTENTS.map(i => <option key={i} value={i}>{i}</option>)}
          </select>
        </div>
        <div>
          <p className="text-xs text-[#64748b] uppercase tracking-widest mb-1">Top N Nodes: {topN}</p>
          <input type="range" min={10} max={200} step={10} value={topN}
            onChange={e => setTopN(Number(e.target.value))}
            className="w-full accent-[#14b8a6]" />
        </div>
        <div>
          <p className="text-xs text-[#64748b] uppercase tracking-widest mb-1">Min Score: {minScore}%</p>
          <input type="range" min={0} max={80} step={5} value={minScore}
            onChange={e => setMinScore(Number(e.target.value))}
            className="w-full accent-[#14b8a6]" />
        </div>
        <label className="flex items-center gap-2 cursor-pointer">
          <input type="checkbox" checked={louvainColors} onChange={e => setLouvainColors(e.target.checked)}
            className="accent-[#a78bfa]" />
          <span className="text-sm text-[#e2e8f0]">Louvain colors</span>
        </label>
        <label className="flex items-center gap-2 cursor-pointer">
          <input type="checkbox" checked={weakTiesOnly} onChange={e => setWeakTiesOnly(e.target.checked)}
            className="accent-[#f87171]" />
          <span className="text-sm text-[#e2e8f0]">Weak ties only</span>
        </label>
        <div className="pt-2 border-t border-[#2a2d3a]">
          <p className="text-xs text-[#64748b] mb-2">Legend</p>
          {[['Person', '#14b8a6'], ['Company', '#f87171'], ['Publisher', '#a78bfa'], ['Community', '#fbbf24'], ['Ego', '#f59e0b']].map(([label, color]) => (
            <div key={label} className="flex items-center gap-2 mb-1">
              <div className="w-3 h-3 rounded-full flex-shrink-0" style={{ background: color as string }} />
              <span className="text-xs text-[#64748b]">{label}</span>
            </div>
          ))}
        </div>
      </aside>

      {/* Graph canvas */}
      <div className="flex-1 relative">
        {isLoading && (
          <div className="absolute inset-0 flex items-center justify-center bg-[#0f1117]/80 z-10">
            <div className="w-8 h-8 border-2 border-[#14b8a6] border-t-transparent rounded-full animate-spin" />
          </div>
        )}
        <div ref={divRef} className="w-full h-full" />
      </div>

      {/* Node info panel */}
      {nodeInfo && (
        <aside className="w-72 border-l border-[#2a2d3a] bg-[#1a1d27] p-4 overflow-y-auto">
          <div className="flex items-center justify-between mb-3">
            <h3 className="font-semibold text-[#e2e8f0] truncate">{nodeInfo.name}</h3>
            <button onClick={() => { setNodeInfo(null); setSelectedId(null); }}
              className="text-[#64748b] hover:text-[#e2e8f0]">✕</button>
          </div>
          <div className="space-y-2 text-sm">
            <div className="flex justify-between">
              <span className="text-[#64748b]">Type</span>
              <span style={{ color: nodeTypeColor(nodeInfo.nodeType) }}>{nodeInfo.nodeType}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-[#64748b]">Opp. Score</span>
              <span style={{ color: scoreColor(nodeInfo.score || 0) }}>{fmtScore(nodeInfo.score || 0)}</span>
            </div>
            <div className="flex justify-between">
              <span className="text-[#64748b]">Confidence</span>
              <span className="text-[#e2e8f0]">{fmtScore(nodeInfo.confidence || 0)}</span>
            </div>
            {nodeInfo.topic_cluster && (
              <div className="flex justify-between">
                <span className="text-[#64748b]">Topic</span>
                <span className="text-[#a78bfa] text-right max-w-32 truncate">{nodeInfo.topic_cluster}</span>
              </div>
            )}
            {nodeInfo.is_weak_tie && (
              <div className="bg-[#f87171]/10 rounded-lg p-2 text-xs text-[#f87171]">
                Bridge node — unexpected opportunity
              </div>
            )}
          </div>
          <button
            onClick={handleExpandNode}
            className="mt-4 w-full py-2 rounded-lg border border-[#14b8a6] text-[#14b8a6] text-sm hover:bg-[#14b8a6]/10 transition-colors"
          >
            Expand 1-hop neighbors
          </button>
        </aside>
      )}
    </div>
  );
}

function _cytoscapeStyle() {
  return [
    {
      selector: 'node',
      style: {
        'background-color': 'data(color)',
        'label': 'data(label)',
        'width': 'data(size)',
        'height': 'data(size)',
        'font-size': 9,
        'color': '#e2e8f0',
        'text-valign': 'bottom',
        'text-margin-y': 4,
        'border-width': 2,
        'border-color': 'data(borderColor)',
        'text-outline-width': 2,
        'text-outline-color': '#0f1117',
      },
    },
    {
      selector: 'edge',
      style: {
        'line-color': '#2a2d3a',
        'width': 'data(lineWidth)',
        'opacity': 0.6,
        'curve-style': 'bezier',
      },
    },
    {
      selector: 'node:selected',
      style: {
        'border-width': 3,
        'border-color': '#14b8a6',
      },
    },
    {
      selector: 'node[isWeak=true]',
      style: {
        'border-color': '#f87171',
        'border-width': 3,
      },
    },
  ];
}

function _louvainColor(communityId: number): string {
  const palette = ['#14b8a6', '#f87171', '#a78bfa', '#fbbf24', '#60a5fa', '#34d399', '#fb923c', '#e879f9'];
  return palette[communityId % palette.length];
}
