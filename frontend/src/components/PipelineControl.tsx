import { useState, useEffect, useRef } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { runPipeline, getPipelineStatus, runScoring } from '../lib/api';
import { timeSince } from '../lib/utils';

export default function PipelineControl() {
  const [logs, setLogs] = useState<string[]>([]);
  const [progress, setProgress] = useState<Record<string, any>>({});
  const [wsConnected, setWsConnected] = useState(false);
  const logsEndRef = useRef<HTMLDivElement>(null);
  const wsRef = useRef<WebSocket | null>(null);
  const qc = useQueryClient();

  const { data: status } = useQuery({
    queryKey: ['pipeline-status'],
    queryFn: getPipelineStatus,
    refetchInterval: 5000,
  });

  const runMutation = useMutation({
    mutationFn: (force: boolean) => runPipeline(force),
    onSuccess: () => {
      connectWebSocket();
      qc.invalidateQueries({ queryKey: ['pipeline-status'] });
    },
  });

  const scoreMutation = useMutation({
    mutationFn: runScoring,
    onSuccess: () => {
      setLogs(prev => [...prev, '✓ Scoring job completed']);
      qc.invalidateQueries({ queryKey: ['pipeline-status'] });
    },
  });

  const connectWebSocket = () => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return;
    const ws = new WebSocket(`ws://localhost:8001/ws/pipeline`);
    wsRef.current = ws;
    ws.onopen    = () => setWsConnected(true);
    ws.onclose   = () => setWsConnected(false);
    ws.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data);
        if (data.stage === 'idle' || data.stage === 'heartbeat') return;
        if (data.stage === 'terminal') {
          ws.close();
          qc.invalidateQueries({ queryKey: ['pipeline-status'] });
          return;
        }
        const logLine = `[${data.source || 'pipeline'}] ${data.message} ${data.pct ? `(${data.pct}%)` : ''}`;
        setLogs(prev => [...prev.slice(-499), logLine]);
        setProgress(prev => ({ ...prev, [data.source]: data }));
      } catch {}
    };
  };

  useEffect(() => {
    if (status?.is_running && !wsConnected) connectWebSocket();
  }, [status?.is_running]);

  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs]);

  const sources = ['feedspot', 'xlist', 'clutch', 'facebook', 'skool'];
  const sourceStats = status?.sources_stats || {};
  const embedStats  = status?.embedding_progress || {};

  return (
    <div className="h-full overflow-y-auto bg-[#0f1117] p-6 space-y-6">
      {/* Action buttons */}
      <div className="flex gap-3 flex-wrap items-center">
        <button
          onClick={() => runMutation.mutate(false)}
          disabled={runMutation.isPending || status?.is_running}
          className="px-5 py-2.5 rounded-xl bg-[#14b8a6] text-black font-semibold text-sm disabled:opacity-40 hover:bg-[#0d9488] transition-colors"
        >
          {status?.is_running ? '⟳ Running...' : '▶ Run Pipeline'}
        </button>
        <button
          onClick={() => runMutation.mutate(true)}
          disabled={runMutation.isPending || status?.is_running}
          className="px-5 py-2.5 rounded-xl border border-[#2a2d3a] text-[#64748b] font-semibold text-sm disabled:opacity-40 hover:border-[#14b8a6] hover:text-[#e2e8f0] transition-colors"
        >
          Force Re-process
        </button>
        <button
          onClick={() => scoreMutation.mutate()}
          disabled={scoreMutation.isPending}
          className="px-5 py-2.5 rounded-xl border border-[#a78bfa]/30 text-[#a78bfa] font-semibold text-sm disabled:opacity-40 hover:bg-[#a78bfa]/10 transition-colors"
        >
          {scoreMutation.isPending ? '⟳ Scoring...' : '⚡ Run Scoring'}
        </button>
        <span className="text-xs text-[#64748b]">
          {status?.is_running ? (
            <span className="text-[#f59e0b]">● Pipeline running</span>
          ) : (
            `Last run: ${timeSince(status?.last_run)}`
          )}
        </span>
      </div>

      {/* Source stats table */}
      <div className="bg-[#1a1d27] border border-[#2a2d3a] rounded-xl overflow-hidden">
        <div className="px-4 py-3 border-b border-[#2a2d3a]">
          <p className="text-xs text-[#64748b] uppercase tracking-widest">Source Statistics</p>
        </div>
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-[#2a2d3a]">
              {['Source', 'Files', 'Rows', 'Nodes', 'Dupes', 'Status'].map(h => (
                <th key={h} className="px-4 py-2 text-left text-xs text-[#64748b] font-medium">{h}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {sources.map(src => {
              const s = sourceStats[src] || {};
              const liveEvt = progress[src];
              return (
                <tr key={src} className="border-b border-[#2a2d3a]/50">
                  <td className="px-4 py-2 font-medium capitalize text-[#e2e8f0]">{src}</td>
                  <td className="px-4 py-2 text-[#64748b]">{s.file_count ?? '—'}</td>
                  <td className="px-4 py-2 text-[#64748b]">{s.rows_processed?.toLocaleString() ?? '—'}</td>
                  <td className="px-4 py-2 text-[#14b8a6]">{s.nodes_created?.toLocaleString() ?? '—'}</td>
                  <td className="px-4 py-2 text-[#64748b]">{s.dupes_skipped?.toLocaleString() ?? '—'}</td>
                  <td className="px-4 py-2">
                    {liveEvt ? (
                      <div className="flex items-center gap-2">
                        <div className="w-16 h-1.5 bg-[#2a2d3a] rounded-full overflow-hidden">
                          <div className="h-full bg-[#14b8a6] rounded-full" style={{ width: `${liveEvt.pct}%` }} />
                        </div>
                        <span className="text-xs text-[#14b8a6]">{liveEvt.pct}%</span>
                      </div>
                    ) : (
                      <span className={`text-xs ${s.status === 'done' ? 'text-[#22c55e]' : 'text-[#64748b]'}`}>
                        {s.status || 'pending'}
                      </span>
                    )}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      {/* Embedding progress */}
      <div className="bg-[#1a1d27] border border-[#2a2d3a] rounded-xl p-4">
        <p className="text-xs text-[#64748b] uppercase tracking-widest mb-3">Embedding Progress</p>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          {['Person', 'Company', 'Publisher', 'Community'].map(type => {
            const count = (embedStats.embedded_counts || {})[type] || 0;
            return (
              <div key={type}>
                <p className="text-xs text-[#64748b] mb-1">{type}</p>
                <p className="text-lg font-bold text-[#e2e8f0]">{count.toLocaleString()}</p>
                <p className="text-xs text-[#64748b]">embedded</p>
              </div>
            );
          })}
        </div>
        <div className="mt-3 text-sm text-[#64748b]">
          Total: <span className="text-[#e2e8f0] font-medium">{(embedStats.total_embedded || 0).toLocaleString()}</span> nodes embedded
        </div>
      </div>

      {/* Log viewer */}
      <div className="bg-[#1a1d27] border border-[#2a2d3a] rounded-xl overflow-hidden">
        <div className="flex items-center justify-between px-4 py-3 border-b border-[#2a2d3a]">
          <p className="text-xs text-[#64748b] uppercase tracking-widest">Live Log</p>
          <div className="flex items-center gap-2">
            <div className={`w-2 h-2 rounded-full ${wsConnected ? 'bg-[#22c55e] animate-pulse' : 'bg-[#64748b]'}`} />
            <span className="text-xs text-[#64748b]">{wsConnected ? 'Connected' : 'Idle'}</span>
            <button onClick={() => setLogs([])}
              className="text-xs text-[#64748b] hover:text-[#e2e8f0] ml-2">Clear</button>
          </div>
        </div>
        <div className="h-64 overflow-y-auto p-4 font-mono text-xs text-[#64748b] space-y-0.5">
          {logs.length === 0 ? (
            <p className="text-[#2a2d3a]">Waiting for pipeline events...</p>
          ) : (
            logs.map((line, i) => (
              <div key={i} className={`${line.includes('error') ? 'text-[#ef4444]' : line.includes('✓') ? 'text-[#22c55e]' : ''}`}>
                {line}
              </div>
            ))
          )}
          <div ref={logsEndRef} />
        </div>
      </div>

      {/* Health warnings */}
      {status && !status.is_running && (
        <HealthWarnings embedStats={embedStats} sourceStats={sourceStats} />
      )}
    </div>
  );
}

function HealthWarnings({ embedStats, sourceStats }: { embedStats: any; sourceStats: any }) {
  const warnings: string[] = [];

  const totalEmbedded = embedStats.total_embedded || 0;
  if (totalEmbedded === 0) {
    warnings.push('No embeddings found. Run the pipeline to generate embeddings.');
  }

  if (Object.keys(sourceStats).length === 0) {
    warnings.push('No sources have been ingested yet. Run the pipeline to get started.');
  }

  if (warnings.length === 0) return null;

  return (
    <div className="bg-[#ef4444]/10 border border-[#ef4444]/20 rounded-xl p-4 space-y-2">
      <p className="text-xs font-semibold text-[#ef4444] uppercase tracking-widest">⚠ Pipeline Health</p>
      {warnings.map((w, i) => (
        <p key={i} className="text-sm text-[#ef4444]">{w}</p>
      ))}
    </div>
  );
}
