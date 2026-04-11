import React, { useState, useRef, useEffect } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import { queryGraph, getQuerySuggestions } from '../lib/api';
import { VENTURE_LABELS, INTENT_LABELS } from '../lib/utils';
import type { VentureContext, IntentMode, ChatResponse, CitedNode } from '../types';

const VENTURES: VentureContext[] = ['applied_insights', 'aegis_t2a', 'rgn_trucking', 'job_search'];

interface Message {
  role: 'user' | 'assistant';
  content: string;
  response?: ChatResponse;
}

export default function RagChat() {
  const [venture, setVenture] = useState<VentureContext>('applied_insights');
  const [intent, setIntent]   = useState<IntentMode>('Exploit');
  const [input, setInput]     = useState('');
  const [messages, setMessages] = useState<Message[]>([]);
  const bottomRef = useRef<HTMLDivElement>(null);

  const { data: suggestions } = useQuery({
    queryKey: ['suggestions', venture],
    queryFn: () => getQuerySuggestions(venture),
  });

  const queryMutation = useMutation({
    mutationFn: (query: string) =>
      queryGraph({
        query,
        venture_context: venture,
        intent_mode: intent,
        history: messages.map(m => ({ role: m.role, content: m.content })),
      }),
    onSuccess: (data) => {
      setMessages(prev => [
        ...prev,
        { role: 'assistant', content: data.answer_text, response: data },
      ]);
    },
  });

  const handleSend = () => {
    const q = input.trim();
    if (!q) return;
    setMessages(prev => [...prev, { role: 'user', content: q }]);
    setInput('');
    queryMutation.mutate(q);
  };

  const handleSuggestion = (s: string) => {
    setMessages(prev => [...prev, { role: 'user', content: s }]);
    queryMutation.mutate(s);
  };

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages, queryMutation.isPending]);

  const exportChat = () => {
    const text = messages.map(m => `${m.role.toUpperCase()}: ${m.content}`).join('\n\n');
    const blob = new Blob([text], { type: 'text/plain' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = 'pog_chat_export.txt';
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="flex flex-col h-screen bg-[#0f1117]">
      {/* Header */}
      <header className="border-b border-[#2a2d3a] px-6 py-3 flex items-center gap-4 flex-wrap">
        <h1 className="text-sm font-bold text-[#e2e8f0]">Graph RAG Chat</h1>
        <select value={venture} onChange={e => setVenture(e.target.value as VentureContext)}
          className="bg-[#1a1d27] border border-[#2a2d3a] rounded px-2 py-1.5 text-sm text-[#e2e8f0] outline-none">
          {VENTURES.map(v => <option key={v} value={v}>{VENTURE_LABELS[v]}</option>)}
        </select>
        <select value={intent} onChange={e => setIntent(e.target.value as IntentMode)}
          className="bg-[#1a1d27] border border-[#2a2d3a] rounded px-2 py-1.5 text-sm text-[#e2e8f0] outline-none">
          {Object.entries(INTENT_LABELS).map(([k, v]) => <option key={k} value={k}>{v}</option>)}
        </select>
        <button onClick={exportChat}
          className="ml-auto text-xs text-[#64748b] hover:text-[#14b8a6] transition-colors">
          Export ↓
        </button>
      </header>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-6 space-y-4">
        {messages.length === 0 && (
          <div className="text-center py-12">
            <p className="text-[#64748b] mb-6">Ask anything about your opportunity graph</p>
            {suggestions?.suggestions && (
              <div className="flex flex-col gap-2 max-w-lg mx-auto">
                {suggestions.suggestions.map((s, i) => (
                  <button key={i} onClick={() => handleSuggestion(s)}
                    className="text-left px-4 py-3 rounded-xl bg-[#1a1d27] border border-[#2a2d3a] text-sm text-[#e2e8f0] hover:border-[#14b8a6] transition-colors">
                    {s}
                  </button>
                ))}
              </div>
            )}
          </div>
        )}

        {messages.map((msg, i) => (
          <div key={i} className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}>
            {msg.role === 'user' ? (
              <div className="max-w-lg bg-[#14b8a6]/20 text-[#e2e8f0] rounded-2xl rounded-tr-sm px-4 py-3 text-sm">
                {msg.content}
              </div>
            ) : (
              <div className="max-w-2xl space-y-3">
                <div className="bg-[#1a1d27] border border-[#2a2d3a] rounded-2xl rounded-tl-sm px-4 py-3">
                  <p className="text-sm text-[#e2e8f0] leading-relaxed whitespace-pre-wrap">{msg.content}</p>

                  {msg.response && (
                    <div className="mt-3 pt-3 border-t border-[#2a2d3a] space-y-2">
                      {/* Confidence */}
                      <div className="flex items-center gap-2">
                        <span className="text-xs text-[#64748b]">Confidence:</span>
                        <ConfidenceBadge level={msg.response.confidence} />
                      </div>

                      {/* Reasoning path */}
                      {msg.response.reasoning_path.length > 0 && (
                        <div>
                          <p className="text-xs text-[#64748b] mb-1">Path:</p>
                          <div className="flex items-center gap-1 flex-wrap">
                            {msg.response.reasoning_path.map((step, j) => (
                              <React.Fragment key={j}>
                                <span className="text-xs bg-[#0f1117] px-2 py-0.5 rounded text-[#e2e8f0]">{step}</span>
                                {j < msg.response!.reasoning_path.length - 1 && (
                                  <span className="text-[#64748b] text-xs">→</span>
                                )}
                              </React.Fragment>
                            ))}
                          </div>
                        </div>
                      )}

                      {/* Cited nodes */}
                      {msg.response.cited_nodes.length > 0 && (
                        <div>
                          <p className="text-xs text-[#64748b] mb-1">Sources:</p>
                          <div className="space-y-1">
                            {msg.response.cited_nodes.slice(0, 5).map(c => (
                              <CitedNodeBadge key={c.node_id} node={c} />
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </div>
            )}
          </div>
        ))}

        {queryMutation.isPending && (
          <div className="flex justify-start">
            <div className="bg-[#1a1d27] border border-[#2a2d3a] rounded-2xl rounded-tl-sm px-4 py-3">
              <div className="flex gap-1">
                {[0, 150, 300].map(delay => (
                  <div key={delay} className="w-2 h-2 rounded-full bg-[#14b8a6] animate-bounce"
                    style={{ animationDelay: `${delay}ms` }} />
                ))}
              </div>
            </div>
          </div>
        )}
        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div className="border-t border-[#2a2d3a] px-6 py-4">
        <div className="flex gap-3">
          <input
            type="text"
            value={input}
            onChange={e => setInput(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && !e.shiftKey && handleSend()}
            placeholder="Ask about your network, opportunities, or strategy..."
            className="flex-1 bg-[#1a1d27] border border-[#2a2d3a] rounded-xl px-4 py-3 text-sm text-[#e2e8f0] placeholder-[#64748b] outline-none focus:border-[#14b8a6] transition-colors"
          />
          <button
            onClick={handleSend}
            disabled={queryMutation.isPending || !input.trim()}
            className="px-5 py-3 rounded-xl bg-[#14b8a6] text-black font-semibold text-sm disabled:opacity-40 hover:bg-[#0d9488] transition-colors"
          >
            Send
          </button>
        </div>
      </div>
    </div>
  );
}

function ConfidenceBadge({ level }: { level: string }) {
  const colors: Record<string, string> = {
    high:   '#22c55e',
    medium: '#eab308',
    low:    '#ef4444',
  };
  return (
    <span className="text-xs px-2 py-0.5 rounded-full"
      style={{ background: (colors[level] || '#64748b') + '22', color: colors[level] || '#64748b' }}>
      {level}
    </span>
  );
}

function CitedNodeBadge({ node }: { node: CitedNode }) {
  return (
    <div className="text-xs bg-[#0f1117] rounded-lg px-3 py-2">
      <span className="text-[#14b8a6] font-medium">{node.name}</span>
      <span className="text-[#64748b]"> · {node.node_type}</span>
      <p className="text-[#64748b] mt-0.5 truncate">{node.relevance_reason}</p>
    </div>
  );
}
