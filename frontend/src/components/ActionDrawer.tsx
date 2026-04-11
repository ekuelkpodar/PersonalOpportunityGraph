import React, { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { getAction, updateDraft, logInteraction } from '../lib/api';
import { priorityColor, VENTURE_LABELS } from '../lib/utils';
import type { NodeSummary } from '../types';

interface Props {
  node: NodeSummary;
  venture: string;
  onClose: () => void;
  onLogged: () => void;
}

const OUTCOMES = ['no_reply', 'replied', 'meeting', 'converted', 'not_relevant'] as const;
type Outcome = typeof OUTCOMES[number];

export default function ActionDrawer({ node, venture, onClose, onLogged }: Props) {
  const [editingDraft, setEditingDraft] = useState(false);
  const [draftText, setDraftText] = useState('');
  const [outcome, setOutcome] = useState<Outcome | null>(null);
  const qc = useQueryClient();

  const { data: action, isLoading } = useQuery({
    queryKey: ['action', node.id, venture],
    queryFn: () => getAction(node.id, venture),
    staleTime: 60_000,
  });

  const saveDraftMutation = useMutation({
    mutationFn: () => updateDraft(node.id, venture, draftText),
    onSuccess: () => {
      setEditingDraft(false);
      qc.invalidateQueries({ queryKey: ['action', node.id, venture] });
    },
  });

  const logMutation = useMutation({
    mutationFn: (o: Outcome) =>
      logInteraction({
        node_id: node.id,
        venture_context: venture,
        intent_mode: 'Exploit',
        action_taken: action?.action_type || 'DM',
        action_type: action?.action_type || 'DM',
        channel_used: action?.channel || 'Manual',
        outcome: o,
        notes: '',
      }),
    onSuccess: onLogged,
  });

  const handleEditDraft = () => {
    setDraftText(action?.message_draft || '');
    setEditingDraft(true);
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  return (
    <div className="fixed inset-0 z-50 flex items-end sm:items-center justify-center bg-black/60">
      <div className="bg-[#1a1d27] border border-[#2a2d3a] rounded-t-2xl sm:rounded-2xl w-full max-w-2xl max-h-[90vh] overflow-y-auto">
        {/* Header */}
        <div className="flex items-center justify-between p-5 border-b border-[#2a2d3a] sticky top-0 bg-[#1a1d27] z-10">
          <div>
            <h2 className="font-bold text-[#e2e8f0]">Next Best Action</h2>
            <p className="text-sm text-[#64748b]">{node.name} · {VENTURE_LABELS[venture] || venture}</p>
          </div>
          <button onClick={onClose} className="text-[#64748b] hover:text-[#e2e8f0] text-xl font-bold">✕</button>
        </div>

        {isLoading ? (
          <div className="flex items-center justify-center h-48">
            <div className="w-6 h-6 border-2 border-[#14b8a6] border-t-transparent rounded-full animate-spin" />
          </div>
        ) : action ? (
          <div className="p-5 space-y-5">
            {/* Action type + channel + priority */}
            <div className="flex gap-3 flex-wrap">
              <span className="px-3 py-1.5 rounded-full bg-[#14b8a6]/20 text-[#14b8a6] font-semibold text-sm">
                {action.action_type}
              </span>
              <span className="px-3 py-1.5 rounded-full bg-[#2a2d3a] text-[#e2e8f0] text-sm">
                via {action.channel}
              </span>
              <span
                className="px-3 py-1.5 rounded-full text-sm font-semibold"
                style={{ background: priorityColor(action.priority) + '22', color: priorityColor(action.priority) }}
              >
                {action.priority} Priority
              </span>
            </div>

            {/* Routing path */}
            {action.routing_path.length > 0 && (
              <div>
                <p className="text-xs text-[#64748b] uppercase tracking-widest mb-2">Path to Target</p>
                <div className="flex items-center gap-1 flex-wrap">
                  {action.routing_path.map((step, i) => (
                    <React.Fragment key={i}>
                      <span className="text-xs px-2 py-1 rounded bg-[#2a2d3a] text-[#e2e8f0]">{step}</span>
                      {i < action.routing_path.length - 1 && (
                        <span className="text-[#64748b]">→</span>
                      )}
                    </React.Fragment>
                  ))}
                </div>
              </div>
            )}

            {/* Reason */}
            <div className="bg-[#0f1117] rounded-xl p-4">
              <p className="text-xs text-[#64748b] uppercase tracking-widest mb-2">Why Now</p>
              <p className="text-sm text-[#e2e8f0] leading-relaxed">{action.reason}</p>
            </div>

            {/* Message draft */}
            <div>
              <div className="flex items-center justify-between mb-2">
                <p className="text-xs text-[#64748b] uppercase tracking-widest">Message Draft</p>
                <div className="flex gap-2">
                  <button
                    onClick={() => copyToClipboard(action.message_draft)}
                    className="text-xs text-[#64748b] hover:text-[#14b8a6] transition-colors"
                  >
                    Copy
                  </button>
                  <button
                    onClick={handleEditDraft}
                    className="text-xs text-[#64748b] hover:text-[#14b8a6] transition-colors"
                  >
                    Edit
                  </button>
                </div>
              </div>
              {editingDraft ? (
                <div className="space-y-2">
                  <textarea
                    value={draftText}
                    onChange={e => setDraftText(e.target.value)}
                    rows={6}
                    className="w-full bg-[#0f1117] border border-[#2a2d3a] rounded-xl p-4 text-sm text-[#e2e8f0] outline-none focus:border-[#14b8a6] resize-none"
                  />
                  <div className="flex gap-2">
                    <button
                      onClick={() => saveDraftMutation.mutate()}
                      className="px-4 py-2 rounded-lg bg-[#14b8a6] text-black text-sm font-semibold"
                    >
                      Save
                    </button>
                    <button
                      onClick={() => setEditingDraft(false)}
                      className="px-4 py-2 rounded-lg border border-[#2a2d3a] text-sm text-[#64748b]"
                    >
                      Cancel
                    </button>
                  </div>
                </div>
              ) : (
                <div className="bg-[#0f1117] rounded-xl p-4 text-sm text-[#e2e8f0] leading-relaxed border border-[#2a2d3a]">
                  {action.message_draft}
                </div>
              )}
            </div>

            {/* Expected outcome */}
            <div className="bg-[#14b8a6]/10 rounded-xl p-4">
              <p className="text-xs text-[#14b8a6] uppercase tracking-widest mb-1">Expected Outcome</p>
              <p className="text-sm text-[#e2e8f0]">{action.expected_outcome}</p>
            </div>

            {/* Log outcome */}
            <div>
              <p className="text-xs text-[#64748b] uppercase tracking-widest mb-2">Mark Outcome</p>
              <div className="flex gap-2 flex-wrap">
                {OUTCOMES.map(o => (
                  <button
                    key={o}
                    onClick={() => {
                      setOutcome(o);
                      logMutation.mutate(o);
                    }}
                    className={`px-3 py-1.5 rounded-full text-xs font-medium border transition-colors ${
                      outcome === o
                        ? 'bg-[#14b8a6] text-black border-[#14b8a6]'
                        : 'border-[#2a2d3a] text-[#64748b] hover:border-[#14b8a6] hover:text-[#e2e8f0]'
                    }`}
                  >
                    {o.replace(/_/g, ' ')}
                  </button>
                ))}
              </div>
            </div>
          </div>
        ) : (
          <div className="p-5 text-center text-[#64748b]">No action available yet. Run scoring first.</div>
        )}
      </div>
    </div>
  );
}
