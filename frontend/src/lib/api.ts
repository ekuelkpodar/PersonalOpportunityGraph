// API client — all backend calls go through here
import axios from 'axios';
import type {
  OpportunityFeedRequest, OpportunityFeedResponse,
  UnexpectedOpportunity, GraphData, DashboardStats,
  ChatResponse, InteractionLog, NextBestAction,
} from '../types';

const api = axios.create({ baseURL: '/api' });

// ── Dashboard ─────────────────────────────────────────────────────────────────
export const getDashboardStats = () =>
  api.get<DashboardStats>('/dashboard/stats').then(r => r.data);

// ── Opportunities ─────────────────────────────────────────────────────────────
export const getOpportunityFeed = (req: OpportunityFeedRequest) =>
  api.post<OpportunityFeedResponse>('/opportunities/feed', req).then(r => r.data);

export const getUnexpectedOpps = (venture_context: string, limit = 20) =>
  api.get<UnexpectedOpportunity[]>('/opportunities/unexpected', {
    params: { venture_context, limit },
  }).then(r => r.data);

export const getNodeAction = (node_id: string, venture_context: string) =>
  api.get<NextBestAction>(`/opportunities/node/${node_id}/action`, {
    params: { venture_context },
  }).then(r => r.data);

// ── Graph ─────────────────────────────────────────────────────────────────────
export const getEgoGraph = (params: {
  venture_context: string;
  intent_mode: string;
  top_n?: number;
  min_score?: number;
  node_types?: string;
}) =>
  api.get<GraphData>('/graph/ego', { params }).then(r => r.data);

export const getNodeGraph = (node_id: string) =>
  api.get<GraphData>(`/graph/node/${node_id}`).then(r => r.data);

export const getNodeDetail = (node_id: string, venture_context: string) =>
  api.get(`/graph/node/${node_id}/detail`, { params: { venture_context } }).then(r => r.data);

export const getGraphStats = () =>
  api.get('/graph/stats').then(r => r.data);

// ── Actions ───────────────────────────────────────────────────────────────────
export const getAction = (node_id: string, venture_context: string) =>
  api.get<NextBestAction>(`/actions/node/${node_id}`, { params: { venture_context } }).then(r => r.data);

export const updateDraft = (node_id: string, venture_context: string, new_draft: string) =>
  api.put('/actions/draft', { node_id, venture_context, new_draft }).then(r => r.data);

// ── Feedback ──────────────────────────────────────────────────────────────────
export const logInteraction = (payload: InteractionLog) =>
  api.post('/feedback/interaction', payload).then(r => r.data);

export const getNodeInteractions = (node_id: string) =>
  api.get(`/feedback/node/${node_id}`).then(r => r.data);

export const getInteractionSummary = (venture_context?: string) =>
  api.get('/feedback/summary', { params: { venture_context } }).then(r => r.data);

// ── Chat ──────────────────────────────────────────────────────────────────────
export const queryGraph = (payload: {
  query: string;
  venture_context: string;
  intent_mode: string;
  history?: Array<{ role: string; content: string }>;
}) =>
  api.post<ChatResponse>('/chat/query', payload).then(r => r.data);

export const getChatHistory = (limit = 50) =>
  api.get('/chat/history', { params: { limit } }).then(r => r.data);

export const getQuerySuggestions = (venture_context: string) =>
  api.get<{ suggestions: string[] }>('/chat/suggestions', { params: { venture_context } }).then(r => r.data);

// ── Pipeline ──────────────────────────────────────────────────────────────────
export const runPipeline = (force_reprocess = false) =>
  api.post('/pipeline/run', { force_reprocess }).then(r => r.data);

export const getPipelineStatus = () =>
  api.get('/pipeline/status').then(r => r.data);

export const runScoring = () =>
  api.post('/pipeline/score/run').then(r => r.data);
