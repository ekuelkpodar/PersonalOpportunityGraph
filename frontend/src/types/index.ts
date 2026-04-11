// Core types matching backend Pydantic models

export interface ScoreBreakdown {
  relevance: number;
  reachability: number;
  influence: number;
  responsiveness: number;
  confidence: number;
  novelty: number;
}

export interface NextBestAction {
  action_type: 'DM' | 'EngageContent' | 'AskIntro' | 'PitchService' | 'Collaborate';
  channel: 'Skool' | 'X' | 'LinkedIn' | 'Email';
  reason: string;
  message_draft: string;
  expected_outcome: string;
  priority: 'High' | 'Medium' | 'Low';
  routing_path: string[];
}

export interface NodeSummary {
  id: string;
  name: string;
  node_type: string;
  source: string[];
  topic_cluster?: string;
  location?: string;
  opportunity_score: number;
  score_breakdown?: ScoreBreakdown;
  warmth_score: number;
  confidence_score: number;
  is_trending: boolean;
  new_to_network: boolean;
  is_weak_tie: boolean;
  bridged_clusters: string[];
  next_best_action?: NextBestAction;
}

export interface OpportunityFeedRequest {
  venture_context: string;
  intent_mode: string;
  node_types?: string[];
  topic_clusters?: string[];
  warmth_tiers?: string[];
  sources?: string[];
  min_score?: number;
  location?: string;
  page?: number;
  page_size?: number;
}

export interface OpportunityFeedResponse {
  items: NodeSummary[];
  total: number;
  page: number;
  page_size: number;
  venture_context: string;
  intent_mode: string;
}

export interface UnexpectedOpportunity {
  node: NodeSummary;
  bridged_clusters: string[];
  betweenness: number;
  path_length: number;
}

export interface GraphNode {
  id: string;
  name: string;
  node_type: string;
  opportunity_score: number;
  confidence_score: number;
  warmth_score: number;
  topic_cluster?: string;
  is_trending: boolean;
  is_weak_tie: boolean;
  louvain_community?: number;
  x?: number;
  y?: number;
}

export interface GraphEdge {
  source: string;
  target: string;
  rel_type: string;
  weight: number;
}

export interface GraphData {
  nodes: GraphNode[];
  edges: GraphEdge[];
  total_nodes: number;
  total_edges: number;
}

export interface CitedNode {
  node_id: string;
  name: string;
  node_type: string;
  relevance_reason: string;
}

export interface ChatResponse {
  answer_text: string;
  reasoning_path: string[];
  cited_nodes: CitedNode[];
  confidence: 'high' | 'medium' | 'low';
  venture_context: string;
  query_id: string;
}

export interface DashboardStats {
  total_nodes: number;
  total_edges: number;
  total_persons: number;
  total_companies: number;
  total_publishers: number;
  total_communities: number;
  sources_ingested: string[];
  last_ingestion?: string;
  top_opportunities: Record<string, NodeSummary[]>;
  unexpected_opps_count: number;
  trending_nodes: NodeSummary[];
  interaction_summary: Record<string, any>;
  confidence_distribution: Record<string, number>;
  warmth_distribution: Record<string, number>;
  node_type_distribution: Record<string, number>;
}

export interface InteractionLog {
  node_id: string;
  venture_context: string;
  intent_mode: string;
  action_taken: string;
  action_type: string;
  channel_used: string;
  outcome: 'no_reply' | 'replied' | 'meeting' | 'converted' | 'not_relevant';
  notes: string;
}

export type VentureContext = 'applied_insights' | 'aegis_t2a' | 'rgn_trucking' | 'job_search';
export type IntentMode = 'Exploit' | 'Explore' | 'Bridge' | 'Recruit' | 'Sell';

export const VENTURE_LABELS: Record<VentureContext, string> = {
  applied_insights: 'Applied Insights',
  aegis_t2a: 'AEGIS-T2A',
  rgn_trucking: 'RGN Trucking',
  job_search: 'Job Search',
};

export const INTENT_LABELS: Record<IntentMode, string> = {
  Exploit: 'Exploit (max ROI)',
  Explore: 'Explore (new clusters)',
  Bridge:  'Bridge (cross domains)',
  Recruit: 'Recruit (find talent)',
  Sell:    'Sell (find buyers)',
};

export const NODE_COLORS: Record<string, string> = {
  Person:    '#14b8a6',  // teal
  Company:   '#f87171',  // coral
  Publisher: '#a78bfa',  // purple
  Community: '#fbbf24',  // amber
  Ego:       '#f59e0b',  // gold
  Unknown:   '#64748b',  // muted
};
