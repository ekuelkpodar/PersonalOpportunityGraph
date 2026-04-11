"""
models.py — Node dataclasses and Pydantic API response models.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel


# ══════════════════════════════════════════════════════════════════════════════
# INTERNAL DATACLASSES  (used by pipeline, not exposed via API)
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class PersonNode:
    id: str
    name: str
    source: List[str] = field(default_factory=list)
    x_handle: Optional[str] = None
    x_url: Optional[str] = None
    email: Optional[str] = None
    linkedin_url: Optional[str] = None
    bio_raw: Optional[str] = None
    location: Optional[str] = None
    scoble_lists: List[str] = field(default_factory=list)
    fb_followers: Optional[int] = None
    tw_followers: Optional[int] = None
    ig_followers: Optional[int] = None
    skool_dm_url: Optional[str] = None
    skool_last_msg: Optional[str] = None
    warmth_score: float = 0.0
    confidence_score: float = 0.0
    ingested_at: Optional[str] = None
    last_active_signal: Optional[str] = None
    embedding: Optional[List[float]] = None
    topic_cluster: Optional[str] = None

    # Scoring components (set by scorer)
    opportunity_scores: Dict[str, float] = field(default_factory=dict)
    score_breakdown: Dict[str, Dict[str, float]] = field(default_factory=dict)
    trend_signal: float = 0.2
    is_trending: bool = False
    new_to_network: bool = False


@dataclass
class CompanyNode:
    id: str
    name: str
    source: List[str] = field(default_factory=list)
    clutch_url: Optional[str] = None
    x_url: Optional[str] = None
    site_url: Optional[str] = None
    location: Optional[str] = None
    min_project_size: Optional[str] = None
    hourly_rate: Optional[str] = None
    team_size: Optional[str] = None
    services_raw: List[str] = field(default_factory=list)
    primary_service: Optional[str] = None
    description: Optional[str] = None
    scoble_category: Optional[str] = None
    clutch_category: Optional[str] = None
    confidence_score: float = 0.0
    ingested_at: Optional[str] = None
    embedding: Optional[List[float]] = None
    topic_cluster: Optional[str] = None

    opportunity_scores: Dict[str, float] = field(default_factory=dict)
    score_breakdown: Dict[str, Dict[str, float]] = field(default_factory=dict)
    trend_signal: float = 0.2
    is_trending: bool = False
    new_to_network: bool = False


@dataclass
class PublisherNode:
    id: str
    name: str
    source: str = "feedspot"
    site_url: Optional[str] = None
    category: Optional[str] = None
    category_type: Optional[str] = None  # Blog/Podcast/YouTube/Magazine
    topic_cluster: Optional[str] = None
    description: Optional[str] = None
    location: Optional[str] = None
    domain_authority: Optional[int] = None
    fb_followers: Optional[int] = None
    tw_followers: Optional[int] = None
    ig_followers: Optional[int] = None
    reach_score: float = 0.0
    email: Optional[str] = None
    fb_url: Optional[str] = None
    tw_url: Optional[str] = None
    ig_url: Optional[str] = None
    confidence_score: float = 0.0
    ingested_at: Optional[str] = None
    embedding: Optional[List[float]] = None

    opportunity_scores: Dict[str, float] = field(default_factory=dict)
    score_breakdown: Dict[str, Dict[str, float]] = field(default_factory=dict)
    trend_signal: float = 0.2
    is_trending: bool = False
    new_to_network: bool = False


@dataclass
class CommunityNode:
    id: str
    name: str
    platform: str          # "facebook" | "skool"
    source: str
    url: Optional[str] = None
    visibility: Optional[str] = None
    member_count: Optional[int] = None
    daily_posts: Optional[int] = None
    category: Optional[str] = None
    topic_cluster: Optional[str] = None
    joined: bool = False
    confidence_score: float = 0.0
    ingested_at: Optional[str] = None
    embedding: Optional[List[float]] = None

    opportunity_scores: Dict[str, float] = field(default_factory=dict)
    score_breakdown: Dict[str, Dict[str, float]] = field(default_factory=dict)
    trend_signal: float = 0.2
    is_trending: bool = False
    new_to_network: bool = False


@dataclass
class EgoNode:
    id: str = "ego:ekue"
    name: str = "Ekue"
    location: str = "Atlanta, GA"
    ventures: List[str] = field(default_factory=list)
    skills: List[str] = field(default_factory=list)
    interests: List[str] = field(default_factory=list)
    target_roles: List[str] = field(default_factory=list)
    venture_embeddings: Dict[str, List[float]] = field(default_factory=dict)


@dataclass
class EdgeRecord:
    source_id: str
    target_id: str
    rel_type: str
    weight: float = 1.0
    properties: Dict[str, Any] = field(default_factory=dict)


# ══════════════════════════════════════════════════════════════════════════════
# PYDANTIC API MODELS  (returned by FastAPI endpoints)
# ══════════════════════════════════════════════════════════════════════════════

class ScoreBreakdownModel(BaseModel):
    relevance: float = 0.0
    reachability: float = 0.0
    influence: float = 0.0
    responsiveness: float = 0.0
    confidence: float = 0.0
    novelty: float = 0.0


class NextBestActionModel(BaseModel):
    action_type: str        # DM | EngageContent | AskIntro | PitchService | Collaborate
    channel: str            # Skool | X | LinkedIn | Email
    reason: str
    message_draft: str
    expected_outcome: str
    priority: str           # High | Medium | Low
    routing_path: List[str] = []


class NodeSummaryModel(BaseModel):
    id: str
    name: str
    node_type: str          # Person | Company | Publisher | Community
    source: List[str] = []
    topic_cluster: Optional[str] = None
    location: Optional[str] = None
    opportunity_score: float = 0.0
    score_breakdown: Optional[ScoreBreakdownModel] = None
    warmth_score: float = 0.0
    confidence_score: float = 0.0
    is_trending: bool = False
    new_to_network: bool = False
    is_weak_tie: bool = False
    bridged_clusters: List[str] = []
    next_best_action: Optional[NextBestActionModel] = None


class PersonDetailModel(BaseModel):
    id: str
    name: str
    node_type: str = "Person"
    x_handle: Optional[str] = None
    x_url: Optional[str] = None
    email: Optional[str] = None
    linkedin_url: Optional[str] = None
    bio_raw: Optional[str] = None
    location: Optional[str] = None
    scoble_lists: List[str] = []
    fb_followers: Optional[int] = None
    tw_followers: Optional[int] = None
    ig_followers: Optional[int] = None
    skool_dm_url: Optional[str] = None
    warmth_score: float = 0.0
    confidence_score: float = 0.0
    source: List[str] = []
    topic_cluster: Optional[str] = None
    ingested_at: Optional[str] = None
    opportunity_scores: Dict[str, float] = {}
    score_breakdown: Dict[str, ScoreBreakdownModel] = {}
    is_trending: bool = False
    new_to_network: bool = False
    next_best_action: Optional[NextBestActionModel] = None
    similar_nodes: List[NodeSummaryModel] = []
    neighbors: List[NodeSummaryModel] = []
    interaction_history: List[Dict[str, Any]] = []


class CompanyDetailModel(BaseModel):
    id: str
    name: str
    node_type: str = "Company"
    clutch_url: Optional[str] = None
    x_url: Optional[str] = None
    site_url: Optional[str] = None
    location: Optional[str] = None
    min_project_size: Optional[str] = None
    hourly_rate: Optional[str] = None
    team_size: Optional[str] = None
    services_raw: List[str] = []
    primary_service: Optional[str] = None
    description: Optional[str] = None
    scoble_category: Optional[str] = None
    clutch_category: Optional[str] = None
    confidence_score: float = 0.0
    source: List[str] = []
    topic_cluster: Optional[str] = None
    ingested_at: Optional[str] = None
    opportunity_scores: Dict[str, float] = {}
    score_breakdown: Dict[str, ScoreBreakdownModel] = {}
    is_trending: bool = False
    new_to_network: bool = False
    next_best_action: Optional[NextBestActionModel] = None
    similar_nodes: List[NodeSummaryModel] = []
    neighbors: List[NodeSummaryModel] = []
    interaction_history: List[Dict[str, Any]] = []


class PublisherDetailModel(BaseModel):
    id: str
    name: str
    node_type: str = "Publisher"
    site_url: Optional[str] = None
    category: Optional[str] = None
    category_type: Optional[str] = None
    topic_cluster: Optional[str] = None
    description: Optional[str] = None
    location: Optional[str] = None
    domain_authority: Optional[int] = None
    fb_followers: Optional[int] = None
    tw_followers: Optional[int] = None
    ig_followers: Optional[int] = None
    reach_score: float = 0.0
    email: Optional[str] = None
    confidence_score: float = 0.0
    source: str = "feedspot"
    ingested_at: Optional[str] = None
    opportunity_scores: Dict[str, float] = {}
    score_breakdown: Dict[str, ScoreBreakdownModel] = {}
    is_trending: bool = False
    next_best_action: Optional[NextBestActionModel] = None
    similar_nodes: List[NodeSummaryModel] = []
    neighbors: List[NodeSummaryModel] = []
    interaction_history: List[Dict[str, Any]] = []


class CommunityDetailModel(BaseModel):
    id: str
    name: str
    node_type: str = "Community"
    platform: str = "skool"
    url: Optional[str] = None
    visibility: Optional[str] = None
    member_count: Optional[int] = None
    daily_posts: Optional[int] = None
    category: Optional[str] = None
    topic_cluster: Optional[str] = None
    joined: bool = False
    confidence_score: float = 0.0
    source: str = ""
    ingested_at: Optional[str] = None
    opportunity_scores: Dict[str, float] = {}
    score_breakdown: Dict[str, ScoreBreakdownModel] = {}
    is_trending: bool = False
    new_to_network: bool = False
    next_best_action: Optional[NextBestActionModel] = None
    similar_nodes: List[NodeSummaryModel] = []
    neighbors: List[NodeSummaryModel] = []


# ── Opportunity Feed ──────────────────────────────────────────────────────────

class OpportunityFeedRequest(BaseModel):
    venture_context: str = "applied_insights"
    intent_mode: str = "Exploit"
    node_types: List[str] = []
    topic_clusters: List[str] = []
    warmth_tiers: List[str] = []
    sources: List[str] = []
    min_score: float = 0.0
    location: Optional[str] = None
    page: int = 0
    page_size: int = 20


class OpportunityFeedResponse(BaseModel):
    items: List[NodeSummaryModel]
    total: int
    page: int
    page_size: int
    venture_context: str
    intent_mode: str


class UnexpectedOpportunityModel(BaseModel):
    node: NodeSummaryModel
    bridged_clusters: List[str]
    betweenness: float
    path_length: int


# ── Graph Explorer ───────────────────────────────────────────────────────────

class GraphNodeModel(BaseModel):
    id: str
    name: str
    node_type: str
    opportunity_score: float = 0.0
    confidence_score: float = 0.0
    warmth_score: float = 0.0
    topic_cluster: Optional[str] = None
    is_trending: bool = False
    is_weak_tie: bool = False
    louvain_community: Optional[int] = None
    x: Optional[float] = None
    y: Optional[float] = None


class GraphEdgeModel(BaseModel):
    source: str
    target: str
    rel_type: str
    weight: float = 1.0


class GraphDataResponse(BaseModel):
    nodes: List[GraphNodeModel]
    edges: List[GraphEdgeModel]
    total_nodes: int
    total_edges: int


# ── RAG Chat ─────────────────────────────────────────────────────────────────

class ChatMessageRequest(BaseModel):
    query: str
    venture_context: str = "applied_insights"
    intent_mode: str = "Exploit"
    history: List[Dict[str, str]] = []


class CitedNodeModel(BaseModel):
    node_id: str
    name: str
    node_type: str
    relevance_reason: str


class ChatMessageResponse(BaseModel):
    answer_text: str
    reasoning_path: List[str]
    cited_nodes: List[CitedNodeModel]
    confidence: str    # "high" | "medium" | "low"
    venture_context: str
    query_id: str


# ── Pipeline ─────────────────────────────────────────────────────────────────

class PipelineStatusResponse(BaseModel):
    is_running: bool
    current_source: Optional[str] = None
    current_stage: Optional[str] = None
    progress_pct: float = 0.0
    sources_stats: Dict[str, Any] = {}
    embedding_progress: Dict[str, Any] = {}
    last_run: Optional[str] = None
    next_scoring_run: Optional[str] = None
    log_tail: List[str] = []


class SourceStatsModel(BaseModel):
    source_name: str
    file_count: int = 0
    rows_processed: int = 0
    nodes_created: int = 0
    edges_created: int = 0
    dupes_skipped: int = 0
    confidence_avg: float = 0.0
    status: str = "pending"   # pending | running | done | error


# ── Interactions / Feedback ──────────────────────────────────────────────────

class InteractionLogRequest(BaseModel):
    node_id: str
    venture_context: str
    intent_mode: str
    action_taken: str
    action_type: str
    channel_used: str
    outcome: str     # no_reply | replied | meeting | converted | not_relevant
    notes: str = ""


class InteractionRecord(BaseModel):
    id: int
    node_id: str
    venture_context: str
    intent_mode: str
    action_taken: str
    action_type: str
    channel_used: str
    outcome: str
    notes: str
    timestamp: str


# ── Dashboard ─────────────────────────────────────────────────────────────────

class DashboardStatsResponse(BaseModel):
    total_nodes: int = 0
    total_edges: int = 0
    total_persons: int = 0
    total_companies: int = 0
    total_publishers: int = 0
    total_communities: int = 0
    sources_ingested: List[str] = []
    last_ingestion: Optional[str] = None
    top_opportunities: Dict[str, List[NodeSummaryModel]] = {}
    unexpected_opps_count: int = 0
    trending_nodes: List[NodeSummaryModel] = []
    interaction_summary: Dict[str, Any] = {}
    confidence_distribution: Dict[str, int] = {}
    warmth_distribution: Dict[str, int] = {}
    node_type_distribution: Dict[str, int] = {}
