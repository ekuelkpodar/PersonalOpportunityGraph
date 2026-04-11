"""
config.py — All path mappings, DB connection strings, and scoring weight constants.
All file paths are relative to the project root using os.path.join.
"""
import os

# ── Project root ────────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def _p(*parts: str) -> str:
    """Build an absolute path relative to PROJECT_ROOT."""
    return os.path.join(PROJECT_ROOT, *parts)


# ── Database connections ─────────────────────────────────────────────────────
NEO4J_URI      = os.getenv("NEO4J_URI",      "bolt://localhost:7687")
NEO4J_USER     = os.getenv("NEO4J_USER",     "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

QDRANT_HOST    = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT    = int(os.getenv("QDRANT_PORT", "6333"))

OLLAMA_BASE_URL        = os.getenv("OLLAMA_BASE_URL",     "http://localhost:11434")
OLLAMA_EMBED_MODEL     = os.getenv("OLLAMA_EMBED_MODEL",  "nomic-embed-text")
OLLAMA_GENERATE_MODEL  = os.getenv("OLLAMA_GENERATE_MODEL", "llama3")

# ── SQLite databases ─────────────────────────────────────────────────────────
PIPELINE_PROGRESS_DB = _p("pipeline_progress.db")
INTERACTIONS_DB      = _p("interactions.db")
DRAFT_CACHE_DB       = _p("draft_cache.db")

# ── API / Frontend ───────────────────────────────────────────────────────────
API_HOST          = "0.0.0.0"
API_PORT          = 8001
FRONTEND_DEV_PORT = 5173

# ── Qdrant collections ───────────────────────────────────────────────────────
QDRANT_COLLECTION_PROFILES    = "profiles"     # Person + Publisher  (768-dim)
QDRANT_COLLECTION_COMPANIES   = "companies"    # Company             (768-dim)
QDRANT_COLLECTION_COMMUNITIES = "communities"  # Community           (768-dim)
QDRANT_VECTOR_DIM = 768

# ── Embedding ────────────────────────────────────────────────────────────────
EMBED_BATCH_SIZE        = 50
EMBED_MAX_RETRIES       = 5
EMBED_INITIAL_BACKOFF   = 2.0   # seconds
EMBED_BACKOFF_MULTIPLIER = 2.0

# ── Dedup thresholds ─────────────────────────────────────────────────────────
DEDUP_PERSON_THRESHOLD  = 0.92
DEDUP_COMPANY_THRESHOLD = 0.88

# ── Opportunity scoring weights (base formula) ────────────────────────────────
SCORE_W_RELEVANCE      = 0.30
SCORE_W_REACHABILITY   = 0.25
SCORE_W_INFLUENCE      = 0.15
SCORE_W_RESPONSIVENESS = 0.15
SCORE_W_CONFIDENCE     = 0.10
SCORE_W_NOVELTY        = 0.05

# Reachability sub-weights
REACH_W_PATH_LENGTH    = 0.50
REACH_W_WARM_EDGE      = 0.30
REACH_W_SHARED_COMM    = 0.20

# ── Intent mode multipliers ──────────────────────────────────────────────────
INTENT_MULTIPLIERS = {
    "Exploit": {
        "reachability":   1.4,
        "responsiveness": 1.3,
        "relevance":      1.0,
        "influence":      1.0,
        "confidence":     1.0,
        "novelty":        1.0,
    },
    "Explore": {
        "novelty":        2.0,
        "influence":      1.2,
        "reachability":   0.8,
        "relevance":      1.0,
        "responsiveness": 1.0,
        "confidence":     1.0,
    },
    "Bridge": {
        "novelty":        1.8,
        "reachability":   1.2,
        "relevance":      1.0,
        "influence":      1.0,
        "responsiveness": 1.0,
        "confidence":     1.0,
    },
    "Recruit": {
        "relevance":      1.5,
        "influence":      1.0,
        "reachability":   1.0,
        "responsiveness": 1.0,
        "confidence":     1.0,
        "novelty":        1.0,
    },
    "Sell": {
        "reachability":   1.2,
        "relevance":      1.0,
        "influence":      1.0,
        "responsiveness": 1.0,
        "confidence":     1.0,
        "novelty":        1.0,
    },
}

# ── Feedback adjustments ─────────────────────────────────────────────────────
FEEDBACK_POSITIVE_BOOST   = 0.08   # added to similar nodes on "converted"/"replied"
FEEDBACK_NEGATIVE_PENALTY = 0.05   # subtracted on "not_relevant"
FEEDBACK_SIMILAR_TOP_K    = 20

# ── Weak tie (Unexpected Opportunity) thresholds ──────────────────────────────
WEAK_TIE_BETWEENNESS_MIN    = 0.6
WEAK_TIE_COSINE_SIM_MAX     = 0.35
WEAK_TIE_MAX_HOPS           = 3

# ── Temporal signals ─────────────────────────────────────────────────────────
TREND_NEW_NODE_DAYS         = 30
TREND_UPDATED_DAYS          = 7
TREND_ACTIVE_POSTS_PER_DAY  = 50
TREND_SIGNAL_NEW_NODE       = 1.0
TREND_SIGNAL_UPDATED        = 0.8
TREND_SIGNAL_ACTIVE_COMM    = 0.6
TREND_SIGNAL_DEFAULT        = 0.2
TREND_IS_TRENDING_MIN       = 0.7
TREND_IS_TRENDING_SCORE_MIN = 0.4

# ── Confidence scoring weights ────────────────────────────────────────────────
CONF_W_SOURCES      = 0.40
CONF_W_COMPLETENESS = 0.35
CONF_W_AGREEMENT    = 0.25
CONF_MIN_SOURCES    = 3      # denominator for source count ratio
CONF_LOW_THRESHOLD  = 0.3

# ── Warmth scores by source ──────────────────────────────────────────────────
WARMTH_SKOOL_DM   = 1.0
WARMTH_SKOOL      = 0.6
WARMTH_SCOBLE     = 0.3
WARMTH_FEEDSPOT   = 0.1
WARMTH_COLD       = 0.0

# ── Nightly scoring job ──────────────────────────────────────────────────────
SCORING_JOB_HOUR   = 2
SCORING_JOB_MINUTE = 0

# ── Opportunity feed ─────────────────────────────────────────────────────────
OPPORTUNITY_SCORE_MIN_ACTION = 0.5   # min score to generate next_best_action
TOP_K_QDRANT_RETRIEVAL       = 50
MAX_HOP_FILTER               = 3

# ── Ego node ─────────────────────────────────────────────────────────────────
EGO_ID       = "ego:ekue"
EGO_NAME     = "Ekue"
EGO_LOCATION = "Atlanta, GA"
EGO_VENTURES = [
    "Applied Insights (AI automation agency)",
    "AEGIS-T2A (enterprise AI governance platform)",
    "RGN / Truck Dispatch 360 (heavy haul trucking + federal contracting)",
]
EGO_SKILLS = [
    "LLM systems", "multi-agent", "LangGraph", "RAG", "distributed systems",
    "OPA/Rego", "SPIFFE/SPIRE", "Vault", "Temporal.io", "Python", "Java",
    "identity infrastructure", "AI governance", "vector databases", "Neo4j",
    "Oracle IAM",
]
EGO_TARGET_ROLES = [
    "Senior AI/ML Engineer", "AI Architect", "Head of AI", "Staff ML Engineer",
]
EGO_INTERESTS = [
    "AI governance", "enterprise AI", "network science", "systems thinking",
    "federal contracting", "AI automation",
]

# Venture-specific embedding texts for per-context scoring
EGO_VENTURE_CONTEXTS = {
    "applied_insights": (
        "AI automation agency client acquisition applied insights LLM agents "
        "RAG voice agents workflow automation business automation founders operators"
    ),
    "aegis_t2a": (
        "AI governance enterprise compliance OPA Rego SPIFFE SPIRE Vault Temporal "
        "policy enforcement audit trail enterprise security AI risk CISO CTO"
    ),
    "rgn_trucking": (
        "federal contracting trucking heavy haul RGN dispatch motor freight "
        "USASpending prime contractor subcontractor logistics"
    ),
    "job_search": (
        "senior AI ML engineer LLM production RAG LangGraph multi-agent distributed "
        "systems Oracle identity security Java Python AI architect staff engineer"
    ),
}

# ── Source file paths ─────────────────────────────────────────────────────────

# Feedspot
FEEDSPOT_DIR = _p("feedspot")
FEEDSPOT_FILES = [_p("feedspot", f"blogs_{i}.csv") for i in range(1, 17)]

# Feedspot schema variants
FEEDSPOT_V1_INDICES = {1,2,3,4,5,6,7,8,9,10,13}
FEEDSPOT_V2_INDICES = {11,12,14,15,16}

# XList / Scoble
XLIST_DIR = _p("XList")

XLIST_PERSON_FILES = [
    _p("XList", "AI Artists.csv"),
    _p("XList", "AI Community #1 of 7.csv"),
    _p("XList", "AI Community #2 of 7.csv"),
    _p("XList", "AI Community #3 of 7.csv"),
    _p("XList", "AI Community #4 of 7.csv"),
    _p("XList", "AI Community #5 of 7.csv"),
    _p("XList", "AI Community #6 of 7.csv"),
    _p("XList", "AI Community #7 of 7.csv"),
    _p("XList", "AI Influencers.csv"),
    _p("XList", "Devs, Designers, DevRel 1.csv"),
    _p("XList", "Founders #1 of 3.csv"),
    _p("XList", "Founders #2 of 3.csv"),
    _p("XList", "Founders #3 of 3.csv"),
    _p("XList", "Investors #1 of 2.csv"),
    _p("XList", "Investors #2 of 2.csv"),
    _p("XList", "News (no AI) #1 of 3.csv"),
    _p("XList", "News (no AI) #2 of 3.csv"),
    _p("XList", "News (no AI) #3 of 3.csv"),
    _p("XList", "Security.csv"),
    _p("XList", "Tech Journalists.csv"),
]

XLIST_COMPANY_FILES = [
    _p("XList", "AIcompanies-1.csv"),
    _p("XList", "AIcompanies-2.csv"),
    _p("XList", "AI Developer Tools.csv"),
    _p("XList", "AI Enterprise or Work.csv"),
    _p("XList", "Infrastructure.csv"),
    _p("XList", "VC Firms.csv"),
]

# Clutch directories
CLUTCH_DIR = _p("Clutch")
CLUTCH_SUBDIRS = {
    "DigitalMarketing": _p("Clutch", "DigitalMarketing Clutch"),
    "Development":      _p("Clutch", "Development Clutch"),
    "Design":           _p("Clutch", "Design Clutch"),
    "BusinessServices": _p("Clutch", "BusinessServicesClutch"),
    "ITServices":       _p("Clutch", "ITservicesClutch"),
}

# Facebook
FACEBOOK_DIR = _p("FacebookGroups")
FACEBOOK_CSS_CSV_FILES = [
    _p("FacebookGroups", "FacebookTrucking.csv"),
    _p("FacebookGroups", "bisnesses-1.csv"),
    _p("FacebookGroups", "business owners.csv"),
    _p("FacebookGroups", "businessOwners.csv"),
]
FACEBOOK_ALT_CSV_FILE   = _p("FacebookGroups", "facebook.csv")
FACEBOOK_XLSX_ORACLE    = _p("FacebookGroups", "oracle_facebook_groups.xlsx")
FACEBOOK_XLSX_SQL       = _p("FacebookGroups", "SQL_Facebook_Groups_Cleaned.xlsx")
FACEBOOK_XLSX_EXTRA     = _p("FacebookGroups", "facebook-3.xlsx")

# Skool
SKOOL_DIR              = _p("Skool")
SKOOL_COMMUNITIES_FILE = _p("Skool", "SkoolCommunities.csv")
SKOOL_DM_FILE          = _p("Skool", "SkoolDM.csv")

# ── Topic clustering keywords ─────────────────────────────────────────────────
TOPIC_CLUSTERS = {
    "ai_governance":    ["governance", "compliance", "policy", "opa", "rego", "audit",
                         "risk", "security", "spiffe", "spire", "vault", "zero trust"],
    "ai_automation":    ["automation", "llm", "agent", "workflow", "rag", "langchain",
                         "langraph", "voice agent", "chatbot", "n8n", "make", "zapier"],
    "enterprise_ai":    ["enterprise", "b2b", "saas", "platform", "ciso", "cto", "coo",
                         "digital transformation", "ai strategy"],
    "dev_tools":        ["developer", "sdk", "api", "devrel", "open source", "github",
                         "tooling", "framework", "infrastructure", "cloud"],
    "venture_startup":  ["founder", "startup", "venture", "seed", "series a", "vc",
                         "fundraise", "bootstrapped", "operator"],
    "marketing":        ["marketing", "seo", "ppc", "branding", "content", "social media",
                         "growth", "acquisition", "demand gen"],
    "trucking_logistics":["trucking", "logistics", "freight", "dispatch", "supply chain",
                          "haul", "rgn", "federal contract", "dot", "carrier"],
    "design_creative":  ["design", "ux", "ui", "creative", "branding", "visual",
                         "product design"],
    "ai_research":      ["research", "paper", "arxiv", "model", "fine-tuning", "rlhf",
                         "transformer", "llm architecture"],
    "community":        ["community", "group", "network", "skool", "facebook group",
                         "discord", "slack"],
}

# ── Clutch category name normalizer ──────────────────────────────────────────
CLUTCH_SUBDIR_TO_CATEGORY = {
    "DigitalMarketing Clutch": "DigitalMarketing",
    "Development Clutch":      "Development",
    "Design Clutch":           "Design",
    "BusinessServicesClutch":  "BusinessServices",
    "ITservicesClutch":        "ITServices",
}
