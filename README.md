# Personal Opportunity Graph

A full-stack, locally-hosted network intelligence tool that ingests ~274k rows across 5 XLSX/CSV data sources, normalizes them into a graph database (Neo4j) and vector database (Qdrant), computes multi-dimensional opportunity scores, and surfaces the results through a modern web UI.

Built for a single ego node — you — to answer the question: **"Who in my network should I reach out to, and why?"**

---

## Architecture

![System Architecture](Image/opportunity_graph_system_architecture.svg)

---

## UI Screenshots

### Dashboard — Corpus Stats & Top Opportunities
![Dashboard Overview](Image/Dashboard-1.png)

### Opportunity Feed — Ranked by Score
![Opportunity Feed](Image/Dashboard-2.png)

### Graph Explorer — Force-Directed Network
![Graph Explorer](Image/Dashboard-3.png)

### RAG Chat — Graph-Aware Q&A
![RAG Chat](Image/Dashboard-4.png)

---

## Node Types

| Label | Source | Key Fields |
|-------|--------|------------|
| `:Person` | XList, Feedspot, Skool DMs | x_handle, bio_raw, scoble_lists, warmth_score |
| `:Company` | XList, Clutch | services_raw, primary_service, clutch_category, hourly_rate |
| `:Publisher` | Feedspot | site_url, domain_authority, reach_score, category_type |
| `:Community` | Facebook, Skool | platform, member_count, daily_posts, visibility |
| `:Ego` | Config | ventures, skills, interests, target_roles |

## Edge Types

| Relationship | From → To | Weight |
|---|---|---|
| `HAS_AUTHOR` | Publisher → Person | 1.0 |
| `WORKS_AT` | Person → Company | 0.7 |
| `AFFILIATED_WITH` | Person → Company | 0.5 |
| `MEMBER_OF` | Ego → Community | 1.0 |
| `WARM_CONTACT` | Ego → Person | 1.0 |
| `IN_COMMUNITY` | Person → Community | 0.8 |
| `IN_SCOBLE_LIST` | Person/Company → list | categorical |
| `OPPORTUNITY_SCORE` | Ego → any | computed score |

## Opportunity Score Formula

```
opportunity_score =
  0.30 × relevance_score       (cosine sim to ego venture embedding)
+ 0.25 × reachability_score    (path length + warm edges + shared communities)
+ 0.15 × influence_score       (PageRank normalized)
+ 0.15 × responsiveness_score  (warmth tier)
+ 0.10 × confidence_score      (data completeness)
+ 0.05 × novelty_score         (betweenness × inverse similarity)
```

Scores are computed for **4 venture contexts** (Applied Insights, AEGIS-T2A, RGN Trucking, Job Search) and reweighted at query time by **5 intent modes** (Exploit, Explore, Bridge, Recruit, Sell).

---

## Tech Stack

| Layer | Technology |
|---|---|
| Graph DB | Neo4j 5 + GDS plugin (PageRank, Louvain, node2vec) |
| Vector DB | Qdrant (3 collections, 768-dim) |
| LLM / Embeddings | Ollama — `nomic-embed-text` + `llama3` |
| Pipeline | LangGraph + Python 3.11+ |
| Backend | FastAPI + WebSocket progress streaming |
| Frontend | React 18 + Vite + TailwindCSS + Cytoscape.js + Recharts |
| Scheduling | APScheduler (nightly scoring at 2am) |
| Infra | Docker Compose (Neo4j, Qdrant, Ollama) |

---

## Project Structure

```
PersonalOpportunityGraph/
├── backend/
│   ├── main.py                  FastAPI entry point + APScheduler
│   ├── config.py                All paths, DB connections, scoring weights
│   ├── models.py                Pydantic models + node dataclasses
│   ├── utils.py                 ID gen, text cleaning, follower parsing, infer_category_type
│   ├── pipeline/
│   │   ├── orchestrator.py      LangGraph pipeline + WebSocket progress
│   │   ├── scanner.py           Discovers all source files (CSV + XLSX)
│   │   ├── dedup.py             sha256 + rapidfuzz Jaro-Winkler
│   │   ├── embedder.py          Batch Ollama embeddings — /api/embed, batch 50, resume
│   │   ├── loader.py            Neo4j MERGE + Qdrant upsert + collection creation
│   │   └── sources/
│   │       ├── feedspot.py      5 XLSX files, multi-sheet, V1/V2 schema detection
│   │       ├── xlist.py         Person + Company files, handles # in names
│   │       ├── clutch.py        5 subdirectory categories, service % parsing
│   │       ├── facebook.py      CSS-scraped CSV + structured XLSX
│   │       └── skool.py         Communities + DM warm contacts
│   ├── graph/
│   │   ├── gds.py               PageRank, Louvain, betweenness, node2vec
│   │   ├── scorer.py            6-component scoring, ego embeddings cached once per job
│   │   ├── qdrant_client.py     Qdrant wrapper, auto-creates collections on first use
│   │   ├── ego_network.py       1-hop/2-hop subgraph extraction
│   │   ├── weak_ties.py         Bridge node detection
│   │   ├── reachability.py      Path-length + warmth reachability score
│   │   └── temporal.py          Trend signals + recency decay
│   ├── rag/
│   │   ├── retriever.py         Hybrid Qdrant top-K then Neo4j hop filter
│   │   └── agent.py             LangGraph RAG agent (llama3)
│   ├── action/
│   │   ├── engine.py            Next Best Action generator
│   │   ├── drafts.py            Outreach draft cache
│   │   └── routing.py          Channel routing logic
│   ├── feedback/
│   │   └── loop.py              Interaction logging + score adjustment
│   └── api/
│       ├── dashboard.py
│       ├── opportunities.py
│       ├── graph.py
│       ├── chat.py
│       ├── pipeline.py          Scoring job runs in background thread
│       ├── actions.py
│       ├── feedback.py
│       └── websocket.py
├── frontend/
│   └── src/
│       ├── App.tsx              Sidebar nav shell
│       └── components/
│           ├── Dashboard.tsx    Corpus stats + top opportunities
│           ├── GraphExplorer.tsx  Cytoscape.js force-directed graph
│           ├── OpportunityFeed.tsx  Infinite scroll ranked feed, all warmth tiers on by default
│           ├── RagChat.tsx      Graph-aware chat interface
│           ├── PipelineControl.tsx  Live log + pipeline controls
│           ├── ActionDrawer.tsx Next Best Action + outreach drafts
│           └── ScoreRadar.tsx   Radar chart score breakdown
├── Image/
│   ├── opportunity_graph_system_architecture.svg
│   ├── Dashboard-1.png
│   ├── Dashboard-2.png
│   ├── Dashboard-3.png
│   └── Dashboard-4.png
├── docker-compose.yml
├── requirements.txt
├── start.sh
└── README.md
```

---

## Setup

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Python 3.11+
- Node.js 18+

### Data Sources (Required — not included)

This repository does **not** include any data files. You must supply your own CSVs/XLSXs in the following structure:

```
feedspot/
  _BloggerOutreach_Blogs_FullDB_*.xlsx
  _BloggerOutreach_Magazines_FullDB_*.xlsx
  _BloggerOutreach_Podcasts_FullDB_*.xlsx
  _BloggerOutreach_Youtube_FullDB_*.xlsx
  _BloggerOutreach_Newsletters_FullDB_*.xlsx
XList/            — Scoble Twitter/X list exports (CSV)
Clutch/
  DigitalMarketing Clutch/
  Development Clutch/
  Design Clutch/
  BusinessServicesClutch/
  ITservicesClutch/
FacebookGroups/   — CSS-scraped CSVs + structured XLSXs
Skool/            — SkoolCommunities.csv + SkoolDM.csv
```

See `backend/config.py` for the exact expected filenames and column schemas.

### Install & Run

```bash
# 1. Clone
git clone https://github.com/ekuelkpodar/PersonalOpportunityGraph.git
cd PersonalOpportunityGraph

# 2. Add your data files (see structure above)

# 3. Start everything
chmod +x start.sh
./start.sh
```

`start.sh` will:
1. Start Neo4j, Qdrant, and Ollama via Docker Compose
2. Pull `nomic-embed-text` and `llama3` into Ollama
3. Create a Python venv and install dependencies
4. Start the FastAPI backend on **:8001**
5. Start the Vite dev server on **:5173**

### First Run

Open **http://localhost:5173** → go to the **Pipeline** tab → click **Run Pipeline**.

The pipeline will scan all data files, normalize, dedup, embed, load into Neo4j + Qdrant, and run the initial opportunity scoring pass. Progress streams live via WebSocket.

Then click **Run Scoring** to compute opportunity scores across all 4 venture contexts. Scores appear in the Opportunity Feed and Dashboard as they are written.

---

## Ego Configuration

Edit `backend/config.py` to set your own identity:

```python
EGO_ID       = "ego:you"
EGO_NAME     = "Your Name"
EGO_LOCATION = "City, State"
EGO_VENTURES = ["Venture 1", "Venture 2"]
EGO_SKILLS   = ["skill1", "skill2", ...]
EGO_INTERESTS = ["topic1", "topic2", ...]

EGO_VENTURE_CONTEXTS = {
    "venture_1": "describe venture 1 in plain text for embedding...",
    "venture_2": "describe venture 2 in plain text for embedding...",
}
```

---

## UI Pages

| Page | What it shows |
|---|---|
| **Dashboard** | Total nodes/edges, top-10 opportunities per venture, warmth/confidence distribution charts |
| **Graph Explorer** | Cytoscape.js force-directed graph, Louvain community coloring, weak-tie highlighting, 1-hop expansion |
| **Opportunity Feed** | Infinite scroll ranked by score, filter by type/warmth/location, Next Best Action drawer |
| **RAG Chat** | Ask questions over the graph in natural language with cited node sources |
| **Pipeline Control** | Run/monitor ingestion, live log viewer, embedding progress, re-run scoring |

---

## Key Implementation Notes

- **Feedspot normalizer** handles 5 XLSX files with multi-sheet layouts and two schema variants (V1: col0=`Id`, V2: col0=`Sr.no`) detected per sheet from the header row. Sub-author rows (empty Site URL but Author Name present) are linked to the last seen publisher via `HAS_AUTHOR`.
- **Ollama embedding endpoint** uses `/api/embed` (not the deprecated `/api/embeddings`) with batch input — compatible with Ollama 0.20.5+.
- **Qdrant collections** are created automatically on first use by both the loader and the graph query client — no manual setup required.
- **Scoring job** runs in a FastAPI `BackgroundTask` thread so the API stays responsive. Ego venture embeddings are computed once per job run and reused across all 584k nodes.
- **Topic clusters** cover 23 domains including AI governance, trucking/logistics, crypto/web3, health/wellness, real estate, and more — used for `topic_cluster` node property and feed filtering.

---

## License

MIT
