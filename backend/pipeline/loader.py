"""
loader.py — Neo4j MERGE bulk loader + Qdrant upsert.

All Neo4j writes use MERGE (idempotent).
Qdrant collections are created on first use.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from neo4j import GraphDatabase, Driver
from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance, VectorParams, PointStruct, PayloadSchemaType,
    UpdateStatus,
)

from backend.config import (
    NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD,
    QDRANT_HOST, QDRANT_PORT,
    QDRANT_COLLECTION_PROFILES,
    QDRANT_COLLECTION_COMPANIES,
    QDRANT_COLLECTION_COMMUNITIES,
    QDRANT_VECTOR_DIM,
    EGO_ID, EGO_NAME, EGO_LOCATION, EGO_VENTURES, EGO_SKILLS,
    EGO_INTERESTS, EGO_TARGET_ROLES,
)
from backend.models import (
    PersonNode, CompanyNode, PublisherNode, CommunityNode, EdgeRecord,
)


# ── Neo4j driver singleton ────────────────────────────────────────────────────

_neo4j_driver: Optional[Driver] = None


def get_neo4j_driver() -> Driver:
    global _neo4j_driver
    if _neo4j_driver is None:
        _neo4j_driver = GraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
    return _neo4j_driver


def close_neo4j_driver() -> None:
    global _neo4j_driver
    if _neo4j_driver:
        _neo4j_driver.close()
        _neo4j_driver = None


# ── Qdrant client singleton ───────────────────────────────────────────────────

_qdrant_client: Optional[QdrantClient] = None


def get_qdrant_client() -> QdrantClient:
    global _qdrant_client
    if _qdrant_client is None:
        _qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
        _ensure_collections(_qdrant_client)
    return _qdrant_client


def _ensure_collections(client: QdrantClient) -> None:
    """Create Qdrant collections if they don't exist."""
    existing = {c.name for c in client.get_collections().collections}
    for name in [QDRANT_COLLECTION_PROFILES,
                 QDRANT_COLLECTION_COMPANIES,
                 QDRANT_COLLECTION_COMMUNITIES]:
        if name not in existing:
            client.create_collection(
                collection_name=name,
                vectors_config=VectorParams(
                    size=QDRANT_VECTOR_DIM,
                    distance=Distance.COSINE,
                ),
            )


# ── Neo4j constraint setup ────────────────────────────────────────────────────

def setup_neo4j_constraints() -> None:
    """Create uniqueness constraints and indexes in Neo4j."""
    driver = get_neo4j_driver()
    with driver.session() as session:
        constraints = [
            "CREATE CONSTRAINT person_id IF NOT EXISTS FOR (n:Person) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT company_id IF NOT EXISTS FOR (n:Company) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT publisher_id IF NOT EXISTS FOR (n:Publisher) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT community_id IF NOT EXISTS FOR (n:Community) REQUIRE n.id IS UNIQUE",
            "CREATE CONSTRAINT ego_id IF NOT EXISTS FOR (n:Ego) REQUIRE n.id IS UNIQUE",
        ]
        for c in constraints:
            try:
                session.run(c)
            except Exception:
                pass

        indexes = [
            "CREATE INDEX person_warmth IF NOT EXISTS FOR (n:Person) ON (n.warmth_score)",
            "CREATE INDEX person_confidence IF NOT EXISTS FOR (n:Person) ON (n.confidence_score)",
            "CREATE INDEX company_category IF NOT EXISTS FOR (n:Company) ON (n.clutch_category)",
            "CREATE INDEX node_cluster IF NOT EXISTS FOR (n:Person) ON (n.topic_cluster)",
        ]
        for idx in indexes:
            try:
                session.run(idx)
            except Exception:
                pass


# ── Ego node loader ───────────────────────────────────────────────────────────

def load_ego_node() -> None:
    driver = get_neo4j_driver()
    with driver.session() as session:
        session.run(
            """
            MERGE (e:Ego {id: $id})
            SET e.name = $name,
                e.location = $location,
                e.ventures = $ventures,
                e.skills = $skills,
                e.interests = $interests,
                e.target_roles = $target_roles
            """,
            id=EGO_ID,
            name=EGO_NAME,
            location=EGO_LOCATION,
            ventures=EGO_VENTURES,
            skills=EGO_SKILLS,
            interests=EGO_INTERESTS,
            target_roles=EGO_TARGET_ROLES,
        )


# ── Person nodes ──────────────────────────────────────────────────────────────

def load_persons(nodes: List[PersonNode]) -> int:
    driver = get_neo4j_driver()
    count = 0
    with driver.session() as session:
        for node in nodes:
            session.run(
                """
                MERGE (n:Person {id: $id})
                SET n.name           = $name,
                    n.x_handle       = $x_handle,
                    n.x_url          = $x_url,
                    n.email          = $email,
                    n.linkedin_url   = $linkedin_url,
                    n.bio_raw        = $bio_raw,
                    n.location       = $location,
                    n.scoble_lists   = $scoble_lists,
                    n.fb_followers   = $fb_followers,
                    n.tw_followers   = $tw_followers,
                    n.ig_followers   = $ig_followers,
                    n.skool_dm_url   = $skool_dm_url,
                    n.skool_last_msg = $skool_last_msg,
                    n.warmth_score   = $warmth_score,
                    n.confidence_score = $confidence_score,
                    n.source         = $source,
                    n.topic_cluster  = $topic_cluster,
                    n.ingested_at    = $ingested_at
                """,
                id=node.id,
                name=node.name,
                x_handle=node.x_handle,
                x_url=node.x_url,
                email=node.email,
                linkedin_url=node.linkedin_url,
                bio_raw=node.bio_raw,
                location=node.location,
                scoble_lists=node.scoble_lists,
                fb_followers=node.fb_followers,
                tw_followers=node.tw_followers,
                ig_followers=node.ig_followers,
                skool_dm_url=node.skool_dm_url,
                skool_last_msg=node.skool_last_msg,
                warmth_score=node.warmth_score,
                confidence_score=node.confidence_score,
                source=node.source,
                topic_cluster=node.topic_cluster,
                ingested_at=node.ingested_at,
            )
            count += 1
    return count


# ── Company nodes ─────────────────────────────────────────────────────────────

def load_companies(nodes: List[CompanyNode]) -> int:
    driver = get_neo4j_driver()
    count = 0
    with driver.session() as session:
        for node in nodes:
            session.run(
                """
                MERGE (n:Company {id: $id})
                SET n.name            = $name,
                    n.clutch_url      = $clutch_url,
                    n.x_url           = $x_url,
                    n.site_url        = $site_url,
                    n.location        = $location,
                    n.min_project_size= $min_project_size,
                    n.hourly_rate     = $hourly_rate,
                    n.team_size       = $team_size,
                    n.services_raw    = $services_raw,
                    n.primary_service = $primary_service,
                    n.description     = $description,
                    n.scoble_category = $scoble_category,
                    n.clutch_category = $clutch_category,
                    n.confidence_score= $confidence_score,
                    n.source          = $source,
                    n.topic_cluster   = $topic_cluster,
                    n.ingested_at     = $ingested_at
                """,
                id=node.id,
                name=node.name,
                clutch_url=node.clutch_url,
                x_url=node.x_url,
                site_url=node.site_url,
                location=node.location,
                min_project_size=node.min_project_size,
                hourly_rate=node.hourly_rate,
                team_size=node.team_size,
                services_raw=node.services_raw,
                primary_service=node.primary_service,
                description=node.description,
                scoble_category=node.scoble_category,
                clutch_category=node.clutch_category,
                confidence_score=node.confidence_score,
                source=node.source,
                topic_cluster=node.topic_cluster,
                ingested_at=node.ingested_at,
            )
            count += 1
    return count


# ── Publisher nodes ───────────────────────────────────────────────────────────

def load_publishers(nodes: List[PublisherNode]) -> int:
    driver = get_neo4j_driver()
    count = 0
    with driver.session() as session:
        for node in nodes:
            session.run(
                """
                MERGE (n:Publisher {id: $id})
                SET n.name             = $name,
                    n.site_url         = $site_url,
                    n.category         = $category,
                    n.category_type    = $category_type,
                    n.topic_cluster    = $topic_cluster,
                    n.description      = $description,
                    n.location         = $location,
                    n.domain_authority = $domain_authority,
                    n.fb_followers     = $fb_followers,
                    n.tw_followers     = $tw_followers,
                    n.ig_followers     = $ig_followers,
                    n.reach_score      = $reach_score,
                    n.email            = $email,
                    n.fb_url           = $fb_url,
                    n.tw_url           = $tw_url,
                    n.ig_url           = $ig_url,
                    n.confidence_score = $confidence_score,
                    n.source           = $source,
                    n.ingested_at      = $ingested_at
                """,
                id=node.id,
                name=node.name,
                site_url=node.site_url,
                category=node.category,
                category_type=node.category_type,
                topic_cluster=node.topic_cluster,
                description=node.description,
                location=node.location,
                domain_authority=node.domain_authority,
                fb_followers=node.fb_followers,
                tw_followers=node.tw_followers,
                ig_followers=node.ig_followers,
                reach_score=node.reach_score,
                email=node.email,
                fb_url=node.fb_url,
                tw_url=node.tw_url,
                ig_url=node.ig_url,
                confidence_score=node.confidence_score,
                source=node.source,
                ingested_at=node.ingested_at,
            )
            count += 1
    return count


# ── Community nodes ───────────────────────────────────────────────────────────

def load_communities(nodes: List[CommunityNode]) -> int:
    driver = get_neo4j_driver()
    count = 0
    with driver.session() as session:
        for node in nodes:
            session.run(
                """
                MERGE (n:Community {id: $id})
                SET n.name             = $name,
                    n.platform         = $platform,
                    n.url              = $url,
                    n.visibility       = $visibility,
                    n.member_count     = $member_count,
                    n.daily_posts      = $daily_posts,
                    n.category         = $category,
                    n.topic_cluster    = $topic_cluster,
                    n.joined           = $joined,
                    n.confidence_score = $confidence_score,
                    n.source           = $source,
                    n.ingested_at      = $ingested_at
                """,
                id=node.id,
                name=node.name,
                platform=node.platform,
                url=node.url,
                visibility=node.visibility,
                member_count=node.member_count,
                daily_posts=node.daily_posts,
                category=node.category,
                topic_cluster=node.topic_cluster,
                joined=node.joined,
                confidence_score=node.confidence_score,
                source=node.source,
                ingested_at=node.ingested_at,
            )
            count += 1
    return count


# ── Edges ─────────────────────────────────────────────────────────────────────

_REL_CYPHER: Dict[str, str] = {
    "HAS_AUTHOR": """
        MATCH (a {id: $src}), (b {id: $tgt})
        MERGE (a)-[r:HAS_AUTHOR]->(b)
        SET r.weight = $weight
    """,
    "WORKS_AT": """
        MATCH (a:Person {id: $src}), (b:Company {id: $tgt})
        MERGE (a)-[r:WORKS_AT]->(b)
        SET r.weight = $weight
    """,
    "AFFILIATED_WITH": """
        MATCH (a:Person {id: $src}), (b:Company {id: $tgt})
        MERGE (a)-[r:AFFILIATED_WITH]->(b)
        SET r.weight = $weight
    """,
    "MEMBER_OF": """
        MATCH (a {id: $src}), (b:Community {id: $tgt})
        MERGE (a)-[r:MEMBER_OF]->(b)
        SET r.weight = $weight
    """,
    "WARM_CONTACT": """
        MATCH (a {id: $src}), (b:Person {id: $tgt})
        MERGE (a)-[r:WARM_CONTACT]->(b)
        SET r.weight = $weight
    """,
    "IN_COMMUNITY": """
        MATCH (a:Person {id: $src}), (b:Community {id: $tgt})
        MERGE (a)-[r:IN_COMMUNITY]->(b)
        SET r.weight = $weight
    """,
    "IN_SCOBLE_LIST": """
        MATCH (a {id: $src})
        MERGE (a)-[r:IN_SCOBLE_LIST {list_name: $list_name}]->(a)
        SET r.weight = $weight
    """,
}


def load_edges(edges: List[EdgeRecord]) -> int:
    driver = get_neo4j_driver()
    count = 0
    with driver.session() as session:
        for edge in edges:
            cypher = _REL_CYPHER.get(edge.rel_type)
            if not cypher:
                # Generic fallback — skip unknown types
                continue
            try:
                params = {
                    "src": edge.source_id,
                    "tgt": edge.target_id,
                    "weight": edge.weight,
                }
                params.update(edge.properties)
                session.run(cypher, **params)
                count += 1
            except Exception:
                pass
    return count


# ── Qdrant upserts ────────────────────────────────────────────────────────────

def _collection_for_type(node_type: str) -> str:
    if node_type in ("Person", "Publisher"):
        return QDRANT_COLLECTION_PROFILES
    if node_type == "Company":
        return QDRANT_COLLECTION_COMPANIES
    if node_type == "Community":
        return QDRANT_COLLECTION_COMMUNITIES
    return QDRANT_COLLECTION_PROFILES


def _node_id_to_int(node_id: str) -> int:
    """Convert hex node_id to integer for Qdrant point ID."""
    return int(node_id[:16], 16)


def upsert_embeddings(
    nodes: List[Any],
    embeddings: Dict[str, List[float]],
    node_type: str,
    venture_scores: Optional[Dict[str, Dict[str, float]]] = None,
) -> int:
    """
    Upsert vectors into the appropriate Qdrant collection.
    Payload: {node_id, node_type, name, topic_cluster, warmth_score,
              confidence_score, source, venture_scores}
    """
    client = get_qdrant_client()
    collection = _collection_for_type(node_type)
    points: List[PointStruct] = []

    node_map = {n.id: n for n in nodes}

    for node_id, vector in embeddings.items():
        node = node_map.get(node_id)
        if node is None or not vector:
            continue

        payload: Dict[str, Any] = {
            "node_id":        node_id,
            "node_type":      node_type,
            "name":           getattr(node, "name", ""),
            "topic_cluster":  getattr(node, "topic_cluster", None),
            "warmth_score":   getattr(node, "warmth_score", 0.0),
            "confidence_score": getattr(node, "confidence_score", 0.0),
            "source":         getattr(node, "source", []),
            "venture_scores": (venture_scores or {}).get(node_id, {}),
        }

        points.append(PointStruct(
            id=_node_id_to_int(node_id),
            vector=vector,
            payload=payload,
        ))

        if len(points) >= 100:
            client.upsert(collection_name=collection, points=points)
            points = []

    if points:
        client.upsert(collection_name=collection, points=points)

    return len(embeddings)


def upsert_ego_embeddings(venture_embeddings: Dict[str, List[float]]) -> None:
    """Store ego venture embeddings in Qdrant profiles collection."""
    client = get_qdrant_client()
    points = []
    for i, (venture_key, vector) in enumerate(venture_embeddings.items()):
        if vector:
            points.append(PointStruct(
                id=int(f"99999{i}"),
                vector=vector,
                payload={
                    "node_id":     f"ego:ekue:{venture_key}",
                    "node_type":   "Ego",
                    "name":        f"Ekue ({venture_key})",
                    "venture_key": venture_key,
                },
            ))
    if points:
        client.upsert(collection_name=QDRANT_COLLECTION_PROFILES, points=points)
