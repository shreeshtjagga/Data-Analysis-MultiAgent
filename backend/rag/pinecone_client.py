"""
DataPulse Pinecone Client
=========================
Singleton Pinecone client for RAG operations.

Embeddings: Pinecone Inference API (multilingual-e5-large, 1024-dim)
  - Runs on Pinecone's servers → zero RAM on Render free tier
  - ~100ms per embed call (network, not CPU)
  - Free tier: 1M inference units/month (more than enough)

Index: Single serverless index "datapulse-rag"
Namespace: file_hash (isolates each user's dataset)

Operations:
  embed_texts(texts)              → list[list[float]]
  embed_query(text)               → list[float]
  upsert_chunks(namespace, chunks)→ int (count upserted)
  query_chunks(namespace, vec, k) → list[dict]
  namespace_exists(namespace)     → bool
  delete_namespace(namespace)     → None
  ping()                          → bool
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

_pinecone_client: object = None
_pinecone_index:  object = None
_init_lock = threading.Lock()

from dotenv import load_dotenv
load_dotenv()

PINECONE_API_KEY    = os.getenv("PINECONE_API_KEY", "")
PINECONE_INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "datapulse-rag")
PINECONE_CLOUD      = os.getenv("PINECONE_CLOUD", "aws")
PINECONE_REGION     = os.getenv("PINECONE_REGION", "us-east-1")
EMBED_MODEL         = "llama-text-embed-v2"
EMBED_DIMS          = 1024


def _init_pinecone() -> bool:
    """Initialize Pinecone client and ensure index exists. Thread-safe singleton."""
    global _pinecone_client, _pinecone_index

    if _pinecone_index is not None:
        return True

    with _init_lock:
        if _pinecone_index is not None:
            return True

        if not PINECONE_API_KEY:
            logger.error(
                "PINECONE_API_KEY not set. RAG features will be disabled. "
                "Add PINECONE_API_KEY to your environment variables."
            )
            return False

        try:
            from pinecone import Pinecone, ServerlessSpec

            pc = Pinecone(api_key=PINECONE_API_KEY)
            _pinecone_client = pc

            # Create index if it doesn't exist
            existing = [idx.name for idx in pc.list_indexes()]
            if PINECONE_INDEX_NAME not in existing:
                logger.info("Creating Pinecone index '%s'...", PINECONE_INDEX_NAME)
                pc.create_index(
                    name=PINECONE_INDEX_NAME,
                    dimension=EMBED_DIMS,
                    metric="cosine",
                    spec=ServerlessSpec(
                        cloud=PINECONE_CLOUD,
                        region=PINECONE_REGION,
                    ),
                )
                # Wait for index to become ready
                for _ in range(30):
                    time.sleep(2)
                    try:
                        desc = pc.describe_index(PINECONE_INDEX_NAME)
                        if getattr(getattr(desc, "status", None), "ready", False):
                            break
                    except Exception:
                        pass
                logger.info("Pinecone index '%s' ready", PINECONE_INDEX_NAME)

            _pinecone_index = pc.Index(PINECONE_INDEX_NAME)
            logger.info(
                "Pinecone connected: index='%s', embed_model='%s'",
                PINECONE_INDEX_NAME,
                EMBED_MODEL,
            )
            return True

        except Exception as exc:
            logger.error("Pinecone initialization failed: %s", exc)
            return False


def embed_texts(texts: list[str]) -> Optional[list[list[float]]]:
    """
    Embed a list of strings using Pinecone Inference API.
    Runs on Pinecone's servers — zero RAM/CPU cost on Render.
    Returns list of 1024-dim vectors, or None on failure.
    """
    if not texts:
        return []

    if not _init_pinecone():
        return None

    try:
        response = _pinecone_client.inference.embed(  # type: ignore[union-attr]
            model=EMBED_MODEL,
            inputs=texts,
            parameters={"input_type": "passage", "truncate": "END"},
        )
        return [item["values"] for item in response]
    except Exception as exc:
        logger.error("Pinecone embed_texts failed: %s", exc)
        return None


def embed_query(text: str) -> Optional[list[float]]:
    """
    Embed a single query string.
    Uses input_type="query" for asymmetric retrieval (better accuracy).
    Returns 1024-dim vector or None on failure.
    """
    if not _init_pinecone():
        return None

    try:
        response = _pinecone_client.inference.embed(  # type: ignore[union-attr]
            model=EMBED_MODEL,
            inputs=[text],
            parameters={"input_type": "query", "truncate": "END"},
        )
        return response[0]["values"]
    except Exception as exc:
        logger.error("Pinecone embed_query failed: %s", exc)
        return None


def upsert_chunks(namespace: str, chunks: list[dict]) -> int:
    """
    Upsert chunks into Pinecone under the given namespace.

    Each chunk must have:
      chunk_id:   str   (unique ID)
      text:       str   (the factual text)
      embedding:  list  (1024-dim vector)
      chunk_type: str
      column:     str

    Returns count of chunks upserted, 0 on failure.
    Pinecone metadata stores text + chunk_type + column for retrieval.
    Text is truncated to 1000 chars to stay under 40KB metadata limit.
    """
    if not chunks:
        return 0
    if not _init_pinecone():
        return 0

    vectors = []
    for chunk in chunks:
        emb = chunk.get("embedding")
        if not emb:
            continue
        text = chunk.get("text", "")[:1000]
        vectors.append({
            "id": f"{namespace[:8]}_{chunk['chunk_id']}",
            "values": emb,
            "metadata": {
                "text":       text,
                "chunk_type": chunk.get("chunk_type", "unknown"),
                "column":     chunk.get("column", "all"),
                "namespace":  namespace,
            },
        })

    if not vectors:
        return 0

    try:
        batch_size = 100
        total = 0
        for i in range(0, len(vectors), batch_size):
            batch = vectors[i : i + batch_size]
            _pinecone_index.upsert(vectors=batch, namespace=namespace)  # type: ignore[union-attr]
            total += len(batch)
        logger.info(
            "Pinecone upserted %d vectors to namespace '%s'", total, namespace[:8]
        )
        return total
    except Exception as exc:
        logger.error("Pinecone upsert failed: %s", exc)
        return 0


def query_chunks(
    namespace: str,
    query_vector: list[float],
    k: int = 8,
) -> list[dict]:
    """
    Find top-k most relevant chunks for a query vector.
    Returns list of dicts: {text, chunk_type, column, score}
    Sorted by relevance score descending.
    """
    if not _init_pinecone():
        return []

    try:
        result = _pinecone_index.query(  # type: ignore[union-attr]
            vector=query_vector,
            top_k=k,
            namespace=namespace,
            include_metadata=True,
        )
        chunks = []
        for match in (result.get("matches") or []):
            meta = match.get("metadata") or {}
            chunks.append({
                "text":       meta.get("text", ""),
                "chunk_type": meta.get("chunk_type", "unknown"),
                "column":     meta.get("column", "all"),
                "score":      round(float(match.get("score", 0)), 4),
            })
        return chunks
    except Exception as exc:
        logger.error("Pinecone query failed: %s", exc)
        return []


def namespace_exists(namespace: str) -> bool:
    """
    Check if this namespace already has vectors.
    Used to decide whether to re-index or skip.
    """
    if not _init_pinecone():
        return False
    try:
        stats = _pinecone_index.describe_index_stats()  # type: ignore[union-attr]
        ns_stats = (stats.get("namespaces") or {})
        count = ns_stats.get(namespace, {}).get("vector_count", 0)
        return count > 0
    except Exception as exc:
        logger.warning("namespace_exists check failed: %s", exc)
        return False


def delete_namespace(namespace: str) -> None:
    """Delete all vectors in this namespace (e.g. when re-analysing same file)."""
    if not _init_pinecone():
        return
    try:
        _pinecone_index.delete(delete_all=True, namespace=namespace)  # type: ignore[union-attr]
        logger.info("Pinecone namespace '%s' cleared", namespace[:8])
    except Exception as exc:
        logger.warning("Pinecone delete_namespace failed: %s", exc)


def ping() -> bool:
    """Health check — returns True if Pinecone is reachable."""
    return _init_pinecone()
