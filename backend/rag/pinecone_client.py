from __future__ import annotations
import logging
import os
import threading
import time
from typing import Optional
logger = logging.getLogger(__name__)
_pinecone_client: object = None
_pinecone_index: object = None
_init_lock = threading.Lock()
from dotenv import load_dotenv
load_dotenv()
PINECONE_API_KEY = os.getenv('PINECONE_API_KEY', '')
PINECONE_INDEX_NAME = os.getenv('PINECONE_INDEX_NAME', 'datapulse-rag')
PINECONE_CLOUD = os.getenv('PINECONE_CLOUD', 'aws')
PINECONE_REGION = os.getenv('PINECONE_REGION', 'us-east-1')
EMBED_MODEL = 'llama-text-embed-v2'
EMBED_DIMS = 1024

def _init_pinecone() -> bool:
    global _pinecone_client, _pinecone_index
    if _pinecone_index is not None:
        return True
    with _init_lock:
        if _pinecone_index is not None:
            return True
        if not PINECONE_API_KEY:
            logger.error('PINECONE_API_KEY not set. RAG features will be disabled. Add PINECONE_API_KEY to your environment variables.')
            return False
        try:
            from pinecone import Pinecone, ServerlessSpec
            pc = Pinecone(api_key=PINECONE_API_KEY)
            _pinecone_client = pc
            existing = [idx.name for idx in pc.list_indexes()]
            if PINECONE_INDEX_NAME not in existing:
                logger.info("Creating Pinecone index '%s'...", PINECONE_INDEX_NAME)
                pc.create_index(name=PINECONE_INDEX_NAME, dimension=EMBED_DIMS, metric='cosine', spec=ServerlessSpec(cloud=PINECONE_CLOUD, region=PINECONE_REGION))
                for _ in range(30):
                    time.sleep(2)
                    try:
                        desc = pc.describe_index(PINECONE_INDEX_NAME)
                        if getattr(getattr(desc, 'status', None), 'ready', False):
                            break
                    except Exception:
                        pass
                logger.info("Pinecone index '%s' ready", PINECONE_INDEX_NAME)
            _pinecone_index = pc.Index(PINECONE_INDEX_NAME)
            logger.info("Pinecone connected: index='%s', embed_model='%s'", PINECONE_INDEX_NAME, EMBED_MODEL)
            return True
        except Exception as exc:
            logger.error('Pinecone initialization failed: %s', exc)
            return False

def embed_texts(texts: list[str]) -> Optional[list[list[float]]]:
    if not texts:
        return []
    if not _init_pinecone():
        return None
    try:
        response = _pinecone_client.inference.embed(model=EMBED_MODEL, inputs=texts, parameters={'input_type': 'passage', 'truncate': 'END'})
        return [item['values'] for item in response]
    except Exception as exc:
        logger.error('Pinecone embed_texts failed: %s', exc)
        return None

def embed_query(text: str) -> Optional[list[float]]:
    if not _init_pinecone():
        return None
    try:
        response = _pinecone_client.inference.embed(model=EMBED_MODEL, inputs=[text], parameters={'input_type': 'query', 'truncate': 'END'})
        return response[0]['values']
    except Exception as exc:
        logger.error('Pinecone embed_query failed: %s', exc)
        return None

def upsert_chunks(namespace: str, chunks: list[dict]) -> int:
    if not chunks:
        return 0
    if not _init_pinecone():
        return 0
    vectors = []
    for chunk in chunks:
        emb = chunk.get('embedding')
        if not emb:
            continue
        text = chunk.get('text', '')[:1000]
        vectors.append({'id': f"{namespace[:8]}_{chunk['chunk_id']}", 'values': emb, 'metadata': {'text': text, 'chunk_type': chunk.get('chunk_type', 'unknown'), 'column': chunk.get('column', 'all'), 'namespace': namespace}})
    if not vectors:
        return 0
    try:
        batch_size = 100
        total = 0
        for i in range(0, len(vectors), batch_size):
            batch = vectors[i:i + batch_size]
            _pinecone_index.upsert(vectors=batch, namespace=namespace)
            total += len(batch)
        logger.info("Pinecone upserted %d vectors to namespace '%s'", total, namespace[:8])
        return total
    except Exception as exc:
        logger.error('Pinecone upsert failed: %s', exc)
        return 0

def query_chunks(namespace: str, query_vector: list[float], k: int=8) -> list[dict]:
    if not _init_pinecone():
        return []
    try:
        result = _pinecone_index.query(vector=query_vector, top_k=k, namespace=namespace, include_metadata=True)
        chunks = []
        for match in result.get('matches') or []:
            meta = match.get('metadata') or {}
            chunks.append({'text': meta.get('text', ''), 'chunk_type': meta.get('chunk_type', 'unknown'), 'column': meta.get('column', 'all'), 'score': round(float(match.get('score', 0)), 4)})
        return chunks
    except Exception as exc:
        logger.error('Pinecone query failed: %s', exc)
        return []

def namespace_exists(namespace: str) -> bool:
    if not _init_pinecone():
        return False
    try:
        stats = _pinecone_index.describe_index_stats()
        ns_stats = stats.get('namespaces') or {}
        count = ns_stats.get(namespace, {}).get('vector_count', 0)
        return count > 0
    except Exception as exc:
        logger.warning('namespace_exists check failed: %s', exc)
        return False

def delete_namespace(namespace: str) -> None:
    if not _init_pinecone():
        return
    try:
        _pinecone_index.delete(delete_all=True, namespace=namespace)
        logger.info("Pinecone namespace '%s' cleared", namespace[:8])
    except Exception as exc:
        logger.warning('Pinecone delete_namespace failed: %s', exc)

def ping() -> bool:
    return _init_pinecone()