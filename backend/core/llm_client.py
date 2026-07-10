from __future__ import annotations
import os
import logging
import threading
from groq import Groq
logger = logging.getLogger(__name__)
_groq_client = None
_groq_client_key = None   # track which key the cached client was built with
_lock = threading.Lock()

def get_groq_client() -> Groq | None:
    global _groq_client, _groq_client_key
    api_key = os.getenv('GROQ_API_KEY')
    if not api_key:
        logger.warning('GROQ_API_KEY not found in environment. LLM features will be disabled.')
        return None
    # Re-create the client if the key has changed (e.g. after a .env edit + hot-reload)
    if _groq_client is not None and _groq_client_key == api_key:
        return _groq_client
    with _lock:
        if _groq_client is None or _groq_client_key != api_key:
            try:
                _groq_client = Groq(api_key=api_key)
                _groq_client_key = api_key
                logger.info('Shared Groq client initialized (key ...%s).', api_key[-6:])
            except Exception:
                logger.exception('Failed to initialize Groq client')
                return None
    return _groq_client