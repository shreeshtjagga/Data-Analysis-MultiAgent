from __future__ import annotations
import os
import logging
import threading
from groq import Groq
logger = logging.getLogger(__name__)
_groq_client = None
_lock = threading.Lock()

def get_groq_client() -> Groq | None:
    global _groq_client
    if _groq_client is not None:
        return _groq_client
    with _lock:
        if _groq_client is None:
            api_key = os.getenv('GROQ_API_KEY')
            if not api_key:
                logger.warning('GROQ_API_KEY not found in environment. LLM features will be disabled.')
                return None
            try:
                _groq_client = Groq(api_key=api_key)
                logger.info('Shared Groq client initialized.')
            except Exception:
                logger.exception('Failed to initialize Groq client')
                return None
    return _groq_client