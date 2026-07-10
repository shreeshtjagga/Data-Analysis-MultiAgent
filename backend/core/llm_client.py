from __future__ import annotations
import os
import logging
import threading
import asyncio
import time
import httpx
from groq import Groq, RateLimitError

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
                # Set max_retries=0 to disable the SDK's built-in blocking retries
                _groq_client = Groq(api_key=api_key, max_retries=0)
                logger.info('Shared Groq client initialized with max_retries=0.')
            except Exception:
                logger.exception('Failed to initialize Groq client')
                return None
    return _groq_client

def _get_fallback_chain(primary_model: str) -> list[tuple[str, str]]:
    chain = [('groq', primary_model)]
    
    cerebras_key = os.getenv('CEREBRAS_API_KEY')
    has_cerebras = bool(cerebras_key and cerebras_key.strip())
    
    # Check if primary is 70B
    is_70b = '70b' in primary_model
    
    if is_70b:
        if has_cerebras:
            chain.append(('cerebras', 'llama-3.3-70b'))
            chain.append(('cerebras', 'gpt-oss-120b'))
            chain.append(('cerebras', 'gemma-4-31b'))
        
        fallback_model = os.getenv('GROQ_FALLBACK_MODEL', 'llama-3.1-8b-instant')
        chain.append(('groq', fallback_model))
        
        if has_cerebras:
            chain.append(('cerebras', 'llama3.1-8b'))
            chain.append(('cerebras', 'gemma-4-31b'))
            chain.append(('cerebras', 'gpt-oss-120b'))
    else:
        if has_cerebras:
            chain.append(('cerebras', 'llama3.1-8b'))
            chain.append(('cerebras', 'gemma-4-31b'))
            chain.append(('cerebras', 'gpt-oss-120b'))
            
    return chain

async def _call_cerebras_async(messages: list[dict], model: str, temperature: float, max_tokens: int) -> str:
    api_key = os.getenv('CEREBRAS_API_KEY', '').strip()
    if not api_key:
        raise ValueError("CEREBRAS_API_KEY is not set")
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post("https://api.cerebras.ai/v1/chat/completions", json=payload, headers=headers)
        resp.raise_for_status()
        res_data = resp.json()
        return (res_data["choices"][0]["message"]["content"] or "").strip()

def _call_cerebras_sync(messages: list[dict], model: str, temperature: float, max_tokens: int) -> str:
    api_key = os.getenv('CEREBRAS_API_KEY', '').strip()
    if not api_key:
        raise ValueError("CEREBRAS_API_KEY is not set")
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens
    }
    
    with httpx.Client(timeout=30.0) as client:
        resp = client.post("https://api.cerebras.ai/v1/chat/completions", json=payload, headers=headers)
        resp.raise_for_status()
        res_data = resp.json()
        return (res_data["choices"][0]["message"]["content"] or "").strip()

async def call_groq_with_fallback(
    messages: list[dict],
    primary_model: str,
    temperature: float = 0.1,
    max_tokens: int = 700,
) -> str:
    """Asynchronously calls Groq with failover through the provider-model chain (including Cerebras)."""
    chain = _get_fallback_chain(primary_model)
    last_exc = None
    
    for attempt, (provider, model) in enumerate(chain):
        try:
            if provider == 'groq':
                client = get_groq_client()
                if not client:
                    raise RuntimeError("Groq client not initialized")
                completion = await asyncio.to_thread(
                    client.chat.completions.create,
                    model=model,
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
                return (completion.choices[0].message.content or '').strip()
            elif provider == 'cerebras':
                logger.info("Attempting Cerebras fallback with model %s", model)
                return await _call_cerebras_async(messages, model, temperature, max_tokens)
        except Exception as exc:
            exc_str = str(exc).lower()
            is_rate_limit = (
                isinstance(exc, RateLimitError) or
                '429' in exc_str or
                'rate_limit' in exc_str or
                'rate limit' in exc_str or
                'too many' in exc_str
            )
            if is_rate_limit:
                if attempt < len(chain) - 1:
                    next_provider, next_model = chain[attempt + 1]
                    logger.warning(
                        "Rate limit hit on %s (%s). Instantly failing over to %s (%s).",
                        provider, model, next_provider, next_model
                    )
                    continue
                else:
                    logger.warning(
                        "Rate limit hit on final option %s (%s). Retrying in 1.5s...",
                        provider, model
                    )
                    await asyncio.sleep(1.5)
                    try:
                        if provider == 'groq':
                            client = get_groq_client()
                            if not client:
                                raise RuntimeError("Groq client not initialized")
                            completion = await asyncio.to_thread(
                                client.chat.completions.create,
                                model=model,
                                messages=messages,
                                temperature=temperature,
                                max_tokens=max_tokens,
                            )
                            return (completion.choices[0].message.content or '').strip()
                        elif provider == 'cerebras':
                            return await _call_cerebras_async(messages, model, temperature, max_tokens)
                    except Exception as retry_exc:
                        last_exc = retry_exc
                        break
            else:
                if attempt < len(chain) - 1:
                    next_provider, next_model = chain[attempt + 1]
                    logger.warning(
                        "Error on %s (%s). Failing over to %s (%s). Error: %s",
                        provider, model, next_provider, next_model, exc
                    )
                    last_exc = exc
                    continue
                else:
                    last_exc = exc
                    break
                    
    raise last_exc or RuntimeError("All LLM providers and models exhausted")

def call_groq_with_fallback_sync(
    messages: list[dict],
    primary_model: str,
    temperature: float = 0.1,
    max_tokens: int = 700,
) -> str:
    """Synchronously calls Groq with failover through the provider-model chain (including Cerebras)."""
    chain = _get_fallback_chain(primary_model)
    last_exc = None
    
    for attempt, (provider, model) in enumerate(chain):
        try:
            if provider == 'groq':
                client = get_groq_client()
                if not client:
                    raise RuntimeError("Groq client not initialized")
                completion = client.chat.completions.create(
                    model=model,
                    messages=messages,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )
                return (completion.choices[0].message.content or '').strip()
            elif provider == 'cerebras':
                logger.info("Attempting Cerebras fallback with model %s", model)
                return _call_cerebras_sync(messages, model, temperature, max_tokens)
        except Exception as exc:
            exc_str = str(exc).lower()
            is_rate_limit = (
                isinstance(exc, RateLimitError) or
                '429' in exc_str or
                'rate_limit' in exc_str or
                'rate limit' in exc_str or
                'too many' in exc_str
            )
            if is_rate_limit:
                if attempt < len(chain) - 1:
                    next_provider, next_model = chain[attempt + 1]
                    logger.warning(
                        "Rate limit hit on %s (%s). Instantly failing over to %s (%s).",
                        provider, model, next_provider, next_model
                    )
                    continue
                else:
                    logger.warning(
                        "Rate limit hit on final option %s (%s). Retrying in 1.5s...",
                        provider, model
                    )
                    time.sleep(1.5)
                    try:
                        if provider == 'groq':
                            client = get_groq_client()
                            if not client:
                                raise RuntimeError("Groq client not initialized")
                            completion = client.chat.completions.create(
                                model=model,
                                messages=messages,
                                temperature=temperature,
                                max_tokens=max_tokens,
                            )
                            return (completion.choices[0].message.content or '').strip()
                        elif provider == 'cerebras':
                            return _call_cerebras_sync(messages, model, temperature, max_tokens)
                    except Exception as retry_exc:
                        last_exc = retry_exc
                        break
            else:
                if attempt < len(chain) - 1:
                    next_provider, next_model = chain[attempt + 1]
                    logger.warning(
                        "Error on %s (%s). Failing over to %s (%s). Error: %s",
                        provider, model, next_provider, next_model, exc
                    )
                    last_exc = exc
                    continue
                else:
                    last_exc = exc
                    break
                    
    raise last_exc or RuntimeError("All LLM providers and models exhausted")