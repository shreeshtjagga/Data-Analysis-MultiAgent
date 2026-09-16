
from __future__ import annotations

import os

import logging

import threading

import asyncio

import time

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

                _groq_client = Groq(api_key=api_key, max_retries=0)

                logger.info('Shared Groq client initialized with max_retries=0.')

            except Exception:

                logger.exception('Failed to initialize Groq client')

                return None

    return _groq_client

def _get_fallback_chain(primary_model: str) -> list[tuple[str, str]]:
    """Build a robust Groq-only fallback chain with valid models."""
    default_model = os.getenv('GROQ_MODEL', 'qwen/qwen3.8-27b')
    model = primary_model or default_model
    chain = [('groq', model)]
    candidates = [
        os.getenv('GROQ_FALLBACK_MODEL', 'openai/gpt-oss-120b'),
        'openai/gpt-oss-120b',
        'openai/gpt-oss-20b',
        'qwen/qwen3.8-27b',
    ]
    for c in candidates:
        if c and c != model and ('groq', c) not in chain:
            chain.append(('groq', c))
    return chain

async def call_groq_with_fallback(
    messages: list[dict],
    primary_model: str,
    temperature: float = 0.1,
    max_tokens: int = 700,
) -> str:
    chain = _get_fallback_chain(primary_model)
    last_exc = None
    for attempt, (provider, model) in enumerate(chain):
        try:
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
        except Exception as exc:
            last_exc = exc
            if attempt < len(chain) - 1:
                next_provider, next_model = chain[attempt + 1]
                logger.warning(
                    "Error on groq (%s). Failing over to groq (%s). Error: %s",
                    model, next_model, exc
                )
                continue
            else:
                logger.warning("Final Groq model failed: %s", exc)
                break
    raise last_exc or RuntimeError("All Groq models exhausted")

def call_groq_with_fallback_sync(

    messages: list[dict],

    primary_model: str,

    temperature: float = 0.1,

    max_tokens: int = 700,

) -> str:

    chain = _get_fallback_chain(primary_model)

    last_exc = None

    for attempt, (provider, model) in enumerate(chain):

        try:

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

                        "Rate limit hit on groq (%s). Instantly failing over to groq (%s).",

                        model, next_model

                    )

                    continue

                else:

                    logger.warning(

                        "Rate limit hit on final Groq model (%s). Retrying in 1.5s...",

                        model

                    )

                    time.sleep(1.5)

                    try:

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

                    except Exception as retry_exc:

                        last_exc = retry_exc

                        break

            else:

                if attempt < len(chain) - 1:

                    next_provider, next_model = chain[attempt + 1]

                    logger.warning(

                        "Error on groq (%s). Failing over to groq (%s). Error: %s",

                        model, next_model, exc

                    )

                    last_exc = exc

                    continue

                else:

                    last_exc = exc

                    break

    raise last_exc or RuntimeError("All Groq models exhausted")
