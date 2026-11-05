"""Async LLM transport; HTTP API compatible with OpenAI chat completions."""
from __future__ import annotations
from dataclasses import dataclass
import os
import httpx

class LLMError(RuntimeError):
    pass

@dataclass(frozen=True)
class LLMConfig:
    model: str
    provider: str = 'openai_compatible'
    base_url: str = 'http://localhost:11434/v1'
    api_key_env: str | None = None
    temperature: float = 0.1
    timeout_seconds: float = 120

    def __post_init__(self):
        if self.provider not in ('openai', 'openai_compatible'):
            raise ValueError('Unsupported provider; use openai or openai_compatible')
        if not self.model:
            raise ValueError('Model required')
        if not 0 <= self.temperature <= 2:
            raise ValueError('Temperature must be between 0 and 2')

class LLMClient:
    def __init__(self, config: LLMConfig):
        self.config = config

    async def complete(self, messages: list[dict[str, str]]) -> str:
        c = self.config
        key_env = c.api_key_env or ('OPENAI_API_KEY' if c.provider == 'openai' else None)
        key = os.getenv(key_env) if key_env else None
        if c.provider == 'openai' and not key:
            raise LLMError(f'Missing API key in {key_env}')
        headers = {'Authorization': f'Bearer {key}'} if key else {}
        url = ('https://api.openai.com/v1' if c.provider == 'openai' else c.base_url).rstrip('/') + '/chat/completions'
        payload = {'model': c.model, 'messages': messages, 'temperature': c.temperature,
                   'response_format': {'type': 'json_object'}}
        try:
            async with httpx.AsyncClient(timeout=c.timeout_seconds) as client:
                response = await client.post(url, json=payload, headers=headers)
                response.raise_for_status()
                return response.json()['choices'][0]['message']['content']
        except (httpx.HTTPError, KeyError, IndexError, TypeError, ValueError) as exc:
            raise LLMError(f'LLM completion failed: {exc}') from exc
