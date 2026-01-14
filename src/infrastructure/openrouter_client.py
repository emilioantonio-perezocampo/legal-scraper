"""
OpenRouter API client for LLM-based extraction.
Provides unified access to Claude, GPT-4, Llama, and other models.
"""
import os
import instructor
from openai import AsyncOpenAI
from pydantic import BaseModel
from typing import Type, TypeVar, Optional
import logging

logger = logging.getLogger(__name__)

T = TypeVar('T', bound=BaseModel)


class OpenRouterClient:
    """Async client for OpenRouter API with structured extraction."""

    DEFAULT_MODEL = "x-ai/grok-4.1-fast"  # Fast, cheap, 2M context

    # Cost-effective models for scraping (as of 2025)
    RECOMMENDED_MODELS = {
        "fast": "x-ai/grok-4.1-fast",                 # $0.20/1M input, 2M context
        "balanced": "openai/gpt-4o-mini",             # $0.15/1M input tokens
        "cheap": "deepseek/deepseek-chat-v3-0324",    # Very cheap, good quality
        "quality": "anthropic/claude-3.5-sonnet",     # Better but pricier
    }

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        site_url: str = "https://legal-scraper.local",
        site_name: str = "LegalScraper"
    ):
        """
        Initialize OpenRouter client.

        Args:
            api_key: OpenRouter API key (or set OPENROUTER_API_KEY env var)
            model: Model to use (default: claude-3-haiku)
            site_url: Your site URL for OpenRouter tracking
            site_name: Your app name for OpenRouter tracking
        """
        self.api_key = api_key or os.getenv("OPENROUTER_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OPENROUTER_API_KEY environment variable required. "
                "Get a key at https://openrouter.ai/keys"
            )

        self.model = model or self.DEFAULT_MODEL
        self.site_url = site_url
        self.site_name = site_name

        # Create async OpenAI client pointing to OpenRouter
        self._client = AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=self.api_key,
            default_headers={
                "HTTP-Referer": self.site_url,
                "X-Title": self.site_name,
            }
        )

        # Wrap with instructor for structured extraction
        # Use JSON mode for better compatibility across providers
        self._instructor = instructor.from_openai(
            self._client,
            mode=instructor.Mode.JSON
        )

    async def extract(
        self,
        content: str,
        schema: Type[T],
        instruction: str,
        model: Optional[str] = None,
        max_tokens: int = 4096
    ) -> Optional[T]:
        """
        Extract structured data from content using LLM.

        Args:
            content: The text/HTML content to extract from
            schema: Pydantic model defining the extraction schema
            instruction: Instructions for the LLM on what to extract
            model: Override the default model
            max_tokens: Maximum output tokens

        Returns:
            Extracted data as Pydantic model, or None on failure
        """
        try:
            result = await self._instructor.chat.completions.create(
                model=model or self.model,
                response_model=schema,
                messages=[
                    {
                        "role": "system",
                        "content": instruction
                    },
                    {
                        "role": "user",
                        "content": f"Extract the requested information from this content:\n\n{content}"
                    }
                ],
                max_tokens=max_tokens,
            )
            return result
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            return None

    async def extract_from_html(
        self,
        html: str,
        schema: Type[T],
        instruction: str,
        model: Optional[str] = None,
        truncate_html: bool = True,
        max_html_chars: int = 50000
    ) -> Optional[T]:
        """
        Extract structured data from HTML content.

        Args:
            html: Raw HTML content
            schema: Pydantic model for extraction
            instruction: Extraction instructions
            model: Model override
            truncate_html: Whether to truncate long HTML
            max_html_chars: Maximum HTML characters to process

        Returns:
            Extracted data or None
        """
        # Optionally truncate to reduce token usage
        if truncate_html and len(html) > max_html_chars:
            html = html[:max_html_chars] + "\n... [truncated]"
            logger.info(f"HTML truncated to {max_html_chars} chars")

        return await self.extract(html, schema, instruction, model)

    @classmethod
    def list_models(cls) -> dict:
        """Return recommended models for different use cases."""
        return cls.RECOMMENDED_MODELS

    def switch_model(self, model: str) -> None:
        """Switch to a different model."""
        self.model = model
        logger.info(f"Switched to model: {model}")
