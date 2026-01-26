"""
Crawl4AI adapter for LLM-based web scraping.
Uses LiteLLM for provider-agnostic LLM access (OpenRouter, OpenAI, etc.)
"""
import os
import json
import logging
from typing import Type, TypeVar, Optional

from pydantic import BaseModel

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    LLMConfig,
)
from crawl4ai.extraction_strategy import LLMExtractionStrategy

logger = logging.getLogger(__name__)

T = TypeVar('T', bound=BaseModel)


class Crawl4AIAdapter:
    """
    Unified LLM scraper using Crawl4AI with OpenRouter.

    Features:
    - Provider-agnostic LLM support via LiteLLM
    - Optional JavaScript rendering via Playwright
    - Pydantic schema-based structured extraction

    Usage:
        adapter = Crawl4AIAdapter()
        result = await adapter.extract_structured(
            url="https://example.com",
            schema=MyPydanticModel,
            instruction="Extract all items from the page"
        )
    """

    # Default to OpenRouter with Gemini 3 Flash
    DEFAULT_PROVIDER = "openrouter/google/gemini-3-flash-preview"

    def __init__(
        self,
        provider: Optional[str] = None,
        api_token: Optional[str] = None,
        headless: bool = True,
        timeout_ms: int = 60000,
    ):
        """
        Initialize the Crawl4AI adapter.

        Args:
            provider: LLM provider string (e.g., "openrouter/google/gemini-3-flash-preview")
            api_token: API token for the LLM provider (defaults to OPENROUTER_API_KEY env var)
            headless: Whether to run browser in headless mode
            timeout_ms: Page load timeout in milliseconds
        """
        self.provider = provider or self.DEFAULT_PROVIDER
        self.api_token = api_token or os.getenv("OPENROUTER_API_KEY")
        self.headless = headless
        self.timeout_ms = timeout_ms

        if not self.api_token:
            raise ValueError(
                "API token required. Set OPENROUTER_API_KEY env var or pass api_token parameter."
            )

        # Create LLM configuration
        self.llm_config = LLMConfig(
            provider=self.provider,
            api_token=self.api_token,
        )

        # Browser configuration for JavaScript rendering
        self.browser_config = BrowserConfig(
            headless=self.headless,
            browser_type="chromium",
        )

    async def extract_structured(
        self,
        url: str,
        schema: Type[T],
        instruction: str,
        wait_for_js: bool = False,
        css_selector: Optional[str] = None,
        delay_seconds: float = 0.0,
    ) -> Optional[T]:
        """
        Extract structured data from URL using LLM.

        Args:
            url: The URL to scrape
            schema: Pydantic model defining the extraction schema
            instruction: Instructions for the LLM on what to extract
            wait_for_js: Whether to wait for JavaScript rendering
            css_selector: Optional CSS selector to focus extraction
            delay_seconds: Time to wait for dynamic content to load (for AJAX sites)

        Returns:
            Extracted data as Pydantic model, or None on failure
        """
        try:
            # Create extraction strategy with schema
            extraction_strategy = LLMExtractionStrategy(
                llm_config=self.llm_config,
                schema=schema.model_json_schema(),
                instruction=instruction,
                extraction_type="schema",
            )

            # Create run configuration with optional delay for AJAX content
            run_config = CrawlerRunConfig(
                extraction_strategy=extraction_strategy,
                css_selector=css_selector,
                delay_before_return_html=delay_seconds if delay_seconds > 0 else None,
            )

            # Use browser for JS-heavy sites, otherwise direct HTTP
            if wait_for_js:
                async with AsyncWebCrawler(config=self.browser_config) as crawler:
                    result = await crawler.arun(url=url, config=run_config)
            else:
                # For non-JS sites, still use crawler but with default config
                async with AsyncWebCrawler() as crawler:
                    result = await crawler.arun(url=url, config=run_config)

            if not result.success:
                logger.error(f"Crawl failed for {url}: {result.error_message}")
                return None

            # Parse extracted content
            if result.extracted_content:
                try:
                    # extracted_content is JSON string
                    data = json.loads(result.extracted_content)
                    # If it's a list, take the first item (LLM sometimes returns array)
                    if isinstance(data, list) and len(data) > 0:
                        data = data[0]
                    return schema.model_validate(data)
                except (json.JSONDecodeError, Exception) as e:
                    logger.error(f"Failed to parse extracted content: {e}")
                    logger.debug(f"Raw content: {result.extracted_content[:500]}")
                    return None
            else:
                logger.warning(f"No content extracted from {url}")
                return None

        except Exception as e:
            logger.error(f"Extraction failed for {url}: {e}")
            return None

    async def extract_markdown(
        self,
        url: str,
        wait_for_js: bool = False,
        css_selector: Optional[str] = None,
    ) -> Optional[str]:
        """
        Extract page content as clean markdown (without LLM).

        Args:
            url: The URL to scrape
            wait_for_js: Whether to wait for JavaScript rendering
            css_selector: Optional CSS selector to focus extraction

        Returns:
            Clean markdown content, or None on failure
        """
        try:
            run_config = CrawlerRunConfig(
                css_selector=css_selector,
            )

            if wait_for_js:
                async with AsyncWebCrawler(config=self.browser_config) as crawler:
                    result = await crawler.arun(url=url, config=run_config)
            else:
                async with AsyncWebCrawler() as crawler:
                    result = await crawler.arun(url=url, config=run_config)

            if result.success and result.markdown:
                return result.markdown
            else:
                logger.warning(f"Failed to extract markdown from {url}")
                return None

        except Exception as e:
            logger.error(f"Markdown extraction failed for {url}: {e}")
            return None

    async def extract_html(
        self,
        url: str,
        wait_for_js: bool = False,
    ) -> Optional[str]:
        """
        Extract raw HTML content (with optional JS rendering).

        Args:
            url: The URL to scrape
            wait_for_js: Whether to wait for JavaScript rendering

        Returns:
            Raw HTML content, or None on failure
        """
        try:
            run_config = CrawlerRunConfig()

            if wait_for_js:
                async with AsyncWebCrawler(config=self.browser_config) as crawler:
                    result = await crawler.arun(url=url, config=run_config)
            else:
                async with AsyncWebCrawler() as crawler:
                    result = await crawler.arun(url=url, config=run_config)

            if result.success and result.html:
                return result.html
            else:
                logger.warning(f"Failed to extract HTML from {url}")
                return None

        except Exception as e:
            logger.error(f"HTML extraction failed for {url}: {e}")
            return None
