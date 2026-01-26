#!/usr/bin/env python3
"""
Test script for OpenRouter-based SCJN scraper.

Usage:
    export OPENROUTER_API_KEY='your-key-here'
    PYTHONPATH=src python src/test_openrouter_scraper.py

Get your API key at: https://openrouter.ai/keys
"""
import asyncio
import json
import os
import sys
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


async def test_openrouter_connection():
    """Test basic OpenRouter API connection."""
    from infrastructure.openrouter_client import OpenRouterClient

    print("\n[1] Testing OpenRouter Connection...")
    print("-" * 40)

    try:
        client = OpenRouterClient(model="anthropic/claude-3-haiku")
        print(f"    Model: {client.model}")
        print("    Status: CONFIGURED")
        return True
    except ValueError as e:
        print(f"    ERROR: {e}")
        return False


async def test_scjn_fetch():
    """Test fetching SCJN search page."""
    from infrastructure.async_fetcher import AsyncFetcher

    print("\n[2] Testing SCJN Website Fetch...")
    print("-" * 40)

    fetcher = AsyncFetcher(rate_limit_delay=1.0)
    url = "https://legislacion.scjn.gob.mx/Buscador/Paginas/Buscar.aspx"

    html = await fetcher.fetch(url)
    if html:
        print(f"    URL: {url}")
        print(f"    Status: OK")
        print(f"    Size: {len(html):,} chars")
        return html
    else:
        print(f"    Status: FAILED")
        return None


async def test_llm_extraction(html: str):
    """Test LLM extraction from SCJN HTML."""
    from infrastructure.adapters.scjn_llm_parser import (
        SCJNLLMParser,
        ExtractedSearchResults,
    )
    from infrastructure.openrouter_client import OpenRouterClient

    print("\n[3] Testing LLM Extraction...")
    print("-" * 40)

    try:
        client = OpenRouterClient(model="anthropic/claude-3-haiku")

        # Test extraction
        print(f"    Model: {client.model}")
        print("    Extracting documents...")

        result = await client.extract_from_html(
            html=html,
            schema=ExtractedSearchResults,
            instruction=SCJNLLMParser.EXTRACTION_INSTRUCTION,
            truncate_html=True,
            max_html_chars=80000
        )

        if result:
            print(f"    Documents found: {len(result.documents)}")
            print(f"    Total results: {result.total_results}")
            print(f"    Has next page: {result.has_next_page}")
            return result
        else:
            print("    Status: No result returned")
            return None

    except Exception as e:
        print(f"    ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None


async def test_full_parser():
    """Test the full SCJN parser."""
    from infrastructure.adapters.scjn_llm_parser import SCJNLLMParser

    print("\n[4] Testing Full Parser...")
    print("-" * 40)

    try:
        parser = SCJNLLMParser(
            model="anthropic/claude-3-haiku",
            rate_limit_delay=2.0
        )

        print(f"    Model: {parser.model}")
        print("    Fetching and parsing page 1...")

        documents, has_next = await parser.parse_search_page(page=1)

        print(f"\n    Documents extracted: {len(documents)}")
        print(f"    Has next page: {has_next}")

        if documents:
            print("\n    Sample documents:")
            for i, doc in enumerate(documents[:5], 1):
                print(f"\n    [{i}] {doc.title[:60]}...")
                print(f"        Category: {doc.category.name}")
                print(f"        Status: {doc.status.name}")
                if doc.source_url:
                    print(f"        URL: {doc.source_url[:50]}...")

            if len(documents) > 5:
                print(f"\n    ... and {len(documents) - 5} more")

        return documents

    except Exception as e:
        print(f"    ERROR: {e}")
        import traceback
        traceback.print_exc()
        return None


async def save_results(documents):
    """Save extracted documents to JSON files."""
    print("\n[5] Saving Results...")
    print("-" * 40)

    if not documents:
        print("    No documents to save")
        return

    output_dir = "scjn_data/llm_extracted"
    os.makedirs(output_dir, exist_ok=True)

    for i, doc in enumerate(documents, 1):
        filename = f"{output_dir}/doc_{i:03d}.json"
        data = {
            "title": doc.title,
            "category": doc.category.name,
            "scope": doc.scope.name,
            "status": doc.status.name,
            "publication_date": doc.publication_date.isoformat() if doc.publication_date else None,
            "q_param": doc.q_param,
            "source_url": doc.source_url
        }
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"    Saved {len(documents)} files to {output_dir}/")


async def main():
    print("=" * 60)
    print("  SCJN LLM Scraper Test (via OpenRouter)")
    print("=" * 60)

    # Check API key
    if not os.getenv("OPENROUTER_API_KEY"):
        print("\nERROR: OPENROUTER_API_KEY environment variable not set")
        print("\nTo get an API key:")
        print("  1. Go to https://openrouter.ai/keys")
        print("  2. Create a free account")
        print("  3. Generate an API key")
        print("  4. Run: export OPENROUTER_API_KEY='your-key-here'")
        print("\nThen run this script again.")
        return

    # Test 1: OpenRouter connection
    if not await test_openrouter_connection():
        print("\n[ABORT] OpenRouter connection failed")
        return

    # Test 2: Fetch SCJN page
    html = await test_scjn_fetch()
    if not html:
        print("\n[ABORT] Could not fetch SCJN website")
        return

    # Test 3: LLM extraction
    result = await test_llm_extraction(html)
    if not result:
        print("\n[WARN] LLM extraction failed, trying full parser...")

    # Test 4: Full parser
    documents = await test_full_parser()

    # Test 5: Save results
    if documents:
        await save_results(documents)

    print("\n" + "=" * 60)
    print("  Test Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
