#!/usr/bin/env python3
"""
Test LLM-based scraping directly - no browser automation.
Uses OpenRouter + instructor for semantic extraction.
"""
import asyncio
import os
import sys

sys.path.insert(0, "/root/legal-scraper")

# Ensure API key is set
os.environ.setdefault("OPENROUTER_API_KEY", "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")


async def test_scjn_llm():
    """Test SCJN LLM parser."""
    print("\n=== SCJN LLM Parser Test ===")

    from src.infrastructure.adapters.scjn_llm_parser import SCJNLLMParser

    parser = SCJNLLMParser()
    print(f"Model: {parser.model}")

    # Parse first page
    print("Fetching and extracting SCJN documents...")
    documents, has_next = await parser.parse_search_page(page=1)

    print(f"\nResults:")
    print(f"  Documents found: {len(documents)}")
    print(f"  Has more pages: {has_next}")

    if documents:
        print(f"\nSample documents:")
        for i, doc in enumerate(documents[:5]):
            print(f"  {i+1}. {doc.title[:70]}...")
            print(f"     Category: {doc.category.value}, Q-Param: {doc.q_param or 'None'}")

    return len(documents) > 0


async def test_dof_http():
    """Test DOF via simple HTTP - no LLM needed, just HTML parsing."""
    print("\n=== DOF HTTP Parser Test ===")

    import aiohttp
    from src.infrastructure.adapters.dof_index_parser import parse_dof_index

    # Jan 23, 2026 has publications
    url = "https://dof.gob.mx/index.php?year=2026&month=01&day=23"
    print(f"Fetching: {url}")

    async with aiohttp.ClientSession() as session:
        async with session.get(url, ssl=False) as response:
            html = await response.text(encoding='utf-8')
            items = parse_dof_index(html)

            print(f"\nResults:")
            print(f"  Documents found: {len(items)}")

            if items:
                print(f"\nSample documents:")
                for i, item in enumerate(items[:5]):
                    print(f"  {i+1}. {item.get('title', 'No title')[:70]}...")

            return len(items) > 0


async def main():
    print("=" * 50)
    print("LLM-BASED SCRAPER TESTS")
    print("=" * 50)

    results = {}

    # Test DOF (simple HTTP parsing)
    try:
        results["DOF"] = await test_dof_http()
    except Exception as e:
        print(f"DOF Error: {e}")
        results["DOF"] = False

    # Test SCJN (LLM extraction)
    try:
        results["SCJN"] = await test_scjn_llm()
    except Exception as e:
        print(f"SCJN Error: {e}")
        results["SCJN"] = False

    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    for name, passed in results.items():
        print(f"  {name}: {'PASS' if passed else 'FAIL'}")


if __name__ == "__main__":
    asyncio.run(main())
