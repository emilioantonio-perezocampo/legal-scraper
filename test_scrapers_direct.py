#!/usr/bin/env python3
"""
Direct test of scraping logic without the API layer.
Tests each scraper's core functionality.
"""
import asyncio
import os
import sys
from datetime import date

# Add src to path
sys.path.insert(0, "/root/legal-scraper")

# Set environment
os.environ["OPENROUTER_API_KEY"] = os.getenv("OPENROUTER_API_KEY", "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")


async def test_dof_discovery():
    """Test DOF discovery for a date with known publications."""
    print("\n" + "=" * 60)
    print("TEST: DOF Discovery (Jan 23, 2026)")
    print("=" * 60)

    import aiohttp
    from src.infrastructure.adapters.dof_index_parser import parse_dof_index

    # Test date with known publications
    test_date = date(2026, 1, 23)
    url = f"https://dof.gob.mx/index.php?year={test_date.year}&month={test_date.month:02d}&day={test_date.day:02d}"

    print(f"Fetching: {url}")

    async with aiohttp.ClientSession() as session:
        async with session.get(url, ssl=False) as response:
            if response.status == 200:
                html = await response.text(encoding='utf-8')
                print(f"HTML length: {len(html)} characters")

                items = parse_dof_index(html)
                print(f"Documents found: {len(items)}")

                if items:
                    print("\nFirst 3 documents:")
                    for i, item in enumerate(items[:3]):
                        print(f"  {i+1}. {item.get('title', 'No title')[:80]}...")
                        print(f"     URL: {item.get('url', 'No URL')}")
                    return True
                else:
                    print("ERROR: No documents found on a date that should have publications!")
                    return False
            else:
                print(f"ERROR: HTTP {response.status}")
                return False


async def test_scjn_llm_parser():
    """Test SCJN LLM parser directly."""
    print("\n" + "=" * 60)
    print("TEST: SCJN LLM Parser")
    print("=" * 60)

    try:
        from src.infrastructure.adapters.scjn_llm_parser import SCJNLLMParser

        parser = SCJNLLMParser()
        print(f"Using model: {parser.model}")
        print("Parsing SCJN search page (page 1)...")

        documents, has_next = await parser.parse_search_page(page=1)

        print(f"Documents found: {len(documents)}")
        print(f"Has next page: {has_next}")

        if documents:
            print("\nFirst 3 documents:")
            for i, doc in enumerate(documents[:3]):
                print(f"  {i+1}. {doc.title[:80]}...")
                print(f"     Category: {doc.category}, Scope: {doc.scope}")
                print(f"     Q-Param: {doc.q_param or 'None'}")
            return True
        else:
            print("WARNING: No documents found via LLM parser")
            return False

    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        return False


async def test_scjn_http_fetch():
    """Test fetching SCJN page via HTTP to see what we get."""
    print("\n" + "=" * 60)
    print("TEST: SCJN HTTP Fetch (Raw HTML)")
    print("=" * 60)

    import aiohttp

    # Try the reforms page which shows actual results
    url = "https://legislacion.scjn.gob.mx/Buscador/Paginas/wfReformasResultados.aspx?q=78tXSP4D9DqLbEkEfwPY3A=="

    print(f"Fetching: {url}")

    async with aiohttp.ClientSession() as session:
        async with session.get(url, ssl=False) as response:
            if response.status == 200:
                html = await response.text()
                print(f"HTML length: {len(html)} characters")

                # Check for key elements
                has_grid = 'gridResultados' in html
                has_table = 'dxgvTable' in html
                has_rows = 'dxgvDataRow' in html

                print(f"Has gridResultados: {has_grid}")
                print(f"Has dxgvTable: {has_table}")
                print(f"Has dxgvDataRow: {has_rows}")

                # Count links with q parameter
                import re
                q_links = re.findall(r'q=([A-Za-z0-9+/=]+)', html)
                print(f"Q-parameter links found: {len(q_links)}")

                if q_links:
                    print(f"Sample q-params: {q_links[:3]}")
                    return True
                else:
                    print("WARNING: No q-parameter links found")
                    return False
            elif response.status == 302:
                location = response.headers.get('Location', '')
                print(f"REDIRECT to: {location}")
                return False
            else:
                print(f"ERROR: HTTP {response.status}")
                return False


async def test_cas_browser():
    """Test CAS browser adapter."""
    print("\n" + "=" * 60)
    print("TEST: CAS Browser Adapter (Playwright)")
    print("=" * 60)

    try:
        from src.infrastructure.adapters.cas_browser_adapter import CASBrowserAdapter, BrowserConfig

        config = BrowserConfig(headless=True, timeout_ms=30000)
        adapter = CASBrowserAdapter(config)

        print("Starting browser...")
        await adapter.start()

        print("Fetching CAS jurisprudence page...")
        result = await adapter.render_page(
            "https://jurisprudence.tas-cas.org/",
            wait_for_network_idle=True,
        )

        print(f"Page title: {result.title}")
        print(f"HTML length: {len(result.html)} characters")
        print(f"Load time: {result.load_time_ms}ms")

        # Check for case listings
        has_cases = 'CAS' in result.html and ('2024' in result.html or '2025' in result.html)
        print(f"Has CAS cases: {has_cases}")

        await adapter.stop()
        return has_cases

    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        return False


async def main():
    """Run all tests."""
    print("=" * 60)
    print("DIRECT SCRAPER TESTS")
    print("=" * 60)

    results = {}

    # Test DOF
    results["DOF Discovery"] = await test_dof_discovery()

    # Test SCJN HTTP
    results["SCJN HTTP Fetch"] = await test_scjn_http_fetch()

    # Test SCJN LLM
    results["SCJN LLM Parser"] = await test_scjn_llm_parser()

    # Test CAS Browser
    results["CAS Browser"] = await test_cas_browser()

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    for test, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {test}: {status}")

    all_passed = all(results.values())
    print(f"\nOverall: {'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")
    return all_passed


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
