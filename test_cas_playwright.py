#!/usr/bin/env python3
"""Test CAS scraper with Playwright directly."""
import asyncio
import re
import os
import sys

sys.path.insert(0, "/root/legal-scraper")

# Ensure API key is set
if not os.getenv("OPENROUTER_API_KEY"):
    os.environ["OPENROUTER_API_KEY"] = "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"


async def test_cas_with_playwright():
    """Test CAS using Playwright directly with JS interaction."""
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
            ]
        )
        context = await browser.new_context(viewport={"width": 1920, "height": 1080})
        page = await context.new_page()

        print("Navigating to CAS...")
        await page.goto("https://jurisprudence.tas-cas.org", timeout=30000)
        await page.wait_for_load_state("networkidle")
        await page.wait_for_timeout(3000)

        print("\nStep 1: List all filter buttons")
        buttons_info = await page.evaluate("""
            (() => {
                const btns = document.querySelectorAll("button.filter-button");
                return Array.from(btns).map((btn, i) => ({
                    index: i,
                    text: btn.innerText.trim().substring(0, 30)
                }));
            })()
        """)
        print(f"Found {len(buttons_info)} filter buttons:")
        for b in buttons_info:
            print(f"  [{b['index']}] {b['text']}")

        # Find Sport button index
        sport_idx = None
        for b in buttons_info:
            if "Sport" in b["text"]:
                sport_idx = b["index"]
                break

        if sport_idx is None:
            print("Sport button not found!")
            await context.close()
            await browser.close()
            return 0

        print(f"\nStep 2: Click Sport dropdown (index {sport_idx})")
        # First click somewhere else to close any open dropdown
        await page.click("body", position={"x": 10, "y": 10})
        await page.wait_for_timeout(500)

        # Use Playwright's native click on the Sport button
        sport_buttons = page.locator("button.filter-button")
        await sport_buttons.nth(sport_idx).click()
        print("Clicked Sport button using Playwright")
        await page.wait_for_timeout(1500)

        print("\nStep 3: Type 'Football' in search to filter options")
        # Find the search input in the Sport dropdown and type Football
        typed = await page.evaluate(f"""
            (() => {{
                const btns = document.querySelectorAll("button.filter-button");
                const sportBtn = btns[{sport_idx}];
                if (!sportBtn) return "Sport button not found";

                const parent = sportBtn.parentElement;
                const dropdown = parent.querySelector(".dropdown-menu");
                if (!dropdown) return "Dropdown not found";

                const searchInput = dropdown.querySelector("input.input-search, input[type='text']");
                if (!searchInput) return "Search input not found";

                // Focus and set value
                searchInput.focus();
                searchInput.value = "Football";
                // Trigger input event for Angular
                searchInput.dispatchEvent(new Event('input', {{ bubbles: true }}));
                return "Typed Football in search";
            }})()
        """)
        print(typed)
        await page.wait_for_timeout(1000)

        # Now list the filtered options
        filtered_options = await page.evaluate(f"""
            (() => {{
                const btns = document.querySelectorAll("button.filter-button");
                const sportBtn = btns[{sport_idx}];
                const parent = sportBtn.parentElement;
                const dropdown = parent.querySelector(".dropdown-menu");
                const items = dropdown.querySelectorAll(".suggestion-button");
                return Array.from(items).map(i => i.innerText.trim());
            }})()
        """)
        print(f"Filtered options ({len(filtered_options)}): {filtered_options[:10]}")

        print("\nStep 4: Click Football/Soccer option (in Sport dropdown)")
        clicked_text = await page.evaluate(f"""
            (() => {{
                const btns = document.querySelectorAll("button.filter-button");
                const sportBtn = btns[{sport_idx}];
                if (!sportBtn) return "Sport button not found";

                const parent = sportBtn.parentElement;
                const dropdown = parent.querySelector(".dropdown-menu");
                if (!dropdown) return "Dropdown not found";

                const items = dropdown.querySelectorAll(".suggestion-button");
                // Try exact "Football" first, then "Soccer", then any containing football
                for (const item of items) {{
                    const txt = item.innerText.toLowerCase().trim();
                    if (txt === "football" || txt === "soccer") {{
                        item.click();
                        return "Clicked exact: " + item.innerText;
                    }}
                }}
                // Fallback to any containing football (but not American Football)
                for (const item of items) {{
                    const txt = item.innerText.toLowerCase().trim();
                    if (txt.includes("football") && !txt.includes("american") && !txt.includes("australian")) {{
                        item.click();
                        return "Clicked containing: " + item.innerText;
                    }}
                }}
                // Last resort - try Athletics which should have results
                for (const item of items) {{
                    const txt = item.innerText.toLowerCase().trim();
                    if (txt === "athletics") {{
                        item.click();
                        return "Clicked fallback: " + item.innerText;
                    }}
                }}
                return "No suitable sport found in " + items.length + " items";
            }})()
        """)
        print(clicked_text)
        await page.wait_for_timeout(1000)

        print("\nStep 5: Apply filters")
        # Find and click apply filters button
        apply_result = await page.evaluate("""
            (() => {
                const btns = document.querySelectorAll("button");
                for (const btn of btns) {
                    const txt = btn.innerText.toLowerCase();
                    if (txt.includes("apply filter")) {
                        btn.click();
                        return "Clicked: " + btn.innerText.trim();
                    }
                }
                // List available buttons for debugging
                const allBtns = Array.from(btns).map(b => b.innerText.trim().substring(0, 30));
                return "Apply not found. Buttons: " + allBtns.slice(0, 10).join(", ");
            })()
        """)
        print(apply_result)

        print("Waiting for results to load...")
        await page.wait_for_timeout(8000)

        try:
            await page.wait_for_load_state("networkidle", timeout=20000)
            print("Network idle")
        except Exception as e:
            print(f"Timeout waiting for network: {e}")

        # Check current URL
        current_url = page.url
        print(f"Current URL: {current_url}")

        print("\nStep 6: Extract case numbers from table")

        # Extract cases from the results table using JavaScript
        cases = await page.evaluate("""
            (() => {
                // Look for table rows with case data
                const rows = document.querySelectorAll("tr, .result-row, [class*='result']");
                const results = [];

                for (const row of rows) {
                    const cells = row.querySelectorAll("td");
                    if (cells.length >= 4) {
                        // Format: Lang, Year, Proc, Case number, ...
                        const year = cells[1]?.innerText?.trim();
                        const proc = cells[2]?.innerText?.trim();
                        const caseNum = cells[3]?.innerText?.trim();

                        if (year && /^\\d{4}$/.test(year) && proc && /^[A-Z]$/.test(proc) && caseNum) {
                            results.push({
                                caseNumber: `CAS ${year}/${proc}/${caseNum}`,
                                year: year,
                                proc: proc,
                                num: caseNum
                            });
                        }
                    }
                }
                return results;
            })()
        """)

        print(f"Cases extracted from table: {len(cases)}")
        for case in cases[:15]:
            print(f"  {case['caseNumber']}")

        await context.close()
        await browser.close()

        return len(cases)


if __name__ == "__main__":
    count = asyncio.run(test_cas_with_playwright())
    print("\n" + "=" * 50)
    print("CAS PLAYWRIGHT TEST:", "PASS" if count > 0 else "FAIL")
    print("=" * 50)
    sys.exit(0 if count > 0 else 1)
