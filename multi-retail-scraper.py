"""
Multi-retailer PDP scraper (price, sku, specs_html)

- Uses Playwright (sync API)
- Handles:
  - Hidden specs in tabs/accordions
  - "Show more" style buttons
  - Lazy-loaded content via scroll
- Routes multiple URLs from different retailers through a single script.
"""

import csv
import random
import time
from pathlib import Path
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError


# ---------- CONFIG: put your PDP URLs here ----------

PDP_URLS = [
    "https://www.screwfix.com/p/calex-smart-b22-a60-rgb-white-led-light-bulb-9-4w-806lm/647py",
    "https://www.diy.com/departments/glend-stainless-steel-effect-solar-powered-blue-led-decking-light-pack-of-2/5059340323145_BQ.prd",
    "https://www.wickes.co.uk/Saxby-IP44-Derwent-PIR-Wall-Light---Anthracite-Grey/p/308869",
    "https://www.toolstation.com/kudox-premium-type-22-steel-panel-radiator/p80524",
    "https://www.travisperkins.co.uk/smart-electrical/hive-active-plug-white/p/729587",
    "https://www.plumbingsuperstore.co.uk/product/baxi-platinum-compact-combi-boiler-with-magnaclean-micro-2-filter.html",
    "https://www.cityplumbing.co.uk/p/baxi-800-combi-2-30kw-boiler-with-horizontal-flue-and-adey-micro2-system-filter-with-free-proforce-wireless-thermostat-and-adey-chemicals-baxi800combi2mcp2/p/840027",
    "https://www.bes.co.uk/primary-pro-ashp-insulation-28-x-19mm-x-1m-26208/",
    "https://www.uk-plumbing.com/product/15mm-auto-air-vent/",
    "https://www.trade-point.co.uk/departments/mira-atom-erd-chrome-effect-rear-fed-thermostatic-mixer-shower-with-2-heads-1-25m-hose-length/5013181103571_TP.prd",
    "https://candgbathrooms.com/product/avanti-rimless-wall-hung-wc-soft-close-seat-ivory/",
    "https://www.homebase.co.uk/en-uk/cosylyte-winston-3-light-ceiling-light-with-faux-linen-outer-shade/p/0763227",
    "https://www.argos.co.uk/product/1137161?clickPR=plp:1:67",
    "https://www.wilko.com/en-uk/homcom-ava-stove-flame-effect-fireplace-heater-1800w/p/0544978",
]

OUTPUT_CSV = "pdp_scrape_output.csv"


# ---------- UTILITIES ----------

def human_delay(min_s=1.0, max_s=3.0):
    """Random sleep to reduce bot-likeness (still use proxies for serious scraping)."""
    time.sleep(random.uniform(min_s, max_s))


# ---------- GENERIC PAGE INTERACTION HELPERS ----------

def expand_spec_like_sections(page):
    """
    Try to reveal hidden specification/technical/detail sections: tabs, accordions, show-more buttons.
    This is intentionally generic so it works across many retailers.
    """

    # 1) Try role=tab (many React/Vue tab components use this)
    try:
        tabs = page.locator("[role='tab']")
        count = tabs.count()
        for i in range(count):
            try:
                t = tabs.nth(i)
                if t.is_visible():
                    t.click()
                    page.wait_for_timeout(400)
            except Exception:
                pass
    except Exception:
        pass

    # 2) Click headings / buttons with text like "Specification", "Product details", etc.
    texts_to_click = [
        "Specification",
        "Specifications",
        "Technical Details",
        "Technical Specification",
        "Product details",
        "Product Details",
        "Product information",
        "More information",
        "Full details",
    ]
    for label in texts_to_click:
        try:
            el = page.get_by_text(label, exact=False).first
            if el and el.is_visible():
                el.click(timeout=1500)
                page.wait_for_timeout(500)
        except Exception:
            continue

    # 3) Click "Show more" / "Show all"
    expand_texts = [
        "Show more",
        "Show More",
        "Show all",
        "Show All",
        "More details",
        "View more",
    ]
    for label in expand_texts:
        try:
            el = page.get_by_text(label, exact=False).first
            if el and el.is_visible():
                el.click(timeout=1500)
                page.wait_for_timeout(500)
        except Exception:
            continue

    # 4) Scroll down and back up to trigger lazy loading
    try:
        page.evaluate(
            """
            window.scrollTo(0, document.body.scrollHeight);
            """
        )
        page.wait_for_timeout(1200)
        page.evaluate("window.scrollTo(0, 0);")
        page.wait_for_timeout(600)
    except Exception:
        pass


def extract_price(page) -> str | None:
    """
    Try multiple common selectors for price across different retailers.
    Returns a cleaned text string or None.
    """
    selectors = [
        "[itemprop='price']",
        "[data-product-price]",
        "[data-test='product-price']",
        "[data-testid='product-price']",
        ".product-price",
        ".productPrice",
        ".product-price__price",
        ".price__value",
        ".price",
        ".b-product_price",
        ".c-product-price",
        ".product-main-price",
        ".js-product-price",
        ".prd-price",
        ".product-details__price",
        ".t-product-price__amount",
    ]

    for sel in selectors:
        try:
            el = page.locator(sel).first
            if el and el.is_visible():
                text = el.inner_text().strip()
                if text:
                    return " ".join(text.split())
        except Exception:
            continue

    # Fallback: look for any element with "£" or "€" or "$"
    try:
        all_text = page.inner_text("body")
        # Very rough, you can refine later
        for line in all_text.splitlines():
            line = line.strip()
            if line.startswith(("£", "€", "$")) and any(ch.isdigit() for ch in line):
                return " ".join(line.split())
    except Exception:
        pass

    return None


def extract_sku(page, domain: str) -> str | None:
    """
    Extract SKU / product code / catalogue number using generic regex on body text.
    Domain is passed so you can plug in domain-specific patterns later if needed.
    """
    import re

    try:
        body_text = page.inner_text("body")
    except Exception:
        return None

    patterns = [
        r"Product code[:\s]*([A-Za-z0-9\-]+)",
        r"Product Code[:\s]*([A-Za-z0-9\-]+)",
        r"Product ID[:\s]*([A-Za-z0-9\-]+)",
        r"SKU[:\s]*([A-Za-z0-9\-]+)",
        r"Sku[:\s]*([A-Za-z0-9\-]+)",
        r"Model number[:\s]*([A-Za-z0-9\-\/]+)",
        r"Item code[:\s]*([A-Za-z0-9\-]+)",
    ]

    # Domain-specific extra pattern examples
    if "argos.co.uk" in domain:
        patterns.insert(0, r"Catalogue number[:\s]*([0-9\-]+)")

    if "screwfix.com" in domain:
        # Screwfix often: "Product code 647PY"
        patterns.insert(0, r"Product code\s*([A-Za-z0-9\-]+)")

    for pat in patterns:
        m = re.search(pat, body_text, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()

    return None


def extract_specs_html(page) -> str | None:
    """
    Try to capture the main 'specification/technical details' HTML block.
    Strategy:
      1) look for elements with id/class containing 'spec'
      2) look for headings containing 'Specification' then grab the next table/block
      3) fallback: first <table> on the page
    """
    # 1) id/class contains 'spec'
    try:
        candidate = page.locator("[id*='spec'], [class*='spec']").first
        if candidate and candidate.count() > 0 and candidate.is_visible():
            html = candidate.inner_html()
            if html and len(html) > 100:  # avoid tiny snippets
                return html
    except Exception:
        pass

    # 2) heading with "Specification"
    try:
        headings = page.locator(
            "xpath=//*[self::h1 or self::h2 or self::h3 or self::h4][contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'spec')]"
        )
        h_count = headings.count()
        for i in range(h_count):
            h = headings.nth(i)
            # table after heading
            table = h.locator("xpath=following-sibling::table[1]")
            if table.count() > 0:
                return table.first.inner_html()
            # or next block
            block = h.locator("xpath=following-sibling::*[1]")
            if block.count() > 0:
                html = block.first.inner_html()
                if html and len(html) > 100:
                    return html
    except Exception:
        pass

    # 3) Fallback: first table
    try:
        t = page.locator("table").first
        if t and t.count() > 0:
            return t.inner_html()
    except Exception:
        pass

    return None


# ---------- PER-URL SCRAPING ----------

def scrape_single_pdp(page, url: str) -> dict:
    """
    Generic multi-retailer PDP scrape:
      - loads URL
      - expands spec-like sections
      - extracts price, sku, specs_html
    """

    domain = urlparse(url).netloc.lower()

    # Load the page
    try:
        page.goto(url, wait_until="networkidle", timeout=60000)
    except PlaywrightTimeoutError:
        # Second try with a simpler load state
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
        except Exception as e2:
            return {
                "url": url,
                "domain": domain,
                "price": None,
                "sku": None,
                "specs_html": None,
                "error": f"Navigation failed: {e2}",
            }
    except Exception as e:
        return {
            "url": url,
            "domain": domain,
            "price": None,
            "sku": None,
            "specs_html": None,
            "error": f"Navigation failed: {e}",
        }

    # Interact to reveal specs
    expand_spec_like_sections(page)

    # Extract bits
    price = extract_price(page)
    sku = extract_sku(page, domain)
    specs_html = extract_specs_html(page)

    return {
        "url": url,
        "domain": domain,
        "price": price,
        "sku": sku,
        "specs_html": specs_html,
        "error": None,
    }


# ---------- MAIN BATCH RUNNER ----------

def run_batch(urls: list[str], output_csv: str):
    Path(output_csv).parent.mkdir(parents=True, exist_ok=True)

    results: list[dict] = []

    with sync_playwright() as p:
        # If you want to plug in Oxylabs or another proxy, set proxy=... here:
        browser = p.chromium.launch(
            headless=True,
            # proxy={
            #     "server": "http://pr.oxylabs.io:7777",
            #     "username": "YOUR_USER",
            #     "password": "YOUR_PASS",
            # },
        )

        # One page reused for all URLs (faster)
        page = browser.new_page(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )

        for url in urls:
            url = url.strip()
            if not url:
                continue

            print(f"\n=== Scraping: {url} ===")
            try:
                data = scrape_single_pdp(page, url)
                results.append(data)
                print(f"  → Price: {data['price']}")
                print(f"  → SKU:   {data['sku']}")
            except Exception as e:
                print(f"  ! Failed for {url}: {e}")
                results.append({
                    "url": url,
                    "domain": urlparse(url).netloc.lower(),
                    "price": None,
                    "sku": None,
                    "specs_html": None,
                    "error": str(e),
                })

            # Small random delay between pages
            human_delay(1.0, 2.5)

        browser.close()

    # Write to CSV
    fieldnames = ["url", "domain", "price", "sku", "specs_html", "error"]
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\nSaved {len(results)} rows to {output_csv}")


if __name__ == "__main__":
    run_batch(PDP_URLS, OUTPUT_CSV)
