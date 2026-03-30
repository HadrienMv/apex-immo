"""
LeBonCoin scraper — multi-stratégie.

Stratégie 1: curl_cffi (impersonne le TLS fingerprint Chrome)
Stratégie 2: Playwright + stealth
Stratégie 3: Fallback DOM scraping
"""
import json
import os
import re
from datetime import datetime

PROXY_URL = os.getenv("PROXY_URL", "")

# ===== Stratégie 1: curl_cffi (TLS fingerprint impersonation) =====

def _search_via_curl_cffi(
    max_prix: int = 150000,
    min_surface: int = 50,
    keywords: list[str] | None = None,
    limit: int = 50,
) -> list[dict]:
    """Recherche LBC via curl_cffi qui impersonne le fingerprint TLS de Chrome."""
    try:
        from curl_cffi import requests as curl_requests
    except ImportError:
        print("    curl_cffi not installed, skipping. Run: pip install curl_cffi")
        return []

    payload = {
        "limit": limit,
        "limit_alu": 0,
        "filters": {
            "category": {"id": "9"},
            "enums": {
                "real_estate_type": ["1", "2"],
                "ad_type": ["offer"],
            },
            "location": {"departments": ["36"]},
            "ranges": {
                "price": {"min": 10000, "max": max_prix},
                "square": {"min": min_surface},
            },
        },
        "sort_by": "time",
        "sort_order": "desc",
    }

    if keywords:
        payload["filters"]["keywords"] = {"text": " ".join(keywords)}

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": "https://www.leboncoin.fr",
        "Referer": "https://www.leboncoin.fr/recherche?category=9&locations=d_36",
        "api_key": "ba0c2dad52b3ec",
    }

    proxies = {"https": PROXY_URL, "http": PROXY_URL} if PROXY_URL else None

    try:
        resp = curl_requests.post(
            "https://api.leboncoin.fr/finder/search",
            json=payload,
            headers=headers,
            impersonate="chrome",
            proxies=proxies,
            timeout=30,
            verify=False,
        )
        if resp.status_code != 200:
            print(f"    curl_cffi: HTTP {resp.status_code}")
            return []
        data = resp.json()
    except Exception as e:
        print(f"    curl_cffi error: {e}")
        return []

    return [b for b in (_parse_ad(ad) for ad in data.get("ads", [])) if b]


# ===== Stratégie 2: Playwright stealth =====

def _search_via_playwright(max_prix: int = 150000, min_surface: int = 50) -> list[dict]:
    """Recherche LBC via Playwright avec anti-détection."""
    try:
        import asyncio
        from playwright.async_api import async_playwright
    except ImportError:
        print("    playwright not installed, skipping")
        return []

    async def _scrape():
        biens = []
        async with async_playwright() as p:
            # Parse proxy for Playwright
            pw_proxy = None
            if PROXY_URL:
                from urllib.parse import urlparse
                parsed = urlparse(PROXY_URL)
                pw_proxy = {
                    "server": f"{parsed.scheme}://{parsed.hostname}:{parsed.port}",
                    "username": parsed.username or "",
                    "password": parsed.password or "",
                }

            browser = await p.chromium.launch(
                headless=True,
                proxy=pw_proxy,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--no-sandbox",
                ],
            )
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                locale="fr-FR",
                viewport={"width": 1920, "height": 1080},
                java_script_enabled=True,
                ignore_https_errors=True,
            )

            # Remove webdriver flag
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                Object.defineProperty(navigator, 'languages', {get: () => ['fr-FR', 'fr', 'en']});
                window.chrome = {runtime: {}};
            """)

            page = await context.new_page()

            # Intercepter les réponses API
            api_data = []
            async def handle_response(response):
                if "finder/search" in response.url or "api.leboncoin.fr" in response.url:
                    try:
                        data = await response.json()
                        api_data.append(data)
                    except Exception:
                        pass
            page.on("response", handle_response)

            url = f"https://www.leboncoin.fr/recherche?category=9&locations=d_36&real_estate_type=1,2&price=min-{max_prix}&square={min_surface}-max&sort=time"

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                # Wait for content to load
                await page.wait_for_timeout(5000)

                # Try to accept cookies if popup appears
                try:
                    accept_btn = await page.query_selector('button[id*="accept"], button[class*="accept"], #didomi-notice-agree-button')
                    if accept_btn:
                        await accept_btn.click()
                        await page.wait_for_timeout(2000)
                except Exception:
                    pass

                # Method 1: intercepted API data
                for data in api_data:
                    for ad in data.get("ads", []):
                        bien = _parse_ad(ad)
                        if bien:
                            biens.append(bien)

                # Method 2: __NEXT_DATA__
                if not biens:
                    content = await page.content()
                    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', content, re.DOTALL)
                    if match:
                        try:
                            next_data = json.loads(match.group(1))
                            ads = _extract_ads_recursive(next_data)
                            for ad in ads:
                                bien = _parse_ad(ad)
                                if bien:
                                    biens.append(bien)
                        except json.JSONDecodeError:
                            pass

                # Method 3: DOM scraping
                if not biens:
                    cards = await page.query_selector_all('a[data-qa-id="aditem_container"], a[href*="/ad/ventes_immobilieres/"]')
                    for card in cards[:50]:
                        try:
                            titre_el = await card.query_selector('[data-qa-id="aditem_title"], h2')
                            prix_el = await card.query_selector('[data-qa-id="aditem_price"], [class*="Price"]')
                            loc_el = await card.query_selector('[data-qa-id="aditem_location"]')
                            link = await card.get_attribute("href") or ""

                            titre_text = await titre_el.inner_text() if titre_el else ""
                            prix_text = await prix_el.inner_text() if prix_el else ""
                            loc_text = await loc_el.inner_text() if loc_el else ""

                            prix_match = re.search(r"([\d\s]+)\s*€", prix_text.replace("\xa0", " "))
                            prix = float(prix_match.group(1).replace(" ", "")) if prix_match else None

                            if titre_text and prix:
                                biens.append({
                                    "source": "leboncoin",
                                    "url": f"https://www.leboncoin.fr{link}" if link.startswith("/") else link,
                                    "titre": titre_text.strip(),
                                    "prix": prix,
                                    "commune": loc_text.strip(),
                                    "premiere_vue": datetime.now().isoformat(),
                                })
                        except Exception:
                            continue

            except Exception as e:
                print(f"    Playwright page error: {e}")

            await browser.close()

        return biens

    import asyncio
    return asyncio.run(_scrape())


# ===== Stratégie 3: Scrape via Google cache =====

def _search_via_google(max_prix: int = 150000) -> list[dict]:
    """Recherche LBC via Google index (pas bloqué)."""
    try:
        import httpx
        from bs4 import BeautifulSoup
    except ImportError:
        return []

    biens = []
    queries = [
        f"site:leboncoin.fr ventes_immobilieres indre maison",
        f"site:leboncoin.fr ventes_immobilieres 36000 maison",
    ]

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        "Accept-Language": "fr-FR,fr;q=0.9",
    }

    import time
    with httpx.Client(timeout=10, follow_redirects=True) as client:
        for query in queries:
            try:
                resp = client.get("https://www.google.com/search", params={"q": query, "num": 20, "hl": "fr"}, headers=headers)
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.text, "lxml")
                for a in soup.select("a[href*='leboncoin.fr/ad/']"):
                    href = a.get("href", "")
                    title = a.get_text(strip=True)
                    if title and href:
                        biens.append({
                            "source": "leboncoin",
                            "url": href,
                            "titre": title[:100],
                            "premiere_vue": datetime.now().isoformat(),
                        })
                time.sleep(2)
            except Exception:
                continue

    return biens


# ===== Parser commun =====

def _parse_ad(ad: dict) -> dict | None:
    """Parse une annonce LBC depuis le JSON API."""
    attrs = {}
    for a in ad.get("attributes", []):
        attrs[a.get("key", "")] = a.get("value", a.get("values", ""))

    prix = None
    if ad.get("price"):
        prix = ad["price"][0] if isinstance(ad["price"], list) else ad["price"]
    if not prix:
        return None

    surface = None
    if attrs.get("square"):
        try:
            surface = float(str(attrs["square"]).replace("m²", "").strip())
        except (ValueError, TypeError):
            pass

    location = ad.get("location", {})
    return {
        "source": "leboncoin",
        "url": ad.get("url", f"https://www.leboncoin.fr/ad/ventes_immobilieres/{ad.get('list_id', '')}"),
        "titre": ad.get("subject", ""),
        "prix": prix,
        "surface_bati": surface,
        "surface_terrain": float(attrs.get("land_plot_surface", 0) or 0),
        "commune": location.get("city", ""),
        "code_postal": location.get("zipcode", ""),
        "description": ad.get("body", ""),
        "lat": location.get("lat"),
        "lon": location.get("lng"),
        "geo_precision": "annonce_approx" if location.get("lat") else None,
        "nb_pieces": attrs.get("rooms"),
        "nb_chambres": attrs.get("bedrooms"),
        "dpe": attrs.get("energy_rate"),
        "premiere_vue": datetime.now().isoformat(),
        "prix_m2": round(prix / surface, 2) if prix and surface and surface > 0 else None,
    }


def _extract_ads_recursive(obj) -> list[dict]:
    ads = []
    if isinstance(obj, dict):
        if "ads" in obj and isinstance(obj["ads"], list):
            ads.extend(obj["ads"])
        for v in obj.values():
            ads.extend(_extract_ads_recursive(v))
    elif isinstance(obj, list):
        for item in obj:
            ads.extend(_extract_ads_recursive(item))
    return ads


# ===== Interface principale =====

def search_biens(max_prix: int = 150000, min_surface: int = 50) -> list[dict]:
    """Recherche LBC — essaie les stratégies dans l'ordre."""
    # Stratégie 1: curl_cffi
    print("    [LBC] Stratégie 1: curl_cffi...")
    biens = _search_via_curl_cffi(max_prix=max_prix, min_surface=min_surface)
    if biens:
        print(f"    [LBC] curl_cffi: {len(biens)} annonces ✓")
        return biens

    # Stratégie 2: Playwright stealth
    print("    [LBC] Stratégie 2: Playwright stealth...")
    biens = _search_via_playwright(max_prix=max_prix, min_surface=min_surface)
    if biens:
        print(f"    [LBC] Playwright: {len(biens)} annonces ✓")
        return biens

    # Stratégie 3: Google
    print("    [LBC] Stratégie 3: Google index...")
    biens = _search_via_google(max_prix=max_prix)
    if biens:
        print(f"    [LBC] Google: {len(biens)} annonces ✓")
        return biens

    print("    [LBC] Aucune stratégie n'a fonctionné")
    return []


def search_distressed(max_prix: int = 150000, min_surface: int = 50) -> list[dict]:
    """Recherche biens en détresse — essaie curl_cffi avec mots-clés."""
    all_biens = []

    # Recherche générale
    all_biens.extend(search_biens(max_prix=max_prix, min_surface=min_surface))

    # Recherches ciblées (seulement si curl_cffi a marché)
    try:
        from curl_cffi import requests as curl_requests
        for kw in ["succession", "urgent", "travaux", "rénover"]:
            biens = _search_via_curl_cffi(max_prix=max_prix, min_surface=min_surface, keywords=[kw])
            if biens:
                all_biens.extend(biens)
                print(f"    [LBC] +{len(biens)} pour '{kw}'")
    except ImportError:
        pass

    # Déduplicate
    seen = set()
    unique = []
    for b in all_biens:
        key = b.get("url", "")
        if key and key not in seen:
            seen.add(key)
            unique.append(b)

    return unique


if __name__ == "__main__":
    print("=== Recherche LeBonCoin 36 ===")
    biens = search_distressed()
    print(f"\n{len(biens)} biens uniques trouvés")
    for b in biens[:10]:
        prix = b.get("prix", 0) or 0
        surf = b.get("surface_bati") or "?"
        print(f"  {prix:>10,.0f}€  {surf:>5}m²  {b.get('commune', ''):20s}  {b.get('titre', '')[:60]}")
