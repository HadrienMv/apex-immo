"""
LeBonCoin scraper — Playwright direct (pas de Bright Data API).

Ouvre un vrai navigateur, accepte les cookies, scroll, pagine,
et extrait les données du DOM ou du __NEXT_DATA__.
"""
import asyncio
import json
import os
import re
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

PROXY_URL = os.getenv("PROXY_URL", "")

LBC_URL = (
    "https://www.leboncoin.fr/recherche"
    "?category=9"
    "&locations=Ch%C3%A2teauroux_36000__46.8126_1.69694_5000_30000"
    "&real_estate_type=1,2"
    "&immo_sell_type=old"
    "&sort=time&order=desc"
)


async def _create_browser():
    """Crée un browser Playwright avec anti-détection."""
    from playwright.async_api import async_playwright

    p = await async_playwright().__aenter__()

    # Proxy Bright Data résidentiel si dispo
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
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
            "--no-sandbox",
            "--disable-dev-shm-usage",
        ],
        proxy=pw_proxy,
    )

    context = await browser.new_context(
        user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        locale="fr-FR",
        viewport={"width": 1920, "height": 1080},
        ignore_https_errors=True,
    )

    # Anti-détection
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['fr-FR', 'fr', 'en']});
        window.chrome = {runtime: {}};
        const originalQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (parameters) =>
            parameters.name === 'notifications' ?
            Promise.resolve({state: Notification.permission}) :
            originalQuery(parameters);
    """)

    return p, browser, context


async def _accept_cookies(page):
    """Accepte les cookies LBC."""
    try:
        for selector in [
            '#didomi-notice-agree-button',
            'button[aria-label*="accepter"]',
            'button[aria-label*="Accept"]',
            '#footer_tc_privacy_button_2',
            'button:has-text("Accepter")',
            'button:has-text("Tout accepter")',
        ]:
            btn = await page.query_selector(selector)
            if btn:
                await btn.click()
                await page.wait_for_timeout(1000)
                print("      Cookies acceptés")
                return
    except Exception:
        pass


async def _scrape_page(page) -> list[dict]:
    """Extrait les annonces de la page courante."""
    biens = []

    # Attendre que le contenu charge
    await page.wait_for_timeout(3000)

    # Méthode 1: __NEXT_DATA__
    try:
        next_data_el = await page.query_selector('script#__NEXT_DATA__')
        if next_data_el:
            json_text = await next_data_el.inner_text()
            data = json.loads(json_text)
            ads = _extract_ads_recursive(data)
            for ad in ads:
                bien = _parse_ad(ad)
                if bien:
                    biens.append(bien)
            if biens:
                return biens
    except Exception as e:
        print(f"      __NEXT_DATA__ error: {e}")

    # Méthode 2: DOM scraping
    try:
        # Scroll pour charger toutes les annonces
        for _ in range(3):
            await page.evaluate("window.scrollBy(0, window.innerHeight)")
            await page.wait_for_timeout(500)

        cards = await page.query_selector_all(
            'a[data-qa-id="aditem_container"], '
            'a[href*="/ad/ventes_immobilieres/"], '
            'a[href*="/ad/locations/"], '
            '[data-test-id="ad"]'
        )

        for card in cards:
            try:
                href = await card.get_attribute("href") or ""
                if not href or "depot" in href:
                    continue

                text = await card.inner_text()
                lines = [l.strip() for l in text.split("\n") if l.strip()]

                titre = lines[0] if lines else ""
                prix = None
                surface = None
                commune = ""

                for line in lines:
                    if not prix:
                        m = re.search(r"([\d\s\xa0]+)\s*€", line)
                        if m:
                            prix = float(re.sub(r'[^\d]', '', m.group(1)))
                    if not surface:
                        m = re.search(r"(\d+)\s*m[²2]", line)
                        if m:
                            surface = float(m.group(1))
                    if not commune and re.match(r"^[A-ZÀ-Ÿ][\w\s-]+\d{5}", line):
                        commune = line

                if not commune:
                    for line in lines:
                        if re.match(r"^[A-ZÀ-Ÿ]", line) and len(line) < 40 and "€" not in line and "m²" not in line:
                            commune = line
                            break

                url = f"https://www.leboncoin.fr{href}" if href.startswith("/") else href

                if prix and prix > 100:
                    biens.append({
                        "source": "leboncoin",
                        "url": url,
                        "titre": titre,
                        "prix": prix,
                        "surface_bati": surface,
                        "commune": commune,
                        "premiere_vue": datetime.now().isoformat(),
                        "prix_m2": round(prix / surface, 2) if prix and surface and surface > 0 else None,
                    })
            except Exception:
                continue
    except Exception as e:
        print(f"      DOM scraping error: {e}")

    return biens


async def _scrape_all_pages(max_pages: int = 50) -> list[dict]:
    """Scrape toutes les pages LBC."""
    all_biens = []

    p, browser, context = await _create_browser()
    page = await context.new_page()

    try:
        # Page 1
        print(f"    Page 1...")
        await page.goto(LBC_URL, wait_until="domcontentloaded", timeout=30000)
        await _accept_cookies(page)
        await page.wait_for_timeout(2000)

        biens = await _scrape_page(page)
        all_biens.extend(biens)
        print(f"    +{len(biens)} annonces (total: {len(all_biens)})")

        if not biens:
            print("    Page 1 vide — arrêt")
            await browser.close()
            await p.__aexit__(None, None, None)
            return all_biens

        # Pages suivantes
        for page_num in range(2, max_pages + 1):
            url = LBC_URL + f"&page={page_num}"
            print(f"    Page {page_num}...")

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(2000)
            except Exception as e:
                print(f"    Erreur navigation page {page_num}: {e}")
                break

            biens = await _scrape_page(page)
            if not biens:
                print(f"    Page {page_num} vide — fin de pagination")
                break

            all_biens.extend(biens)
            print(f"    +{len(biens)} annonces (total: {len(all_biens)})")

            # Pause entre les pages pour éviter le rate limit
            await page.wait_for_timeout(1500)

    except Exception as e:
        print(f"    Erreur: {e}")
    finally:
        await browser.close()
        await p.__aexit__(None, None, None)

    return all_biens


# ===== Parsers =====

def _parse_ad(ad: dict) -> dict | None:
    """Parse une annonce LBC depuis le JSON."""
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


# ===== Interface =====

def search_distressed(**kwargs) -> list[dict]:
    """Scrape toutes les annonces LBC."""
    all_biens = asyncio.run(_scrape_all_pages(max_pages=50))

    # Déduplicate
    seen = set()
    unique = []
    for b in all_biens:
        key = b.get("url", "")
        if key and key not in seen:
            seen.add(key)
            unique.append(b)

    return unique


def search_biens(**kwargs) -> list[dict]:
    return search_distressed(**kwargs)


if __name__ == "__main__":
    print("=== Recherche LeBonCoin 36 (Playwright direct) ===")
    biens = search_distressed()
    print(f"\n{len(biens)} biens uniques trouvés")
    for b in biens[:15]:
        prix = b.get("prix", 0) or 0
        surf = b.get("surface_bati") or "?"
        print(f"  {prix:>10,.0f}€  {surf:>5}m²  {b.get('commune', ''):20s}  {b.get('titre', '')[:60]}")
