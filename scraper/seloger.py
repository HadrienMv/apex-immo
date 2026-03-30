"""
BienIci/SeLoger scraper — via Playwright.

L'API JSON de BienIci ignore les filtres de zone.
On passe par le navigateur pour scraper la page de résultats.
"""
import asyncio
import json
import os
import re
from datetime import datetime

PROXY_URL = os.getenv("PROXY_URL", "")
from playwright.async_api import async_playwright

# URL de recherche BienIci filtrée sur le département 36
BIENICI_SEARCH_URL = "https://www.bienici.com/recherche/achat/indre-36/bien-immobilier?prix-max=150000&surface-min=50"


async def _scrape_bienici(max_pages: int = 3) -> list[dict]:
    """Scrape BienIci via Playwright."""
    biens = []

    async with async_playwright() as p:
        # Parse proxy
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
            args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="fr-FR",
            viewport={"width": 1920, "height": 1080},
            ignore_https_errors=True,
        )
        page = await context.new_page()

        # Intercepter les réponses API
        api_data = []

        async def handle_response(response):
            url = response.url
            if "realEstateAds" in url or "api.bienici.com" in url or "search" in url:
                try:
                    data = await response.json()
                    api_data.append(data)
                except Exception:
                    pass

        page.on("response", handle_response)

        for page_num in range(1, max_pages + 1):
            url = BIENICI_SEARCH_URL + (f"&page={page_num}" if page_num > 1 else "")
            print(f"    BienIci page {page_num}...")

            try:
                await page.goto(url, wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(2000)
            except Exception as e:
                print(f"      ⚠ Timeout: {e}")
                break

        # Parse API data interceptée
        for data in api_data:
            for ad in data.get("realEstateAds", []):
                bien = _parse_bienici_ad(ad)
                if bien:
                    biens.append(bien)

        # Fallback : scrape DOM si pas de données API
        if not biens:
            print("    Fallback DOM scraping...")
            # Extraire depuis __NEXT_DATA__ ou script embarqué
            content = await page.content()
            match = re.search(r'window\.__INITIAL_STATE__\s*=\s*({.*?});', content, re.DOTALL)
            if not match:
                match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', content, re.DOTALL)
            if match:
                try:
                    state = json.loads(match.group(1))
                    ads = _extract_ads_recursive(state)
                    for ad in ads:
                        bien = _parse_bienici_ad(ad)
                        if bien:
                            biens.append(bien)
                except json.JSONDecodeError:
                    pass

        await browser.close()

    return biens


def _parse_bienici_ad(ad: dict) -> dict | None:
    """Parse une annonce BienIci."""
    prix = ad.get("price")
    if isinstance(prix, list):
        prix = prix[0] if prix else None
    surface = ad.get("surfaceArea")
    if isinstance(surface, list):
        surface = surface[0] if surface else None

    if not prix:
        return None

    terrain = ad.get("landSurfaceArea")
    if isinstance(terrain, list):
        terrain = terrain[0] if terrain else 0
    terrain = terrain or 0

    # Coordonnées GPS
    lat = ad.get("blurInfo", {}).get("latitude") if isinstance(ad.get("blurInfo"), dict) else None
    lon = ad.get("blurInfo", {}).get("longitude") if isinstance(ad.get("blurInfo"), dict) else None
    if not lat:
        lat = ad.get("latitude")
    if not lon:
        lon = ad.get("longitude")

    cp = ad.get("postalCode", "")
    # Filtrer hors 36
    if cp and not str(cp).startswith("36"):
        return None

    return {
        "source": "bienici",
        "url": f"https://www.bienici.com/annonce/{ad.get('id', '')}",
        "titre": ad.get("title", ""),
        "prix": prix,
        "surface_bati": surface,
        "surface_terrain": terrain,
        "commune": ad.get("city", ""),
        "code_postal": cp,
        "description": ad.get("description", ""),
        "lat": lat,
        "lon": lon,
        "geo_precision": "annonce_approx" if lat else None,
        "nb_pieces": ad.get("roomsQuantity"),
        "nb_chambres": ad.get("bedroomsQuantity"),
        "dpe": ad.get("energyClassification"),
        "premiere_vue": datetime.now().isoformat(),
        "prix_m2": round(prix / surface, 2) if prix and surface and surface > 0 else None,
    }


def _extract_ads_recursive(obj) -> list[dict]:
    """Extrait les annonces de n'importe quelle structure imbriquée."""
    ads = []
    if isinstance(obj, dict):
        if "realEstateAds" in obj:
            ads.extend(obj["realEstateAds"])
        if "id" in obj and "price" in obj and "surfaceArea" in obj:
            ads.append(obj)
        for v in obj.values():
            ads.extend(_extract_ads_recursive(v))
    elif isinstance(obj, list):
        for item in obj:
            ads.extend(_extract_ads_recursive(item))
    return ads


def search_all_pages(max_prix: int = 150000, min_surface: int = 50, max_pages: int = 3) -> list[dict]:
    """Recherche BienIci via Playwright."""
    biens = asyncio.run(_scrape_bienici(max_pages=max_pages))

    # Déduplicate
    seen = set()
    unique = []
    for b in biens:
        key = b.get("url", "")
        if key and key not in seen:
            seen.add(key)
            unique.append(b)

    return unique


if __name__ == "__main__":
    print("=== Recherche BienIci (Playwright) — Indre 36 ===")
    biens = search_all_pages()
    print(f"\n{len(biens)} biens trouvés dans le 36")
    for b in biens[:10]:
        prix = b.get("prix", 0) or 0
        surf = b.get("surface_bati") or "?"
        lat = f"{b['lat']:.4f}" if b.get("lat") else "N/A"
        print(f"  {prix:>10,.0f}€  {surf:>5}m²  GPS:{lat}  {b.get('commune', ''):20s}  {b.get('titre', '')[:50]}")
