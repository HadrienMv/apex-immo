"""
LeBonCoin scraper — via Playwright (headless browser).

L'API LBC bloque les requêtes directes (403). On passe par le navigateur
pour charger la page de résultats et extraire les données du JSON embarqué.
"""
import json
import asyncio
import re
from datetime import datetime
from playwright.async_api import async_playwright


LBC_SEARCH_URL = "https://www.leboncoin.fr/recherche?category=9&locations=d_36&real_estate_type=1,2&price=min-150000&square=50-max&sort=time"

# Variantes avec mots-clés de détresse
DISTRESS_URLS = [
    LBC_SEARCH_URL,
    LBC_SEARCH_URL + "&text=succession",
    LBC_SEARCH_URL + "&text=urgent",
    LBC_SEARCH_URL + "&text=travaux",
    LBC_SEARCH_URL + "&text=r%C3%A9nover",
]


async def _scrape_page(page, url: str) -> list[dict]:
    """Scrape une page de résultats LeBonCoin."""
    biens = []

    try:
        # Intercepter les réponses API internes
        api_data = []

        async def handle_response(response):
            if "finder/search" in response.url or "api.leboncoin.fr" in response.url:
                try:
                    data = await response.json()
                    api_data.append(data)
                except Exception:
                    pass

        page.on("response", handle_response)

        await page.goto(url, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(3000)

        # Méthode 1 : données interceptées de l'API
        for data in api_data:
            for ad in data.get("ads", []):
                bien = _parse_ad(ad)
                if bien:
                    biens.append(bien)

        # Méthode 2 : extraire du __NEXT_DATA__ ou state embarqué
        if not biens:
            content = await page.content()
            # Chercher le JSON dans le script __NEXT_DATA__
            match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', content, re.DOTALL)
            if match:
                try:
                    next_data = json.loads(match.group(1))
                    ads = _extract_ads_from_next_data(next_data)
                    for ad in ads:
                        bien = _parse_ad(ad)
                        if bien:
                            biens.append(bien)
                except json.JSONDecodeError:
                    pass

        # Méthode 3 : scrape le DOM directement
        if not biens:
            cards = await page.query_selector_all('[data-qa-id="aditem_container"], [class*="adCard"], a[href*="/ad/ventes_immobilieres/"]')
            for card in cards:
                try:
                    titre = await card.query_selector('[data-qa-id="aditem_title"], h2, [class*="title"]')
                    prix_el = await card.query_selector('[data-qa-id="aditem_price"], [class*="price"]')
                    location_el = await card.query_selector('[class*="location"], [data-qa-id="aditem_location"]')
                    link = await card.get_attribute("href") or ""

                    titre_text = await titre.inner_text() if titre else ""
                    prix_text = await prix_el.inner_text() if prix_el else ""
                    loc_text = await location_el.inner_text() if location_el else ""

                    # Extraire le prix
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
        print(f"    LBC page error: {e}")

    return biens


def _parse_ad(ad: dict) -> dict | None:
    """Parse une annonce LBC depuis le JSON API."""
    attrs = {}
    for a in ad.get("attributes", []):
        attrs[a.get("key", "")] = a.get("value", a.get("values", ""))

    prix = None
    if ad.get("price"):
        prix = ad["price"][0] if isinstance(ad["price"], list) else ad["price"]

    surface = None
    if attrs.get("square"):
        try:
            surface = float(str(attrs["square"]).replace("m²", "").strip())
        except (ValueError, TypeError):
            pass

    if not prix:
        return None

    location = ad.get("location", {})
    lat = location.get("lat")
    lng = location.get("lng")

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
        "lat": lat,
        "lon": lng,
        "geo_precision": "annonce_approx" if lat else None,
        "nb_pieces": attrs.get("rooms"),
        "nb_chambres": attrs.get("bedrooms"),
        "dpe": attrs.get("energy_rate"),
        "premiere_vue": datetime.now().isoformat(),
        "prix_m2": round(prix / surface, 2) if prix and surface and surface > 0 else None,
    }


def _extract_ads_from_next_data(data: dict) -> list[dict]:
    """Extrait les annonces depuis __NEXT_DATA__."""
    ads = []
    # Parcourir récursivement pour trouver les ads
    def _walk(obj):
        if isinstance(obj, dict):
            if "ads" in obj and isinstance(obj["ads"], list):
                ads.extend(obj["ads"])
            for v in obj.values():
                _walk(v)
        elif isinstance(obj, list):
            for item in obj:
                _walk(item)
    _walk(data)
    return ads


async def _scrape_all(urls: list[str]) -> list[dict]:
    """Scrape plusieurs pages LBC avec Playwright."""
    all_biens = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="fr-FR",
            viewport={"width": 1920, "height": 1080},
        )

        page = await context.new_page()

        for url in urls:
            print(f"    Scraping: {url[:80]}...")
            biens = await _scrape_page(page, url)
            all_biens.extend(biens)
            print(f"      → {len(biens)} annonces")

        await browser.close()

    return all_biens


def search_biens(max_prix: int = 150000, min_surface: int = 50) -> list[dict]:
    """Recherche LeBonCoin via Playwright."""
    url = f"https://www.leboncoin.fr/recherche?category=9&locations=d_36&real_estate_type=1,2&price=min-{max_prix}&square={min_surface}-max&sort=time"
    return asyncio.run(_scrape_all([url]))


def search_distressed(max_prix: int = 150000, min_surface: int = 50) -> list[dict]:
    """Recherche de biens en détresse via Playwright."""
    biens = asyncio.run(_scrape_all(DISTRESS_URLS))

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
    print("=== Recherche LeBonCoin 36 (Playwright) ===")
    biens = search_distressed()
    print(f"\n{len(biens)} biens uniques trouvés")
    for b in biens[:10]:
        prix = b.get("prix", 0) or 0
        surf = b.get("surface_bati") or "?"
        print(f"  {prix:>10,.0f}€  {surf:>5}m²  {b.get('commune', ''):20s}  {b.get('titre', '')[:60]}")
