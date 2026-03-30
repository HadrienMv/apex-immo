"""
SeLoger/BienIci scraper — via Bright Data Web Unlocker.

SeLoger et BienIci sont protégés par DataDome/Cloudflare.
On utilise Bright Data pour fetcher le HTML rendu et parser les annonces.
"""
import json
import os
import re
import httpx
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

BRIGHTDATA_API = "https://api.brightdata.com/request"
BRIGHTDATA_KEY = os.getenv("BRIGHTDATA_KEY", "")
BRIGHTDATA_ZONE = os.getenv("BRIGHTDATA_ZONE", "apexlbc")

SELOGER_URLS = [
    "https://www.seloger.com/immobilier/achat/immo-chateauroux-36/bien-maison/",
    "https://www.seloger.com/immobilier/achat/immo-issoudun-36/bien-maison/",
    "https://www.seloger.com/immobilier/achat/immo-argenton-sur-creuse-36/bien-maison/",
    "https://www.seloger.com/immobilier/achat/immo-le-blanc-36/bien-maison/",
    "https://www.seloger.com/immobilier/achat/immo-la-chatre-36/bien-maison/",
]
BIENICI_URL = "https://www.bienici.com/recherche/achat/chateauroux-36000?page={page}"


def _fetch_page(url: str) -> str | None:
    """Fetch une page via Bright Data Web Unlocker."""
    if not BRIGHTDATA_KEY:
        print("    BRIGHTDATA_KEY not set, skipping")
        return None

    try:
        resp = httpx.post(
            BRIGHTDATA_API,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {BRIGHTDATA_KEY}",
            },
            json={
                "zone": BRIGHTDATA_ZONE,
                "url": url,
                "format": "raw",
            },
            timeout=120,
        )
        if resp.status_code == 200:
            return resp.text
        else:
            print(f"    Bright Data HTTP {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        print(f"    Bright Data error: {e}")
        return None


def _parse_seloger_html(html: str) -> list[dict]:
    """Parse le HTML SeLoger."""
    biens = []
    soup = BeautifulSoup(html, "lxml")

    # Méthode 1: __NEXT_DATA__
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if match:
        try:
            data = json.loads(match.group(1))
            cards = _find_cards_recursive(data)
            for card in cards:
                bien = _parse_seloger_card(card)
                if bien:
                    biens.append(bien)
        except (json.JSONDecodeError, Exception):
            pass

    # Méthode 2: chercher dans les scripts JSON
    if not biens:
        for script in soup.find_all("script"):
            text = script.string or ""
            if '"price"' in text and '"livingArea"' in text:
                for m in re.finditer(r'\{[^{}]*"price"\s*:\s*\d+[^{}]*"livingArea"[^{}]*\}', text):
                    try:
                        card = json.loads(m.group())
                        bien = _parse_seloger_card(card)
                        if bien:
                            biens.append(bien)
                    except json.JSONDecodeError:
                        continue

    # Méthode 3: DOM parsing
    if not biens:
        for card in soup.select('[class*="Card"], [class*="listing"], [data-testid*="card"]'):
            prix_el = card.select_one('[class*="rice"], [data-testid*="price"]')
            titre_el = card.select_one('[class*="itle"], h2, h3')
            link = card.select_one('a[href*="/annonce"]')
            loc_el = card.select_one('[class*="ocation"], [class*="city"]')

            prix_text = prix_el.get_text(strip=True) if prix_el else ""
            prix_match = re.search(r"([\d\s\xa0]+)\s*€", prix_text)
            prix = float(prix_match.group(1).replace(" ", "").replace("\xa0", "")) if prix_match else None

            titre = titre_el.get_text(strip=True) if titre_el else ""
            href = link.get("href", "") if link else ""
            commune = loc_el.get_text(strip=True) if loc_el else ""

            surface = None
            surf_match = re.search(r"(\d+)\s*m[²2]", titre + " " + prix_text)
            if surf_match:
                surface = float(surf_match.group(1))

            if prix and titre:
                biens.append({
                    "source": "seloger",
                    "url": href if href.startswith("http") else f"https://www.seloger.com{href}",
                    "titre": titre,
                    "prix": prix,
                    "surface_bati": surface,
                    "commune": commune,
                    "premiere_vue": datetime.now().isoformat(),
                    "prix_m2": round(prix / surface, 2) if prix and surface and surface > 0 else None,
                })

    return biens


def _parse_seloger_card(card: dict) -> dict | None:
    """Parse une carte SeLoger depuis le JSON."""
    prix = card.get("price") or card.get("pricing", {}).get("price")
    if isinstance(prix, dict):
        prix = prix.get("value")
    if not prix:
        return None

    surface = card.get("livingArea") or card.get("surfaceArea") or card.get("surface")
    commune = card.get("city") or card.get("cityLabel", "")
    cp = card.get("zipCode") or card.get("postalCode", "")

    if cp and not str(cp).startswith("36"):
        return None

    lat = card.get("latitude") or card.get("coordinates", {}).get("lat")
    lon = card.get("longitude") or card.get("coordinates", {}).get("lon")

    ad_id = card.get("id") or card.get("classifiedId") or ""
    url = card.get("classifiedURL") or card.get("permalink") or f"https://www.seloger.com/annonces/{ad_id}"

    return {
        "source": "seloger",
        "url": url,
        "titre": card.get("title", f"Bien {surface}m² — {commune}"),
        "prix": prix,
        "surface_bati": surface,
        "surface_terrain": card.get("landArea", 0) or 0,
        "commune": commune,
        "code_postal": str(cp),
        "description": card.get("description", ""),
        "lat": lat,
        "lon": lon,
        "geo_precision": "annonce_approx" if lat else None,
        "nb_pieces": card.get("rooms") or card.get("roomsQuantity"),
        "nb_chambres": card.get("bedrooms") or card.get("bedroomsQuantity"),
        "dpe": card.get("energyClassification"),
        "premiere_vue": datetime.now().isoformat(),
        "prix_m2": round(prix / surface, 2) if prix and surface and surface > 0 else None,
    }


def _find_cards_recursive(obj) -> list[dict]:
    """Cherche les cartes d'annonces dans une structure JSON."""
    cards = []
    if isinstance(obj, dict):
        if ("price" in obj or "pricing" in obj) and ("livingArea" in obj or "surfaceArea" in obj or "rooms" in obj):
            cards.append(obj)
        for v in obj.values():
            cards.extend(_find_cards_recursive(v))
    elif isinstance(obj, list):
        for item in obj:
            cards.extend(_find_cards_recursive(item))
    return cards


def search_all_pages(max_prix: int = 150000, min_surface: int = 50, max_pages: int = 3) -> list[dict]:
    """Scrape SeLoger par ville via Bright Data, puis BienIci en fallback."""
    all_biens = []

    # SeLoger par ville (URLs plus légères que la recherche département)
    for url in SELOGER_URLS:
        ville = url.split("immo-")[1].split("-36")[0] if "immo-" in url else "?"
        print(f"    SeLoger {ville}...")
        html = _fetch_page(url)
        if not html:
            continue

        biens = _parse_seloger_html(html)
        all_biens.extend(biens)
        print(f"    +{len(biens)} annonces")

    # BienIci en fallback si SeLoger n'a rien donné
    if not all_biens:
        for page in range(1, max_pages + 1):
            url_bi = BIENICI_URL.format(page=page)
            print(f"    BienIci page {page}...")
            html_bi = _fetch_page(url_bi)
            if not html_bi:
                break
            biens = _parse_bienici_html(html_bi)
            all_biens.extend(biens)
            print(f"    +{len(biens)} annonces")
            if not biens:
                break

    # Déduplicate
    seen = set()
    unique = []
    for b in all_biens:
        key = b.get("url", "")
        if key and key not in seen:
            seen.add(key)
            unique.append(b)

    return unique


def _parse_bienici_html(html: str) -> list[dict]:
    """Parse le HTML BienIci."""
    biens = []

    # __NEXT_DATA__ ou state
    for pattern in [
        r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        r'window\.__INITIAL_STATE__\s*=\s*({.*?});',
    ]:
        match = re.search(pattern, html, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group(1))
                ads = _find_bienici_ads(data)
                for ad in ads:
                    bien = _parse_bienici_ad(ad)
                    if bien:
                        biens.append(bien)
            except (json.JSONDecodeError, Exception):
                pass
            if biens:
                break

    return biens


def _find_bienici_ads(obj) -> list[dict]:
    ads = []
    if isinstance(obj, dict):
        if "realEstateAds" in obj:
            ads.extend(obj["realEstateAds"])
        if "id" in obj and "price" in obj and "surfaceArea" in obj:
            ads.append(obj)
        for v in obj.values():
            ads.extend(_find_bienici_ads(v))
    elif isinstance(obj, list):
        for item in obj:
            ads.extend(_find_bienici_ads(item))
    return ads


def _parse_bienici_ad(ad: dict) -> dict | None:
    prix = ad.get("price")
    surface = ad.get("surfaceArea")
    cp = ad.get("postalCode", "")
    if cp and not str(cp).startswith("36"):
        return None
    if not prix:
        return None

    lat = ad.get("blurInfo", {}).get("latitude") if isinstance(ad.get("blurInfo"), dict) else ad.get("latitude")
    lon = ad.get("blurInfo", {}).get("longitude") if isinstance(ad.get("blurInfo"), dict) else ad.get("longitude")

    return {
        "source": "bienici",
        "url": f"https://www.bienici.com/annonce/{ad.get('id', '')}",
        "titre": ad.get("title", ""),
        "prix": prix,
        "surface_bati": surface,
        "surface_terrain": ad.get("landSurfaceArea", 0) or 0,
        "commune": ad.get("city", ""),
        "code_postal": str(cp),
        "description": ad.get("description", ""),
        "lat": lat,
        "lon": lon,
        "geo_precision": "annonce_approx" if lat else None,
        "premiere_vue": datetime.now().isoformat(),
        "prix_m2": round(prix / surface, 2) if prix and surface and surface > 0 else None,
    }


if __name__ == "__main__":
    print("=== Recherche SeLoger/BienIci — Indre 36 (Bright Data) ===")
    biens = search_all_pages()
    print(f"\n{len(biens)} biens trouvés")
    for b in biens[:15]:
        prix = b.get("prix", 0) or 0
        surf = b.get("surface_bati") or "?"
        print(f"  {prix:>10,.0f}€  {surf:>5}m²  [{b.get('source')}]  {b.get('commune', ''):20s}  {b.get('titre', '')[:50]}")
