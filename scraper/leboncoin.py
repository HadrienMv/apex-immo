"""
LeBonCoin scraper — via Bright Data Web Unlocker API.

Le Web Unlocker gère automatiquement le bypass des protections anti-bot.
On récupère le HTML de la page de résultats et on parse les données.
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

LBC_BASE = "https://www.leboncoin.fr/recherche"


def _fetch_lbc_page(url: str) -> str | None:
    """Fetch une page LBC via Bright Data Web Unlocker."""
    if not BRIGHTDATA_KEY:
        print("    BRIGHTDATA_KEY not set, skipping LBC")
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
            timeout=60,
        )
        if resp.status_code == 200:
            return resp.text
        else:
            print(f"    Bright Data HTTP {resp.status_code}: {resp.text[:200]}")
            return None
    except Exception as e:
        print(f"    Bright Data error: {e}")
        return None


def _parse_lbc_html(html: str) -> list[dict]:
    """Parse le HTML LBC pour extraire les annonces."""
    biens = []

    # Méthode 1: Extraire depuis __NEXT_DATA__
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if match:
        try:
            next_data = json.loads(match.group(1))
            ads = _extract_ads_recursive(next_data)
            for ad in ads:
                bien = _parse_ad(ad)
                if bien:
                    biens.append(bien)
        except (json.JSONDecodeError, Exception) as e:
            print(f"    __NEXT_DATA__ parse error: {e}")

    # Méthode 2: Parse le HTML directement
    if not biens:
        soup = BeautifulSoup(html, "lxml")

        # Chercher les scripts contenant des données d'annonces
        for script in soup.find_all("script"):
            text = script.string or ""
            if '"ads"' in text and '"list_id"' in text:
                # Trouver le JSON dans le script
                for m in re.finditer(r'\{[^{}]*"ads"\s*:\s*\[.*?\][^{}]*\}', text, re.DOTALL):
                    try:
                        data = json.loads(m.group())
                        for ad in data.get("ads", []):
                            bien = _parse_ad(ad)
                            if bien:
                                biens.append(bien)
                    except json.JSONDecodeError:
                        continue

    # Méthode 3: Extraire les liens et infos basiques du DOM
    if not biens:
        soup = BeautifulSoup(html, "lxml") if not biens else soup
        for link in soup.select('a[href*="/ad/ventes_immobilieres/"], a[data-qa-id="aditem_container"]'):
            href = link.get("href", "")
            title_el = link.select_one('[data-qa-id="aditem_title"], h2, [class*="title"]')
            price_el = link.select_one('[data-qa-id="aditem_price"], [class*="rice"]')
            loc_el = link.select_one('[data-qa-id="aditem_location"], [class*="ocation"]')

            title = title_el.get_text(strip=True) if title_el else ""
            price_text = price_el.get_text(strip=True) if price_el else ""
            location = loc_el.get_text(strip=True) if loc_el else ""

            prix_match = re.search(r"([\d\s\xa0]+)\s*€", price_text)
            prix = float(prix_match.group(1).replace(" ", "").replace("\xa0", "")) if prix_match else None

            # Extraire surface du titre
            surface = None
            surf_match = re.search(r"(\d+)\s*m[²2]", title + " " + price_text)
            if surf_match:
                surface = float(surf_match.group(1))

            if title and (prix or href):
                url = f"https://www.leboncoin.fr{href}" if href.startswith("/") else href
                biens.append({
                    "source": "leboncoin",
                    "url": url,
                    "titre": title,
                    "prix": prix,
                    "surface_bati": surface,
                    "commune": location,
                    "premiere_vue": datetime.now().isoformat(),
                    "prix_m2": round(prix / surface, 2) if prix and surface and surface > 0 else None,
                })

    return biens


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


def search_biens(max_prix: int = 150000, min_surface: int = 50) -> list[dict]:
    """Alias pour search_distressed."""
    return search_distressed(max_prix=max_prix, min_surface=min_surface)


def search_distressed(max_prix: int = 150000, min_surface: int = 50) -> list[dict]:
    """Récupère TOUTES les annonces du 36, le scoring se fait après."""
    all_biens = []

    # Toutes les annonces, paginées (3 pages max = ~105 annonces)
    for page in range(1, 4):
        url = f"{LBC_BASE}?category=9&locations=d_36&real_estate_type=1,2&price=min-{max_prix}&square={min_surface}-max&sort=time"
        if page > 1:
            url += f"&page={page}"
        print(f"    Fetching LBC page {page}...")
        html = _fetch_lbc_page(url)
        if not html:
            break
        biens = _parse_lbc_html(html)
        if not biens:
            break
        all_biens.extend(biens)
        print(f"    +{len(biens)} annonces")

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
    print("=== Recherche LeBonCoin 36 (Bright Data) ===")
    biens = search_distressed()
    print(f"\n{len(biens)} biens uniques trouvés")
    for b in biens[:15]:
        prix = b.get("prix", 0) or 0
        surf = b.get("surface_bati") or "?"
        print(f"  {prix:>10,.0f}€  {surf:>5}m²  {b.get('commune', ''):20s}  {b.get('titre', '')[:60]}")
