"""
LeBonCoin scraper — Bright Data Web Unlocker API.

Stratégie : tranches de prix de 5000€ pour couvrir toutes les annonces.
Chaque requête passe par une IP résidentielle différente (Bright Data).
10 retries par tranche avec rotation d'IP automatique.
"""
import json
import os
import re
import time
import httpx
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

BRIGHTDATA_API = "https://api.brightdata.com/request"
BRIGHTDATA_KEY = os.getenv("BRIGHTDATA_KEY", "")
BRIGHTDATA_ZONE = os.getenv("BRIGHTDATA_ZONE", "apexlbc")

LBC_BASE = (
    "https://www.leboncoin.fr/recherche"
    "?category=9"
    "&locations=Ch%C3%A2teauroux_36000__46.8126_1.69694_5000_30000"
    "&real_estate_type=1,2"
    "&immo_sell_type=old"
    "&sort=time&order=desc"
)

# Tranches de 5000€ de 0 à 300k+
TRANCHES = []
for start in range(0, 300000, 5000):
    end = start + 5000
    TRANCHES.append((f"{start//1000}-{end//1000}k", f"{start}-{end}"))
TRANCHES.append(("300k+", "300000-max"))


def _fetch_page(url: str, max_retries: int = 10) -> str | None:
    """Fetch une page LBC via Bright Data. Retry jusqu'à 10x (IP différente à chaque fois)."""
    if not BRIGHTDATA_KEY:
        print("    BRIGHTDATA_KEY not set")
        return None

    for attempt in range(max_retries):
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
                timeout=90,
            )
            if resp.status_code == 200 and len(resp.text) > 10000:
                return resp.text
            elif resp.status_code == 200:
                if attempt < 3:  # Log seulement les premiers
                    print(f"      retry {attempt+1}/{max_retries} (empty response)")
            else:
                if attempt < 3:
                    print(f"      retry {attempt+1}/{max_retries} (HTTP {resp.status_code})")
        except Exception as e:
            if attempt < 3:
                print(f"      retry {attempt+1}/{max_retries} ({type(e).__name__})")
        time.sleep(2)

    return None


def _parse_html(html: str) -> list[dict]:
    """Parse le HTML LBC — extrait les annonces depuis __NEXT_DATA__."""
    biens = []

    # __NEXT_DATA__
    start_marker = '<script id="__NEXT_DATA__" type="application/json">'
    end_marker = '</script>'
    start_idx = html.find(start_marker)
    if start_idx != -1:
        start_idx += len(start_marker)
        end_idx = html.find(end_marker, start_idx)
        if end_idx != -1:
            try:
                data = json.loads(html[start_idx:end_idx])
                ads = _extract_ads(data)
                for ad in ads:
                    bien = _parse_ad(ad)
                    if bien:
                        biens.append(bien)
            except json.JSONDecodeError:
                pass

    # Fallback: regex sur le HTML brut pour extraire les ads JSON
    if not biens and '"ads"' in html:
        for match in re.finditer(r'"ads"\s*:\s*(\[.*?\])\s*[,}]', html, re.DOTALL):
            try:
                ads = json.loads(match.group(1))
                for ad in ads:
                    bien = _parse_ad(ad)
                    if bien:
                        biens.append(bien)
                if biens:
                    break
            except json.JSONDecodeError:
                continue

    return biens


def _parse_ad(ad: dict) -> dict | None:
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


def _extract_ads(obj) -> list[dict]:
    ads = []
    if isinstance(obj, dict):
        if "ads" in obj and isinstance(obj["ads"], list):
            ads.extend(obj["ads"])
        for v in obj.values():
            ads.extend(_extract_ads(v))
    elif isinstance(obj, list):
        for item in obj:
            ads.extend(_extract_ads(item))
    return ads


def search_distressed(**kwargs) -> list[dict]:
    """Scrape LBC par tranches de 5000€."""
    all_biens = []
    empty_streak = 0

    for label, price_range in TRANCHES:
        url = f"{LBC_BASE}&price={price_range}"
        print(f"    [{label}]", end=" ", flush=True)

        html = _fetch_page(url)
        if not html:
            print("✗")
            empty_streak += 1
            if empty_streak >= 5:
                print(f"    5 échecs consécutifs après {label}, arrêt")
                break
            continue

        biens = _parse_html(html)
        if biens:
            all_biens.extend(biens)
            empty_streak = 0
            print(f"+{len(biens)} ({len(all_biens)} total)")
        else:
            # HTML reçu mais pas d'annonces — tranche vide ou parsing raté
            has_ads = '"ads"' in html
            print(f"0 (HTML:{len(html)//1000}k, ads:{has_ads})")
            # Ne compte pas comme échec si on a du HTML
            if len(html) < 50000:
                empty_streak += 1

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
    print("=== Recherche LeBonCoin (Bright Data, tranches 5k€) ===")
    biens = search_distressed()
    print(f"\n{len(biens)} biens uniques trouvés")
    for b in biens[:20]:
        prix = b.get("prix", 0) or 0
        surf = b.get("surface_bati") or "?"
        print(f"  {prix:>10,.0f}€  {surf:>5}m²  {b.get('commune', ''):20s}  {b.get('titre', '')[:55]}")
