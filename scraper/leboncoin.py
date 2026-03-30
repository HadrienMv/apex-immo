"""
LeBonCoin scraper — Bright Data Web Unlocker API.

Stratégie : tranches de prix avec dichotomie automatique.
Si une tranche retourne 35+ résultats → on la split en deux.
Les tranches échouées sont retentées à la fin.
Max budget: 100k€.
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

PAGE_SIZE = 35  # LBC returns ~35 ads per page


def _fetch_page(url: str) -> str | None:
    """Fetch une page LBC via Bright Data. Single attempt."""
    if not BRIGHTDATA_KEY:
        print("    BRIGHTDATA_KEY not set")
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
            timeout=90,
        )
        if resp.status_code == 200 and len(resp.text) > 10000:
            return resp.text
        return None
    except Exception:
        return None


def _parse_html(html: str) -> list[dict]:
    """Parse le HTML LBC."""
    biens = []

    # __NEXT_DATA__
    start_marker = '<script id="__NEXT_DATA__" type="application/json">'
    start_idx = html.find(start_marker)
    if start_idx != -1:
        start_idx += len(start_marker)
        end_idx = html.find('</script>', start_idx)
        if end_idx != -1:
            try:
                data = json.loads(html[start_idx:end_idx])
                for ad in _extract_ads(data):
                    bien = _parse_ad(ad)
                    if bien:
                        biens.append(bien)
            except json.JSONDecodeError:
                pass

    # Fallback: regex
    if not biens and '"ads"' in html:
        for match in re.finditer(r'"ads"\s*:\s*(\[.*?\])\s*[,}]', html, re.DOTALL):
            try:
                for ad in json.loads(match.group(1)):
                    bien = _parse_ad(ad)
                    if bien:
                        biens.append(bien)
                if biens:
                    break
            except json.JSONDecodeError:
                continue

    return biens


def _fetch_tranche(price_min: int, price_max: int | None) -> tuple[list[dict], bool]:
    """
    Fetch une tranche de prix.
    Returns (biens, success).
    """
    if price_max:
        price_str = f"{price_min}-{price_max}"
        label = f"{price_min//1000}k-{price_max//1000}k"
    else:
        price_str = f"{price_min}-max"
        label = f"{price_min//1000}k+"

    url = f"{LBC_BASE}&price={price_str}"
    html = _fetch_page(url)

    if not html:
        return [], False

    biens = _parse_html(html)
    return biens, True


def search_distressed(**kwargs) -> list[dict]:
    """
    Scrape LBC avec dichotomie automatique.
    - Tranches initiales de 10k€ de 0 à 100k
    - Si une tranche a 35+ résultats → split en deux sous-tranches
    - Les échecs sont retentés à la fin (max 3 passes)
    """
    all_biens = []
    seen_urls = set()

    # File de tranches à traiter: (min, max, depth)
    queue = []
    for start in range(0, 100000, 5000):
        queue.append((start, start + 5000, 0))

    pass_num = 1

    while queue and pass_num <= 5:
        if pass_num > 1:
            print(f"\n    === Passe {pass_num}: {len(queue)} tranches ===")

        next_queue = []

        for price_min, price_max, depth in queue:
            width = price_max - price_min
            if width >= 1000:
                label = f"{price_min//1000}k-{price_max//1000}k"
            else:
                label = f"{price_min}-{price_max}"
            indent = "  " * min(depth, 4)
            print(f"    {indent}[{label}]", end=" ", flush=True)

            biens, success = _fetch_tranche(price_min, price_max)

            if not success:
                print("✗")
                next_queue.append((price_min, price_max, depth))
                time.sleep(1)
                continue

            # Déduplicate
            new_biens = []
            for b in biens:
                url = b.get("url", "")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    new_biens.append(b)

            if len(biens) >= PAGE_SIZE and width > 500:
                # Tranche pleine → dichotomiser
                mid = (price_min + price_max) // 2
                print(f"FULL ({len(biens)}) → split")
                next_queue.append((price_min, mid, depth + 1))
                next_queue.append((mid, price_max, depth + 1))
                # Garder les biens récupérés (page 1 de la tranche large)
                all_biens.extend(new_biens)
            elif new_biens:
                all_biens.extend(new_biens)
                print(f"+{len(new_biens)} ({len(all_biens)} total)")
            else:
                print(f"0")

            time.sleep(1)

        queue = next_queue
        pass_num += 1

    if queue:
        print(f"\n    {len(queue)} tranches non résolues")

    # Déduplicate final
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


# ===== Parsers =====

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


if __name__ == "__main__":
    print("=== LeBonCoin (Bright Data, dichotomie auto) ===")
    biens = search_distressed()
    print(f"\n{len(biens)} biens uniques")
    for b in biens[:20]:
        prix = b.get("prix", 0) or 0
        surf = b.get("surface_bati") or "?"
        print(f"  {prix:>10,.0f}€  {surf:>5}m²  {b.get('commune', ''):20s}  {b.get('titre', '')[:55]}")
