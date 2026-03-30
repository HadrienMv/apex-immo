"""
DVF Géolocalisé — analyse fine par section cadastrale et géolocalisation.
Utilise l'endpoint /geomutations/ qui renvoie les coordonnées GPS des parcelles.
"""
import httpx
import math
from dataclasses import dataclass

GEO_URL = "https://apidf-preprod.cerema.fr/dvf_opendata/geomutations/"

# API Adresse pour géocoder (gratuite, data.gouv.fr)
GEOCODE_URL = "https://api-adresse.data.gouv.fr/search/"


@dataclass
class GeoTransaction:
    date: str
    prix: float
    surface_bati: float
    prix_m2: float
    type_bien: str
    lat: float
    lon: float
    section_cadastrale: str
    id_parcelle: str


def geocode_adresse(adresse: str, code_postal: str = "") -> tuple[float, float] | None:
    """Géocode une adresse via api-adresse.data.gouv.fr."""
    q = f"{adresse} {code_postal}".strip()
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(GEOCODE_URL, params={"q": q, "limit": 1})
            resp.raise_for_status()
            features = resp.json().get("features", [])
            if features:
                coords = features[0]["geometry"]["coordinates"]
                return coords[1], coords[0]  # lat, lon
    except Exception:
        pass
    return None


def geocode_commune(code_insee: str) -> tuple[float, float] | None:
    """Géocode le centre d'une commune par code INSEE."""
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.get(
                "https://geo.api.gouv.fr/communes/" + code_insee,
                params={"fields": "centre"}
            )
            resp.raise_for_status()
            data = resp.json()
            coords = data.get("centre", {}).get("coordinates", [])
            if len(coords) == 2:
                return coords[1], coords[0]  # lat, lon
    except Exception:
        pass
    return None


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance en km entre deux points GPS."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def extract_section(id_parcelle: str) -> str:
    """Extrait la section cadastrale d'un id parcelle (ex: 36044000BI0203 → BI)."""
    if not id_parcelle or len(id_parcelle) < 11:
        return ""
    # Format: CCCCCPPPSSNNNN (commune 5, prefixe 3, section 2, numero 4)
    return id_parcelle[8:10].strip("0")


def fetch_geo_transactions(
    code_insee: str,
    annee_min: int = 2022,
    codtypbien: str = "111",
    max_pages: int = 20,
) -> list[GeoTransaction]:
    """Récupère les transactions géolocalisées pour une commune."""
    transactions = []
    params = {
        "code_insee": code_insee,
        "anneemut_min": annee_min,
        "codtypbien": codtypbien,
        "ordering": "-datemut",
        "page_size": 50,
    }

    with httpx.Client(timeout=30) as client:
        for page in range(1, max_pages + 1):
            params["page"] = page
            resp = client.get(GEO_URL, params=params)
            resp.raise_for_status()
            data = resp.json()

            for feature in data.get("features", []):
                props = feature.get("properties", {})
                geom = feature.get("geometry", {})
                coords = geom.get("coordinates", [[]])

                # Centroid des coordonnées du polygone
                lat, lon = 0, 0
                if coords:
                    # Flatten nested polygon coordinates
                    flat = []
                    def _flatten(c):
                        if not c:
                            return
                        if isinstance(c[0], (int, float)):
                            flat.append(c)
                        else:
                            for item in c:
                                _flatten(item)
                    _flatten(coords)
                    if flat:
                        lons = [c[0] for c in flat if len(c) >= 2]
                        lats = [c[1] for c in flat if len(c) >= 2]
                        if lats and lons:
                            lat = sum(lats) / len(lats)
                            lon = sum(lons) / len(lons)

                prix = float(props.get("valeurfonc") or 0)
                surface = float(props.get("sbati") or 0)
                id_parcelle = (props.get("l_idpar") or [""])[0]

                if surface > 0 and prix > 0 and (lat != 0 or lon != 0):
                    transactions.append(GeoTransaction(
                        date=props["datemut"],
                        prix=prix,
                        surface_bati=surface,
                        prix_m2=round(prix / surface, 2),
                        type_bien=props.get("libtypbien", ""),
                        lat=lat,
                        lon=lon,
                        section_cadastrale=extract_section(id_parcelle),
                        id_parcelle=id_parcelle,
                    ))

            if not data.get("next"):
                break

    return transactions


def median_in_radius(
    transactions: list[GeoTransaction],
    lat: float,
    lon: float,
    radius_km: float = 0.5,
) -> dict:
    """Médiane prix/m² des transactions dans un rayon donné."""
    nearby = [t for t in transactions if haversine_km(lat, lon, t.lat, t.lon) <= radius_km]

    if not nearby:
        return {"median_prix_m2": None, "count": 0, "radius_km": radius_km}

    prix_list = sorted(t.prix_m2 for t in nearby)
    n = len(prix_list)
    median = prix_list[n // 2] if n % 2 else (prix_list[n // 2 - 1] + prix_list[n // 2]) / 2

    return {
        "median_prix_m2": round(median, 2),
        "mean_prix_m2": round(sum(prix_list) / n, 2),
        "min_prix_m2": prix_list[0],
        "max_prix_m2": prix_list[-1],
        "count": n,
        "radius_km": radius_km,
        "transactions": [
            {"date": t.date, "prix": t.prix, "surface": t.surface_bati, "prix_m2": t.prix_m2}
            for t in sorted(nearby, key=lambda x: x.date, reverse=True)[:10]
        ]
    }


def median_by_section(
    transactions: list[GeoTransaction],
    section: str,
) -> dict:
    """Médiane prix/m² pour une section cadastrale spécifique."""
    section_txns = [t for t in transactions if t.section_cadastrale == section]

    if not section_txns:
        return {"median_prix_m2": None, "count": 0, "section": section}

    prix_list = sorted(t.prix_m2 for t in section_txns)
    n = len(prix_list)
    median = prix_list[n // 2] if n % 2 else (prix_list[n // 2 - 1] + prix_list[n // 2]) / 2

    return {
        "section": section,
        "median_prix_m2": round(median, 2),
        "count": n,
    }


def fine_grain_analysis(
    code_insee: str,
    adresse: str = "",
    code_postal: str = "",
    lat: float = None,
    lon: float = None,
    annee_min: int = 2022,
) -> dict:
    """
    Analyse fine : médiane dans un rayon de 500m autour du bien.
    Géocode l'adresse si lat/lon non fournis.
    """
    # Géocoder si nécessaire
    if lat is None or lon is None:
        if adresse:
            coords = geocode_adresse(adresse, code_postal)
        else:
            coords = geocode_commune(code_insee)
        if coords:
            lat, lon = coords
        else:
            return {"error": "Impossible de géolocaliser le bien"}

    # Récupérer les transactions géolocalisées
    transactions = fetch_geo_transactions(code_insee, annee_min)

    # Analyses à différents rayons
    analysis = {
        "position": {"lat": lat, "lon": lon},
        "r500m": median_in_radius(transactions, lat, lon, 0.5),
        "r1km": median_in_radius(transactions, lat, lon, 1.0),
        "r2km": median_in_radius(transactions, lat, lon, 2.0),
        "commune": median_in_radius(transactions, lat, lon, 50.0),  # toute la commune
        "nb_total_transactions": len(transactions),
    }

    return analysis


if __name__ == "__main__":
    print("=== Test DVF géolocalisé — Châteauroux centre ===\n")
    result = fine_grain_analysis(
        code_insee="36044",
        adresse="Place de la République, Châteauroux",
        code_postal="36000",
        annee_min=2022,
    )
    for key in ["r500m", "r1km", "r2km", "commune"]:
        r = result.get(key, {})
        print(f"  {key:10s}: {r.get('median_prix_m2', 'N/A'):>8}€/m²  ({r.get('count', 0)} transactions)")

    if result["r500m"].get("transactions"):
        print(f"\n  Dernières ventes à 500m:")
        for t in result["r500m"]["transactions"][:5]:
            print(f"    {t['date']}  {t['prix']:>10,.0f}€  {t['surface']:>5}m²  = {t['prix_m2']:>6,.0f}€/m²")
