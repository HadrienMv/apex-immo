"""
Extraction d'adresse précise depuis le texte d'annonce.
Géocode via api-adresse.data.gouv.fr pour obtenir lat/lon exact.
"""
import re
import httpx

GEOCODE_URL = "https://api-adresse.data.gouv.fr/search/"


# Patterns pour extraire des adresses dans les annonces immo françaises
ADDRESS_PATTERNS = [
    # "rue de la Paix" / "avenue Jean Jaurès" / "place de la République"
    r"(?:(?:rue|avenue|boulevard|impasse|allée|chemin|place|cours|passage|route|voie|square|résidence|lotissement|cité|hameau|lieu[- ]dit|quartier)\s+(?:de\s+(?:la\s+|l['\u2019])?|du\s+|des\s+|d['\u2019])?[\w\s\-éèêëàâäôùûüïîç]+?)(?=[,.\n]|$)",
    # Numéro + voie: "12 rue de la Gare"
    r"\d{1,4}\s*(?:bis|ter)?\s*,?\s*(?:rue|avenue|boulevard|impasse|allée|chemin|place|cours|passage|route)\s+[\w\s\-éèêëàâäôùûüïîç]+?(?=[,.\n]|$)",
    # Quartier/lieu-dit explicite: "quartier Saint-Jacques", "lieu-dit Les Bordes"
    r"(?:quartier|lieu[- ]dit|hameau|le\s+bourg)\s+[\w\s\-éèêëàâäôùûüïîç]+?(?=[,.\n]|$)",
]

# Mots-clés de localisation dans les annonces
LOCATION_KEYWORDS = [
    "situé", "située", "proche", "à proximité", "centre", "centre-ville",
    "secteur", "quartier", "en plein", "à deux pas",
]


def extract_addresses(text: str) -> list[str]:
    """Extrait les adresses potentielles d'un texte d'annonce."""
    addresses = []
    text_clean = text.replace("\n", " ").replace("\r", " ")

    for pattern in ADDRESS_PATTERNS:
        matches = re.findall(pattern, text_clean, re.IGNORECASE)
        for m in matches:
            addr = m.strip().strip(",. ")
            if len(addr) > 5 and len(addr) < 100:
                addresses.append(addr)

    return addresses


def extract_quartier(text: str) -> str | None:
    """Extrait un nom de quartier ou indication de localisation."""
    text_lower = text.lower()

    # Patterns spécifiques
    patterns = [
        r"(?:quartier|secteur)\s+([\w\s\-éèêëàâäôùûüïîç]+?)(?=[,.\n]|$)",
        r"centre[- ]ville",
        r"proche\s+(?:de\s+)?(?:la\s+)?(?:gare|mairie|école|marché|hôpital|lycée|collège|commerces?)",
    ]
    for p in patterns:
        m = re.search(p, text_lower)
        if m:
            return m.group(0).strip()

    return None


def geocode_best_effort(
    text: str,
    commune: str = "",
    code_postal: str = "",
) -> dict | None:
    """
    Tente de géocoder au plus précis possible :
    1. Adresse extraite du texte + commune
    2. Quartier/lieu-dit + commune
    3. Commune seule (fallback)

    Retourne {lat, lon, precision, label} ou None.
    """
    candidates = []

    # 1. Adresses extraites
    addresses = extract_addresses(text)
    for addr in addresses:
        q = f"{addr}, {commune} {code_postal}".strip()
        candidates.append(("rue", q))

    # 2. Quartier
    quartier = extract_quartier(text)
    if quartier:
        q = f"{quartier}, {commune} {code_postal}".strip()
        candidates.append(("quartier", q))

    # 3. Commune (fallback)
    if commune:
        candidates.append(("commune", f"{commune} {code_postal}".strip()))

    # Geocode each candidate, take the best
    with httpx.Client(timeout=10) as client:
        for precision, query in candidates:
            try:
                resp = client.get(GEOCODE_URL, params={
                    "q": query,
                    "limit": 1,
                    "postcode": code_postal or None,
                })
                resp.raise_for_status()
                features = resp.json().get("features", [])
                if features:
                    f = features[0]
                    coords = f["geometry"]["coordinates"]
                    props = f["properties"]
                    score = props.get("score", 0)

                    # On veut un score > 0.5 pour être confiant
                    if score > 0.4:
                        result_type = props.get("type", "")  # housenumber, street, locality, municipality
                        actual_precision = {
                            "housenumber": "adresse_exacte",
                            "street": "rue",
                            "locality": "lieu_dit",
                            "municipality": "commune",
                        }.get(result_type, precision)

                        return {
                            "lat": coords[1],
                            "lon": coords[0],
                            "precision": actual_precision,
                            "label": props.get("label", ""),
                            "score": score,
                            "type": result_type,
                        }
            except Exception:
                continue

    return None


if __name__ == "__main__":
    # Tests
    tests = [
        ("Maison 4 pièces située 12 rue de la Gare à Châteauroux, proche centre-ville", "Châteauroux", "36000"),
        ("Belle maison dans le quartier Saint-Jacques, Issoudun, à rénover", "Issoudun", "36100"),
        ("Propriété au lieu-dit Les Bordes, commune de Déols", "Déols", "36130"),
        ("Maison de ville, centre-ville d'Argenton-sur-Creuse", "Argenton-sur-Creuse", "36200"),
        ("Pavillon résidence Les Musiciens, Le Blanc", "Le Blanc", "36300"),
    ]

    for text, commune, cp in tests:
        print(f"\n📍 {text[:70]}...")
        addrs = extract_addresses(text)
        if addrs:
            print(f"   Adresses trouvées: {addrs}")
        quartier = extract_quartier(text)
        if quartier:
            print(f"   Quartier: {quartier}")
        geo = geocode_best_effort(text, commune, cp)
        if geo:
            print(f"   → {geo['precision']}: {geo['label']} (score: {geo['score']:.2f})")
            print(f"     GPS: {geo['lat']:.6f}, {geo['lon']:.6f}")
        else:
            print(f"   → Pas de géocodage possible")
