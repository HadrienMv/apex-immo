"""
Immobilier.notaires.fr scraper — annonces notariales.
API JSON ouverte, pas d'auth, 247 biens dans le 36.
"""
import httpx
from datetime import datetime

API_URL = "https://www.immobilier.notaires.fr/pub-services/inotr-www-annonces/v1/annonces"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Accept": "application/json",
}

# Mapping type bien notaires → label
TYPE_BIEN = {
    "MAI": "Maison",
    "APP": "Appartement",
    "TER": "Terrain",
    "IMM": "Immeuble",
    "LOC": "Local commercial",
}


def fetch_annonces(
    departement: str = "36",
    type_transaction: str = "VENTE",
    type_bien: str = None,
    page: int = 0,
    par_page: int = 50,
) -> tuple[list[dict], int]:
    """Récupère les annonces notariales. Retourne (biens, total)."""
    params = {
        "typeTransaction": type_transaction,
        "departement": departement,
        "page": page,
        "parPage": par_page,
    }
    if type_bien:
        params["typeBien"] = type_bien  # MAI, APP, etc.

    try:
        with httpx.Client(timeout=15) as client:
            resp = client.get(API_URL, params=params, headers=HEADERS)
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        print(f"Notaires API error: {e}")
        return [], 0

    total = data.get("nbTotalAnnonces", 0)
    annonces = data.get("annonceResumeDto", data.get("annonces", []))

    biens = []
    for a in annonces:
        prix = a.get("prixAffiche") or a.get("prixTotal")
        surface = a.get("surface")
        terrain = a.get("surfaceTerrain", 0)

        # URL de détail
        annonce_id = a.get("annonceId", a.get("id", ""))
        url = a.get("urlDetailAnnonceFr", f"https://www.immobilier.notaires.fr/fr/annonce-{annonce_id}")

        bien = {
            "source": "notaires",
            "url": url,
            "titre": f"{TYPE_BIEN.get(a.get('typeBien', ''), 'Bien')} {a.get('nbPieces', '?')}p — {a.get('communeNom', '')}",
            "prix": prix,
            "surface_bati": surface,
            "surface_terrain": terrain or 0,
            "commune": a.get("communeNom", ""),
            "code_postal": a.get("codePostal", ""),
            "code_insee": a.get("inseeCommune", ""),
            "quartier": a.get("quartierNom", ""),
            "description": a.get("descriptionFr", a.get("description", "")),
            "nb_pieces": a.get("nbPieces"),
            "nb_chambres": a.get("nbChambres"),
            "type_bien_code": a.get("typeBien"),
            "photo_url": a.get("urlPhotoPrincipale", ""),
            "telephone": a.get("telephone", ""),
            "date_creation": a.get("dateCreation"),
            "date_maj": a.get("dateMaj"),
            "frais_notaire": a.get("emoluments"),
            "prix_total_fai": a.get("prixTotal"),
            "premiere_vue": datetime.now().isoformat(),
        }

        if prix and surface and surface > 0:
            bien["prix_m2"] = round(prix / surface, 2)

        biens.append(bien)

    return biens, total


def fetch_all(departement: str = "36", type_bien: str = None) -> list[dict]:
    """Récupère toutes les annonces (pagination automatique)."""
    all_biens = []
    page = 0

    while True:
        biens, total = fetch_annonces(
            departement=departement,
            type_bien=type_bien,
            page=page,
            par_page=50,
        )
        if not biens:
            break

        all_biens.extend(biens)
        print(f"    Notaires page {page + 1}: {len(biens)} annonces (total: {total})")

        if len(all_biens) >= total:
            break
        page += 1

    return all_biens


if __name__ == "__main__":
    print("=== Immobilier.notaires.fr — Indre (36) ===\n")
    biens = fetch_all("36")
    print(f"\n{len(biens)} biens trouvés\n")

    # Stats
    with_prix = [b for b in biens if b.get("prix") and b.get("surface_bati")]
    if with_prix:
        prix_m2_list = sorted(b["prix_m2"] for b in with_prix)
        n = len(prix_m2_list)
        median = prix_m2_list[n // 2]
        print(f"  Médiane prix/m² (annonces): {median:,.0f}€/m²")
        print(f"  Min: {prix_m2_list[0]:,.0f}€/m²  Max: {prix_m2_list[-1]:,.0f}€/m²")
        print()

    for b in biens[:15]:
        prix = b.get("prix", 0) or 0
        surf = b.get("surface_bati") or 0
        pm2 = f"{b['prix_m2']:,.0f}" if b.get("prix_m2") else "N/A"
        print(f"  {prix:>10,.0f}€  {surf:>5.0f}m²  {pm2:>6s}€/m²  {b.get('commune', ''):20s}  {b['titre'][:50]}")
