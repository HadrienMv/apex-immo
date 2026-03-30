"""
Enchères publiques scraper — ventes judiciaires via GraphQL API.
Source: encheres-publiques.com (TJ Châteauroux pour le 36)
"""
import httpx
from datetime import datetime

GRAPHQL_URL = "https://www.encheres-publiques.com/back/graphql"

HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
}

LOTS_QUERY = """
query($termine: Boolean, $place: String, $sous_categorie: String) {
  lots(sort: tendance, first: 100, filters: {
    categorie: "immobilier",
    termine: $termine,
    place: $place,
    sous_categorie: $sous_categorie
  }) {
    collection {
      ... on Lot {
        id
        nom
        mise_a_prix
        prix_adjuge
        description
        photo
        type
        sous_categorie
        criteres_resume
        ouverture_date
        fermeture_reelle_date
        en_surenchere
        estimation_basse
        estimation_haute
        nbr_photos
        nbr_vues
        nbr_suivis
        adresse_defaut {
          id
          ville
          ville_slug
          region
        }
        organisateur {
          id
          nom
          categorie
          adresse { ville }
        }
        evenement {
          id
          titre
          ouverture_date
        }
      }
    }
    total
  }
}
"""


def fetch_lots(
    place: str = "indre",
    termine: bool = False,
    sous_categorie: str = None,
) -> list[dict]:
    """Récupère les lots d'enchères immobilières."""
    variables = {
        "termine": termine,
        "place": place,
    }
    if sous_categorie:
        variables["sous_categorie"] = sous_categorie

    try:
        with httpx.Client(timeout=15) as client:
            resp = client.post(
                GRAPHQL_URL,
                json={"query": LOTS_QUERY, "variables": variables},
                headers=HEADERS,
            )
            resp.raise_for_status()
            data = resp.json()
    except Exception as e:
        print(f"Enchères API error: {e}")
        return []

    lots_data = data.get("data", {}).get("lots", {})
    total = lots_data.get("total", 0)
    collection = lots_data.get("collection", [])

    biens = []
    for lot in collection:
        if not lot:
            continue

        mise_a_prix = lot.get("mise_a_prix") or 0
        prix_adjuge = lot.get("prix_adjuge")
        ville = (lot.get("adresse_defaut") or {}).get("ville", "")
        ville_slug = (lot.get("adresse_defaut") or {}).get("ville_slug", "")
        organisateur = (lot.get("organisateur") or {}).get("nom", "")
        evenement_titre = (lot.get("evenement") or {}).get("titre", "")

        # Date d'audience
        date_audience = None
        ouverture = lot.get("ouverture_date")
        if ouverture:
            date_audience = datetime.fromtimestamp(ouverture).isoformat()

        # Extraire surface depuis criteres_resume ou nom/description
        surface = None
        import re
        for text in [lot.get("criteres_resume", ""), lot.get("nom", ""), lot.get("description", "")]:
            if not text:
                continue
            # "74,62 m²" / "74.62 m2" / "74,62 m2"
            m = re.search(r"([\d]+[.,]?\d*)\s*m[²2]", text)
            if m:
                surface = float(m.group(1).replace(",", "."))
                break

        # URL du lot
        lot_id = lot.get("id", "")
        nom_slug = (lot.get("nom") or "lot").lower().replace(" ", "-")[:50]
        url = f"https://www.encheres-publiques.com/encheres/immobilier/{nom_slug}_{lot_id}"

        bien = {
            "source": "encheres_judiciaires",
            "url": url,
            "titre": lot.get("nom", ""),
            "prix": mise_a_prix,
            "prix_adjuge": prix_adjuge,
            "surface_bati": surface,
            "surface_terrain": 0,
            "commune": ville,
            "code_postal": "",  # Pas fourni, on dérivera du ville_slug
            "description": lot.get("description", ""),
            "date_audience": date_audience,
            "organisateur": organisateur,
            "evenement": evenement_titre,
            "type_vente": lot.get("type", ""),
            "sous_categorie": lot.get("sous_categorie", ""),
            "estimation_basse": lot.get("estimation_basse"),
            "estimation_haute": lot.get("estimation_haute"),
            "nb_vues": lot.get("nbr_vues", 0),
            "nb_suivis": lot.get("nbr_suivis", 0),
            "en_surenchere": lot.get("en_surenchere", False),
            "premiere_vue": datetime.now().isoformat(),
        }

        if bien["prix"] and surface and surface > 0:
            bien["prix_m2"] = round(bien["prix"] / surface, 2)

        # Les enchères sont par nature des biens en détresse
        bien["mots_cles_detresse"] = ["adjudication", "vente judiciaire", "enchères"]

        biens.append(bien)

    print(f"  Enchères publiques: {len(biens)} lots ({total} total), terminés={termine}")
    return biens


def fetch_upcoming(place: str = "indre") -> list[dict]:
    """Enchères à venir."""
    return fetch_lots(place=place, termine=False)


def fetch_past(place: str = "indre") -> list[dict]:
    """Enchères passées (pour analyse des prix adjugés)."""
    return fetch_lots(place=place, termine=True)


def fetch_all(place: str = "indre") -> list[dict]:
    """Toutes les enchères (à venir + passées)."""
    upcoming = fetch_upcoming(place)
    past = fetch_past(place)
    return upcoming + past


if __name__ == "__main__":
    print("=== Enchères publiques — Indre (36) ===\n")

    print("À venir:")
    upcoming = fetch_upcoming()
    for b in upcoming:
        print(f"  🔨 {b['prix']:>10,.0f}€ (mise à prix)  {b.get('surface_bati', '?')}m²  {b['commune']}")
        print(f"     {b['titre'][:80]}")
        print(f"     📅 {b.get('date_audience', 'N/A')}  — {b.get('organisateur', '')}")
        if b.get("estimation_basse"):
            print(f"     Estimation: {b['estimation_basse']:,.0f}€ - {b.get('estimation_haute', 0):,.0f}€")
        print()

    print("\nPassées (prix adjugés):")
    past = fetch_past()
    for b in past[:10]:
        adj = f"{b['prix_adjuge']:,.0f}€" if b.get("prix_adjuge") else "N/A"
        print(f"  ✓ Mise: {b['prix']:>10,.0f}€  Adjugé: {adj:>12s}  {b.get('surface_bati', '?')}m²  {b['commune']}")
