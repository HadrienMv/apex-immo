"""
DVF (Demandes de Valeurs Foncières) — API Cerema
Récupère les vraies transactions immobilières autour d'une commune.
"""
import httpx
from dataclasses import dataclass

BASE_URL = "https://apidf-preprod.cerema.fr/dvf_opendata/mutations/"

# Codes INSEE des communes clés du 36
COMMUNES_36 = {
    "36044": "Châteauroux",
    "36088": "Issoudun",
    "36046": "La Châtre",
    "36006": "Argenton-sur-Creuse",
    "36063": "Déols",
    "36018": "Le Blanc",
    "36005": "Ardentes",
    "36195": "Saint-Maur",
    "36101": "Levroux",
    "36026": "Buzançais",
}

# codtypbien mapping
TYPE_MAISON = "111"
TYPE_APPARTEMENT = "121"


@dataclass
class Transaction:
    date: str
    prix: float
    surface_bati: float
    surface_terrain: float
    prix_m2: float
    type_bien: str
    commune: str
    code_insee: str
    id_parcelle: str


def fetch_transactions(
    code_insee: str,
    annee_min: int = 2022,
    type_bien: str = TYPE_MAISON,
    max_pages: int = 10,
) -> list[Transaction]:
    """Récupère toutes les transactions pour une commune."""
    transactions = []
    params = {
        "code_insee": code_insee,
        "anneemut_min": annee_min,
        "codtypbien": type_bien,
        "ordering": "-datemut",
        "page_size": 50,
    }

    with httpx.Client(timeout=30) as client:
        for page in range(1, max_pages + 1):
            params["page"] = page
            resp = client.get(BASE_URL, params=params)
            resp.raise_for_status()
            data = resp.json()

            for m in data.get("results", []):
                prix = float(m.get("valeurfonc") or 0)
                surface = float(m.get("sbati") or 0)
                terrain = float(m.get("sterr") or 0)

                if surface > 0 and prix > 0:
                    transactions.append(Transaction(
                        date=m["datemut"],
                        prix=prix,
                        surface_bati=surface,
                        surface_terrain=terrain,
                        prix_m2=round(prix / surface, 2),
                        type_bien=m.get("libtypbien", ""),
                        commune=COMMUNES_36.get(code_insee, code_insee),
                        code_insee=code_insee,
                        id_parcelle=(m.get("l_idpar") or [""])[0],
                    ))

            if not data.get("next"):
                break

    return transactions


def get_median_prix_m2(code_insee: str, annee_min: int = 2022) -> dict:
    """Calcule la médiane prix/m² pour une commune."""
    txns = fetch_transactions(code_insee, annee_min)
    if not txns:
        return {"commune": COMMUNES_36.get(code_insee, code_insee), "median": None, "count": 0}

    prix_m2_list = sorted(t.prix_m2 for t in txns)
    n = len(prix_m2_list)
    median = prix_m2_list[n // 2] if n % 2 else (prix_m2_list[n // 2 - 1] + prix_m2_list[n // 2]) / 2

    return {
        "commune": COMMUNES_36.get(code_insee, code_insee),
        "code_insee": code_insee,
        "median_prix_m2": round(median, 2),
        "mean_prix_m2": round(sum(prix_m2_list) / n, 2),
        "min_prix_m2": prix_m2_list[0],
        "max_prix_m2": prix_m2_list[-1],
        "nb_transactions": n,
        "periode": f"{annee_min}-now",
    }


def scan_all_communes(annee_min: int = 2022) -> list[dict]:
    """Scan toutes les communes clés du 36."""
    results = []
    for code_insee in COMMUNES_36:
        try:
            stats = get_median_prix_m2(code_insee, annee_min)
            results.append(stats)
            print(f"  ✓ {stats['commune']}: {stats['nb_transactions']} transactions, médiane {stats.get('median_prix_m2', 'N/A')}€/m²")
        except Exception as e:
            print(f"  ✗ {COMMUNES_36[code_insee]}: {e}")
    return results


if __name__ == "__main__":
    print("=== Scan DVF département 36 ===\n")
    results = scan_all_communes(2023)
    print("\n=== Résumé ===")
    for r in sorted(results, key=lambda x: x.get("median_prix_m2") or 0):
        if r.get("median_prix_m2"):
            print(f"  {r['commune']:25s} — {r['median_prix_m2']:>8.0f}€/m²  ({r['nb_transactions']} ventes)")
