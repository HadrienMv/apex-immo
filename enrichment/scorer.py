"""
Scoring engine — évalue chaque bien sur 2 axes :
1. Score de détresse vendeur (0-100)
2. Score d'arbitrage (0-100) basé sur DVF réel
"""
import re
from dataclasses import dataclass

# Mots-clés de détresse avec poids
MOTS_DETRESSE = {
    # Urgence
    "urgent": 15, "urgence": 15, "vite": 10, "rapidement": 10, "rapide": 8,
    # Succession / divorce
    "succession": 20, "indivision": 20, "divorce": 15, "séparation": 15, "partage": 10,
    # État du bien
    "à rénover": 12, "a renover": 12, "travaux": 8, "rafraîchir": 6,
    "gros travaux": 15, "toiture": 10, "charpente": 10,
    # Motivation vendeur
    "cause départ": 12, "cause mutation": 12, "déménagement": 8,
    "retraite": 5, "santé": 10, "âge": 5,
    # Prix
    "prix en baisse": 15, "baisse de prix": 15, "négociable": 10,
    "faire offre": 12, "à saisir": 10, "bonne affaire": 8, "prix cassé": 15,
    # Vente judiciaire
    "adjudication": 20, "vente judiciaire": 20, "saisie": 18,
    "liquidation": 18, "enchère": 15, "enchères": 15,
}


@dataclass
class ScoreResult:
    score_detresse: int
    score_arbitrage: int
    verdict: str  # arbitrage_evident, a_surveiller, ignorer
    mots_trouves: list[str]
    prix_m2_demande: float
    dvf_median_m2: float
    ecart_pct: float  # % en dessous de la médiane DVF
    cout_total_m2: float  # prix + travaux estimés
    marge_estimee_pct: float


def score_detresse(texte: str) -> tuple[int, list[str]]:
    """Score de détresse du vendeur basé sur les mots-clés."""
    texte_lower = texte.lower()
    score = 0
    mots = []
    for mot, poids in MOTS_DETRESSE.items():
        if mot in texte_lower:
            score += poids
            mots.append(mot)
    return min(score, 100), mots


def score_arbitrage(
    prix_demande: float,
    surface: float,
    dvf_median_m2: float,
    cout_travaux_m2: float = 0,
) -> tuple[int, float, float, float]:
    """
    Score d'arbitrage basé sur l'écart prix demandé vs marché DVF.

    Règle Apex : prix achat + travaux < 900€/m² = bonne affaire
    Revente estimée : 1200-1350€/m²
    """
    if surface <= 0 or dvf_median_m2 <= 0:
        return 0, 0, 0, 0

    prix_m2 = prix_demande / surface
    cout_total_m2 = prix_m2 + cout_travaux_m2

    # Écart par rapport à la médiane DVF
    ecart_pct = ((dvf_median_m2 - prix_m2) / dvf_median_m2) * 100

    # Marge estimée (revente à la médiane DVF ou 1200€/m² min)
    prix_revente_m2 = max(dvf_median_m2, 1200)
    marge_pct = ((prix_revente_m2 - cout_total_m2) / cout_total_m2) * 100

    # Score 0-100
    score = 0

    # Composante prix/m² vs médiane (0-50 points)
    if ecart_pct >= 50:
        score += 50
    elif ecart_pct >= 30:
        score += 40
    elif ecart_pct >= 20:
        score += 30
    elif ecart_pct >= 10:
        score += 20
    elif ecart_pct >= 0:
        score += 10

    # Composante coût total vs seuil 900€/m² (0-30 points)
    if cout_total_m2 <= 500:
        score += 30
    elif cout_total_m2 <= 700:
        score += 25
    elif cout_total_m2 <= 900:
        score += 15
    elif cout_total_m2 <= 1000:
        score += 5

    # Composante marge (0-20 points)
    if marge_pct >= 50:
        score += 20
    elif marge_pct >= 30:
        score += 15
    elif marge_pct >= 20:
        score += 10
    elif marge_pct >= 10:
        score += 5

    return min(score, 100), ecart_pct, cout_total_m2, marge_pct


def evaluate(
    prix: float,
    surface: float,
    description: str,
    dvf_median_m2: float,
    cout_travaux_m2: float = 300,  # estimation par défaut pour "à rénover"
) -> ScoreResult:
    """Évaluation complète d'un bien."""
    s_detresse, mots = score_detresse(description)

    # Ajuster coût travaux selon mots-clés
    if any(m in ["gros travaux", "toiture", "charpente"] for m in mots):
        cout_travaux_m2 = max(cout_travaux_m2, 500)
    elif any(m in ["à rénover", "a renover", "travaux"] for m in mots):
        cout_travaux_m2 = max(cout_travaux_m2, 300)
    elif any(m in ["rafraîchir"] for m in mots):
        cout_travaux_m2 = max(cout_travaux_m2, 150)
    else:
        cout_travaux_m2 = 100  # cosmétique seulement

    s_arb, ecart, cout_total, marge = score_arbitrage(prix, surface, dvf_median_m2, cout_travaux_m2)

    # Verdict
    score_combine = (s_detresse * 0.3) + (s_arb * 0.7)
    if score_combine >= 60:
        verdict = "arbitrage_evident"
    elif score_combine >= 35:
        verdict = "a_surveiller"
    else:
        verdict = "ignorer"

    return ScoreResult(
        score_detresse=s_detresse,
        score_arbitrage=s_arb,
        verdict=verdict,
        mots_trouves=mots,
        prix_m2_demande=round(prix / surface, 2) if surface > 0 else 0,
        dvf_median_m2=dvf_median_m2,
        ecart_pct=round(ecart, 1),
        cout_total_m2=round(cout_total, 2),
        marge_estimee_pct=round(marge, 1),
    )
