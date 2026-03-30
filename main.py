"""
Apex Scanner — Orchestrateur principal.

Scrape → Enrichit DVF (fin, géolocalisé) → Score → Store → Digest
"""
import sys
from dotenv import load_dotenv
load_dotenv()

from enrichment.dvf import get_median_prix_m2, COMMUNES_36, fetch_transactions
from enrichment.dvf_geo import fine_grain_analysis, fetch_geo_transactions, median_in_radius
from enrichment.address_extract import geocode_best_effort
from enrichment.scorer import evaluate, score_detresse
from scraper.leboncoin import search_distressed
from scraper.seloger import search_all_pages as search_bienici
from scraper.facebook import search_fb_marketplace_direct
from scraper.encheres import fetch_all as fetch_encheres
from scraper.notaires import fetch_all as fetch_notaires
from storage.db import upsert_bien, get_biens, get_stats, cache_dvf, get_dvf_cache


# Mapping code postal → code INSEE (principales communes du 36)
CP_TO_INSEE = {
    "36000": "36044",  # Châteauroux
    "36100": "36088",  # Issoudun
    "36400": "36046",  # La Châtre
    "36200": "36006",  # Argenton-sur-Creuse
    "36130": "36063",  # Déols
    "36300": "36018",  # Le Blanc
    "36120": "36005",  # Ardentes
    "36250": "36195",  # Saint-Maur
    "36110": "36101",  # Levroux
    "36500": "36026",  # Buzançais
}


def resolve_code_insee(bien: dict) -> str:
    """Résout le code INSEE par code postal, puis par nom de commune via API geo."""
    # 1. Code postal connu
    cp = bien.get("code_postal", "")
    if cp and cp in CP_TO_INSEE:
        return CP_TO_INSEE[cp]

    # 2. Code INSEE déjà renseigné
    if bien.get("code_insee"):
        return bien["code_insee"]

    # 3. Recherche par nom de commune via geo.api.gouv.fr
    commune = bien.get("commune", "").strip()
    if commune:
        try:
            import httpx
            resp = httpx.get(
                "https://geo.api.gouv.fr/communes",
                params={"nom": commune, "codeDepartement": "36", "fields": "code", "limit": 1},
                timeout=5,
            )
            if resp.status_code == 200:
                results = resp.json()
                if results:
                    return results[0]["code"]
        except Exception:
            pass

    # 4. Code postal hors mapping mais dans le 36
    if cp and cp.startswith("36"):
        try:
            import httpx
            resp = httpx.get(
                "https://geo.api.gouv.fr/communes",
                params={"codePostal": cp, "fields": "code", "limit": 1},
                timeout=5,
            )
            if resp.status_code == 200:
                results = resp.json()
                if results:
                    return results[0]["code"]
        except Exception:
            pass

    return ""


def get_dvf_for_commune(code_insee: str, annee_min: int = 2022) -> dict:
    """Get DVF stats with caching."""
    cached = get_dvf_cache(code_insee, annee_min)
    if cached:
        return cached
    stats = get_median_prix_m2(code_insee, annee_min)
    if stats.get("median_prix_m2"):
        cache_dvf(code_insee, annee_min, stats)
    return stats


def resolve_location(bien: dict) -> dict:
    """
    Résout la position GPS du bien en combinant :
    1. Coords de la carte de l'annonce (LBC/BienIci) — priorité max
    2. Extraction d'adresse du texte → géocodage
    3. Centre de la commune — fallback
    """
    lat = bien.get("lat")
    lon = bien.get("lon")
    precision = bien.get("geo_precision")

    # 1. Coords de l'annonce déjà disponibles
    if lat and lon:
        return {"lat": lat, "lon": lon, "precision": precision or "annonce_approx"}

    # 2. Extraction d'adresse du texte + géocodage
    texte = f"{bien.get('titre', '')} {bien.get('description', '')}"
    commune = bien.get("commune", "")
    cp = bien.get("code_postal", "")

    geo = geocode_best_effort(texte, commune, cp)
    if geo and geo.get("lat"):
        return {
            "lat": geo["lat"],
            "lon": geo["lon"],
            "precision": geo["precision"],
            "geo_label": geo.get("label", ""),
        }

    # 3. Fallback commune
    from enrichment.dvf_geo import geocode_commune
    code_insee = bien.get("code_insee") or CP_TO_INSEE.get(cp, "")
    if code_insee:
        coords = geocode_commune(code_insee)
        if coords:
            return {"lat": coords[0], "lon": coords[1], "precision": "commune"}

    return {"lat": None, "lon": None, "precision": None}


# Cache des transactions géo par commune pour éviter de re-fetcher
_geo_txn_cache: dict[str, list] = {}


def _is_terrain(bien: dict) -> bool:
    """Détecte si un bien est un terrain (pas une maison/appart)."""
    # Notaires : type explicite
    if bien.get("type_bien_code") == "TER":
        return True
    # URL contient /terrain/
    url = bien.get("url", "").lower()
    if "/terrain/" in url:
        return True
    # Mots-clés terrain dans titre/description
    titre = (bien.get("titre", "") + " " + bien.get("description", "")).lower()
    terrain_kw = ["terrain", "parcelle", "terre agricole", "bois", "pré ", "prairie", "terrain constructible", "terrain à bâtir"]
    if any(kw in titre for kw in terrain_kw) and not any(kw in titre for kw in ["maison", "pavillon", "villa", "appartement"]):
        return True
    # Prix/m² < 50€ = terrain (une maison même pourrie c'est > 100€/m²)
    surface = bien.get("surface_bati", 0) or 0
    prix = bien.get("prix", 0) or 0
    if surface > 0 and prix > 0:
        prix_m2 = prix / surface
        if prix_m2 < 50:
            return True
    return False


def enrich_and_score(bien: dict, use_geo: bool = True) -> dict:
    """Enrichit un bien avec DVF (fin si possible) et calcule le score."""
    # Skip scoring for past auctions (reference only)
    if bien.get("verdict") == "reference_passee":
        return bien

    # Skip terrains — on cherche du bâti
    if _is_terrain(bien):
        bien["verdict"] = "terrain"
        bien["score_arbitrage"] = 0
        bien["score_detresse"] = 0
        return bien

    code_insee = resolve_code_insee(bien)
    bien["code_insee"] = code_insee

    if not code_insee:
        bien["verdict"] = "ignorer"
        bien["score_arbitrage"] = 0
        bien["score_detresse"] = 0
        return bien

    # Résoudre la position GPS (combine carte annonce + extraction texte + commune)
    loc = resolve_location(bien)
    bien["lat"] = loc.get("lat")
    bien["lon"] = loc.get("lon")
    bien["geo_precision"] = loc.get("precision")
    if loc.get("geo_label"):
        bien["geo_label"] = loc["geo_label"]

    # DVF enrichment — granularité fine si on a un GPS
    median = None
    dvf_detail = {}

    if use_geo and bien.get("lat") and bien.get("lon"):
        try:
            # Cache les transactions géo par commune
            if code_insee not in _geo_txn_cache:
                _geo_txn_cache[code_insee] = fetch_geo_transactions(code_insee, annee_min=2022)
            txns = _geo_txn_cache[code_insee]

            # Analyse par rayons concentriques autour de la position du bien
            for radius_km, label in [(0.5, "r500m"), (1.0, "r1km"), (2.0, "r2km")]:
                r = median_in_radius(txns, bien["lat"], bien["lon"], radius_km)
                if r.get("count", 0) >= 5 and r.get("median_prix_m2"):
                    median = r["median_prix_m2"]
                    dvf_detail = {
                        "dvf_rayon": label,
                        "dvf_nb_local": r["count"],
                        "dvf_min_m2": r.get("min_prix_m2"),
                        "dvf_max_m2": r.get("max_prix_m2"),
                        "dvf_mean_m2": r.get("mean_prix_m2"),
                        "dvf_dernieres_ventes": r.get("transactions", [])[:5],
                    }
                    break
        except Exception as e:
            print(f"    ⚠ DVF géo error: {e}")

    # Fallback sur médiane commune
    if not median:
        dvf = get_dvf_for_commune(code_insee)
        median = dvf.get("median_prix_m2", 0)
        dvf_detail["dvf_rayon"] = "commune"
        dvf_detail["dvf_nb_local"] = dvf.get("nb_transactions", 0)

    bien["dvf_median_m2"] = median
    bien["dvf_nb_transactions"] = dvf_detail.get("dvf_nb_local", 0)
    bien.update(dvf_detail)

    if not median or not bien.get("prix") or not bien.get("surface_bati"):
        bien["verdict"] = "ignorer"
        bien["score_arbitrage"] = 0
        s, mots = score_detresse(bien.get("description", "") + " " + bien.get("titre", ""))
        bien["score_detresse"] = s
        bien["mots_cles_detresse"] = mots
        return bien

    # Pour les enchères : utiliser prix_adjuge si disponible, sinon mise à prix
    prix_score = bien["prix"]
    is_enchere = bien.get("source") == "encheres_judiciaires"
    if is_enchere:
        if bien.get("prix_adjuge"):
            # Vente passée : on a le vrai prix
            prix_score = bien["prix_adjuge"]
            bien["prix_type"] = "adjuge"
        else:
            # Vente à venir : mise à prix = plancher, prix final sera ~2-3x plus haut
            # On estime le prix final à 2.5x la mise à prix pour le scoring
            prix_score = bien["prix"] * 2.5
            bien["prix_type"] = "mise_a_prix"
            bien["prix_estime_adjudication"] = prix_score

    # Score
    result = evaluate(
        prix=prix_score,
        surface=bien["surface_bati"],
        description=bien.get("description", "") + " " + bien.get("titre", ""),
        dvf_median_m2=median,
    )

    bien["score_detresse"] = result.score_detresse
    bien["score_arbitrage"] = result.score_arbitrage
    bien["verdict"] = result.verdict
    bien["mots_cles_detresse"] = result.mots_trouves
    bien["prix_m2"] = result.prix_m2_demande
    bien["ecart_dvf_pct"] = result.ecart_pct
    bien["marge_estimee_pct"] = result.marge_estimee_pct

    # Pour les enchères, ajouter le contexte
    if is_enchere and not bien.get("prix_adjuge"):
        bien["note_scoring"] = f"Mise à prix {bien['prix']:,.0f}€ — score basé sur estimation adjudication ~{prix_score:,.0f}€ (2.5x)"

    return bien


def scrape_all_sources(max_prix: int = 150000, min_surface: int = 50) -> list[dict]:
    """Scrape toutes les sources et déduplique."""
    all_biens = []

    # LeBonCoin (multi-stratégie: curl_cffi → Playwright → Google)
    print("  [LeBonCoin]...")
    try:
        lbc = search_distressed(max_prix=max_prix, min_surface=min_surface)
        all_biens.extend(lbc)
        print(f"    → {len(lbc)} annonces")
    except Exception as e:
        print(f"    ✗ Erreur: {e}")

    # BienIci / SeLoger (via Playwright + proxy)
    print("  [BienIci/SeLoger]...")
    try:
        bi = search_bienici(max_prix=max_prix, min_surface=min_surface)
        all_biens.extend(bi)
        print(f"    → {len(bi)} annonces")
    except Exception as e:
        print(f"    ✗ Erreur: {e}")

    # Notaires (immobilier.notaires.fr)
    print("  [Notaires]...")
    try:
        notaires = fetch_notaires("36")
        all_biens.extend(notaires)
        print(f"    → {len(notaires)} annonces")
    except Exception as e:
        print(f"    ✗ Erreur: {e}")

    # Enchères publiques (ventes judiciaires)
    print("  [Enchères judiciaires]...")
    try:
        from scraper.encheres import fetch_upcoming, fetch_past
        upcoming = fetch_upcoming(place="indre")
        all_biens.extend(upcoming)
        print(f"    → {len(upcoming)} lots à venir (scorés)")

        # Passées = référence uniquement, on les stocke mais ne les score pas
        past = fetch_past(place="indre")
        for b in past:
            b["statut"] = "vendu"
            b["verdict"] = "reference_passee"
        all_biens.extend(past)
        print(f"    + {len(past)} lots passés (référence)")
    except Exception as e:
        print(f"    ✗ Erreur: {e}")

    # Déduplicate (par titre+prix combo si pas d'URL unique)
    seen = set()
    unique = []
    for b in all_biens:
        key = b.get("url") or f"{b.get('titre', '')}-{b.get('prix', '')}"
        if key not in seen:
            seen.add(key)
            unique.append(b)

    print(f"  Total unique: {len(unique)} biens")
    return unique


def run_pipeline(use_geo: bool = True):
    """Pipeline complet : scrape → enrich → score → store."""
    print("=" * 60)
    print("  APEX SCANNER — Pipeline complet")
    print("=" * 60)

    # 1. Scrape toutes les sources
    print("\n[1/3] Scraping multi-sources...")
    biens = scrape_all_sources()

    if not biens:
        print("  Aucune annonce trouvée. Fin.")
        return

    # 2. Enrich + Score
    print(f"\n[2/3] Enrichissement DVF {'géolocalisé' if use_geo else 'commune'} + Scoring...")
    for i, bien in enumerate(biens):
        bien = enrich_and_score(bien, use_geo=use_geo)
        biens[i] = bien
        verdict_icon = {"arbitrage_evident": "🟢", "a_surveiller": "🟡", "ignorer": "⚪"}.get(bien["verdict"], "?")
        rayon = bien.get("dvf_rayon", "?")
        prix = bien.get("prix") or 0
        surf = bien.get("surface_bati") or 0
        print(f"  {verdict_icon} {prix:>10,.0f}€  {surf:>5.0f}m²  "
              f"score={bien.get('score_arbitrage', 0):>3}  DVF@{rayon}  {bien.get('commune', '?')}")

    # 3. Store
    print("\n[3/3] Stockage MongoDB...")
    stored = 0
    for bien in biens:
        try:
            upsert_bien(bien)
            stored += 1
        except Exception as e:
            print(f"  ✗ Store error: {e}")
    print(f"  {stored}/{len(biens)} stockés")

    # Summary
    print_summary()


def print_summary():
    stats = get_stats()
    print(f"\n{'=' * 60}")
    print(f"  RÉSUMÉ")
    print(f"  Total en base : {stats['total']}")
    print(f"  Par verdict   : {stats['by_verdict']}")
    print(f"  Par source    : {stats['by_source']}")

    top = get_biens(verdict="arbitrage_evident", limit=10)
    if top:
        print(f"\n  🟢 TOP ARBITRAGES ({len(top)}) :")
        for b in top:
            ecart = b.get("ecart_dvf_pct", 0)
            marge = b.get("marge_estimee_pct", 0)
            print(f"    {b.get('prix', 0):>10,.0f}€  {b.get('surface_bati', '?'):>5}m²  "
                  f"{b.get('prix_m2', 0):>6,.0f}€/m²  (DVF: {b.get('dvf_median_m2', 0):,.0f}€/m²  "
                  f"écart: {ecart:+.0f}%  marge: {marge:.0f}%)  "
                  f"[{b.get('source', '')}] {b.get('commune', '')}")
            print(f"      → {b.get('url', '')}")
            if b.get("mots_cles_detresse"):
                print(f"      🚩 {', '.join(b['mots_cles_detresse'])}")

    surveiller = get_biens(verdict="a_surveiller", limit=5)
    if surveiller:
        print(f"\n  🟡 À SURVEILLER ({len(surveiller)}) :")
        for b in surveiller:
            print(f"    {b.get('prix', 0):>10,.0f}€  {b.get('surface_bati', '?'):>5}m²  "
                  f"{b.get('prix_m2', 0):>6,.0f}€/m²  [{b.get('source', '')}] {b.get('commune', '')}")
    print()


def scan_dvf_only():
    """Scan DVF uniquement — état du marché."""
    print("=" * 60)
    print("  APEX SCANNER — Scan DVF Indre (36)")
    print("=" * 60)
    from enrichment.dvf import scan_all_communes
    results = scan_all_communes(2023)
    print(f"\nDone. {len(results)} communes scannées.")


def quick_check(url_or_text: str, prix: float = None, surface: float = None, code_postal: str = "36000"):
    """Check rapide d'un bien — colle une annonce ou entre les chiffres."""
    print("=" * 60)
    print("  APEX SCANNER — Quick Check")
    print("=" * 60)

    bien = {
        "source": "manual",
        "url": url_or_text[:200] if url_or_text.startswith("http") else "",
        "titre": url_or_text[:100],
        "description": url_or_text,
        "prix": prix,
        "surface_bati": surface,
        "code_postal": code_postal,
    }

    bien = enrich_and_score(bien, use_geo=True)

    verdict_icon = {"arbitrage_evident": "🟢", "a_surveiller": "🟡", "ignorer": "⚪"}.get(bien["verdict"], "?")
    print(f"\n  Verdict: {verdict_icon} {bien['verdict'].upper()}")
    print(f"  Prix demandé:  {bien.get('prix', 0):>10,.0f}€  ({bien.get('prix_m2', 0):,.0f}€/m²)")
    print(f"  DVF médiane:   {bien.get('dvf_median_m2', 0):>10,.0f}€/m²  (rayon: {bien.get('dvf_rayon', '?')}, {bien.get('dvf_nb_transactions', 0)} ventes)")
    print(f"  Écart DVF:     {bien.get('ecart_dvf_pct', 0):>+10.1f}%")
    print(f"  Marge estimée: {bien.get('marge_estimee_pct', 0):>10.1f}%")
    print(f"  Score arb.:    {bien.get('score_arbitrage', 0):>10}/100")
    print(f"  Score détresse:{bien.get('score_detresse', 0):>10}/100")
    if bien.get("mots_cles_detresse"):
        print(f"  Mots détresse: {', '.join(bien['mots_cles_detresse'])}")
    if bien.get("dvf_dernieres_ventes"):
        print(f"\n  Dernières ventes DVF à proximité:")
        for v in bien["dvf_dernieres_ventes"]:
            print(f"    {v['date']}  {v['prix']:>10,.0f}€  {v['surface']:>5}m²  = {v['prix_m2']:>6,.0f}€/m²")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "dvf":
            scan_dvf_only()
        elif cmd == "check":
            # python main.py check 65000 80 36000 "Maison à rénover succession"
            prix = float(sys.argv[2]) if len(sys.argv) > 2 else None
            surface = float(sys.argv[3]) if len(sys.argv) > 3 else None
            cp = sys.argv[4] if len(sys.argv) > 4 else "36000"
            text = sys.argv[5] if len(sys.argv) > 5 else ""
            quick_check(text, prix=prix, surface=surface, code_postal=cp)
        elif cmd == "summary":
            print_summary()
        elif cmd == "fast":
            run_pipeline(use_geo=False)
        elif cmd == "digest":
            from notifier.digest import send_digest
            send_digest()
        elif cmd == "cron":
            # Pipeline + digest — pour le crontab
            run_pipeline(use_geo=True)
            from notifier.digest import send_digest
            send_digest()
        else:
            print(f"Usage: python main.py [dvf|check|summary|fast|digest|cron]")
    else:
        run_pipeline(use_geo=True)
