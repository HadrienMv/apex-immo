"""
Facebook Marketplace scraper — via recherche web.

Facebook n'a pas d'API publique pour Marketplace. Deux approches :
1. Scraper les résultats Google avec site:facebook.com/marketplace
2. Selenium/Playwright pour naviguer directement (nécessite un compte)

On commence par l'approche Google qui ne nécessite pas de login.
"""
import httpx
from bs4 import BeautifulSoup
from datetime import datetime
import re
import time

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "fr-FR,fr;q=0.9",
}


def search_google_marketplace(
    query: str = "maison à vendre Indre 36",
    max_results: int = 20,
) -> list[dict]:
    """
    Scrape Google pour trouver des annonces Facebook Marketplace.
    Retourne les URLs et titres trouvés.
    """
    search_query = f"site:facebook.com/marketplace {query}"
    params = {
        "q": search_query,
        "num": max_results,
        "hl": "fr",
        "gl": "fr",
    }

    try:
        with httpx.Client(timeout=15, follow_redirects=True) as client:
            resp = client.get("https://www.google.com/search", params=params, headers=HEADERS)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
    except Exception as e:
        print(f"Google search error: {e}")
        return []

    results = []
    for g in soup.select("div.g, div[data-sokoban-container]"):
        link = g.select_one("a[href*='facebook.com/marketplace']")
        if not link:
            continue

        url = link.get("href", "")
        title = link.get_text(strip=True)

        # Try to extract price from snippet
        snippet = g.get_text(" ", strip=True)
        prix_match = re.search(r"(\d[\d\s]*)\s*€", snippet)
        prix = None
        if prix_match:
            prix = float(prix_match.group(1).replace(" ", ""))

        results.append({
            "source": "facebook",
            "url": url,
            "titre": title,
            "prix": prix,
            "description": snippet[:500],
            "premiere_vue": datetime.now().isoformat(),
        })

    return results


def search_fb_marketplace_direct(
    location: str = "chateauroux",
    max_prix: int = 150000,
    min_prix: int = 10000,
    category: str = "propertyrentals",  # propertyrentals covers both sale and rent on FB
) -> list[dict]:
    """
    Accès direct à Facebook Marketplace (nécessite cookies/session).
    À implémenter avec Playwright quand on passe sur EC2.

    Pour l'instant, retourne les résultats Google.
    """
    queries = [
        f"maison à vendre {location} Indre",
        f"maison succession {location}",
        f"maison travaux {location} 36",
    ]

    all_results = []
    seen_urls = set()

    for q in queries:
        results = search_google_marketplace(q)
        for r in results:
            if r["url"] not in seen_urls:
                seen_urls.add(r["url"])
                all_results.append(r)
        time.sleep(2)  # Respect Google rate limit

    return all_results


if __name__ == "__main__":
    print("=== Recherche Facebook Marketplace — Indre 36 ===")
    results = search_fb_marketplace_direct()
    print(f"\n{len(results)} résultats trouvés")
    for r in results[:10]:
        prix_str = f"{r['prix']:,.0f}€" if r.get("prix") else "N/A"
        print(f"  {prix_str:>12s}  {r['titre'][:60]}")
        print(f"    → {r['url']}")
