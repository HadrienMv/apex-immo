"""
Digest email quotidien — shortlist des meilleurs biens scorés.
Utilise Gmail SMTP (nécessite un mot de passe d'application).
"""
import smtplib
import os
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from storage.db import get_biens, get_stats


SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")  # Gmail app password
EMAIL_TO = os.getenv("EMAIL_TO", "mauve.hadrien@gmail.com")
EMAIL_FROM = os.getenv("EMAIL_FROM", SMTP_USER)


def build_digest_html() -> tuple[str, str]:
    """Génère le contenu HTML du digest. Retourne (subject, html)."""
    stats = get_stats()
    top = get_biens(verdict="arbitrage_evident", limit=15)
    surveiller = get_biens(verdict="a_surveiller", limit=10)

    date = datetime.now().strftime("%d/%m/%Y")

    # Subject
    nb_top = len(top)
    subject = f"🏠 Apex Scanner — {nb_top} arbitrages détectés ({date})"

    # HTML
    rows_top = ""
    for b in top:
        prix = b.get("prix", 0) or 0
        surf = b.get("surface_bati", 0) or 0
        pm2 = b.get("prix_m2", 0) or 0
        dvf = b.get("dvf_median_m2", 0) or 0
        ecart = b.get("ecart_dvf_pct", 0) or 0
        marge = b.get("marge_estimee_pct", 0) or 0
        commune = b.get("commune", "?")
        source = b.get("source", "?")
        url = b.get("url", "#")
        mots = ", ".join(b.get("mots_cles_detresse", []))
        rayon = b.get("dvf_rayon", "?")
        note = b.get("note_scoring", "")

        rows_top += f"""
        <tr style="border-bottom:1px solid #eee;">
            <td style="padding:8px;"><a href="{url}" style="color:#2563eb;text-decoration:none;font-weight:bold;">{commune}</a><br>
                <small style="color:#666;">[{source}] {rayon}</small>
                {f'<br><small style="color:#888;">{note}</small>' if note else ''}
            </td>
            <td style="padding:8px;text-align:right;font-weight:bold;">{prix:,.0f}€</td>
            <td style="padding:8px;text-align:right;">{surf:.0f}m²</td>
            <td style="padding:8px;text-align:right;">{pm2:,.0f}€</td>
            <td style="padding:8px;text-align:right;color:#059669;">{dvf:,.0f}€</td>
            <td style="padding:8px;text-align:right;color:#059669;font-weight:bold;">+{ecart:.0f}%</td>
            <td style="padding:8px;text-align:right;">{marge:.0f}%</td>
            <td style="padding:8px;"><small style="color:#dc2626;">{mots}</small></td>
        </tr>"""

    rows_watch = ""
    for b in surveiller:
        prix = b.get("prix", 0) or 0
        surf = b.get("surface_bati", 0) or 0
        pm2 = b.get("prix_m2", 0) or 0
        commune = b.get("commune", "?")
        url = b.get("url", "#")
        source = b.get("source", "?")

        rows_watch += f"""
        <tr style="border-bottom:1px solid #eee;">
            <td style="padding:6px;"><a href="{url}" style="color:#2563eb;">{commune}</a> <small>[{source}]</small></td>
            <td style="padding:6px;text-align:right;">{prix:,.0f}€</td>
            <td style="padding:6px;text-align:right;">{surf:.0f}m²</td>
            <td style="padding:6px;text-align:right;">{pm2:,.0f}€/m²</td>
        </tr>"""

    html = f"""
    <html>
    <body style="font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,sans-serif;max-width:900px;margin:0 auto;padding:20px;color:#1a1a1a;">
        <h1 style="color:#1e40af;margin-bottom:5px;">Apex Scanner — Digest {date}</h1>
        <p style="color:#666;margin-top:0;">
            {stats['total']} biens en base |
            {stats['by_verdict'].get('arbitrage_evident', 0)} arbitrages |
            {stats['by_verdict'].get('a_surveiller', 0)} à surveiller |
            Sources: {', '.join(f'{k}: {v}' for k, v in stats.get('by_source', {}).items())}
        </p>

        <h2 style="color:#059669;">🟢 Arbitrages évidents ({len(top)})</h2>
        <table style="width:100%;border-collapse:collapse;font-size:14px;">
            <thead>
                <tr style="background:#f1f5f9;border-bottom:2px solid #cbd5e1;">
                    <th style="padding:8px;text-align:left;">Lieu</th>
                    <th style="padding:8px;text-align:right;">Prix</th>
                    <th style="padding:8px;text-align:right;">Surface</th>
                    <th style="padding:8px;text-align:right;">€/m²</th>
                    <th style="padding:8px;text-align:right;">DVF</th>
                    <th style="padding:8px;text-align:right;">Écart</th>
                    <th style="padding:8px;text-align:right;">Marge</th>
                    <th style="padding:8px;text-align:left;">Signaux</th>
                </tr>
            </thead>
            <tbody>{rows_top}</tbody>
        </table>

        <h2 style="color:#d97706;margin-top:30px;">🟡 À surveiller ({len(surveiller)})</h2>
        <table style="width:100%;border-collapse:collapse;font-size:14px;">
            <thead>
                <tr style="background:#f1f5f9;border-bottom:2px solid #cbd5e1;">
                    <th style="padding:6px;text-align:left;">Lieu</th>
                    <th style="padding:6px;text-align:right;">Prix</th>
                    <th style="padding:6px;text-align:right;">Surface</th>
                    <th style="padding:6px;text-align:right;">€/m²</th>
                </tr>
            </thead>
            <tbody>{rows_watch}</tbody>
        </table>

        <p style="color:#999;font-size:12px;margin-top:30px;">
            Apex Scanner — scraping automatique notaires.fr + enchères-publiques.com<br>
            DVF géolocalisé (rayon 500m-2km) via API Cerema
        </p>
    </body>
    </html>"""

    return subject, html


def send_digest():
    """Envoie le digest par email."""
    subject, html = build_digest_html()

    if not SMTP_USER or not SMTP_PASS:
        # Pas de SMTP configuré — print le digest
        print(f"\n📧 {subject}")
        print("(SMTP non configuré — configure SMTP_USER et SMTP_PASS dans .env)")
        print("Aperçu du digest envoyé à", EMAIL_TO)

        # Print version texte
        stats = get_stats()
        top = get_biens(verdict="arbitrage_evident", limit=10)
        print(f"\nTotal: {stats['total']} biens")
        print(f"Arbitrages: {stats['by_verdict'].get('arbitrage_evident', 0)}")
        print(f"À surveiller: {stats['by_verdict'].get('a_surveiller', 0)}")
        print("\nTOP:")
        for b in top:
            print(f"  {b.get('prix', 0):>10,.0f}€  {b.get('surface_bati', 0):>5.0f}m²  "
                  f"{b.get('commune', ''):20s}  {b.get('url', '')[:80]}")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = EMAIL_FROM
    msg["To"] = EMAIL_TO
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        print(f"✓ Digest envoyé à {EMAIL_TO}")
        return True
    except Exception as e:
        print(f"✗ Erreur envoi email: {e}")
        return False


if __name__ == "__main__":
    send_digest()
