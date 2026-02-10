"""
Système d'Alertes Intelligentes - Projet Amazon Webscraping
ISSEA MDSMS2 - 2025/2026

Gère les notifications par email pour :
- Baisses de prix significatives (>10%)
- Nouveaux produits ajoutés
- Produits avec excellent rapport qualité-prix
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import os
import logging
from typing import List, Dict
import pandas as pd

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration Email (Variables d'environnement)
SMTP_SERVER = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
SMTP_PORT = int(os.getenv('SMTP_PORT', '587'))
SMTP_USER = os.getenv('SMTP_USER', '')  # Email expéditeur
SMTP_PASSWORD = os.getenv('SMTP_PASSWORD', '')  # Mot de passe ou App Password
EMAIL_DESTINATAIRE = os.getenv('EMAIL_DESTINATAIRE', SMTP_USER)  # Email destinataire

# Seuils d'alerte
SEUIL_BAISSE_PRIX = float(os.getenv('SEUIL_BAISSE_PRIX', '10'))  # Pourcentage
SEUIL_RAPPORT_QUALITE_PRIX = float(os.getenv('SEUIL_RAPPORT_QP', '8.0'))  # Ratio


class AlerteManager:
    """Gestionnaire d'alertes intelligentes"""

    def __init__(self):
        """Initialise le gestionnaire d'alertes"""
        self.smtp_server = SMTP_SERVER
        self.smtp_port = SMTP_PORT
        self.smtp_user = SMTP_USER
        self.smtp_password = SMTP_PASSWORD
        self.email_dest = EMAIL_DESTINATAIRE

        # Vérifier configuration
        if not self.smtp_user or not self.smtp_password:
            logger.warning("⚠️ Configuration email incomplète - Alertes désactivées")
            self.enabled = False
        else:
            self.enabled = True
            logger.info(f"✅ Alertes email activées : {self.email_dest}")

    def envoyer_email(self, sujet: str, corps_html: str) -> bool:
        """
        Envoie un email d'alerte.

        Args:
            sujet: Sujet de l'email
            corps_html: Corps du message en HTML

        Returns:
            True si envoyé, False sinon
        """
        if not self.enabled:
            logger.info(f"📧 [SIMULATION] Email : {sujet}")
            return False

        try:
            # Créer le message
            msg = MIMEMultipart('alternative')
            msg['From'] = self.smtp_user
            msg['To'] = self.email_dest
            msg['Subject'] = sujet

            # Ajouter le corps HTML
            html_part = MIMEText(corps_html, 'html')
            msg.attach(html_part)

            # Connexion SMTP
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)

            logger.info(f"✅ Email envoyé : {sujet}")
            return True

        except Exception as e:
            logger.error(f"❌ Erreur envoi email : {e}")
            return False

    def verifier_et_alerter(self):
        """
        Vérifie les conditions d'alerte et envoie les notifications.

        Consulte la base de données pour détecter :
        1. Baisses de prix récentes
        2. Nouveaux produits ajoutés aujourd'hui
        3. Produits avec excellent rapport qualité-prix
        """
        from database import get_db

        try:
            db = get_db()

            # 1. Récupérer les alertes récentes (dernières 24h)
            df_alertes = db.get_alertes_recentes(limit=50)

            if len(df_alertes) > 0:
                self._envoyer_alerte_baisses_prix(df_alertes)

            # 2. Vérifier les nouveaux produits ajoutés aujourd'hui
            self._verifier_nouveaux_produits(db)

            # 3. Identifier les bonnes affaires (rapport qualité-prix élevé)
            self._verifier_bonnes_affaires(db)

        except Exception as e:
            logger.error(f"❌ Erreur vérification alertes : {e}")

    def _envoyer_alerte_baisses_prix(self, df_alertes: pd.DataFrame):
        """Envoie une alerte pour les baisses de prix"""
        # Filtrer uniquement les baisses de prix
        df_baisses = df_alertes[df_alertes['type_alerte'] == 'BAISSE_PRIX']

        if len(df_baisses) == 0:
            return

        # Générer le HTML
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                h2 {{ color: #d9534f; }}
                table {{ border-collapse: collapse; width: 100%; }}
                th {{ background-color: #d9534f; color: white; padding: 10px; text-align: left; }}
                td {{ border: 1px solid #ddd; padding: 8px; }}
                tr:nth-child(even) {{ background-color: #f2f2f2; }}
                .footer {{ margin-top: 20px; font-size: 12px; color: #777; }}
            </style>
        </head>
        <body>
            <h2>🚨 Alerte Baisses de Prix Amazon</h2>
            <p>Les produits suivants ont connu des baisses de prix significatives :</p>

            <table>
                <thead>
                    <tr>
                        <th>Message</th>
                        <th>Ancien Prix</th>
                        <th>Nouveau Prix</th>
                        <th>Date</th>
                    </tr>
                </thead>
                <tbody>
        """

        for _, row in df_baisses.iterrows():
            html += f"""
                    <tr>
                        <td>{row['message']}</td>
                        <td>${row['ancien_prix']:.2f}</td>
                        <td><strong>${row['nouveau_prix']:.2f}</strong></td>
                        <td>{row['date_alerte']}</td>
                    </tr>
            """

        html += """
                </tbody>
            </table>

            <div class="footer">
                <p>🤖 Généré automatiquement par le système de scraping Amazon - ISSEA MDSMS2</p>
            </div>
        </body>
        </html>
        """

        sujet = f"🚨 {len(df_baisses)} Baisse(s) de Prix Détectée(s) - Amazon"
        self.envoyer_email(sujet, html)

    def _verifier_nouveaux_produits(self, db):
        """Vérifie et alerte sur les nouveaux produits du jour"""
        # Requête pour compter les nouveaux produits d'aujourd'hui
        cursor = db.connection.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM produits
            WHERE DATE(date_scraping) = DATE('now')
        """ if db.db_type == 'sqlite' else """
            SELECT COUNT(*) FROM produits
            WHERE DATE(date_scraping) = CURRENT_DATE
        """)

        nb_nouveaux = cursor.fetchone()[0]

        if nb_nouveaux > 0:
            logger.info(f"📦 {nb_nouveaux} nouveaux produits ajoutés aujourd'hui")

            # Récupérer quelques exemples (top 5)
            df_nouveaux = pd.read_sql_query("""
                SELECT titre, prix, vote, categorie_prix
                FROM produits
                WHERE DATE(date_scraping) = DATE('now')
                ORDER BY rapport_qualite_prix DESC NULLS LAST
                LIMIT 5
            """ if db.db_type == 'sqlite' else """
                SELECT titre, prix, vote, categorie_prix
                FROM produits
                WHERE DATE(date_scraping) = CURRENT_DATE
                ORDER BY rapport_qualite_prix DESC NULLS LAST
                LIMIT 5
            """, db.connection)

            # Générer email
            html = f"""
            <html>
            <head>
                <style>
                    body {{ font-family: Arial, sans-serif; }}
                    h2 {{ color: #5cb85c; }}
                    .count {{ font-size: 24px; font-weight: bold; color: #5cb85c; }}
                    ul {{ list-style-type: none; padding: 0; }}
                    li {{ background: #f9f9f9; margin: 10px 0; padding: 10px; border-left: 4px solid #5cb85c; }}
                </style>
            </head>
            <body>
                <h2>📦 Nouveaux Produits Amazon</h2>
                <p class="count">{nb_nouveaux} nouveaux produits ajoutés aujourd'hui !</p>

                <h3>Top 5 (meilleur rapport qualité-prix) :</h3>
                <ul>
            """

            for _, row in df_nouveaux.iterrows():
                prix_str = f"${row['prix']:.2f}" if pd.notna(row['prix']) else "N/A"
                vote_str = f"{row['vote']:.1f}/5" if pd.notna(row['vote']) else "N/A"

                html += f"""
                    <li>
                        <strong>{row['titre'][:80]}...</strong><br>
                        Prix: {prix_str} | Note: {vote_str} | Catégorie: {row['categorie_prix']}
                    </li>
                """

            html += """
                </ul>
                <div style="margin-top: 20px; font-size: 12px; color: #777;">
                    <p>🤖 Alerte générée automatiquement - ISSEA MDSMS2</p>
                </div>
            </body>
            </html>
            """

            sujet = f"📦 {nb_nouveaux} Nouveaux Produits Amazon Détectés"
            self.envoyer_email(sujet, html)

    def _verifier_bonnes_affaires(self, db):
        """Identifie et alerte sur les produits avec excellent rapport qualité-prix"""
        # Requête pour trouver les produits avec rapport Q/P élevé
        df_deals = pd.read_sql_query(f"""
            SELECT titre, prix, vote, rapport_qualite_prix, categorie_prix
            FROM produits
            WHERE rapport_qualite_prix > {SEUIL_RAPPORT_QUALITE_PRIX}
              AND DATE(date_scraping) = DATE('now')
            ORDER BY rapport_qualite_prix DESC
            LIMIT 10
        """ if db.db_type == 'sqlite' else f"""
            SELECT titre, prix, vote, rapport_qualite_prix, categorie_prix
            FROM produits
            WHERE rapport_qualite_prix > {SEUIL_RAPPORT_QUALITE_PRIX}
              AND DATE(date_scraping) = CURRENT_DATE
            ORDER BY rapport_qualite_prix DESC
            LIMIT 10
        """, db.connection)

        if len(df_deals) == 0:
            return

        # Générer email
        html = """
        <html>
        <head>
            <style>
                body { font-family: Arial, sans-serif; }
                h2 { color: #f0ad4e; }
                table { border-collapse: collapse; width: 100%; }
                th { background-color: #f0ad4e; color: white; padding: 10px; text-align: left; }
                td { border: 1px solid #ddd; padding: 8px; }
                tr:nth-child(even) { background-color: #f2f2f2; }
            </style>
        </head>
        <body>
            <h2>💎 Bonnes Affaires Détectées - Rapport Qualité-Prix Élevé</h2>
            <p>Les produits suivants offrent un excellent rapport qualité-prix :</p>

            <table>
                <thead>
                    <tr>
                        <th>Produit</th>
                        <th>Prix</th>
                        <th>Note</th>
                        <th>Ratio Q/P</th>
                    </tr>
                </thead>
                <tbody>
        """

        for _, row in df_deals.iterrows():
            html += f"""
                    <tr>
                        <td>{row['titre'][:60]}...</td>
                        <td>${row['prix']:.2f}</td>
                        <td>{row['vote']:.1f}/5</td>
                        <td><strong>{row['rapport_qualite_prix']:.2f}</strong></td>
                    </tr>
            """

        html += """
                </tbody>
            </table>

            <div style="margin-top: 20px; font-size: 12px; color: #777;">
                <p>🤖 Alerte générée automatiquement - ISSEA MDSMS2</p>
            </div>
        </body>
        </html>
        """

        sujet = f"💎 {len(df_deals)} Bonne(s) Affaire(s) Amazon Détectée(s)"
        self.envoyer_email(sujet, html)


# Instance globale
_alerte_instance = None

def get_alerte_manager() -> AlerteManager:
    """Retourne l'instance unique d'AlerteManager"""
    global _alerte_instance
    if _alerte_instance is None:
        _alerte_instance = AlerteManager()
    return _alerte_instance


# Fonction pour Cloud Functions / Cloud Scheduler
def run_alerte_job():
    """
    Lance la vérification et l'envoi des alertes.

    Cette fonction est appelée par Cloud Scheduler quotidiennement.
    """
    logger.info("🔔 Lancement du job d'alertes...")

    try:
        alerte_manager = get_alerte_manager()
        alerte_manager.verifier_et_alerter()

        logger.info("✅ Job d'alertes terminé avec succès")
        return {'status': 'SUCCESS'}

    except Exception as e:
        logger.error(f"❌ Erreur job alertes : {e}")
        return {'status': 'ERROR', 'error': str(e)}


if __name__ == "__main__":
    # Test local
    print("🧪 Test du système d'alertes...")
    result = run_alerte_job()
    print(f"Résultat : {result}")
