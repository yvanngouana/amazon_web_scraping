"""
Script d'Automatisation - Scraping Amazon
ISSEA MDSMS2 - 2025/2026

Ce script permet d'automatiser le scraping Amazon à intervalles réguliers.

Usage:
    python automation.py

Le script tourne en continu et exécute le scraping selon la planification définie.
"""

import schedule
import time
from datetime import datetime
import pandas as pd
import sys
import os

# Ajouter le répertoire courant au path pour les imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configuration
MOTS_CLES = "laptop"  # Mots-clés à scraper
NB_PRODUITS_MIN = 100  # Nombre minimum de produits par scraping
MAX_PAGES = 5  # Nombre maximum de pages à scraper
DOSSIER_SAUVEGARDE = "scraping_auto"  # Dossier pour sauvegarder les résultats

# Créer le dossier de sauvegarde s'il n'existe pas
os.makedirs(DOSSIER_SAUVEGARDE, exist_ok=True)


def log_message(message):
    """Affiche un message avec timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def job_scraping():
    """
    Fonction principale de scraping automatisé.

    Cette fonction :
    1. Lance le scraping
    2. Sauvegarde les données avec timestamp
    3. Génère un rapport de synthèse
    """
    log_message("=" * 80)
    log_message("🚀 DÉBUT DU SCRAPING AUTOMATISÉ")
    log_message("=" * 80)

    try:
        # Import de la fonction de scraping
        # Note: Vous devrez adapter cet import selon votre structure
        log_message("📦 Import des modules de scraping...")

        # OPTION 1: Si vous avez extrait les fonctions dans un fichier séparé
        # from scraper import scraper_amazon

        # OPTION 2: Si vous utilisez le notebook, utilisez cette approche
        log_message("⚠️  Pour utiliser ce script, extrayez la fonction scraper_amazon du notebook")
        log_message("⚠️  et placez-la dans un fichier scraper.py")

        # Exemple de code à mettre dans scraper.py :
        """
        from selenium import webdriver
        # ... (tout le code de setup_driver, extraire_info_produit, etc.)

        def scraper_amazon(mots_cles, nb_min_produits=300, max_pages=15):
            # ... (votre fonction de scraping)
            pass
        """

        # Pour la démo, on simule un scraping
        log_message(f"🔍 Scraping des produits '{MOTS_CLES}'...")
        log_message(f"   Objectif: {NB_PRODUITS_MIN} produits minimum")
        log_message(f"   Pages max: {MAX_PAGES}")

        # Simuler un délai de scraping
        time.sleep(2)

        # Dans un cas réel, vous appelleriez :
        # df = scraper_amazon(MOTS_CLES, nb_min_produits=NB_PRODUITS_MIN, max_pages=MAX_PAGES)

        # Pour la démo, créons un DataFrame factice
        log_message("✅ Scraping terminé (démo)")

        # Générer le nom de fichier avec timestamp
        timestamp_fichier = datetime.now().strftime("%Y%m%d_%H%M%S")
        fichier_csv = os.path.join(DOSSIER_SAUVEGARDE, f"bd_{timestamp_fichier}.csv")

        # Dans un cas réel, vous sauvegarderiez :
        # df.to_csv(fichier_csv, index=False, encoding='utf-8-sig')
        # log_message(f"💾 Données sauvegardées: {fichier_csv}")
        # log_message(f"   Nombre de produits: {len(df)}")

        log_message(f"💾 Fichier de sauvegarde prévu: {fichier_csv}")

        # Générer un rapport de synthèse
        generer_rapport_synthese()

        log_message("=" * 80)
        log_message("✅ SCRAPING AUTOMATISÉ TERMINÉ AVEC SUCCÈS")
        log_message("=" * 80)

    except Exception as e:
        log_message(f"❌ ERREUR durant le scraping: {e}")
        log_message("=" * 80)
        # En production, vous pourriez envoyer un email d'alerte ici


def generer_rapport_synthese():
    """
    Génère un rapport de synthèse des scrapings effectués.
    """
    log_message("📊 Génération du rapport de synthèse...")

    # Lister tous les fichiers CSV dans le dossier
    fichiers = [f for f in os.listdir(DOSSIER_SAUVEGARDE) if f.endswith('.csv')]

    if not fichiers:
        log_message("   Aucun fichier de scraping trouvé")
        return

    log_message(f"   Nombre de scrapings effectués: {len(fichiers)}")

    # Analyser les fichiers (dans un cas réel)
    # for fichier in fichiers:
    #     df = pd.read_csv(os.path.join(DOSSIER_SAUVEGARDE, fichier))
    #     log_message(f"   - {fichier}: {len(df)} produits")

    # Créer un rapport consolidé
    rapport_fichier = os.path.join(DOSSIER_SAUVEGARDE, "rapport_synthese.txt")
    with open(rapport_fichier, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("RAPPORT DE SYNTHÈSE - SCRAPING AMAZON AUTOMATISÉ\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Date du rapport: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Nombre de scrapings: {len(fichiers)}\n\n")
        f.write("Fichiers générés:\n")
        for fichier in sorted(fichiers):
            f.write(f"  - {fichier}\n")

    log_message(f"   Rapport sauvegardé: {rapport_fichier}")


def envoyer_alerte_email(sujet, message):
    """
    Envoie une alerte par email (à implémenter).

    Args:
        sujet: Sujet de l'email
        message: Corps du message
    """
    # À implémenter avec smtplib si nécessaire
    log_message(f"📧 Alerte email: {sujet}")
    log_message(f"   Message: {message}")


def afficher_configuration():
    """Affiche la configuration actuelle"""
    log_message("=" * 80)
    log_message("⚙️  CONFIGURATION DU BOT DE SCRAPING")
    log_message("=" * 80)
    log_message(f"Mots-clés: {MOTS_CLES}")
    log_message(f"Produits minimum: {NB_PRODUITS_MIN}")
    log_message(f"Pages maximum: {MAX_PAGES}")
    log_message(f"Dossier de sauvegarde: {DOSSIER_SAUVEGARDE}")
    log_message("=" * 80)


def main():
    """
    Fonction principale - Configure et lance le scheduler.
    """
    afficher_configuration()

    log_message("\n🤖 BOT DE SCRAPING AMAZON DÉMARRÉ")
    log_message("=" * 80)

    # OPTION 1: Planifier tous les jours à une heure fixe
    schedule.every().day.at("08:00").do(job_scraping)
    log_message("📅 Planification: Tous les jours à 08:00")

    # OPTION 2: Planifier toutes les X heures (décommentez si nécessaire)
    # schedule.every(6).hours.do(job_scraping)
    # log_message("📅 Planification: Toutes les 6 heures")

    # OPTION 3: Planifier toutes les X minutes (pour tests)
    # schedule.every(30).minutes.do(job_scraping)
    # log_message("📅 Planification: Toutes les 30 minutes")

    # Pour tester immédiatement, décommentez cette ligne :
    # log_message("🧪 Exécution immédiate pour test...")
    # job_scraping()

    log_message("\n⏰ En attente de la prochaine exécution...")
    log_message("   (Appuyez sur Ctrl+C pour arrêter)\n")

    # Boucle principale
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Vérifier toutes les minutes
    except KeyboardInterrupt:
        log_message("\n" + "=" * 80)
        log_message("🛑 BOT DE SCRAPING ARRÊTÉ PAR L'UTILISATEUR")
        log_message("=" * 80)


if __name__ == "__main__":
    main()
