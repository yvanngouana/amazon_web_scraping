"""
Module de Scraping Amazon - Projet Webscraping
ISSEA MDSMS2 - 2025/2026

Module extrait du notebook principal, optimisé pour Cloud Run.
Supporte Selenium avec anti-détection avancée.
"""

from selenium import webdriver
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
import pandas as pd
import time
import random
import logging
from typing import List, Dict, Optional
import os

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
HEADLESS_MODE = os.getenv('HEADLESS', 'true').lower() == 'true'
BROWSER_TYPE = os.getenv('BROWSER', 'chrome')  # 'chrome' pour Cloud Run, 'edge' pour local


def setup_driver(headless: bool = HEADLESS_MODE):
    """
    Configure et retourne un WebDriver avec anti-détection.

    Args:
        headless: Mode sans interface graphique (requis pour Cloud Run)

    Returns:
        WebDriver configuré
    """
    logger.info(f"🔧 Configuration du driver ({BROWSER_TYPE}, headless={headless})")

    if BROWSER_TYPE == 'chrome':
        options = ChromeOptions()

        if headless:
            options.add_argument('--headless=new')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')

        # Anti-détection
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        driver = webdriver.Chrome(options=options)

    else:  # Microsoft Edge (pour tests locaux)
        options = EdgeOptions()
        options.use_chromium = True

        if headless:
            options.add_argument('--headless=new')

        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        driver = webdriver.Edge(options=options)

    # Masquer les propriétés WebDriver
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    logger.info("✅ Driver configuré avec succès")
    return driver


def generer_url_amazon(mots_cles: str, numero_page: int = 1) -> str:
    """
    Génère l'URL Amazon pour les mots-clés donnés.

    Args:
        mots_cles: Termes de recherche (ex: "laptop")
        numero_page: Numéro de la page (commence à 1)

    Returns:
        URL complète
    """
    mots_cles_formatted = mots_cles.replace(' ', '+')
    url = f"https://www.amazon.com/s?k={mots_cles_formatted}&page={numero_page}"
    return url


def extraire_info_produit(element_produit) -> Dict:
    """
    Extrait les informations d'un élément produit.

    Utilise plusieurs sélecteurs CSS en fallback pour robustesse.

    Args:
        element_produit: Élément Selenium représentant un produit

    Returns:
        Dict avec {titre, prix, vote, lien_image}
    """
    info = {
        'Titre': None,
        'Prix': None,
        'Vote': None,
        'Lien_Image': None
    }

    # TITRE - Multiples sélecteurs
    selecteurs_titre = [
        "h2 a span",
        "h2 span",
        "h2 a",
        ".s-title-instructions-style span"
    ]

    for selecteur in selecteurs_titre:
        try:
            info['Titre'] = element_produit.find_element(By.CSS_SELECTOR, selecteur).text.strip()
            if info['Titre']:
                break
        except NoSuchElementException:
            continue

    # PRIX - Multiples sélecteurs
    selecteurs_prix = [
        ".a-price-whole",
        "span.a-price > span.a-offscreen",
        ".a-price .a-price-whole"
    ]

    for selecteur in selecteurs_prix:
        try:
            prix_text = element_produit.find_element(By.CSS_SELECTOR, selecteur).text.strip()
            # Nettoyer et convertir
            prix_text = prix_text.replace('$', '').replace(',', '').replace('.', '')
            if prix_text:
                info['Prix'] = float(prix_text)
                break
        except (NoSuchElementException, ValueError):
            continue

    # VOTE/RATING - Multiples sélecteurs
    selecteurs_vote = [
        "span.a-icon-alt",
        ".a-icon-star-small .a-icon-alt",
        "i.a-icon-star-small span"
    ]

    for selecteur in selecteurs_vote:
        try:
            vote_text = element_produit.find_element(By.CSS_SELECTOR, selecteur).get_attribute('textContent').strip()
            # Extraire le nombre (ex: "4.5 out of 5 stars" → 4.5)
            import re
            match = re.search(r'(\d+\.?\d*)', vote_text)
            if match:
                info['Vote'] = float(match.group(1))
                break
        except (NoSuchElementException, ValueError, AttributeError):
            continue

    # LIEN IMAGE
    selecteurs_image = [
        "img.s-image",
        "img",
        ".s-product-image-container img"
    ]

    for selecteur in selecteurs_image:
        try:
            info['Lien_Image'] = element_produit.find_element(By.CSS_SELECTOR, selecteur).get_attribute('src')
            if info['Lien_Image']:
                break
        except NoSuchElementException:
            continue

    return info


def simuler_comportement_humain(driver):
    """
    Simule un comportement humain pour éviter la détection.

    - Scroll progressif
    - Pause de "lecture"
    """
    try:
        # Scroll progressif
        hauteur_page = driver.execute_script("return document.body.scrollHeight")
        position_actuelle = 0

        while position_actuelle < hauteur_page:
            # Scroll par petits pas aléatoires
            scroll_step = random.randint(300, 600)
            position_actuelle += scroll_step
            driver.execute_script(f"window.scrollTo(0, {position_actuelle});")
            time.sleep(random.uniform(0.3, 0.8))

        # Pause de "lecture" en haut de page
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(random.uniform(1.5, 3.0))

    except Exception as e:
        logger.warning(f"⚠️ Erreur simulation comportement : {e}")


def scraper_amazon(mots_cles: str, nb_min_produits: int = 100, max_pages: int = 5) -> pd.DataFrame:
    """
    Fonction principale de scraping Amazon.

    Args:
        mots_cles: Termes de recherche (ex: "laptop")
        nb_min_produits: Nombre minimum de produits à scraper
        max_pages: Nombre maximum de pages à parcourir

    Returns:
        DataFrame avec colonnes [Titre, Prix, Vote, Lien_Image]
    """
    logger.info("=" * 80)
    logger.info(f"🚀 DÉBUT DU SCRAPING AMAZON")
    logger.info(f"   Mots-clés: {mots_cles}")
    logger.info(f"   Objectif: {nb_min_produits} produits minimum")
    logger.info(f"   Pages max: {max_pages}")
    logger.info("=" * 80)

    driver = None
    produits = []

    try:
        driver = setup_driver()
        page_actuelle = 1

        while len(produits) < nb_min_produits and page_actuelle <= max_pages:
            logger.info(f"\n📄 Page {page_actuelle}/{max_pages}")

            # Générer URL
            url = generer_url_amazon(mots_cles, page_actuelle)
            logger.info(f"   URL: {url}")

            # Charger la page
            driver.get(url)

            # Attendre le chargement
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-component-type='s-search-result']"))
                )
            except TimeoutException:
                logger.warning(f"⚠️ Timeout page {page_actuelle}")
                break

            # Simuler comportement humain
            simuler_comportement_humain(driver)

            # Extraire les produits
            elements_produits = driver.find_elements(By.CSS_SELECTOR, "[data-component-type='s-search-result']")
            logger.info(f"   Produits trouvés sur la page: {len(elements_produits)}")

            for element in elements_produits:
                try:
                    info = extraire_info_produit(element)

                    # Vérifier que le titre existe au minimum
                    if info['Titre']:
                        produits.append(info)
                except Exception as e:
                    logger.debug(f"   ⚠️ Erreur extraction produit : {e}")
                    continue

            logger.info(f"   ✅ Total cumulé: {len(produits)} produits")

            # Pause entre les pages (4-8 secondes)
            if page_actuelle < max_pages and len(produits) < nb_min_produits:
                pause = random.uniform(4, 8)
                logger.info(f"   ⏳ Pause de {pause:.1f}s avant page suivante...")
                time.sleep(pause)

            page_actuelle += 1

    except Exception as e:
        logger.error(f"❌ ERREUR durant le scraping : {e}")
        raise

    finally:
        if driver:
            driver.quit()
            logger.info("✅ Driver fermé")

    # Créer DataFrame
    df = pd.DataFrame(produits)

    logger.info("=" * 80)
    logger.info(f"✅ SCRAPING TERMINÉ")
    logger.info(f"   Total produits: {len(df)}")
    logger.info(f"   Colonnes: {list(df.columns)}")
    logger.info("=" * 80)

    return df


def appliquer_pipelines(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applique les pipelines de nettoyage et feature engineering.

    Args:
        df: DataFrame brut

    Returns:
        DataFrame enrichi avec nouvelles features
    """
    logger.info("🔧 Application des pipelines...")

    df_clean = df.copy()

    # Feature : Catégorie de prix
    def categoriser_prix(prix):
        if pd.isna(prix):
            return 'Inconnu'
        elif prix < 150:
            return 'Économique'
        elif prix < 500:
            return 'Moyen'
        elif prix < 1000:
            return 'Cher'
        else:
            return 'Premium'

    df_clean['Categorie_Prix'] = df_clean['Prix'].apply(categoriser_prix)

    # Feature : Qualité du vote
    def categoriser_vote(vote):
        if pd.isna(vote):
            return 'Non noté'
        elif vote < 3.5:
            return 'Faible'
        elif vote < 4.0:
            return 'Moyen'
        elif vote < 4.5:
            return 'Bon'
        else:
            return 'Excellent'

    df_clean['Qualite_Vote'] = df_clean['Vote'].apply(categoriser_vote)

    # Feature : Rapport qualité-prix
    # Normaliser prix et vote, puis calculer ratio
    if df_clean['Prix'].notna().sum() > 0:
        prix_max = df_clean['Prix'].max()
        if prix_max > 0:
            df_clean['Prix_Norm'] = df_clean['Prix'] / prix_max
            df_clean['Rapport_Qualite_Prix'] = df_clean.apply(
                lambda row: row['Vote'] / row['Prix_Norm'] if pd.notna(row['Vote']) and pd.notna(row['Prix_Norm']) and row['Prix_Norm'] > 0 else None,
                axis=1
            )
            df_clean.drop('Prix_Norm', axis=1, inplace=True)
        else:
            df_clean['Rapport_Qualite_Prix'] = None
    else:
        df_clean['Rapport_Qualite_Prix'] = None

    logger.info(f"✅ Pipelines appliqués - {len(df_clean.columns)} colonnes")
    return df_clean


# Fonction principale pour Cloud Run
def run_scraping_job(mots_cles: str = "laptop", nb_produits: int = 100, max_pages: int = 5):
    """
    Lance un job de scraping complet (scraping + pipelines + BDD).

    Cette fonction est appelée par le scheduler Cloud.

    Args:
        mots_cles: Termes de recherche
        nb_produits: Nombre minimum de produits
        max_pages: Pages maximum à scraper

    Returns:
        Dict avec statistiques du scraping
    """
    import time as time_module
    start_time = time_module.time()

    try:
        # 1. Scraping
        df_brut = scraper_amazon(mots_cles, nb_produits, max_pages)

        # 2. Pipelines
        df_clean = appliquer_pipelines(df_brut)

        # 3. Sauvegarde en base de données
        from database import get_db
        db = get_db()
        stats = db.inserer_produits(df_clean)

        # 4. Log du scraping
        duree = time_module.time() - start_time
        db.log_scraping(
            nb_produits=len(df_clean),
            nb_nouveaux=stats['nouveaux'],
            nb_mises_a_jour=stats['mises_a_jour'],
            duree=duree,
            statut='SUCCESS'
        )

        logger.info(f"✅ Job terminé avec succès en {duree:.1f}s")
        return {
            'status': 'SUCCESS',
            'nb_produits': len(df_clean),
            'nouveaux': stats['nouveaux'],
            'mises_a_jour': stats['mises_a_jour'],
            'duree': duree
        }

    except Exception as e:
        logger.error(f"❌ Erreur job scraping : {e}")

        # Log de l'erreur
        try:
            from database import get_db
            db = get_db()
            duree = time_module.time() - start_time
            db.log_scraping(
                nb_produits=0,
                nb_nouveaux=0,
                nb_mises_a_jour=0,
                duree=duree,
                statut='ERROR',
                erreur=str(e)
            )
        except:
            pass

        return {
            'status': 'ERROR',
            'error': str(e)
        }


if __name__ == "__main__":
    # Test local
    result = run_scraping_job(mots_cles="laptop", nb_produits=50, max_pages=3)
    print(f"\n📊 Résultat : {result}")
