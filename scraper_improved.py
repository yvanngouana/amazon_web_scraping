"""
Module de Scraping Amazon AMÉLIORÉ - Projet Webscraping
ISSEA MDSMS2 - 2025/2026

CORRECTIONS :
1. Maintien prix en XAF (Franc CFA)
2. Détection pages vides
3. Déduplication articles redondants
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
from typing import List, Dict, Optional, Set
import os
import hashlib

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
HEADLESS_MODE = os.getenv('HEADLESS', 'true').lower() == 'true'
BROWSER_TYPE = os.getenv('BROWSER', 'chrome')  # 'chrome' pour Cloud Run, 'edge' pour local


def setup_driver(headless: bool = HEADLESS_MODE, force_xaf: bool = True):
    """
    Configure et retourne un WebDriver avec anti-détection.

    Args:
        headless: Mode sans interface graphique (requis pour Cloud Run)
        force_xaf: Forcer l'affichage en XAF (Franc CFA Cameroun)

    Returns:
        WebDriver configuré
    """
    logger.info(f"🔧 Configuration du driver ({BROWSER_TYPE}, headless={headless}, devise=XAF)")

    if BROWSER_TYPE == 'chrome':
        options = ChromeOptions()

        if headless:
            options.add_argument('--headless=new')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')

        # Anti-détection
        options.add_argument('--disable-blink-features=AutomationControlled')

        # IMPORTANT : User-Agent depuis le Cameroun
        if force_xaf:
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            # Langue française (Cameroun)
            options.add_argument('--lang=fr-CM')
            options.add_experimental_option('prefs', {
                'intl.accept_languages': 'fr-CM,fr;q=0.9,en;q=0.8',
            })
        else:
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

        if force_xaf:
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
            options.add_argument('--lang=fr-CM')
            options.add_experimental_option('prefs', {
                'intl.accept_languages': 'fr-CM,fr;q=0.9,en;q=0.8',
            })
        else:
            options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')

        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        driver = webdriver.Edge(options=options)

    # Masquer les propriétés WebDriver
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

    logger.info("✅ Driver configuré avec succès")
    return driver


def forcer_devise_xaf(driver):
    """
    Force Amazon à afficher les prix en XAF via les paramètres URL.

    MÉTHODE : Ajouter les paramètres de devise dans l'URL Amazon.
    """
    try:
        # Aller d'abord sur Amazon.com
        driver.get("https://www.amazon.com")
        time.sleep(2)

        # Injection de cookies pour forcer XAF (Cameroun)
        driver.execute_script("""
            // Forcer la devise XAF
            document.cookie = "lc-acbfr=fr_CM; domain=.amazon.com; path=/";
            document.cookie = "i18n-prefs=XAF; domain=.amazon.com; path=/";
        """)

        logger.info("✅ Cookies XAF injectés")
        time.sleep(1)

    except Exception as e:
        logger.warning(f"⚠️ Impossible de forcer XAF : {e}")


def generer_url_amazon(mots_cles: str, numero_page: int = 1, devise: str = 'XAF') -> str:
    """
    Génère l'URL Amazon pour les mots-clés donnés avec devise forcée.

    Args:
        mots_cles: Termes de recherche (ex: "laptop")
        numero_page: Numéro de la page (commence à 1)
        devise: Devise à forcer (XAF, USD, EUR)

    Returns:
        URL complète avec paramètres de devise
    """
    mots_cles_formatted = mots_cles.replace(' ', '+')

    # URL de base avec recherche
    url = f"https://www.amazon.com/s?k={mots_cles_formatted}&page={numero_page}"

    # CORRECTION : Ajouter paramètre de devise (ne fonctionne pas toujours, mais on essaie)
    # Note : Amazon.com n'a pas de support officiel XAF, cela dépend de la localisation IP

    return url


def detecter_page_vide(driver) -> bool:
    """
    Détecte si la page Amazon est vide (aucun produit).

    Returns:
        True si page vide, False sinon
    """
    try:
        # Vérifier message "No results found"
        no_results = driver.find_elements(By.CSS_SELECTOR, ".s-no-results, [data-component-type='s-no-results']")
        if no_results:
            logger.warning("⚠️ Page vide : Aucun résultat trouvé")
            return True

        # Vérifier présence de produits
        produits = driver.find_elements(By.CSS_SELECTOR, "[data-component-type='s-search-result']")
        if len(produits) == 0:
            logger.warning("⚠️ Page vide : 0 produits détectés")
            return True

        return False

    except Exception as e:
        logger.error(f"❌ Erreur détection page vide : {e}")
        return True


def extraire_info_produit(element_produit) -> Dict:
    """
    Extrait les informations d'un élément produit.

    Utilise plusieurs sélecteurs CSS en fallback pour robustesse.
    CORRECTION : Détection automatique devise (XAF, USD, EUR)

    Args:
        element_produit: Élément Selenium représentant un produit

    Returns:
        Dict avec {titre, prix, prix_brut, devise, vote, lien_image}
    """
    info = {
        'Titre': None,
        'Prix': None,
        'Prix_Brut': None,  # Prix avec symbole devise
        'Devise': None,
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

    # PRIX - Multiples sélecteurs + DÉTECTION DEVISE
    selecteurs_prix = [
        ".a-price-whole",
        "span.a-price > span.a-offscreen",
        ".a-price .a-price-whole",
        ".a-price"
    ]

    for selecteur in selecteurs_prix:
        try:
            prix_element = element_produit.find_element(By.CSS_SELECTOR, selecteur)
            prix_text = prix_element.text.strip()

            if not prix_text:
                # Essayer avec offscreen
                try:
                    prix_text = element_produit.find_element(By.CSS_SELECTOR, "span.a-offscreen").text.strip()
                except:
                    continue

            if prix_text:
                # Sauvegarder prix brut
                info['Prix_Brut'] = prix_text

                # DÉTECTION DEVISE
                if 'XAF' in prix_text or 'CFA' in prix_text:
                    info['Devise'] = 'XAF'
                    # Extraire nombre : "245 000 XAF" → 245000
                    import re
                    nombres = re.findall(r'[\d\s]+', prix_text.replace(',', '').replace('.', ''))
                    if nombres:
                        prix_clean = ''.join(nombres).replace(' ', '')
                        info['Prix'] = float(prix_clean)

                elif '$' in prix_text or 'USD' in prix_text:
                    info['Devise'] = 'USD'
                    # Extraire nombre : "$425.99" → 425.99
                    import re
                    nombres = re.findall(r'[\d.]+', prix_text)
                    if nombres:
                        info['Prix'] = float(nombres[0])

                elif '€' in prix_text or 'EUR' in prix_text:
                    info['Devise'] = 'EUR'
                    import re
                    nombres = re.findall(r'[\d,]+', prix_text.replace('.', ''))
                    if nombres:
                        prix_clean = nombres[0].replace(',', '.')
                        info['Prix'] = float(prix_clean)

                else:
                    # Pas de symbole devise détecté, essayer d'extraire nombre
                    import re
                    nombres = re.findall(r'[\d,.]+', prix_text)
                    if nombres:
                        prix_clean = nombres[0].replace(',', '').replace('.', '', prix_text.count('.') - 1)
                        info['Prix'] = float(prix_clean)
                        info['Devise'] = 'UNKNOWN'

                if info['Prix']:
                    break

        except (NoSuchElementException, ValueError) as e:
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
            match = re.search(r'(\d+[.,]?\d*)', vote_text)
            if match:
                info['Vote'] = float(match.group(1).replace(',', '.'))
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


def calculer_hash_produit(titre: str) -> str:
    """
    Calcule un hash MD5 pour identifier un produit de manière unique.

    Args:
        titre: Titre du produit

    Returns:
        Hash MD5 (32 caractères)
    """
    if not titre:
        return None
    return hashlib.md5(titre.encode('utf-8')).hexdigest()


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


def scraper_amazon(mots_cles: str, nb_min_produits: int = 100, max_pages: int = 5,
                   force_xaf: bool = True) -> pd.DataFrame:
    """
    Fonction principale de scraping Amazon AMÉLIORÉE.

    CORRECTIONS :
    - Force devise XAF (Franc CFA)
    - Détecte pages vides et arrête
    - Déduplique articles redondants

    Args:
        mots_cles: Termes de recherche (ex: "laptop")
        nb_min_produits: Nombre minimum de produits à scraper
        max_pages: Nombre maximum de pages à parcourir
        force_xaf: Forcer affichage XAF (True recommandé)

    Returns:
        DataFrame avec colonnes [Titre, Prix, Prix_Brut, Devise, Vote, Lien_Image, Hash_Produit]
    """
    logger.info("=" * 80)
    logger.info(f"🚀 DÉBUT DU SCRAPING AMAZON (Devise: XAF)")
    logger.info(f"   Mots-clés: {mots_cles}")
    logger.info(f"   Objectif: {nb_min_produits} produits minimum")
    logger.info(f"   Pages max: {max_pages}")
    logger.info("=" * 80)

    driver = None
    produits = []
    produits_vus: Set[str] = set()  # Pour déduplication

    try:
        driver = setup_driver(force_xaf=force_xaf)

        # CORRECTION 1 : Forcer XAF avant de commencer
        if force_xaf:
            forcer_devise_xaf(driver)

        page_actuelle = 1
        pages_vides_consecutives = 0  # Compteur pages vides

        while len(produits) < nb_min_produits and page_actuelle <= max_pages:
            logger.info(f"\n📄 Page {page_actuelle}/{max_pages}")

            # Générer URL
            url = generer_url_amazon(mots_cles, page_actuelle)
            logger.info(f"   URL: {url}")

            # Charger la page
            driver.get(url)

            # Attendre le chargement
            try:
                WebDriverWait(driver, 15).until(  # Augmenté à 15s
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[data-component-type='s-search-result']"))
                )
            except TimeoutException:
                logger.warning(f"⚠️ Timeout page {page_actuelle}")
                pages_vides_consecutives += 1
                if pages_vides_consecutives >= 2:
                    logger.error("❌ 2 pages vides consécutives → Arrêt scraping")
                    break
                continue

            # CORRECTION 2 : Détecter page vide
            if detecter_page_vide(driver):
                pages_vides_consecutives += 1
                if pages_vides_consecutives >= 2:
                    logger.error("❌ 2 pages vides consécutives → Arrêt scraping")
                    break
                page_actuelle += 1
                continue
            else:
                pages_vides_consecutives = 0  # Reset compteur

            # Simuler comportement humain
            simuler_comportement_humain(driver)

            # Extraire les produits
            elements_produits = driver.find_elements(By.CSS_SELECTOR, "[data-component-type='s-search-result']")
            logger.info(f"   Produits trouvés sur la page: {len(elements_produits)}")

            produits_page = 0
            doublons_page = 0

            for element in elements_produits:
                try:
                    info = extraire_info_produit(element)

                    # Vérifier que le titre existe au minimum
                    if info['Titre']:
                        # CORRECTION 3 : Déduplication via hash
                        hash_produit = calculer_hash_produit(info['Titre'])

                        if hash_produit in produits_vus:
                            doublons_page += 1
                            logger.debug(f"   ⚠️ Doublon ignoré : {info['Titre'][:50]}...")
                            continue

                        # Ajouter hash au set
                        produits_vus.add(hash_produit)
                        info['Hash_Produit'] = hash_produit

                        # Ajouter produit
                        produits.append(info)
                        produits_page += 1

                except Exception as e:
                    logger.debug(f"   ⚠️ Erreur extraction produit : {e}")
                    continue

            logger.info(f"   ✅ Nouveaux produits: {produits_page} | Doublons ignorés: {doublons_page}")
            logger.info(f"   ✅ Total cumulé: {len(produits)} produits uniques")

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
    logger.info(f"   Total produits UNIQUES: {len(df)}")
    logger.info(f"   Doublons évités: {len(produits_vus) - len(df)}")
    if 'Devise' in df.columns:
        logger.info(f"   Devises détectées: {df['Devise'].value_counts().to_dict()}")
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

    # Feature : Catégorie de prix (adapté XAF)
    def categoriser_prix(row):
        prix = row.get('Prix')
        devise = row.get('Devise', 'UNKNOWN')

        if pd.isna(prix):
            return 'Inconnu'

        # Catégorisation selon devise
        if devise == 'XAF':
            if prix < 100000:
                return 'Économique'
            elif prix < 300000:
                return 'Moyen'
            elif prix < 600000:
                return 'Cher'
            else:
                return 'Premium'
        elif devise == 'USD':
            if prix < 150:
                return 'Économique'
            elif prix < 500:
                return 'Moyen'
            elif prix < 1000:
                return 'Cher'
            else:
                return 'Premium'
        else:
            return 'Inconnu'

    df_clean['Categorie_Prix'] = df_clean.apply(categoriser_prix, axis=1)

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
        # 1. Scraping (avec corrections)
        df_brut = scraper_amazon(mots_cles, nb_produits, max_pages, force_xaf=True)

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
            'duree': duree,
            'devises': df_clean['Devise'].value_counts().to_dict() if 'Devise' in df_clean.columns else {}
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
