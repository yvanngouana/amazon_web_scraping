"""
Configuration Selenium optimisée pour déploiement Cloud
Supporte : Local, Heroku, Render, Railway, Fly.io
"""

import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager


def get_chrome_driver():
    """
    Retourne un driver Chrome configuré selon l'environnement.
    
    Détecte automatiquement :
    - Local (développement)
    - Heroku (DYNO)
    - Render (RENDER)
    - Railway (RAILWAY_ENVIRONMENT)
    - Fly.io (FLY_APP_NAME)
    """
    chrome_options = Options()
    
    # Options communes à tous les environnements cloud
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # User-Agent pour éviter la détection
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
    
    # Détection de l'environnement
    is_heroku = os.getenv('DYNO') is not None
    is_render = os.getenv('RENDER') is not None
    is_railway = os.getenv('RAILWAY_ENVIRONMENT') is not None
    is_fly = os.getenv('FLY_APP_NAME') is not None
    
    # Configuration spécifique selon l'environnement
    if is_heroku:
        print("🟣 Environnement détecté : Heroku")
        chrome_options.binary_location = "/app/.apt/usr/bin/chromium-browser"
        service = Service("/app/.apt/usr/bin/chromedriver")
        
    elif is_render:
        print("🎨 Environnement détecté : Render")
        chrome_options.binary_location = "/usr/bin/chromium"
        service = Service("/usr/bin/chromedriver")
        
    elif is_railway:
        print("🚂 Environnement détecté : Railway")
        # Railway utilise la configuration standard
        service = Service(ChromeDriverManager().install())
        
    elif is_fly:
        print("✈️ Environnement détecté : Fly.io")
        chrome_options.binary_location = "/usr/bin/chromium"
        service = Service("/usr/bin/chromedriver")
        
    else:
        print("💻 Environnement détecté : Local")
        # Développement local avec webdriver-manager
        service = Service(ChromeDriverManager().install())
    
    # Créer et retourner le driver
    try:
        driver = webdriver.Chrome(service=service, options=chrome_options)
        print("✅ Chrome Driver initialisé avec succès")
        return driver
    except Exception as e:
        print(f"❌ Erreur lors de l'initialisation du driver : {e}")
        raise


def test_driver():
    """Test rapide du driver"""
    try:
        driver = get_chrome_driver()
        driver.get("https://www.google.com")
        print(f"✅ Test réussi - Titre de la page : {driver.title}")
        driver.quit()
        return True
    except Exception as e:
        print(f"❌ Test échoué : {e}")
        return False


if __name__ == "__main__":
    # Test du driver au lancement
    test_driver()
