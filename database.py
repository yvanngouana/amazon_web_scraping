"""
Gestion de la Base de Données - Projet Amazon Webscraping
ISSEA MDSMS2 - 2025/2026

Ce module gère le stockage et la récupération des données scrapées.
Supporte SQLite (local) et PostgreSQL (production Cloud SQL).
"""

import sqlite3
import os
from datetime import datetime
import pandas as pd
from typing import List, Dict, Optional
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration base de données
DB_TYPE = os.getenv('DB_TYPE', 'sqlite')  # 'sqlite' ou 'postgresql'
DB_PATH = os.getenv('DB_PATH', 'amazon_scraping.db')

# Pour PostgreSQL (Cloud SQL)
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'amazon_db')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', '')


class DatabaseManager:
    """Gestionnaire de base de données avec support SQLite et PostgreSQL"""

    def __init__(self):
        """Initialise la connexion à la base de données"""
        self.db_type = DB_TYPE
        self.connection = None
        self._connect()
        self._create_tables()

    def _connect(self):
        """Établit la connexion à la base de données"""
        try:
            if self.db_type == 'sqlite':
                self.connection = sqlite3.connect(DB_PATH, check_same_thread=False)
                logger.info(f"✅ Connexion SQLite établie : {DB_PATH}")
            elif self.db_type == 'postgresql':
                import psycopg2
                self.connection = psycopg2.connect(
                    host=DB_HOST,
                    port=DB_PORT,
                    database=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD
                )
                logger.info(f"✅ Connexion PostgreSQL établie : {DB_HOST}:{DB_PORT}/{DB_NAME}")
        except Exception as e:
            logger.error(f"❌ Erreur connexion base de données : {e}")
            raise

    def _create_tables(self):
        """Crée les tables nécessaires si elles n'existent pas"""
        cursor = self.connection.cursor()

        # Table principale : produits avec historique
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS produits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                titre TEXT NOT NULL,
                prix REAL,
                vote REAL,
                lien_image TEXT,
                categorie_prix TEXT,
                qualite_vote TEXT,
                rapport_qualite_prix REAL,
                date_scraping TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                hash_produit TEXT,
                UNIQUE(hash_produit, date_scraping)
            )
        """ if self.db_type == 'sqlite' else """
            CREATE TABLE IF NOT EXISTS produits (
                id SERIAL PRIMARY KEY,
                titre TEXT NOT NULL,
                prix REAL,
                vote REAL,
                lien_image TEXT,
                categorie_prix TEXT,
                qualite_vote TEXT,
                rapport_qualite_prix REAL,
                date_scraping TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                hash_produit TEXT,
                UNIQUE(hash_produit, date_scraping)
            )
        """)

        # Table : historique des prix (pour analyse temporelle)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS prix_historique (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash_produit TEXT NOT NULL,
                prix REAL,
                vote REAL,
                date_observation TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """ if self.db_type == 'sqlite' else """
            CREATE TABLE IF NOT EXISTS prix_historique (
                id SERIAL PRIMARY KEY,
                hash_produit TEXT NOT NULL,
                prix REAL,
                vote REAL,
                date_observation TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Table : alertes déclenchées
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS alertes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                type_alerte TEXT NOT NULL,
                message TEXT,
                hash_produit TEXT,
                ancien_prix REAL,
                nouveau_prix REAL,
                date_alerte TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """ if self.db_type == 'sqlite' else """
            CREATE TABLE IF NOT EXISTS alertes (
                id SERIAL PRIMARY KEY,
                type_alerte TEXT NOT NULL,
                message TEXT,
                hash_produit TEXT,
                ancien_prix REAL,
                nouveau_prix REAL,
                date_alerte TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Table : logs de scraping
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scraping_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date_scraping TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                nb_produits_scrapes INTEGER,
                nb_nouveaux INTEGER,
                nb_mises_a_jour INTEGER,
                duree_secondes REAL,
                statut TEXT,
                message_erreur TEXT
            )
        """ if self.db_type == 'sqlite' else """
            CREATE TABLE IF NOT EXISTS scraping_logs (
                id SERIAL PRIMARY KEY,
                date_scraping TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                nb_produits_scrapes INTEGER,
                nb_nouveaux INTEGER,
                nb_mises_a_jour INTEGER,
                duree_secondes REAL,
                statut TEXT,
                message_erreur TEXT
            )
        """)

        self.connection.commit()
        logger.info("✅ Tables créées avec succès")

    def inserer_produits(self, df: pd.DataFrame) -> Dict[str, int]:
        """
        Insère les produits dans la base de données (mode incrémental).

        Args:
            df: DataFrame avec colonnes [Titre, Prix, Vote, Lien_Image, ...]

        Returns:
            Dict avec statistiques : {'nouveaux': N, 'mises_a_jour': M, 'total': T}
        """
        cursor = self.connection.cursor()
        nouveaux = 0
        mises_a_jour = 0

        for _, row in df.iterrows():
            # Créer un hash unique pour chaque produit (basé sur titre)
            import hashlib
            hash_produit = hashlib.md5(str(row.get('Titre', '')).encode()).hexdigest()

            # Vérifier si le produit existe déjà aujourd'hui
            cursor.execute("""
                SELECT id, prix, vote FROM produits
                WHERE hash_produit = ? AND DATE(date_scraping) = DATE('now')
            """ if self.db_type == 'sqlite' else """
                SELECT id, prix, vote FROM produits
                WHERE hash_produit = %s AND DATE(date_scraping) = CURRENT_DATE
            """, (hash_produit,))

            existing = cursor.fetchone()

            if existing:
                # Produit existe déjà aujourd'hui - mise à jour si prix/vote change
                old_prix, old_vote = existing[1], existing[2]
                new_prix = row.get('Prix')
                new_vote = row.get('Vote')

                if old_prix != new_prix or old_vote != new_vote:
                    cursor.execute("""
                        UPDATE produits
                        SET prix = ?, vote = ?, categorie_prix = ?,
                            qualite_vote = ?, rapport_qualite_prix = ?
                        WHERE id = ?
                    """ if self.db_type == 'sqlite' else """
                        UPDATE produits
                        SET prix = %s, vote = %s, categorie_prix = %s,
                            qualite_vote = %s, rapport_qualite_prix = %s
                        WHERE id = %s
                    """, (
                        new_prix, new_vote,
                        row.get('Categorie_Prix'),
                        row.get('Qualite_Vote'),
                        row.get('Rapport_Qualite_Prix'),
                        existing[0]
                    ))

                    # Enregistrer changement de prix
                    self._enregistrer_changement_prix(cursor, hash_produit, old_prix, new_prix)
                    mises_a_jour += 1
            else:
                # Nouveau produit - insertion
                cursor.execute("""
                    INSERT INTO produits (
                        titre, prix, vote, lien_image, categorie_prix,
                        qualite_vote, rapport_qualite_prix, hash_produit
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """ if self.db_type == 'sqlite' else """
                    INSERT INTO produits (
                        titre, prix, vote, lien_image, categorie_prix,
                        qualite_vote, rapport_qualite_prix, hash_produit
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row.get('Titre'),
                    row.get('Prix'),
                    row.get('Vote'),
                    row.get('Lien_Image'),
                    row.get('Categorie_Prix'),
                    row.get('Qualite_Vote'),
                    row.get('Rapport_Qualite_Prix'),
                    hash_produit
                ))
                nouveaux += 1

            # Enregistrer dans historique des prix
            cursor.execute("""
                INSERT INTO prix_historique (hash_produit, prix, vote)
                VALUES (?, ?, ?)
            """ if self.db_type == 'sqlite' else """
                INSERT INTO prix_historique (hash_produit, prix, vote)
                VALUES (%s, %s, %s)
            """, (hash_produit, row.get('Prix'), row.get('Vote')))

        self.connection.commit()
        logger.info(f"✅ Insertion terminée : {nouveaux} nouveaux, {mises_a_jour} mis à jour")

        return {
            'nouveaux': nouveaux,
            'mises_a_jour': mises_a_jour,
            'total': len(df)
        }

    def _enregistrer_changement_prix(self, cursor, hash_produit: str, ancien_prix: float, nouveau_prix: float):
        """Enregistre un changement de prix significatif"""
        if ancien_prix and nouveau_prix:
            variation_pct = ((nouveau_prix - ancien_prix) / ancien_prix) * 100

            # Alerte si baisse > 10%
            if variation_pct < -10:
                cursor.execute("""
                    INSERT INTO alertes (type_alerte, message, hash_produit, ancien_prix, nouveau_prix)
                    VALUES (?, ?, ?, ?, ?)
                """ if self.db_type == 'sqlite' else """
                    INSERT INTO alertes (type_alerte, message, hash_produit, ancien_prix, nouveau_prix)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    'BAISSE_PRIX',
                    f'Prix baissé de {abs(variation_pct):.1f}%',
                    hash_produit,
                    ancien_prix,
                    nouveau_prix
                ))

    def get_produits_recents(self, limit: int = 1000) -> pd.DataFrame:
        """Récupère les produits les plus récents"""
        query = """
            SELECT titre, prix, vote, lien_image, categorie_prix,
                   qualite_vote, rapport_qualite_prix, date_scraping
            FROM produits
            ORDER BY date_scraping DESC
            LIMIT ?
        """ if self.db_type == 'sqlite' else """
            SELECT titre, prix, vote, lien_image, categorie_prix,
                   qualite_vote, rapport_qualite_prix, date_scraping
            FROM produits
            ORDER BY date_scraping DESC
            LIMIT %s
        """

        df = pd.read_sql_query(query, self.connection, params=(limit,))
        return df

    def get_alertes_recentes(self, limit: int = 100) -> pd.DataFrame:
        """Récupère les alertes récentes"""
        query = """
            SELECT type_alerte, message, ancien_prix, nouveau_prix, date_alerte
            FROM alertes
            ORDER BY date_alerte DESC
            LIMIT ?
        """ if self.db_type == 'sqlite' else """
            SELECT type_alerte, message, ancien_prix, nouveau_prix, date_alerte
            FROM alertes
            ORDER BY date_alerte DESC
            LIMIT %s
        """

        df = pd.read_sql_query(query, self.connection, params=(limit,))
        return df

    def get_statistiques_scraping(self) -> Dict:
        """Récupère des statistiques globales"""
        cursor = self.connection.cursor()

        # Nombre total de produits
        cursor.execute("SELECT COUNT(*) FROM produits")
        total_produits = cursor.fetchone()[0]

        # Nombre de scrapings effectués
        cursor.execute("SELECT COUNT(*) FROM scraping_logs")
        total_scrapings = cursor.fetchone()[0]

        # Dernier scraping
        cursor.execute("SELECT date_scraping, nb_produits_scrapes FROM scraping_logs ORDER BY date_scraping DESC LIMIT 1")
        dernier_scraping = cursor.fetchone()

        return {
            'total_produits': total_produits,
            'total_scrapings': total_scrapings,
            'dernier_scraping': dernier_scraping[0] if dernier_scraping else None,
            'dernier_nb_produits': dernier_scraping[1] if dernier_scraping else 0
        }

    def log_scraping(self, nb_produits: int, nb_nouveaux: int, nb_mises_a_jour: int,
                     duree: float, statut: str = 'SUCCESS', erreur: str = None):
        """Enregistre un log de scraping"""
        cursor = self.connection.cursor()
        cursor.execute("""
            INSERT INTO scraping_logs (
                nb_produits_scrapes, nb_nouveaux, nb_mises_a_jour,
                duree_secondes, statut, message_erreur
            ) VALUES (?, ?, ?, ?, ?, ?)
        """ if self.db_type == 'sqlite' else """
            INSERT INTO scraping_logs (
                nb_produits_scrapes, nb_nouveaux, nb_mises_a_jour,
                duree_secondes, statut, message_erreur
            ) VALUES (%s, %s, %s, %s, %s, %s)
        """, (nb_produits, nb_nouveaux, nb_mises_a_jour, duree, statut, erreur))

        self.connection.commit()
        logger.info(f"✅ Log scraping enregistré : {statut}")

    def close(self):
        """Ferme la connexion à la base de données"""
        if self.connection:
            self.connection.close()
            logger.info("✅ Connexion base de données fermée")


# Instance globale (singleton)
_db_instance = None

def get_db() -> DatabaseManager:
    """Retourne l'instance unique de DatabaseManager"""
    global _db_instance
    if _db_instance is None:
        _db_instance = DatabaseManager()
    return _db_instance
