"""
Dashboard Streamlit Intégré - Projet Amazon Webscraping
ISSEA MDSMS2 - 2025/2026

Application complète pour Cloud Run :
- Dashboard de visualisation temps réel
- Contrôle du scraping
- Historique et alertes
- Scraping automatisé en background

Pour lancer : streamlit run app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime, timedelta
import logging
import os
import threading
from apscheduler.schedulers.background import BackgroundScheduler

# Imports locaux
from database import get_db
from scraper import run_scraping_job
from alertes import run_alerte_job

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration de la page
st.set_page_config(
    page_title="Amazon Analytics Dashboard - ISSEA",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS personnalisé
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 20px;
        padding-right: 20px;
    }
    </style>
    """, unsafe_allow_html=True)


# ===========================
# SCHEDULER BACKGROUND
# ===========================

@st.cache_resource
def init_scheduler():
    """
    Initialise le scheduler pour scraping automatique.

    ⚠️ IMPORTANT : Sur Cloud Run, utiliser Cloud Scheduler à la place.
    Cette fonction est utile pour tests locaux uniquement.
    """
    # Désactiver sur Cloud Run (utiliser Cloud Scheduler à la place)
    if os.getenv('K_SERVICE'):  # Variable d'environnement Cloud Run
        logger.info("🌐 Cloud Run détecté - Scheduler désactivé (utiliser Cloud Scheduler)")
        return None

    scheduler = BackgroundScheduler()

    # Configuration : Scraping quotidien à 2h du matin
    scheduler.add_job(
        func=job_scraping_background,
        trigger='cron',
        hour=2,
        minute=0,
        id='scraping_quotidien'
    )

    # Configuration : Alertes quotidiennes à 8h du matin
    scheduler.add_job(
        func=job_alertes_background,
        trigger='cron',
        hour=8,
        minute=0,
        id='alertes_quotidiennes'
    )

    scheduler.start()
    logger.info("✅ Scheduler initialisé - Scraping quotidien à 2h00")

    return scheduler


def job_scraping_background():
    """Job de scraping en background"""
    logger.info("🤖 Lancement scraping automatique...")
    try:
        result = run_scraping_job(mots_cles="laptop", nb_produits=100, max_pages=5)
        logger.info(f"✅ Scraping automatique terminé : {result}")
    except Exception as e:
        logger.error(f"❌ Erreur scraping automatique : {e}")


def job_alertes_background():
    """Job d'alertes en background"""
    logger.info("🔔 Lancement alertes automatiques...")
    try:
        result = run_alerte_job()
        logger.info(f"✅ Alertes automatiques terminées : {result}")
    except Exception as e:
        logger.error(f"❌ Erreur alertes automatiques : {e}")


# Initialiser le scheduler (désactivé sur Cloud Run)
scheduler = init_scheduler()


# ===========================
# FONCTIONS UTILITAIRES
# ===========================

@st.cache_data(ttl=300)  # Cache 5 minutes
def load_data():
    """Charge les données depuis la base de données"""
    try:
        db = get_db()
        df = db.get_produits_recents(limit=1000)

        # Conversion des types
        df['Prix'] = pd.to_numeric(df['prix'], errors='coerce')
        df['Vote'] = pd.to_numeric(df['vote'], errors='coerce')

        return df
    except Exception as e:
        st.error(f"❌ Erreur chargement données : {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_alertes():
    """Charge les alertes récentes"""
    try:
        db = get_db()
        return db.get_alertes_recentes(limit=50)
    except Exception as e:
        st.error(f"❌ Erreur chargement alertes : {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_stats():
    """Charge les statistiques globales"""
    try:
        db = get_db()
        return db.get_statistiques_scraping()
    except Exception as e:
        st.error(f"❌ Erreur chargement stats : {e}")
        return {}


# ===========================
# EN-TÊTE
# ===========================

st.title("🛒 Amazon Analytics Dashboard")
st.markdown("### Analyse en temps réel des produits scrapés - ISSEA MDSMS2")
st.markdown("---")


# ===========================
# TABS PRINCIPAUX
# ===========================

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Visualisations",
    "🤖 Contrôle Scraping",
    "🔔 Alertes & Historique",
    "📈 Statistiques Avancées"
])


# ===========================
# TAB 1 : VISUALISATIONS
# ===========================

with tab1:
    # Chargement des données
    df = load_data()

    if df.empty:
        st.warning("⚠️ Aucune donnée disponible. Lancez un scraping depuis l'onglet 'Contrôle Scraping'.")
    else:
        # Sidebar - Filtres
        with st.sidebar:
            st.header("🔍 Filtres")

            # Filtre prix
            st.subheader("Prix (USD)")
            prix_valides = df['Prix'].dropna()
            if len(prix_valides) > 0:
                prix_min = int(prix_valides.min())
                prix_max = int(prix_valides.max())
                prix_range = st.slider(
                    "Fourchette de prix",
                    min_value=prix_min,
                    max_value=prix_max,
                    value=(prix_min, prix_max),
                    step=10
                )
            else:
                prix_range = (0, 0)

            # Filtre vote
            st.subheader("Note minimale")
            vote_valides = df['Vote'].dropna()
            if len(vote_valides) > 0:
                vote_min = st.slider(
                    "Note minimale (étoiles)",
                    min_value=0.0,
                    max_value=5.0,
                    value=0.0,
                    step=0.5
                )
            else:
                vote_min = 0.0

        # Appliquer les filtres
        df_filtered = df.copy()
        if len(prix_valides) > 0:
            df_filtered = df_filtered[
                (df_filtered['Prix'].notna()) &
                (df_filtered['Prix'].between(prix_range[0], prix_range[1]))
            ]
        if len(vote_valides) > 0:
            df_filtered = df_filtered[
                (df_filtered['Vote'].notna()) &
                (df_filtered['Vote'] >= vote_min)
            ]

        st.sidebar.markdown("---")
        st.sidebar.info(f"📊 **{len(df_filtered)}** produits affichés sur **{len(df)}** au total")

        # Métriques principales
        st.header("📊 Métriques Clés")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.metric(
                label="Produits Total",
                value=f"{len(df_filtered):,}",
                delta=f"{len(df_filtered) - len(df)} filtrés" if len(df_filtered) != len(df) else "Tous"
            )

        with col2:
            prix_moy = df_filtered['Prix'].mean()
            if pd.notna(prix_moy):
                st.metric(label="Prix Moyen", value=f"${prix_moy:,.2f}")
            else:
                st.metric(label="Prix Moyen", value="N/A")

        with col3:
            vote_moy = df_filtered['Vote'].mean()
            if pd.notna(vote_moy):
                st.metric(label="Note Moyenne", value=f"{vote_moy:.2f}/5.0")
            else:
                st.metric(label="Note Moyenne", value="N/A")

        with col4:
            prix_median = df_filtered['Prix'].median()
            if pd.notna(prix_median):
                st.metric(label="Prix Médian", value=f"${prix_median:,.2f}")
            else:
                st.metric(label="Prix Médian", value="N/A")

        st.markdown("---")

        # Graphiques principaux
        col_left, col_right = st.columns(2)

        with col_left:
            st.subheader("📈 Distribution des Prix")
            df_prix = df_filtered[df_filtered['Prix'].notna()]
            if len(df_prix) > 0:
                fig_hist = px.histogram(
                    df_prix,
                    x='Prix',
                    nbins=30,
                    title="Distribution des prix",
                    labels={'Prix': 'Prix (USD)', 'count': 'Nombre de produits'},
                    color_discrete_sequence=['#636EFA']
                )
                fig_hist.update_layout(showlegend=False, height=400)
                st.plotly_chart(fig_hist, use_container_width=True)
            else:
                st.warning("Aucune donnée de prix disponible")

        with col_right:
            st.subheader("⭐ Distribution des Notes")
            df_vote = df_filtered[df_filtered['Vote'].notna()]
            if len(df_vote) > 0:
                fig_vote = px.histogram(
                    df_vote,
                    x='Vote',
                    nbins=20,
                    title="Distribution des notes",
                    labels={'Vote': 'Note (étoiles)', 'count': 'Nombre de produits'},
                    color_discrete_sequence=['#EF553B']
                )
                fig_vote.update_layout(showlegend=False, height=400)
                st.plotly_chart(fig_vote, use_container_width=True)
            else:
                st.warning("Aucune donnée de note disponible")

        # Scatter plot Prix vs Vote
        st.subheader("💰 Relation Prix - Note")
        df_complet = df_filtered[(df_filtered['Prix'].notna()) & (df_filtered['Vote'].notna())]
        if len(df_complet) > 0:
            fig_scatter = px.scatter(
                df_complet,
                x='Vote',
                y='Prix',
                hover_data=['titre'],
                title="Prix en fonction de la note",
                labels={'Vote': 'Note (étoiles)', 'Prix': 'Prix (USD)'},
                color='Vote',
                color_continuous_scale='Viridis'
            )

            # Ligne de tendance
            if len(df_complet) > 1:
                z = np.polyfit(df_complet['Vote'], df_complet['Prix'], 1)
                p = np.poly1d(z)
                x_line = np.linspace(df_complet['Vote'].min(), df_complet['Vote'].max(), 100)
                fig_scatter.add_trace(
                    go.Scatter(
                        x=x_line,
                        y=p(x_line),
                        mode='lines',
                        name='Tendance',
                        line=dict(color='red', dash='dash')
                    )
                )

                # Corrélation
                correlation = df_complet['Prix'].corr(df_complet['Vote'])
                st.info(f"📈 Corrélation Prix-Note: **{correlation:.3f}**")

            fig_scatter.update_layout(height=500)
            st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.warning("Pas assez de données pour afficher la corrélation")


# ===========================
# TAB 2 : CONTRÔLE SCRAPING
# ===========================

with tab2:
    st.header("🤖 Contrôle du Scraping")

    col1, col2 = st.columns([2, 1])

    with col1:
        st.subheader("Lancer un Scraping Manuel")

        with st.form("scraping_form"):
            mots_cles = st.text_input("Mots-clés de recherche", value="laptop")
            nb_produits = st.number_input("Nombre minimum de produits", min_value=10, max_value=500, value=100)
            max_pages = st.number_input("Pages maximum à scraper", min_value=1, max_value=20, value=5)

            submit_button = st.form_submit_button("🚀 Lancer le Scraping")

            if submit_button:
                with st.spinner(f"Scraping en cours de '{mots_cles}'..."):
                    try:
                        result = run_scraping_job(mots_cles=mots_cles, nb_produits=nb_produits, max_pages=max_pages)

                        if result['status'] == 'SUCCESS':
                            st.success(f"✅ Scraping terminé avec succès !")
                            st.metric("Produits scrapés", result['nb_produits'])
                            st.metric("Nouveaux produits", result['nouveaux'])
                            st.metric("Mis à jour", result['mises_a_jour'])
                            st.metric("Durée (secondes)", f"{result['duree']:.1f}s")

                            # Rafraîchir le cache
                            st.cache_data.clear()
                            st.rerun()
                        else:
                            st.error(f"❌ Erreur : {result.get('error', 'Erreur inconnue')}")

                    except Exception as e:
                        st.error(f"❌ Erreur durant le scraping : {e}")

    with col2:
        st.subheader("Configuration")

        stats = load_stats()

        if stats:
            st.metric("Total Produits en BDD", stats.get('total_produits', 0))
            st.metric("Total Scrapings", stats.get('total_scrapings', 0))

            if stats.get('dernier_scraping'):
                st.info(f"**Dernier scraping :**\n{stats['dernier_scraping']}")

        st.markdown("---")

        # Informations sur le scheduler
        if scheduler:
            st.success("✅ Scheduler actif")
            st.info("📅 Scraping quotidien à 2h00\n🔔 Alertes quotidiennes à 8h00")
        else:
            st.warning("⚠️ Scheduler désactivé (Cloud Run)")
            st.info("Utilisez Cloud Scheduler pour automatisation")


# ===========================
# TAB 3 : ALERTES
# ===========================

with tab3:
    st.header("🔔 Alertes & Historique")

    df_alertes = load_alertes()

    if df_alertes.empty:
        st.info("📭 Aucune alerte pour le moment.")
    else:
        st.subheader(f"📬 {len(df_alertes)} Alerte(s) Récente(s)")

        # Tableau des alertes
        df_alertes_display = df_alertes.copy()
        df_alertes_display['ancien_prix'] = df_alertes_display['ancien_prix'].apply(
            lambda x: f"${x:.2f}" if pd.notna(x) else "N/A"
        )
        df_alertes_display['nouveau_prix'] = df_alertes_display['nouveau_prix'].apply(
            lambda x: f"${x:.2f}" if pd.notna(x) else "N/A"
        )

        st.dataframe(df_alertes_display, use_container_width=True, hide_index=True)

    st.markdown("---")

    # Bouton pour déclencher alertes manuellement
    if st.button("🔔 Vérifier et Envoyer Alertes Maintenant"):
        with st.spinner("Vérification des alertes..."):
            try:
                result = run_alerte_job()
                if result['status'] == 'SUCCESS':
                    st.success("✅ Alertes vérifiées et envoyées !")
                else:
                    st.error(f"❌ Erreur : {result.get('error')}")
            except Exception as e:
                st.error(f"❌ Erreur : {e}")


# ===========================
# TAB 4 : STATISTIQUES AVANCÉES
# ===========================

with tab4:
    st.header("📈 Statistiques Avancées")

    df = load_data()

    if not df.empty:
        # Top 10 produits
        col_top1, col_top2 = st.columns(2)

        with col_top1:
            st.subheader("🏆 Top 10 - Mieux Notés")
            top_votes = df.nlargest(10, 'Vote')[['titre', 'Prix', 'Vote']]
            if len(top_votes) > 0:
                top_votes_display = top_votes.copy()
                top_votes_display['Prix'] = top_votes_display['Prix'].apply(
                    lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A"
                )
                top_votes_display['Vote'] = top_votes_display['Vote'].apply(
                    lambda x: f"{x:.1f}/5" if pd.notna(x) else "N/A"
                )
                st.dataframe(top_votes_display, use_container_width=True, hide_index=True)

        with col_top2:
            st.subheader("💎 Top 10 - Plus Chers")
            top_prix = df.nlargest(10, 'Prix')[['titre', 'Prix', 'Vote']]
            if len(top_prix) > 0:
                top_prix_display = top_prix.copy()
                top_prix_display['Prix'] = top_prix_display['Prix'].apply(
                    lambda x: f"${x:,.2f}" if pd.notna(x) else "N/A"
                )
                top_prix_display['Vote'] = top_prix_display['Vote'].apply(
                    lambda x: f"{x:.1f}/5" if pd.notna(x) else "N/A"
                )
                st.dataframe(top_prix_display, use_container_width=True, hide_index=True)

        st.markdown("---")

        # Statistiques détaillées
        with st.expander("📊 Statistiques Détaillées"):
            col_stat1, col_stat2 = st.columns(2)

            with col_stat1:
                st.markdown("#### Prix")
                prix_stats = df['Prix'].describe()
                st.dataframe(prix_stats, use_container_width=True)

            with col_stat2:
                st.markdown("#### Notes")
                vote_stats = df['Vote'].describe()
                st.dataframe(vote_stats, use_container_width=True)


# ===========================
# FOOTER
# ===========================

st.markdown("---")
st.markdown(
    f"""
    <div style='text-align: center; color: gray;'>
    <p>Dashboard créé avec ❤️ par ISSEA MDSMS2 | Projet Amazon Webscraping</p>
    <p>Dernière mise à jour: {datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
    <p>🌐 Déployé sur Google Cloud Run</p>
    </div>
    """,
    unsafe_allow_html=True
)
