"""
Dashboard Interactif - Projet Amazon Webscraping
ISSEA MDSMS2 - 2025/2026

Pour lancer : streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
from datetime import datetime

# Configuration de la page
st.set_page_config(
    page_title="Amazon Analytics Dashboard",
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
    </style>
    """, unsafe_allow_html=True)

# Titre principal
st.title("🛒 Amazon Analytics Dashboard")
st.markdown("### Analyse en temps réel des produits scrapés")
st.markdown("---")

# Chargement des données
@st.cache_data
def load_data():
    """Charge les données du fichier CSV"""
    try:
        df = pd.read_csv("bd.csv")
        # Conversion des types
        df['Prix'] = pd.to_numeric(df['Prix'], errors='coerce')
        df['Vote'] = pd.to_numeric(df['Vote'], errors='coerce')
        return df
    except FileNotFoundError:
        st.error("❌ Fichier bd.csv non trouvé! Veuillez d'abord exécuter le scraping.")
        return None
    except Exception as e:
        st.error(f"❌ Erreur lors du chargement: {e}")
        return None

df = load_data()

if df is not None:
    # Sidebar - Filtres
    st.sidebar.header("🔍 Filtres")

    # Filtre prix
    st.sidebar.subheader("Prix (XAF)")
    prix_valides = df['Prix'].dropna()
    if len(prix_valides) > 0:
        prix_min = int(prix_valides.min())
        prix_max = int(prix_valides.max())
        prix_range = st.sidebar.slider(
            "Sélectionner une fourchette de prix",
            min_value=prix_min,
            max_value=prix_max,
            value=(prix_min, prix_max),
            step=10000
        )
    else:
        prix_range = (0, 0)

    # Filtre vote
    st.sidebar.subheader("Note minimale")
    vote_valides = df['Vote'].dropna()
    if len(vote_valides) > 0:
        vote_min = st.sidebar.slider(
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
            st.metric(
                label="Prix Moyen",
                value=f"XAF {prix_moy:,.0f}"
            )
        else:
            st.metric(label="Prix Moyen", value="N/A")

    with col3:
        vote_moy = df_filtered['Vote'].mean()
        if pd.notna(vote_moy):
            st.metric(
                label="Note Moyenne",
                value=f"{vote_moy:.2f}/5.0"
            )
        else:
            st.metric(label="Note Moyenne", value="N/A")

    with col4:
        prix_median = df_filtered['Prix'].median()
        if pd.notna(prix_median):
            st.metric(
                label="Prix Médian",
                value=f"XAF {prix_median:,.0f}"
            )
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
                labels={'Prix': 'Prix (XAF)', 'count': 'Nombre de produits'},
                color_discrete_sequence=['#636EFA']
            )
            fig_hist.update_layout(
                showlegend=False,
                height=400
            )
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
            fig_vote.update_layout(
                showlegend=False,
                height=400
            )
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
            hover_data=['Titre'],
            title="Prix en fonction de la note",
            labels={'Vote': 'Note (étoiles)', 'Prix': 'Prix (XAF)'},
            color='Vote',
            color_continuous_scale='Viridis',
            size_max=10
        )

        # Ajouter ligne de tendance
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

            # Calculer corrélation
            correlation = df_complet['Prix'].corr(df_complet['Vote'])
            st.info(f"📈 Corrélation Prix-Note: **{correlation:.3f}**")

        fig_scatter.update_layout(height=500)
        st.plotly_chart(fig_scatter, use_container_width=True)
    else:
        st.warning("Pas assez de données pour afficher la corrélation")

    st.markdown("---")

    # Top produits
    col_top1, col_top2 = st.columns(2)

    with col_top1:
        st.subheader("🏆 Top 10 - Mieux Notés")
        top_votes = df_filtered.nlargest(10, 'Vote')[['Titre', 'Prix', 'Vote']]
        if len(top_votes) > 0:
            # Formater l'affichage
            top_votes_display = top_votes.copy()
            top_votes_display['Prix'] = top_votes_display['Prix'].apply(
                lambda x: f"XAF {x:,.0f}" if pd.notna(x) else "N/A"
            )
            top_votes_display['Vote'] = top_votes_display['Vote'].apply(
                lambda x: f"{x:.1f}/5" if pd.notna(x) else "N/A"
            )
            top_votes_display['Titre'] = top_votes_display['Titre'].apply(
                lambda x: x[:50] + "..." if len(str(x)) > 50 else x
            )
            st.dataframe(top_votes_display, use_container_width=True, hide_index=True)
        else:
            st.warning("Aucune donnée disponible")

    with col_top2:
        st.subheader("💎 Top 10 - Plus Chers")
        top_prix = df_filtered.nlargest(10, 'Prix')[['Titre', 'Prix', 'Vote']]
        if len(top_prix) > 0:
            # Formater l'affichage
            top_prix_display = top_prix.copy()
            top_prix_display['Prix'] = top_prix_display['Prix'].apply(
                lambda x: f"XAF {x:,.0f}" if pd.notna(x) else "N/A"
            )
            top_prix_display['Vote'] = top_prix_display['Vote'].apply(
                lambda x: f"{x:.1f}/5" if pd.notna(x) else "N/A"
            )
            top_prix_display['Titre'] = top_prix_display['Titre'].apply(
                lambda x: x[:50] + "..." if len(str(x)) > 50 else x
            )
            st.dataframe(top_prix_display, use_container_width=True, hide_index=True)
        else:
            st.warning("Aucune donnée disponible")

    st.markdown("---")

    # Statistiques détaillées
    with st.expander("📊 Statistiques Détaillées"):
        col_stat1, col_stat2 = st.columns(2)

        with col_stat1:
            st.markdown("#### Prix")
            prix_stats = df_filtered['Prix'].describe()
            st.dataframe(prix_stats, use_container_width=True)

        with col_stat2:
            st.markdown("#### Notes")
            vote_stats = df_filtered['Vote'].describe()
            st.dataframe(vote_stats, use_container_width=True)

    # Tableau de données complet
    with st.expander("📋 Voir toutes les données"):
        st.dataframe(df_filtered, use_container_width=True)

    # Footer
    st.markdown("---")
    st.markdown(
        f"""
        <div style='text-align: center; color: gray;'>
        <p>Dashboard créé avec ❤️ par ISSEA MDSMS2</p>
        <p>Dernière mise à jour: {datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
        </div>
        """,
        unsafe_allow_html=True
    )
else:
    st.error("Impossible de charger les données. Veuillez vérifier que bd.csv existe.")
