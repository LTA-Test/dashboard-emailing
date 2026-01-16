import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
import time

@st.cache_data(ttl=600)
def load_data():
    # On récupère les clés depuis les Secrets de Streamlit
    client = boto3.client(
        "athena", 
        region_name=st.secrets["aws"]["region_name"],
        aws_access_key_id=st.secrets["aws"]["aws_access_key_id"],
        aws_secret_access_key=st.secrets["aws"]["aws_secret_access_key"]
    )

# --- CONFIGURATION ---
st.set_page_config(page_title="Agency LTA - Dashboard Emailing", layout="wide")

# Récupération des secrets (Configurés plus tard dans le Cloud)
# En local, il faudra créer un fichier .streamlit/secrets.toml
AWS_REGION = "eu-north-1"
ATHENA_BUCKET = "athena-results-lta"
DATABASE = "default"

# --- FONCTION DE CHARGEMENT DES DONNÉES (Mise en cache pour la vitesse) ---
@st.cache_data(ttl=600) # Rafraîchit les données toutes les 10 min
def load_data():
    client = boto3.client(
        "athena", 
        region_name=AWS_REGION,
        # Si hébergé, les clés seront gérées par les variables d'environnement
        # Si local, il prend vos clés ~/.aws/credentials
    )
    
    # Requête pour avoir les stats par jour et par campagne
    query = """
    SELECT 
        date_trunc('day', from_iso8601_timestamp(mail.timestamp)) AS Jour,
        element_at(mail.tags.CampaignID, 1) AS Campagne,
        eventType,
        count(*) as Total
    FROM ses_logs
    WHERE eventType IN ('Send', 'Delivery', 'Open', 'Click', 'Bounce', 'Complaint')
    GROUP BY 1, 2, 3
    ORDER BY 1 DESC
    """
    
    # Lancement de la requête (méthode simplifiée)
    # Note : Pour un vrai code de prod, on gère l'attente comme dans vos scripts précédents
    # Ici, on utilise une librairie qui simplifie tout si vous l'installez : awswrangler
    # MAIS pour rester simple avec boto3 pur, on fait ça :
    
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': DATABASE},
        ResultConfiguration={'OutputLocation': f"s3://{ATHENA_BUCKET}/dashboard-temp/"},
        WorkGroup='primary'
    )
    query_id = response['QueryExecutionId']
    
    # Attente
    while True:
        stats = client.get_query_execution(QueryExecutionId=query_id)
        status = stats['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(0.5)
        
    if status == 'SUCCEEDED':
        path = f"s3://{ATHENA_BUCKET}/dashboard-temp/{query_id}.csv"
        # On lit directement le CSV depuis S3 avec Pandas
        df = pd.read_csv(path)
        return df
    else:
        st.error("Erreur Athena")
        return pd.DataFrame()

# --- INTERFACE GRAPHIQUE ---
st.title("📊 Monitor Emailing - Agency LTA")

with st.spinner('Chargement des données depuis AWS...'):
    try:
        df = load_data()
    except Exception as e:
        st.error(f"Erreur de connexion : {e}")
        st.stop()

if df.empty:
    st.warning("Aucune donnée trouvée.")
    st.stop()

# Nettoyage des dates
df['Jour'] = pd.to_datetime(df['Jour']).dt.date

# --- FILTRES (SIDEBAR) ---
st.sidebar.header("Filtres")
campagnes = ['Toutes'] + list(df['Campagne'].unique())
choix_campagne = st.sidebar.selectbox("Choisir une campagne", campagnes)

if choix_campagne != 'Toutes':
    df_filtered = df[df['Campagne'] == choix_campagne]
else:
    df_filtered = df

# --- KPIS (CHIFFRES CLÉS) ---
# On pivote les données pour compter facilement
kpi_df = df_filtered.groupby('eventType')['Total'].sum()

col1, col2, col3, col4 = st.columns(4)

total_sent = kpi_df.get('Send', 0)
total_open = kpi_df.get('Open', 0)
total_click = kpi_df.get('Click', 0)
total_bounce = kpi_df.get('Bounce', 0)

# Calculs de taux
taux_ouverture = round((total_open / total_sent * 100), 2) if total_sent > 0 else 0
taux_clic = round((total_click / total_open * 100), 2) if total_open > 0 else 0

col1.metric("Emails Envoyés", total_sent)
col2.metric("Ouvertures", total_open, f"{taux_ouverture}% (Taux)")
col3.metric("Clics", total_click, f"{taux_clic}% (CTO)")
col4.metric("Bounces (Erreurs)", total_bounce, delta_color="inverse")

# --- GRAPHIQUES ---
st.markdown("### 📈 Évolution dans le temps")

# Graphique de ligne
chart_data = df_filtered.groupby(['Jour', 'eventType'])['Total'].sum().reset_index()
fig = px.line(chart_data, x='Jour', y='Total', color='eventType', markers=True,
              title="Activité journalière (Ouvertures, Clics, Envois)")
st.plotly_chart(fig, use_container_width=True)

# Tableau de données
with st.expander("Voir les données brutes"):
    st.dataframe(df_filtered)

# Bouton de rafraichissement manuel
if st.button('Rafraîchir les données'):

    st.cache_data.clear()
