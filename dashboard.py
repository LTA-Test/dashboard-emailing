import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
import time

# --- CONFIGURATION GLOBALE ---
st.set_page_config(page_title="Agency LTA - Dashboard Emailing", layout="wide")

# 👇 REMPLACEZ CECI PAR VOTRE VRAI BUCKET DE RÉSULTATS ATHENA 👇
ATHENA_BUCKET = "athena-results-lta" 
DATABASE = "default"

# --- FONCTION DE CHARGEMENT DES DONNÉES ---
@st.cache_data(ttl=600) # Rafraîchit les données toutes les 10 min
def load_data():
    # 1. Gestion de l'authentification (Cloud vs Local)
    if "aws" in st.secrets:
        # On est sur Streamlit Cloud
        region = st.secrets["aws"]["region_name"]
        ak = st.secrets["aws"]["aws_access_key_id"]
        sk = st.secrets["aws"]["aws_secret_access_key"]
        
        athena_client = boto3.client("athena", region_name=region, aws_access_key_id=ak, aws_secret_access_key=sk)
        s3_client = boto3.client("s3", region_name=region, aws_access_key_id=ak, aws_secret_access_key=sk)
    else:
        # On est en Local (PC)
        region = "eu-north-1"
        athena_client = boto3.client("athena", region_name=region)
        s3_client = boto3.client("s3", region_name=region)

    # 2. La Requête SQL
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
    
    # 3. Lancement de la requête Athena
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': f"s3://{ATHENA_BUCKET}/dashboard-temp/"},
            WorkGroup='primary'
        )
        query_id = response['QueryExecutionId']
        
        # 4. Attente du résultat
        while True:
            stats = athena_client.get_query_execution(QueryExecutionId=query_id)
            status = stats['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(0.5)
            
        if status == 'SUCCEEDED':
            # 5. Lecture du fichier CSV généré dans S3
            # On utilise le client S3 configuré (avec les secrets) pour lire le fichier
            obj = s3_client.get_object(Bucket=ATHENA_BUCKET, Key=f"dashboard-temp/{query_id}.csv")
            df = pd.read_csv(obj['Body'])
            return df
        else:
            st.error(f"Erreur Athena : {status}")
            st.error(stats['QueryExecution']['Status'].get('StateChangeReason', ''))
            return pd.DataFrame()

    except Exception as e:
        st.error(f"Erreur Technique : {e}")
        return pd.DataFrame()

# --- INTERFACE GRAPHIQUE ---
st.title("📊 Monitor Emailing - Agency LTA")

with st.spinner('Chargement des données depuis AWS...'):
    df = load_data()

if df.empty:
    st.warning("Aucune donnée trouvée ou erreur de connexion.")
    st.stop()

# Nettoyage des dates
df['Jour'] = pd.to_datetime(df['Jour']).dt.date

# --- SIDEBAR (FILTRES) ---
st.sidebar.header("Filtres")
campagnes = ['Toutes'] + list(df['Campagne'].unique())
choix_campagne = st.sidebar.selectbox("Choisir une campagne", campagnes)

if choix_campagne != 'Toutes':
    df_filtered = df[df['Campagne'] == choix_campagne]
else:
    df_filtered = df

# --- KPIS ---
kpi_df = df_filtered.groupby('eventType')['Total'].sum()
col1, col2, col3, col4 = st.columns(4)

total_sent = kpi_df.get('Send', 0)
total_open = kpi_df.get('Open', 0)
total_click = kpi_df.get('Click', 0)
total_bounce = kpi_df.get('Bounce', 0)

taux_ouverture = round((total_open / total_sent * 100), 2) if total_sent > 0 else 0
taux_clic = round((total_click / total_open * 100), 2) if total_open > 0 else 0

col1.metric("Emails Envoyés", int(total_sent))
col2.metric("Ouvertures", int(total_open), f"{taux_ouverture}%")
col3.metric("Clics", int(total_click), f"{taux_clic}%")
col4.metric("Bounces", int(total_bounce), delta_color="inverse")

# --- GRAPHIQUES ---
st.markdown("### 📈 Évolution temporelle")
chart_data = df_filtered.groupby(['Jour', 'eventType'])['Total'].sum().reset_index()
fig = px.line(chart_data, x='Jour', y='Total', color='eventType', markers=True)
st.plotly_chart(fig, use_container_width=True)

with st.expander("Voir les données brutes"):
    st.dataframe(df_filtered)

if st.button('Rafraîchir'):
    st.cache_data.clear()
