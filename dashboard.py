import streamlit as st
import boto3
import pandas as pd
import plotly.express as px
import time
import os

# --- CONFIGURATION ---
st.set_page_config(page_title="Automation LTA - Dashboard Emailing", layout="wide")

# VOS PARAMÈTRES FIXES (Stockholm)
AWS_REGION = "eu-west-3"
ATHENA_BUCKET = "athena-results-l3a" 
DATABASE = "default"

# --- GESTION ROBUSTE DES CLÉS (CLOUD vs LOCAL) ---
athena_client = None
s3_client = None
mode_cloud = False

# 1. On essaie de détecter les secrets sans faire planter le script
try:
    # Cette vérification est maintenant protégée
    if st.secrets is not None and "aws" in st.secrets:
        mode_cloud = True
except FileNotFoundError:
    mode_cloud = False
except Exception:
    mode_cloud = False

# 2. Connexion selon le résultat
if mode_cloud:
    # --- MODE CLOUD (Streamlit Share) ---
    region = st.secrets["aws"]["region_name"]
    ak = st.secrets["aws"]["aws_access_key_id"]
    sk = st.secrets["aws"]["aws_secret_access_key"]
    
    athena_client = boto3.client("athena", region_name=region, aws_access_key_id=ak, aws_secret_access_key=sk)
    s3_client = boto3.client("s3", region_name=region, aws_access_key_id=ak, aws_secret_access_key=sk)

else:
    # --- MODE LOCAL (Votre PC) ---
    # Chemin vers votre fichier CSV de clés
    path_keys = r"D:\Business\Emailing\SES\William_admin_accessKeys.csv"
    
    try:
        keys_df = pd.read_csv(path_keys)
        # Récupération des clés depuis le CSV
        ACCESS_KEY = keys_df.iloc[0]['Access key ID']
        SECRET_KEY = keys_df.iloc[0]['Secret access key']
        
        athena_client = boto3.client("athena", region_name=AWS_REGION, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        s3_client = boto3.client("s3", region_name=AWS_REGION, aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
        
        st.success("✅ Mode Local activé : Clés chargées depuis le CSV.")
        
    except Exception as e:
        st.error("❌ ERREUR CRITIQUE (Mode Local)")
        st.error(f"Impossible de lire le fichier de clés ici : {path_keys}")
        st.error(f"Détail : {e}")
        st.stop()

# --- FONCTION DE CHARGEMENT DES DONNÉES ---
@st.cache_data(ttl=600)
def load_data():
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

    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': f"s3://{ATHENA_BUCKET}/dashboard-temp/"},
            WorkGroup='primary'
        )
        query_id = response['QueryExecutionId']

        while True:
            stats = athena_client.get_query_execution(QueryExecutionId=query_id)
            status = stats['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            time.sleep(0.5)

        if status == 'SUCCEEDED':
            obj = s3_client.get_object(Bucket=ATHENA_BUCKET, Key=f"dashboard-temp/{query_id}.csv")
            df = pd.read_csv(obj['Body'])
            return df
        else:
            st.error(f"Erreur Athena : {status}")
            st.error(stats['QueryExecution']['Status'].get('StateChangeReason', 'Raison inconnue'))
            return pd.DataFrame()

    except Exception as e:
        st.error(f"Erreur Technique : {e}")
        return pd.DataFrame()

# --- AFFICHAGE ---
st.title("📊 Monitor Emailing - Automation LTA")

with st.spinner('Chargement des données...'):
    df = load_data()

if df.empty:
    st.warning("Aucune donnée trouvée.")
    st.stop()

# Nettoyage des dates
try:
    df['Jour'] = pd.to_datetime(df['Jour']).dt.date
except:
    pass

# Filtres
st.sidebar.header("Filtres")
campagnes = ['Toutes'] + list(df['Campagne'].unique())
choix_campagne = st.sidebar.selectbox("Campagne", campagnes)

if choix_campagne != 'Toutes':
    df_filtered = df[df['Campagne'] == choix_campagne]
else:
    df_filtered = df

# KPIs
kpi_df = df_filtered.groupby('eventType')['Total'].sum()
col1, col2, col3, col4 = st.columns(4)

total_sent = kpi_df.get('Send', 0)
total_open = kpi_df.get('Open', 0)
total_click = kpi_df.get('Click', 0)
total_bounce = kpi_df.get('Bounce', 0)

taux_ouverture = round((total_open / total_sent * 100), 2) if total_sent > 0 else 0
taux_clic = round((total_click / total_open * 100), 2) if total_open > 0 else 0

col1.metric("Envoyés", int(total_sent))
col2.metric("Ouvertures", int(total_open), f"{taux_ouverture}%")
col3.metric("Clics", int(total_click), f"{taux_clic}%")
col4.metric("Bounces", int(total_bounce), delta_color="inverse")

# Graphique
st.markdown("### 📈 Chronologie")
if not df_filtered.empty:
    chart_data = df_filtered.groupby(['Jour', 'eventType'])['Total'].sum().reset_index()
    fig = px.line(chart_data, x='Jour', y='Total', color='eventType', markers=True)
    st.plotly_chart(fig)

with st.expander("Données brutes"):
    st.dataframe(df_filtered)

if st.button('Rafraîchir'):
    st.cache_data.clear()
