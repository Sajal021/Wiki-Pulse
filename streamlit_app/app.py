import streamlit as st

st.set_page_config(page_title="WikiPulse", page_icon="📡", layout="wide")

st.title("📡 WikiPulse")
st.subheader("Real-time Wikipedia edit stream → breaking event detection")

st.info(
    "This is the Phase 0 placeholder dashboard. Once Phase 5 (anomaly detection) "
    "and Phase 6 (Gold layer / dbt models) are built, this page will show live "
    "edit-velocity anomalies, trending articles, and bot-vs-human breakdowns."
)

st.write("Environment check:")
st.write("- Kafka broker: `kafka:9092`")
st.write("- MinIO (S3-compatible): `http://minio:9000`")
st.write("- Spark master: `spark://spark-master:7077`")
st.write("- Airflow UI: `http://localhost:8081`")
