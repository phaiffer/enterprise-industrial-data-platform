import os
import pandas as pd
import psycopg2
import streamlit as st


def read_kpis_from_postgres() -> pd.DataFrame:
    """
    Read KPIs from Postgres. This is the recommended approach for Streamlit,
    keeping the dashboard decoupled from Spark runtime.
    """
    host = os.getenv("PG_HOST", "localhost")
    port = int(os.getenv("PG_PORT", "5432"))
    db = os.getenv("PG_DB", "eidp")
    user = os.getenv("PG_USER", "eidp")
    password = os.getenv("PG_PASSWORD", "eidp")
    table = os.getenv("PG_TABLE", "public.sensor_kpis")

    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
    try:
        query = f"SELECT * FROM {table} ORDER BY site, sensor_id;"
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()


def main() -> None:
    st.set_page_config(page_title="EIDP Dashboard", layout="wide")
    st.title("Enterprise Industrial Data Platform (EIDP) — KPIs")

    st.caption("Data source: Postgres (Gold KPIs).")

    try:
        df = read_kpis_from_postgres()
    except Exception as e:
        st.error("Could not read KPIs from Postgres. Make sure the Gold job wrote to Postgres.")
        st.code(str(e))
        st.stop()

    if df.empty:
        st.warning("No KPI rows found. Run the Gold aggregation job with ENABLE_POSTGRES_SINK=true.")
        st.stop()

    # Summary
    col1, col2, col3 = st.columns(3)
    col1.metric("Sites", df["site"].nunique())
    col2.metric("Sensors", df["sensor_id"].nunique())
    col3.metric("Total KPI rows", len(df))

    st.subheader("KPIs Table")
    st.dataframe(df, use_container_width=True)

    st.subheader("Filters")
    sites = sorted(df["site"].unique().tolist())
    selected_site = st.selectbox("Site", ["ALL"] + sites)

    if selected_site != "ALL":
        df = df[df["site"] == selected_site]

    st.subheader("Average Temperature by Sensor")
    chart_df = df[["sensor_id", "avg_temperature"]].set_index("sensor_id")
    st.bar_chart(chart_df)


if __name__ == "__main__":
    main()
