import streamlit as st
import pandas as pd
import psycopg2
from io import BytesIO
import openai

# OpenAI API Configuration
openai_client = openai.Client(api_key="")

# Redshift environment configurations
environments = {
    "Migration": {"host": "", "database": "rsdev01", "user": "", "password": ""},
    "Modernization": {"host": "", "database": "rs_edw_db", "user": "", "password": ""},
    "IBD": {"host": "", "database": "rs_edw_prd", "user": "", "password": ""}
}

st.set_page_config(layout="wide", page_title="Redshift Fetcher", page_icon="📊")
st.title("🚀 Redshift Data Fetcher")

environment = st.selectbox("🌍 Select Environment", list(environments.keys()))

def get_redshift_connection(env):
    config = environments[env]
    return psycopg2.connect(
        host=config["host"],
        dbname=config["database"],
        user=config["user"],
        password=config["password"],
        port=5439
    )

def fetch_data():
    if environment:
        conn = get_redshift_connection(environment)
        schema_query = "SELECT DISTINCT table_schema FROM information_schema.tables;"
        schemas_df = pd.read_sql(schema_query, conn)
        schema = st.selectbox("📁 Select Schema", schemas_df['table_schema'].tolist())

        object_type = st.radio("🗂️ Select Object Type", ("Table", "View"))
        query_objects = f"""
            SELECT table_name FROM information_schema.{'tables' if object_type == 'Table' else 'views'} 
            WHERE table_schema = '{schema}';
        """
        objects_df = pd.read_sql(query_objects, conn)
        selected_object = st.selectbox("📋 Select Table or View", objects_df['table_name'].tolist())

        if selected_object:
            preview_query = f"SELECT * FROM {schema}.\"{selected_object}\" LIMIT 100;"
            try:
                df_preview = pd.read_sql(preview_query, conn)
                st.write(df_preview.head(20))
            except Exception as e:
                st.error(f"❌ Error fetching preview: {e}")

        retrieval_mode = st.radio("🔍 Select Data Retrieval Mode", ("SQL Mode", "Date Range Mode"))
        df = None

        if retrieval_mode == "SQL Mode":
            query_input = st.text_area("💻 Enter SQL Query", f"SELECT * FROM {schema}.\"{selected_object}\" LIMIT 100;")
            if st.button("🔍 Fetch Data"):
                try:
                    df = pd.read_sql(query_input, conn)
                    st.write(df.head(20))
                except Exception as e:
                    st.error(f"❌ Error executing SQL: {e}")
                    suggestion = openai_client.chat.completions.create(
                        model="gpt-4",
                        messages=[
                            {"role": "system", "content": "You are an SQL expert helping users debug queries."},
                            {"role": "user", "content": f"Fix the following SQL error: {e}"}
                        ]
                    )
                    st.warning(f"⚠️ Suggested Fix: {suggestion.choices[0].message.content}")
        
        elif retrieval_mode == "Date Range Mode":
            df_preview = pd.read_sql(f"SELECT * FROM {schema}.\"{selected_object}\" LIMIT 1;", conn)
            date_columns = [col for col in df_preview.columns if 'date' in col.lower()]
            if date_columns:
                date_column = st.selectbox("📆 Select Date Column", date_columns)
            else:
                st.error("❌ No date columns found in this table.")
                return
            
            date_type = st.radio("📅 Date Type", ("Custom", "MTD", "YTD"))
            
            if date_type == "Custom":
                start_date = st.date_input("📅 Start Date")
                end_date = st.date_input("📅 End Date")
                if st.button("📊 Fetch Data"):
                    custom_query = f"SELECT * FROM {schema}.\"{selected_object}\" WHERE {date_column} BETWEEN '{start_date}' AND '{end_date}';"
                    df = pd.read_sql(custom_query, conn)
                    st.write(df.head(20))
            
            elif date_type == "MTD":
                months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
                month = st.selectbox("📅 Select Month", months)
                fiscal_year = st.selectbox("📅 Select Fiscal Year", range(2020, 2026))
                if st.button("📊 Fetch Data"):
                    mtd_query = f"""
                        SELECT * FROM {schema}.\"{selected_object}\"
                        WHERE {date_column} >= '{fiscal_year}-{months.index(month) + 1}-01'
                        AND {date_column} < '{fiscal_year}-{months.index(month) + 2}-01';
                    """
                    df = pd.read_sql(mtd_query, conn)
                    st.write(df.head(20))
            
            elif date_type == "YTD":
                fiscal_year = st.selectbox("📅 Select Fiscal Year", range(2020, 2026))
                if st.button("📊 Fetch Data"):
                    ytd_query = f"SELECT * FROM {schema}.\"{selected_object}\" WHERE {date_column} BETWEEN '{fiscal_year}-04-01' AND '{fiscal_year + 1}-03-31';"
                    df = pd.read_sql(ytd_query, conn)
                    st.write(df.head(20))
        
        if df is not None and not df.empty:
            buffer = BytesIO()
            df.to_csv(buffer, index=False)
            buffer.seek(0)
            st.download_button("📥 Download CSV", buffer, f"{schema}_{selected_object}.csv", "text/csv")
        
        conn.close()

fetch_data()
