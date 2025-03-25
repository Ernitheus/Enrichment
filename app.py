import streamlit as st
import pandas as pd
import requests
import asyncio
import aiohttp
import os
import glob
import zipfile
import io
from fuzzywuzzy import process

# Constants
IRS_ZIP_URL = "https://apps.irs.gov/pub/epostcard/data-download-pub78.zip"
BMF_FOLDER_PATH = "IRS_EO_BMF"
PROPUBLICA_API_URL = "https://projects.propublica.org/nonprofits/api/v2/organizations/"

# 🚀 Download & Extract IRS BMF ZIP
def download_and_extract_bmf():
    if not os.path.exists(BMF_FOLDER_PATH) or not os.listdir(BMF_FOLDER_PATH):
        st.info("📦 Downloading latest IRS BMF data...")
        try:
            response = requests.get(IRS_ZIP_URL)
            if response.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    z.extractall(BMF_FOLDER_PATH)
                st.success("✅ IRS BMF files downloaded and extracted.")
            else:
                st.error("❌ Failed to download IRS BMF data.")
        except Exception as e:
            st.error(f"❌ Download error: {e}")

# 🚀 Load IRS BMF Data
@st.cache_data
def load_bmf_data():
    csv_files = glob.glob(os.path.join(BMF_FOLDER_PATH, "*.csv"))
    if not csv_files:
        st.error("No IRS BMF CSV files found.")
        return pd.DataFrame()

    combined_data = pd.concat(
        [pd.read_csv(file, dtype=str, low_memory=False) for file in csv_files],
        ignore_index=True,
    )
    combined_data.columns = combined_data.columns.str.lower().str.strip()

    if "ein" not in combined_data.columns or "name" not in combined_data.columns:
        st.error("Missing 'EIN' or 'name' column in BMF data.")
        return pd.DataFrame()

    return combined_data

# 🔍 Fuzzy Match Column
def find_best_column_match(possible_columns):
    keywords = ["company", "organization", "name", "nonprofit", "business", "entity"]
    normalized = [col.lower().strip() for col in possible_columns]
    for keyword in keywords:
        match, score = process.extractOne(keyword, normalized)
        if score >= 60:
            return possible_columns[normalized.index(match)]
    return possible_columns[0] if possible_columns else None

# 🚀 Clean Uploaded Data
def clean_uploaded_data(uploaded_file):
    uploaded_data = pd.read_csv(uploaded_file, dtype=str)
    if uploaded_data.empty:
        st.error("❌ Uploaded file is empty.")
        return None, None

    uploaded_data.columns = uploaded_data.columns.str.lower().str.strip()
    org_name_column = find_best_column_match(uploaded_data.columns.tolist())
    uploaded_data[org_name_column] = uploaded_data[org_name_column].str.lower().str.strip()
    return uploaded_data, org_name_column

# 🚀 EIN Matching
def match_eins_in_bmf(uploaded_df, org_name_column, bmf_data):
    bmf_data["name"] = bmf_data["name"].str.lower().str.strip()
    uploaded_df[org_name_column] = uploaded_df[org_name_column].str.lower().str.strip()

    enriched = uploaded_df.merge(
        bmf_data[['name', 'ein', 'ntee_cd', 'revenue_amt', 'income_amt', 'asset_amt']],
        left_on=org_name_column,
        right_on='name',
        how='left'
    )
    enriched.rename(columns={"ein": "EIN"}, inplace=True)
    return enriched

# 🚀 Deduplicate Data
def deduplicate_data(df, org_name_column):
    if "EIN" in df.columns:
        df = df.sort_values(by=["EIN", "revenue_amt"], ascending=[True, False])
        df = df.drop_duplicates(subset=["EIN"], keep="first")
    if org_name_column in df.columns:
        df = df.sort_values(by=["EIN", "revenue_amt"], ascending=[True, False])
        df = df.drop_duplicates(subset=[org_name_column], keep="first")
    return df

# 🚀 Async API Request
async def fetch_propublica_async(session, ein):
    url = f"{PROPUBLICA_API_URL}{ein}.json"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                org_data = data.get("organization", {})
                return {
                    "EIN": ein,
                    "Number of Employees": org_data.get("employee_count", "N/A"),
                    "Website": org_data.get("website", "N/A"),
                    "Mission Statement": org_data.get("mission", "N/A"),
                    "IRS 990 Filing": f"https://projects.propublica.org/nonprofits/organizations/{ein}/full",
                    "Key Employees": "; ".join([
                        f"{officer.get('name', 'N/A')} ({officer.get('title', 'N/A')}) - ${officer.get('compensation', 'N/A')}"
                        for officer in org_data.get("officers", [])
                    ]) or "N/A"
                }
    except:
        return None

async def fetch_all_propublica(ein_list):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_propublica_async(session, ein) for ein in ein_list if ein and ein != "N/A"]
        return await asyncio.gather(*tasks)

# 🚀 Streamlit App UI
st.set_page_config(page_title="Nonprofit Enrichment Tool", layout="wide")
st.title("🚀 Nonprofit Data Enrichment Tool")

# Download IRS BMF files if not present
download_and_extract_bmf()

# Load the IRS data
bmf_data = load_bmf_data()

# ✅ ProPublica Test
if st.button("🔍 Test ProPublica API"):
    test_ein = "131624102"  # American Red Cross
    result = asyncio.run(fetch_all_propublica([test_ein]))
    if result and result[0]:
        st.success("✅ ProPublica API is working!")
        st.json(result[0])
    else:
        st.error("❌ Failed to fetch data from ProPublica API.")

# File Upload
uploaded_csv = st.file_uploader("📤 Upload a CSV File with Organization Names", type=["csv"])

if uploaded_csv is not None:
    uploaded_data, org_name_column = clean_uploaded_data(uploaded_csv)
    if uploaded_data is not None:
        st.subheader("📄 Preview Uploaded Data")
        st.dataframe(uploaded_data.head())

        if st.button("🚀 Enrich Data"):
            st.info("🔄 Matching EINs with IRS BMF...")
            enriched_data = match_eins_in_bmf(uploaded_data, org_name_column, bmf_data)

            if "EIN" not in enriched_data.columns:
                enriched_data["EIN"] = "N/A"

            eins_to_fetch = enriched_data["EIN"].dropna().unique().tolist()
            eins_to_fetch = [ein for ein in eins_to_fetch if ein != "N/A"]

            st.info("🔄 Fetching additional data from ProPublica...")
            propublica_data = asyncio.run(fetch_all_propublica(eins_to_fetch))
            propublica_df = pd.DataFrame([item for item in propublica_data if item])

            if not propublica_df.empty:
                enriched_data = enriched_data.merge(propublica_df, on="EIN", how="left")
            else:
                st.warning("⚠️ No additional data found via ProPublica.")

            enriched_data = deduplicate_data(enriched_data, org_name_column)

            st.success("✅ Enrichment Complete!")
            st.dataframe(enriched_data.head())

            csv = enriched_data.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Enriched CSV",
                data=csv,
                file_name="enriched_data.csv",
                mime="text/csv"
            )
