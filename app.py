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

# 🧩 Download IRS BMF file if needed
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

# 🧩 Load IRS BMF Data
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
    return combined_data

# 🧠 Find best-matching name column
def find_best_column_match(columns):
    if not columns:
        return None
    keywords = ["company", "organization", "name", "nonprofit", "business", "entity"]
    normalized = [col.lower().strip() for col in columns]
    try:
        for keyword in keywords:
            result = process.extractOne(keyword, normalized)
            if result:
                match, score = result
                if score >= 60:
                    return columns[normalized.index(match)]
    except Exception as e:
        st.warning(f"Fuzzy match failed: {e}")
    return columns[0]

# 🧼 Clean Uploaded Data
def clean_uploaded_data(uploaded_file):
    uploaded_data = pd.read_csv(uploaded_file, dtype=str)
    uploaded_data.columns = uploaded_data.columns.str.lower().str.strip()
    org_name_column = find_best_column_match(uploaded_data.columns.tolist())
    uploaded_data[org_name_column] = uploaded_data[org_name_column].str.lower().str.strip()
    return uploaded_data, org_name_column

# 🔍 EIN match
def match_eins_exact(uploaded_df, org_name_column, bmf_df, bmf_name_col):
    if bmf_name_col not in bmf_df.columns:
        st.error(f"❌ IRS BMF column '{bmf_name_col}' not found.")
        st.stop()

    bmf_df[bmf_name_col] = bmf_df[bmf_name_col].str.lower().str.strip()
    return uploaded_df.merge(
        bmf_df[[bmf_name_col, 'ein', 'ntee_cd', 'revenue_amt', 'income_amt', 'asset_amt']],
        left_on=org_name_column,
        right_on=bmf_name_col,
        how='left'
    )

# 🚀 Async ProPublica EIN enrichment
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
        tasks = [fetch_propublica_async(session, ein) for ein in ein_list if ein]
        return await asyncio.gather(*tasks)

# ✅ Deduplication
def deduplicate(df, org_name_column):
    sort_cols = ["ein"]
    if "revenue_amt" in df.columns:
        sort_cols.append("revenue_amt")
    try:
        df = df.sort_values(by=sort_cols, ascending=[True] + [False] * (len(sort_cols) - 1))
    except:
        pass
    df = df.drop_duplicates(subset=["ein"], keep="first")
    df = df.drop_duplicates(subset=[org_name_column], keep="first")
    return df

# 🌐 Streamlit UI
st.set_page_config(page_title="Nonprofit Enrichment Tool", layout="wide")
st.title("🚀 Nonprofit Enrichment Tool with BMF + ProPublica")

# Step 1: Load BMF
download_and_extract_bmf()
bmf_data = load_bmf_data()
st.write("📄 IRS BMF Columns:", bmf_data.columns.tolist())  # Show IRS column names

# 🛠 Hardcoded fix — override fuzzy logic
bmf_name_col = "name"  # If this doesn't work, update to match what you see in the columns printed above

# Step 2: File Upload
uploaded_file = st.file_uploader("📤 Upload CSV with Organization Names Only", type=["csv"])
if uploaded_file:
    uploaded_df, org_name_column = clean_uploaded_data(uploaded_file)
    st.subheader("📄 Uploaded Preview")
    st.dataframe(uploaded_df.head())

    if st.button("🚀 Enrich Now"):
        st.info("Step 1️⃣: Matching EINs from IRS...")
        enriched_df = match_eins_exact(uploaded_df, org_name_column, bmf_data, bmf_name_col)
        enriched_df.rename(columns={"ein": "EIN"}, inplace=True)

        st.info("Step 2️⃣: ProPublica EIN enrichment...")
        eins_to_fetch = enriched_df["EIN"].dropna().unique().tolist()
        propublica_results = asyncio.run(fetch_all_propublica(eins_to_fetch))
        pro_df = pd.DataFrame([row for row in propublica_results if row])

        if not pro_df.empty:
            enriched_df = enriched_df.merge(pro_df, on="EIN", how="left")

        enriched_df = deduplicate(enriched_df, org_name_column)

        st.success("✅ Enrichment Complete!")
        st.dataframe(enriched_df.head())

        csv = enriched_df.to_csv(index=False).encode('utf-8')
        st.download_button("📥 Download Enriched CSV", data=csv, file_name="enriched_data.csv", mime="text/csv")
