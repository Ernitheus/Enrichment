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

IRS_ZIP_URL = "https://apps.irs.gov/pub/epostcard/data-download-pub78.zip"
BMF_FOLDER_PATH = "IRS_EO_BMF"
PROPUBLICA_API_URL = "https://projects.propublica.org/nonprofits/api/v2/organizations/"

# Download IRS BMF ZIP
def download_and_extract_bmf():
    if not os.path.exists(BMF_FOLDER_PATH) or not os.listdir(BMF_FOLDER_PATH):
        st.info("📦 Downloading IRS BMF data...")
        try:
            response = requests.get(IRS_ZIP_URL)
            if response.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    z.extractall(BMF_FOLDER_PATH)
                st.success("✅ IRS BMF data downloaded.")
            else:
                st.error("❌ IRS BMF download failed.")
        except Exception as e:
            st.error(f"❌ Download error: {e}")

# Load BMF
@st.cache_data
def load_bmf_data():
    csv_files = glob.glob(os.path.join(BMF_FOLDER_PATH, "*.csv"))
    if not csv_files:
        return pd.DataFrame()
    data = pd.concat([pd.read_csv(f, dtype=str, low_memory=False) for f in csv_files], ignore_index=True)
    data.columns = data.columns.str.lower().str.strip()
    return data

# Auto-pick org name column
def get_bmf_name_col(bmf_columns):
    preferred_names = ["name", "organizationname", "org_name", "orgname", "entityname"]
    for col in preferred_names:
        if col in bmf_columns:
            return col
    # fallback: fuzzy match
    result = process.extractOne("name", bmf_columns)
    if result:
        match, score = result
        return match if score >= 60 else None
    return None

# Clean uploaded data
def clean_uploaded_data(uploaded_file):
    df = pd.read_csv(uploaded_file, dtype=str)
    df.columns = df.columns.str.lower().str.strip()
    org_col = process.extractOne("name", df.columns.tolist())[0]
    df[org_col] = df[org_col].str.lower().str.strip()
    return df, org_col

# Match EINs
def match_eins(uploaded_df, org_col, bmf_df, bmf_name_col):
    bmf_df[bmf_name_col] = bmf_df[bmf_name_col].str.lower().str.strip()
    return uploaded_df.merge(
        bmf_df[[bmf_name_col, 'ein', 'ntee_cd', 'revenue_amt', 'income_amt', 'asset_amt']],
        left_on=org_col,
        right_on=bmf_name_col,
        how='left'
    )

# ProPublica API
async def fetch_propublica_async(session, ein):
    url = f"{PROPUBLICA_API_URL}{ein}.json"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                org = data.get("organization", {})
                return {
                    "EIN": ein,
                    "Number of Employees": org.get("employee_count", "N/A"),
                    "Website": org.get("website", "N/A"),
                    "Mission Statement": org.get("mission", "N/A"),
                    "IRS 990 Filing": f"https://projects.propublica.org/nonprofits/organizations/{ein}/full",
                    "Key Employees": "; ".join([
                        f"{o.get('name', 'N/A')} ({o.get('title', 'N/A')}) - ${o.get('compensation', 'N/A')}"
                        for o in org.get("officers", [])
                    ]) or "N/A"
                }
    except:
        return None

async def fetch_all_propublica(eins):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_propublica_async(session, ein) for ein in eins if ein and ein != "N/A"]
        return await asyncio.gather(*tasks)

# Deduplicate
def deduplicate(df, org_col):
    df = df.sort_values(by=["ein"], ascending=True)
    df = df.drop_duplicates(subset=["ein"], keep="first")
    df = df.drop_duplicates(subset=[org_col], keep="first")
    return df

# Streamlit UI
st.set_page_config(page_title="Nonprofit Enrichment Tool", layout="wide")
st.title("🚀 Nonprofit Enrichment Tool (Auto-Match)")

# Step 1: IRS BMF
download_and_extract_bmf()
bmf_data = load_bmf_data()

if bmf_data.empty:
    st.error("❌ IRS BMF data failed to load.")
    st.stop()

bmf_name_col = get_bmf_name_col(bmf_data.columns.tolist())
if not bmf_name_col:
    st.error("❌ Could not determine name column from IRS BMF.")
    st.write("📄 Available columns:", bmf_data.columns.tolist())
    st.stop()

# Step 2: Upload
uploaded_file = st.file_uploader("📤 Upload a CSV with organization names", type=["csv"])
if uploaded_file:
    uploaded_df, org_name_col = clean_uploaded_data(uploaded_file)
    st.subheader("📄 Uploaded Preview")
    st.dataframe(uploaded_df.head())

    if st.button("🚀 Enrich Now"):
        st.info("🔎 Matching EINs from IRS...")
        enriched = match_eins(uploaded_df, org_name_col, bmf_data, bmf_name_col)
        enriched.rename(columns={"ein": "EIN"}, inplace=True)

        eins = enriched["EIN"].dropna().unique().tolist()
        st.info("🔎 Enriching from ProPublica...")
        pro_data = asyncio.run(fetch_all_propublica(eins))
        pro_df = pd.DataFrame([r for r in pro_data if r])

        if not pro_df.empty:
            enriched = enriched.merge(pro_df, on="EIN", how="left")

        enriched = deduplicate(enriched, org_name_col)

        st.success("✅ Enrichment Complete!")
        st.dataframe(enriched.head())

        csv = enriched.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Download Enriched CSV", data=csv, file_name="enriched.csv", mime="text/csv")
