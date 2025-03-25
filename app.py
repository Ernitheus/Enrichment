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

@st.cache_data
def load_bmf_data():
    files = glob.glob(os.path.join(BMF_FOLDER_PATH, "*"))
    if not files:
        st.warning("⚠️ No files found in extracted IRS BMF folder.")
        return pd.DataFrame()

    st.write("📁 Extracted files:", files)

    all_data = []
    for file in files:
        if file.endswith(".csv") or file.endswith(".txt"):
            try:
                st.write(f"🔍 Trying to read: {file}")
                df_preview = pd.read_csv(file, dtype=str, sep=None, engine="python", nrows=5)
                st.write("✅ File preview:")
                st.dataframe(df_preview)

                df = pd.read_csv(file, dtype=str, sep=None, engine="python")
                all_data.append(df)
            except Exception as e:
                st.warning(f"⚠️ Skipping file: {file} — {e}")

    if not all_data:
        st.warning("⚠️ No usable IRS BMF files loaded.")
        return pd.DataFrame()

    combined = pd.concat(all_data, ignore_index=True)
    combined.columns = combined.columns.str.lower().str.strip()
    return combined

def get_bmf_name_col(columns):
    preferred = ["name", "organizationname", "org_name", "orgname", "entityname"]
    for col in preferred:
        if col in columns:
            return col
    result = process.extractOne("name", columns)
    if result:
        match, score = result
        return match if score >= 60 else None
    return None

def clean_uploaded_data(uploaded_file):
    df = pd.read_csv(uploaded_file, dtype=str)
    df.columns = df.columns.str.lower().str.strip()
    org_col = process.extractOne("name", df.columns.tolist())[0]
    df[org_col] = df[org_col].str.lower().str.strip()
    return df, org_col

def match_eins(uploaded_df, org_col, bmf_df, bmf_name_col):
    bmf_df[bmf_name_col] = bmf_df[bmf_name_col].str.lower().str.strip()
    return uploaded_df.merge(
        bmf_df[[bmf_name_col, 'ein', 'ntee_cd', 'revenue_amt', 'income_amt', 'asset_amt']],
        left_on=org_col,
        right_on=bmf_name_col,
        how='left'
    )

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

def deduplicate(df, org_col):
    df = df.sort_values(by=["ein"], ascending=True)
    df = df.drop_duplicates(subset=["ein"], keep="first")
    df = df.drop_duplicates(subset=[org_col], keep="first")
    return df

# Streamlit App UI
st.set_page_config(page_title="Nonprofit Enrichment Tool", layout="wide")
st.title("🚀 Nonprofit Enrichment Tool (IRS BMF + ProPublica)")

# Step 1: Download IRS BMF
download_and_extract_bmf()
bmf_data = load_bmf_data()

if bmf_data.empty:
    st.error("❌ IRS BMF data failed to load.")
    st.stop()

bmf_name_col = get_bmf_name_col(bmf_data.columns.tolist())
if not bmf_name_col:
    st.error("❌ Could not find a usable name column in the IRS BMF.")
    st.write("📄 IRS Columns:", bmf_data.columns.tolist())
    st.stop()

# Step 2: Upload CSV
uploaded_file = st.file_uploader("📤 Upload a CSV with organization names", type=["csv"])
if uploaded_file:
    uploaded_df, org_name_col = clean_uploaded_data(uploaded_file)
    st.subheader("📄 Uploaded Preview")
    st.dataframe(uploaded_df.head())

    if st.button("🚀 Enrich Now"):
        st.info("🔎 Matching EINs from IRS BMF...")
        enriched = match_eins(uploaded_df, org_name_col, bmf_data, bmf_name_col)
        enriched.rename(columns={"ein": "EIN"}, inplace=True)

        eins = enriched["EIN"].dropna().unique().tolist()
        st.info("🔎 Enriching with ProPublica API...")
        pro_data = asyncio.run(fetch_all_propublica(eins))
        pro_df = pd.DataFrame([r for r in pro_data if r])

        if not pro_df.empty:
            enriched = enriched.merge(pro_df, on="EIN", how="left")

        enriched = deduplicate(enriched, org_name_col)

        st.success("✅ Enrichment Complete!")
        st.dataframe(enriched.head())

        csv = enriched.to_csv(index=False).encode("utf-8")
        st.download_button("📥 Download Enriched CSV", data=csv, file_name="enriched.csv", mime="text/csv")
