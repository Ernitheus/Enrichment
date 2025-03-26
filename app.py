import streamlit as st
import pandas as pd
import asyncio
import aiohttp
import glob
import os
from fuzzywuzzy import process

PROPUBLICA_API_URL = "https://projects.propublica.org/nonprofits/api/v2/organizations/"
BMF_FOLDER_PATH = "IRS_EO_BMF"

# Load local IRS BMF files
@st.cache_data
def load_bmf_data():
    files = glob.glob(os.path.join(BMF_FOLDER_PATH, "*.csv")) + glob.glob(os.path.join(BMF_FOLDER_PATH, "*.txt"))
    all_data = []

    for file in files:
        try:
            df = pd.read_csv(file, dtype=str, sep=None, engine="python")
            all_data.append(df)
        except Exception as e:
            st.warning(f"⚠️ Skipping {file} — {e}")

    if not all_data:
        return pd.DataFrame()

    combined = pd.concat(all_data, ignore_index=True)
    combined.columns = combined.columns.str.lower().str.strip()
    return combined

# Detect org name column
def get_best_name_col(columns):
    preferred = ["name", "organizationname", "orgname", "entityname"]
    for col in preferred:
        if col in columns:
            return col
    match, score = process.extractOne("name", columns)
    return match if score >= 60 else columns[0]

# Clean uploaded org list
def clean_uploaded(file):
    df = pd.read_csv(file, dtype=str)
    df.columns = df.columns.str.lower().str.strip()
    org_col = get_best_name_col(df.columns)
    df[org_col] = df[org_col].str.lower().str.strip()
    return df, org_col

# Match EINs locally
def match_eins(uploaded_df, org_col, bmf_df, bmf_name_col):
    bmf_df[bmf_name_col] = bmf_df[bmf_name_col].str.lower().str.strip()
    return uploaded_df.merge(
        bmf_df[[bmf_name_col, "ein", "ntee_cd", "revenue_amt", "income_amt", "asset_amt"]],
        left_on=org_col,
        right_on=bmf_name_col,
        how="left"
    )

# ProPublica enrichment
async def fetch_propublica(session, ein):
    try:
        url = f"{PROPUBLICA_API_URL}{ein}.json"
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                org = data.get("organization", {})
                return {
                    "EIN": ein,
                    "Employees": org.get("employee_count", "N/A"),
                    "Website": org.get("website", "N/A"),
                    "Mission": org.get("mission", "N/A"),
                    "990 Link": f"https://projects.propublica.org/nonprofits/organizations/{ein}/full"
                }
    except:
        return None

async def enrich_with_propublica(eins):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_propublica(session, ein) for ein in eins if ein]
        return await asyncio.gather(*tasks)

# Deduplicate
def dedupe(df, org_col):
    df = df.drop_duplicates(subset=["ein"]).drop_duplicates(subset=[org_col])
    return df

# === Streamlit UI ===
st.set_page_config(page_title="Nonprofit Enrichment Tool", layout="wide")
st.title("🚀 Nonprofit Enrichment Tool (Local IRS + ProPublica)")

bmf_data = load_bmf_data()

if bmf_data.empty:
    st.error("❌ No IRS BMF data found in IRS_EO_BMF/.")
    st.stop()

bmf_name_col = get_best_name_col(bmf_data.columns)

uploaded_file = st.file_uploader("📤 Upload your org list (CSV)", type=["csv"])
if uploaded_file:
    df, org_col = clean_uploaded(uploaded_file)
    st.subheader("📄 Uploaded Data")
    st.dataframe(df.head())

    if st.button("🚀 Enrich Now"):
        st.info("🔎 Matching EINs locally...")
        enriched = match_eins(df, org_col, bmf_data, bmf_name_col)
        enriched.rename(columns={"ein": "EIN"}, inplace=True)

        eins = enriched["EIN"].dropna().unique().tolist()
        st.info("🔗 Fetching from ProPublica...")
        pro_results = asyncio.run(enrich_with_propublica(eins))
        pro_df = pd.DataFrame([r for r in pro_results if r])

        if not pro_df.empty:
            enriched = enriched.merge(pro_df, on="EIN", how="left")

        enriched = dedupe(enriched, org_col)

        st.success("✅ Enrichment Complete!")
        st.dataframe(enriched.head())

        st.download_button(
            "📥 Download Enriched CSV",
            data=enriched.to_csv(index=False).encode("utf-8"),
            file_name="enriched_data.csv",
            mime="text/csv"
        )
