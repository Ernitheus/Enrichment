import streamlit as st
import pandas as pd
import requests
import asyncio
import aiohttp
from fuzzywuzzy import process, fuzz
import os
import glob

#test comment

# Constants
BMF_FOLDER_PATH = "IRS_EO_BMF"
PROPUBLICA_API_URL = "https://projects.propublica.org/nonprofits/api/v2/organizations/"

# 🚀 Load IRS BMF Data
@st.cache_data
def load_bmf_data():
    if not os.path.exists(BMF_FOLDER_PATH):
        st.error(f"The IRS BMF folder '{BMF_FOLDER_PATH}' does not exist.")
        return pd.DataFrame()

    csv_files = glob.glob(os.path.join(BMF_FOLDER_PATH, "*.csv"))
    if not csv_files:
        st.error("No IRS BMF CSV files found.")
        return pd.DataFrame()

    combined_data = pd.concat(
        [pd.read_csv(file, dtype=str, low_memory=False) for file in csv_files],
        ignore_index=True,
    )

    combined_data.columns = combined_data.columns.str.lower().str.strip()
    
    if "ein" not in combined_data.columns:
        st.error("No EIN column found in BMF data.")
        return pd.DataFrame()

    return combined_data

bmf_data = load_bmf_data()

# 🔍 Auto-detect column for Organization Name
def find_best_column_match(possible_columns):
    keywords = ["company", "organization", "name", "nonprofit", "business", "entity"]
    normalized_columns = [col.lower().strip() for col in possible_columns]

    for keyword in keywords:
        match, score = process.extractOne(keyword, normalized_columns)
        if score >= 60:
            return possible_columns[normalized_columns.index(match)]
    
    return possible_columns[0] if possible_columns else None

# 🚀 Process uploaded CSV
def clean_uploaded_data(uploaded_file):
    uploaded_data = pd.read_csv(uploaded_file, dtype=str)

    if uploaded_data.empty:
        st.error("❌ Error: Uploaded file is empty.")
        return None, None

    uploaded_data.columns = uploaded_data.columns.str.lower().str.strip()
    org_name_column = find_best_column_match(uploaded_data.columns.tolist())

    if not org_name_column:
        st.error("❌ No suitable column found for organization names.")
        return None, None

    uploaded_data[org_name_column] = uploaded_data[org_name_column].str.lower().str.strip()
    return uploaded_data, org_name_column

# 🚀 EIN Matching
def match_eins_in_bmf(uploaded_df, org_name_column):
    global bmf_data
    if bmf_data.empty:
        st.error("IRS BMF Data not loaded.")
        return uploaded_df

    bmf_data["name"] = bmf_data["name"].str.lower().str.strip()
    uploaded_df[org_name_column] = uploaded_df[org_name_column].str.lower().str.strip()

    uploaded_df = uploaded_df.merge(
        bmf_data[['name', 'ein', 'ntee_cd', 'revenue_amt', 'income_amt', 'asset_amt']],
        left_on=org_name_column,
        right_on='name',
        how='left'
    )

    uploaded_df.rename(columns={"ein": "EIN"}, inplace=True)
    return uploaded_df

# 🚀 Deduplicate & Keep Best EIN Record
def deduplicate_data(uploaded_data, org_name_column):
    if "EIN" in uploaded_data.columns:
        uploaded_data = uploaded_data.sort_values(by=["EIN", "revenue_amt"], ascending=[True, False])
        uploaded_data = uploaded_data.drop_duplicates(subset=["EIN"], keep="first")

    if org_name_column in uploaded_data.columns:
        uploaded_data = uploaded_data.sort_values(by=["EIN", "revenue_amt"], ascending=[True, False])
        uploaded_data = uploaded_data.drop_duplicates(subset=[org_name_column], keep="first")

    return uploaded_data

# 🚀 Async API Requests for ProPublica
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
                    "Key Employees": "; ".join([f"{officer.get('name', 'N/A')} ({officer.get('title', 'N/A')}) - ${officer.get('compensation', 'N/A')}"
                                                for officer in org_data.get("officers", [])]) or "N/A"
                }
            else:
                return None
    except:
        return None

async def fetch_all_propublica(ein_list):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_propublica_async(session, ein) for ein in ein_list if ein != "N/A"]
        return await asyncio.gather(*tasks)

# 🚀 Streamlit UI
st.title("🚀 Nonprofit Data Enrichment (Cross-Verified)")

uploaded_csv = st.file_uploader("Upload CSV", type=["csv"])

if uploaded_csv is not None:
    uploaded_data, org_name_column = clean_uploaded_data(uploaded_csv)
    if uploaded_data is not None:
        st.write("### Original Data:")
        st.dataframe(uploaded_data.head())

        if st.button("Enrich Data"):
            st.write("✅ Processing...")

            # Step 1: 🚀 Fast EIN Matching using IRS BMF
            uploaded_data = match_eins_in_bmf(uploaded_data, org_name_column)

            # Step 2: 🚀 Ensure EIN exists before ProPublica lookup
            if "EIN" not in uploaded_data.columns:
                uploaded_data["EIN"] = "N/A"

            eins_to_fetch = uploaded_data["EIN"].dropna().unique().tolist()
            eins_to_fetch = [ein for ein in eins_to_fetch if ein != "N/A"]

            # Step 3: 🚀 Fetch missing EINs from ProPublica API
            propublica_df = pd.DataFrame()
            if eins_to_fetch:
                enriched_data_list = asyncio.run(fetch_all_propublica(eins_to_fetch))
                propublica_df = pd.DataFrame(enriched_data_list)

            # Step 4: 🚀 Merge Data (Ensuring EIN Exists)
            if not propublica_df.empty:
                if "EIN" not in propublica_df.columns:
                    st.warning("⚠️ ProPublica data missing EIN column. Creating manually.")
                    propublica_df["EIN"] = "N/A"

                uploaded_data = uploaded_data.merge(propublica_df, on="EIN", how="left")
            else:
                st.warning("⚠️ Skipping ProPublica merge—no EINs available.")

            # Step 5: 🚀 Deduplicate EINs & Organization Names
            uploaded_data = deduplicate_data(uploaded_data, org_name_column)

            st.write("✅ Enrichment Complete!")
            st.dataframe(uploaded_data.head())

            # Download enriched CSV
            csv = uploaded_data.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Verified Data",
                data=csv,
                file_name="verified_enriched_data.csv",
                mime="text/csv"
            )
