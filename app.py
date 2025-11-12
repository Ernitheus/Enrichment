# app.py
import streamlit as st
import pandas as pd
import asyncio
import aiohttp
from pathlib import Path
import zipfile
import os

# --------- Config ----------
PROPUBLICA_API_URL = "https://projects.propublica.org/nonprofits/api/v2/organizations/"
BMF_DEFAULT_FOLDER = "IRS_EO_BMF"  # still supported, but we'll also scan repo root
# ---------------------------

st.set_page_config(page_title="Nonprofit Enrichment Tool", layout="wide")
st.title("🚀 Nonprofit Enrichment Tool (Local IRS + ProPublica)")

# fuzzy matching: prefer rapidfuzz, fallback to fuzzywuzzy
def _extract_one(query, choices):
    try:
        from rapidfuzz import process as rf_process, fuzz as rf_fuzz
        m = rf_process.extractOne(query, choices, scorer=rf_fuzz.WRatio)
        if m: return m[0], int(m[1])
    except Exception:
        try:
            from fuzzywuzzy import process as fw_process
            m = fw_process.extractOne(query, choices)
            if m: return m[0], int(m[1])
        except Exception:
            pass
    return (choices[0], 0) if choices else (None, 0)

# ---------- File discovery & loading ----------
def _unzip_in_place(folder: Path):
    if not folder.exists() or not folder.is_dir():
        return
    for z in folder.glob("*.zip"):
        try:
            with zipfile.ZipFile(z, "r") as zf:
                zf.extractall(folder)
        except Exception:
            pass

def _list_bmf_files(bases):
    patterns = ["eo_*.csv", "*.csv", "*.txt", "*.tsv", "*.CSV", "*.TXT", "*.TSV", "*.zip", "*.ZIP"]
    hits = []
    for base in bases:
        if base.exists() and base.is_dir():
            for pat in patterns:
                hits.extend(sorted(base.glob(pat)))
    # de-dupe by resolved path
    seen, out = set(), []
    for p in hits:
        rp = str(p.resolve())
        if rp not in seen:
            seen.add(rp)
            out.append(p)
    return out

@st.cache_data(show_spinner=False)
def load_bmf_data(bmf_folder_input: str):
    # bases to search: the provided folder (defaults to IRS_EO_BMF) + repo root (cwd)
    bmf_folder = Path(bmf_folder_input).expanduser().resolve()
    bases = [bmf_folder, Path.cwd()]
    # unzip first so subsequent listing captures extracted files
    for b in bases:
        _unzip_in_place(b)

    files = _list_bmf_files(bases)

    # separate and drop .zip from reading list (already extracted)
    files_to_read = [p for p in files if p.suffix.lower() not in [".zip"]]

    all_data, read_names = [], []
    for file in files_to_read:
        df = None
        # try flexible sniff
        try:
            df = pd.read_csv(file, dtype=str, sep=None, engine="python")
        except Exception:
            # common fallbacks: comma, tab, pipe, semicolon
            for sep in [",", "\t", "|", ";"]:
                try:
                    df = pd.read_csv(file, dtype=str, sep=sep, engine="python")
                    break
                except Exception:
                    df = None
        if df is None:
            continue
        df.columns = df.columns.str.lower().str.strip()
        all_data.append(df)
        read_names.append(file.name)

    if not all_data:
        return pd.DataFrame(), read_names

    combined = pd.concat(all_data, ignore_index=True, sort=False)
    combined.columns = combined.columns.str.lower().str.strip()
    return combined, read_names

def get_best_name_col(columns):
    preferred = ["name", "organizationname", "orgname", "entityname", "organization_name"]
    cols = list(columns)
    for c in preferred:
        if c in cols:
            return c
    match, score = _extract_one("name", cols)
    return match if match else (cols[0] if cols else None)

def normalize_bmf_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    col_map = {
        "ein": ["ein", "ein number", "einnum", "ein_number"],
        "ntee_cd": ["ntee_cd", "ntee", "ntee_code"],
        "revenue_amt": ["revenue_amt", "revenue", "totrevenue", "total_revenue"],
        "income_amt": ["income_amt", "income", "netincome", "net_income"],
        "asset_amt": ["asset_amt", "assets", "totalassets", "total_assets"],
    }
    for canonical, variants in col_map.items():
        if canonical not in df.columns:
            for v in variants:
                if v in df.columns:
                    df[canonical] = df[v]
                    break
            if canonical not in df.columns:
                df[canonical] = None
    return df

# ---------- Matching & enrichment ----------
def clean_uploaded(file):
    df = pd.read_csv(file, dtype=str)
    df.columns = df.columns.str.lower().str.strip()
    org_col = get_best_name_col(df.columns)
    if not org_col:
        raise ValueError("Could not detect an organization name column in uploaded file.")
    df[org_col] = df[org_col].astype(str).str.lower().str.strip()
    return df, org_col

def match_eins(uploaded_df, org_col, bmf_df, bmf_name_col):
    left = uploaded_df.copy()
    right = bmf_df.copy()
    left[org_col] = left[org_col].astype(str).str.lower().str.strip()
    right[bmf_name_col] = right[bmf_name_col].astype(str).str.lower().str.strip()
    cols = [c for c in ["ein", "ntee_cd", "revenue_amt", "income_amt", "asset_amt"] if c in right.columns]
    return left.merge(right[[bmf_name_col, *cols]], left_on=org_col, right_on=bmf_name_col, how="left")

def dedupe(df, org_col):
    out = df.copy()
    if "ein" in out.columns:
        out = out.drop_duplicates(subset=["ein"], keep="first")
    if org_col in out.columns:
        out = out.drop_duplicates(subset=[org_col], keep="first")
    return out

async def fetch_propublica(session, ein):
    try:
        url = f"{PROPUBLICA_API_URL}{ein}.json"
        async with session.get(url, timeout=30) as resp:
            if resp.status == 200:
                data = await resp.json()
                org = data.get("organization", {}) or {}
                return {
                    "EIN": ein,
                    "Employees": org.get("employee_count", "N/A"),
                    "Website": org.get("website", "N/A"),
                    "Mission": org.get("mission", "N/A"),
                    "990 Link": f"https://projects.propublica.org/nonprofits/organizations/{ein}/full",
                }
    except Exception:
        pass
    return None

async def enrich_with_propublica(eins):
    if not eins:
        return []
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_propublica(session, e) for e in eins if e and str(e).strip()]
        return await asyncio.gather(*tasks) if tasks else []

# ---------- UI ----------
# Allow path override but still discover root
bmf_folder_input = st.text_input("📁 IRS BMF folder to scan (we also scan repo root)", value=BMF_DEFAULT_FOLDER)

bmf_data, bmf_read_files = load_bmf_data(bmf_folder_input)

# Diagnostics
st.caption("🧪 Diagnostics")
st.write({
    "cwd": os.getcwd(),
    "scan_folder": str(Path(bmf_folder_input).expanduser().resolve()),
    "found_files": bmf_read_files[:20] + (["..."] if len(bmf_read_files) > 20 else []),
    "rows_loaded": int(len(bmf_data)) if not bmf_data.empty else 0,
})

if bmf_data.empty:
    st.error("❌ No IRS BMF data found or parsable in IRS_EO_BMF/ or repo root. "
             "Ensure your eo_*.csv (or other BMF files) are present.")
    st.stop()

bmf_data = normalize_bmf_columns(bmf_data)
bmf_name_col = get_best_name_col(bmf_data.columns)
if not bmf_name_col or bmf_name_col not in bmf_data.columns:
    st.error("❌ Could not infer an organization name column in BMF data.")
    st.stop()

st.success(f"✅ Loaded BMF with {len(bmf_data):,} rows from {len(bmf_read_files)} file(s). Using name column: **{bmf_name_col}**.")

uploaded_file = st.file_uploader("📤 Upload your org list (CSV)", type=["csv"])
if not uploaded_file:
    st.info("Upload an org list CSV to start enrichment.")
    st.stop()

uploaded_df, org_col = clean_uploaded(uploaded_file)
st.subheader("📄 Uploaded Data (preview)")
st.dataframe(uploaded_df.head(50))

if st.button("🚀 Enrich Now"):
    st.info("🔎 Matching EINs locally...")
    enriched = match_eins(uploaded_df, org_col, bmf_data, bmf_name_col)
    if "ein" in enriched.columns:
        enriched.rename(columns={"ein": "EIN"}, inplace=True)

    eins = enriched["EIN"].dropna().unique().tolist() if "EIN" in enriched.columns else []
    st.info(f"🔗 Found {len(eins)} unique EIN(s) for ProPublica.")

    pro_df = pd.DataFrame()
    if eins:
        with st.spinner("Fetching ProPublica details..."):
            try:
                results = asyncio.run(enrich_with_propublica(eins))
                pro_df = pd.DataFrame([r for r in results if r])
            except Exception as e:
                st.warning(f"ProPublica enrichment failed: {e}")

    if not pro_df.empty:
        enriched = enriched.merge(pro_df, on="EIN", how="left")

    enriched = dedupe(enriched, org_col)
    st.success("✅ Enrichment Complete!")
    st.dataframe(enriched.head(200))

    st.download_button(
        "📥 Download Enriched CSV",
        data=enriched.to_csv(index=False).encode("utf-8"),
        file_name="enriched_data.csv",
        mime="text/csv",
    )

