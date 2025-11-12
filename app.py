import os
from pathlib import Path
import zipfile
import asyncio
import pandas as pd
import streamlit as st

# ---- Config ----
PROPUBLICA_API_URL = "https://projects.propublica.org/nonprofits/api/v2/organizations/"
BMF_DEFAULT_FOLDER = "IRS_EO_BMF"   # we also scan repo root

st.set_page_config(page_title="Nonprofit Enrichment Tool", layout="wide")
st.title("🚀 Nonprofit Enrichment Tool (Local IRS + ProPublica)")

# ---- Optional deps (don’t crash if missing) ----
def _extract_one(query, choices):
    try:
        from rapidfuzz import process as rf_process, fuzz as rf_fuzz
        m = rf_process.extractOne(query, choices, scorer=rf_fuzz.WRatio)
        if m: return m[0], int(m[1])
    except Exception:
        try:
            from fuzzywuzzy import process as fw_process  # only if you added it
            m = fw_process.extractOne(query, choices)
            if m: return m[0], int(m[1])
        except Exception:
            pass
    return (choices[0], 0) if choices else (None, 0)

# ---- Utilities ----
def get_best_name_col(columns):
    preferred = ["name", "organizationname", "orgname", "entityname", "organization_name"]
    cols = list(columns)
    for c in preferred:
        if c in cols:
            return c
    match, _ = _extract_one("name", cols)
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
    seen, out = set(), []
    for p in hits:
        rp = str(p.resolve())
        if rp not in seen:
            seen.add(rp)
            out.append(p)
    return out

@st.cache_data(show_spinner=False)
def scan_bmf(bmf_folder_input: str):
    """Scan + parse BMF files. Called *on demand* so boot is fast."""
    bmf_folder = Path(bmf_folder_input).expanduser().resolve()
    bases = [bmf_folder, Path.cwd()]
    for b in bases:
        _unzip_in_place(b)

    files = _list_bmf_files(bases)
    files_to_read = [p for p in files if p.suffix.lower() != ".zip"]

    all_data, read_names = [], []
    for file in files_to_read:
        df = None
        try:
            df = pd.read_csv(file, dtype=str, sep=None, engine="python")
        except Exception:
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
    cols = [c for c in ["ein","ntee_cd","revenue_amt","income_amt","asset_amt"] if c in right.columns]
    return left.merge(right[[bmf_name_col, *cols]], left_on=org_col, right_on=bmf_name_col, how="left")

def dedupe(df, org_col):
    out = df.copy()
    if "ein" in out.columns:
        out = out.drop_duplicates(subset=["ein"], keep="first")
    if org_col in out.columns:
        out = out.drop_duplicates(subset=[org_col], keep="first")
    return out

# ---- ProPublica (aiohttp OR requests fallback) ----
USE_AIOHTTP = False
try:
    import aiohttp  # type: ignore
    USE_AIOHTTP = True
except Exception:
    import requests  # fallback

async def _fetch_propublica_aiohttp(session, ein):
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

def _fetch_propublica_requests(ein):
    try:
        import requests
        url = f"{PROPUBLICA_API_URL}{ein}.json"
        r = requests.get(url, timeout=30)
        if r.status_code == 200:
            data = r.json()
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
    if USE_AIOHTTP:
        async with aiohttp.ClientSession() as session:  # type: ignore
            tasks = [_fetch_propublica_aiohttp(session, e) for e in eins if e and str(e).strip()]
            return await asyncio.gather(*tasks) if tasks else []
    # requests fallback – run sync in thread
    loop = asyncio.get_event_loop()
    tasks = [loop.run_in_executor(None, _fetch_propublica_requests, e) for e in eins if e and str(e).strip()]
    return await asyncio.gather(*tasks) if tasks else []

# ================= UI =================
st.markdown("**Step 1 — (Optional) set folder to scan for BMF data**")
bmf_folder_input = st.text_input("📁 We scan *this* folder and the repo root", value=BMF_DEFAULT_FOLDER)

with st.expander("🧪 Diagnostics", expanded=False):
    st.write({
        "cwd": os.getcwd(),
        "scan_folder_resolved": str(Path(bmf_folder_input).expanduser().resolve()),
    })

# --- Always show org uploader (never block app boot) ---
st.markdown("---")
st.markdown("**Step 2 — Upload your org sheet (CSV)**")
uploaded_file = st.file_uploader("📤 Choose your org CSV", type=["csv"], key="org_csv")
uploaded_df, org_col = (None, None)
if uploaded_file:
    try:
        uploaded_df, org_col = clean_uploaded(uploaded_file)
        st.caption(f"Detected org/name column: **{org_col}**")
        st.dataframe(uploaded_df.head(50), use_container_width=True)
    except Exception as e:
        st.error(f"Could not read your CSV: {e}")

# --- Load BMF on demand (lazy) ---
st.markdown("---")
if st.button("📂 Scan BMF files now"):
    with st.spinner("Scanning & loading BMF data..."):
        bmf_data, bmf_read_files = scan_bmf(bmf_folder_input)
    if bmf_data.empty:
        st.error("No BMF files found or parsable in IRS_EO_BMF/ or repo root.")
    else:
        st.success(f"BMF ready: {len(bmf_data):,} rows from {len(bmf_read_files)} file(s).")
        st.session_state["bmf_ready"] = True
        st.session_state["bmf_data"] = normalize_bmf_columns(bmf_data)
        st.session_state["bmf_name_col"] = get_best_name_col(bmf_data.columns)

# Show BMF status
bmf_ready = st.session_state.get("bmf_ready", False)
if bmf_ready:
    st.info(f"BMF loaded • using name column: **{st.session_state['bmf_name_col']}**")
else:
    st.warning("BMF not loaded yet. Click **Scan BMF files now** when ready.")

# --- Enrich ---
st.markdown("---")
if st.button("🚀 Enrich Now"):
    if not uploaded_df is not None or not org_col:
        st.error("Please upload your org CSV first.")
    elif not bmf_ready:
        st.error("Please load BMF data first (click **Scan BMF files now**).")
    else:
        bmf_data = st.session_state["bmf_data"]
        bmf_name_col = st.session_state["bmf_name_col"]

        st.info("🔎 Matching EINs locally...")
        enriched = match_eins(uploaded_df, org_col, bmf_data, bmf_name_col)
        if "ein" in enriched.columns:
            enriched.rename(columns={"ein": "EIN"}, inplace=True)

        eins = enriched["EIN"].dropna().unique().tolist() if "EIN" in enriched.columns else []
        st.info(f"🔗 Found {len(eins)} unique EIN(s) for ProPublica.")

        if eins:
            with st.spinner("Fetching ProPublica details..."):
                # Safe even if Streamlit already has a loop; run in a fresh task
                try:
                    results = asyncio.run(enrich_with_propublica(eins))
                except RuntimeError:
                    # fallback if event loop is already running
                    results = asyncio.get_event_loop().run_until_complete(enrich_with_propublica(eins))
            pro_df = pd.DataFrame([r for r in results if r]) if results else pd.DataFrame()
            if not pro_df.empty:
                enriched = enriched.merge(pro_df, on="EIN", how="left")

        enriched = dedupe(enriched, org_col)
        st.success("✅ Enrichment complete!")
        st.dataframe(enriched.head(200), use_container_width=True)

        st.download_button(
            "📥 Download Enriched CSV",
            data=enriched.to_csv(index=False).encode("utf-8"),
            file_name="enriched_data.csv",
            mime="text/csv",
        )

