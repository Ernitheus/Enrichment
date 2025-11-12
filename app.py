# app.py — ultra-stable, no network, no external scrapers
import os, re, zipfile
from pathlib import Path

import streamlit as st
import pandas as pd

# -------------------- CONFIG --------------------
st.set_page_config(page_title="Nonprofit Enrichment Tool", layout="wide")
APP_TITLE = "🚀 Nonprofit Enrichment Tool (Local IRS only – ultra stable)"

BMF_DEFAULT_FOLDER = "IRS_EO_BMF"   # we also scan repo root
MAX_PREVIEW_ROWS = 200

# -------------------- helpers (no external deps) --------------------
def _extract_one(query, choices):
    # extremely safe fallback, no rapidfuzz dependency
    # prefer exact/contains "name"
    choices = list(choices)
    q = query.lower()
    exact = [c for c in choices if c.lower() == q]
    if exact:
        return exact[0], 100
    contains = [c for c in choices if q in c.lower()]
    if contains:
        return contains[0], 75
    return (choices[0], 0) if choices else (None, 0)

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
    # map common variants into expected columns
    col_map = {
        "ein": ["ein", "ein number", "einnum", "ein_number"],
        "ntee_cd": ["ntee_cd", "ntee", "ntee_code"],
        "revenue_amt": ["revenue_amt", "revenue", "totrevenue", "total_revenue"],
        "income_amt": ["income_amt", "income", "netincome", "net_income"],
        "asset_amt": ["asset_amt", "assets", "totalassets", "total_assets"],
        # sometimes org name differs in BMF dumps:
        "organizationname": ["organizationname", "orgname", "name", "entityname"],
        "state": ["state", "state_cd", "st", "stateabbr", "state_abbr"],
        "city": ["city", "town", "locality", "mailingcity", "mailing_city"],
        "website": ["website", "url", "web", "homepage"],
    }
    for canonical, variants in col_map.items():
        if canonical not in df.columns:
            for v in variants:
                if v in df.columns:
                    df[canonical] = df[v]; break
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
            seen.add(rp); out.append(p)
    return out

@st.cache_data(show_spinner=False)
def scan_bmf(bmf_folder_input: str):
    """Scan + parse BMF files. On-demand (button) only — keeps boot safe."""
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
                    df = pd.read_csv(file, dtype=str, sep=sep, engine="python"); break
                except Exception:
                    df = None
        if df is None:
            continue
        df.columns = df.columns.str.lower().str.strip()
        all_data.append(df); read_names.append(file.name)

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
        raise ValueError("Could not detect a name column in your CSV.")
    df[org_col] = df[org_col].astype(str).str.lower().str.strip()
    return df, org_col

def match_eins(uploaded_df, org_col, bmf_df, bmf_name_col):
    left = uploaded_df.copy()
    right = bmf_df.copy()
    left[org_col] = left[org_col].astype(str).str.lower().str.strip()
    right[bmf_name_col] = right[bmf_name_col].astype(str).str.lower().str.strip()
    cols = [c for c in ["ein","ntee_cd","revenue_amt","income_amt","asset_amt","state","city","website"] if c in right.columns]
    return left.merge(right[[bmf_name_col, *cols]], left_on=org_col, right_on=bmf_name_col, how="left")

def dedupe(df, org_col):
    out = df.copy()
    if "ein" in out.columns:
        out = out.drop_duplicates(subset=["ein"], keep="first")
    if org_col in out.columns:
        out = out.drop_duplicates(subset=[org_col], keep="first")
    return out

# ------------ domain "best guess" (no network, ultra-safe) ------------
def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()

def _slugify_name(name: str) -> str:
    n = _norm(name).lower()
    n = re.sub(r"[^a-z0-9\s\-&]", "", n)
    n = n.replace("&", "and")
    n = re.sub(r"\s+", " ", n).replace(" ", "")
    return n[:63]

def guess_domain_from_name(name: str) -> str:
    """Zero-network domain guess. Tries .org first, then .com."""
    base = _slugify_name(name)
    if not base:
        return ""
    return base + ".org"  # simple and safe; users can edit later if needed

# --------------------- UI (SAFE) -----------------------
st.title(APP_TITLE)

st.markdown("**Step 1 — (Optional) set folder to scan for BMF data**")
bmf_folder_input = st.text_input("📁 We scan *this* folder and the repo root", value=BMF_DEFAULT_FOLDER)

with st.expander("🧪 Diagnostics", expanded=False):
    st.write({
        "cwd": os.getcwd(),
        "scan_folder_resolved": str(Path(bmf_folder_input).expanduser().resolve()),
    })

st.markdown("---")
st.markdown("**Step 2 — Upload your org sheet (CSV)**")
uploaded_file = st.file_uploader("📤 Choose your org CSV", type=["csv"], key="org_csv")
uploaded_df, org_col = (None, None)
if uploaded_file:
    try:
        uploaded_df, org_col = clean_uploaded(uploaded_file)
        st.caption(f"Detected org/name column: **{org_col}**")
        st.dataframe(uploaded_df.head(MAX_PREVIEW_ROWS), use_container_width=True)
    except Exception as e:
        st.error(f"Could not read your CSV: {e}")

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

bmf_ready = st.session_state.get("bmf_ready", False)
if bmf_ready:
    st.info(f"BMF loaded • using name column: **{st.session_state['bmf_name_col']}**")
else:
    st.warning("BMF not loaded yet. Click **Scan BMF files now** when ready.")

# ---------- Enrich (no external network) ----------
st.markdown("---")
if st.button("🚀 Enrich Now"):
    try:
        if uploaded_df is None or not org_col:
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

            enriched = dedupe(enriched, org_col)

            # Domain guess column (ultra-safe)
            # 1) If a website field already exists from BMF, keep it (raw).
            if "website" in enriched.columns:
                enriched["WebsiteRaw"] = enriched["website"]
            else:
                enriched["WebsiteRaw"] = ""

            # 2) Create a best-guess domain purely from name (no network)
            # prefer the uploaded org name if present, else BMF name
            name_for_guess = org_col
            if name_for_guess not in enriched.columns and bmf_name_col in enriched.columns:
                name_for_guess = bmf_name_col

            if name_for_guess in enriched.columns:
                enriched["WebsiteGuess"] = enriched[name_for_guess].fillna("").apply(guess_domain_from_name)
            else:
                enriched["WebsiteGuess"] = ""

            st.success("✅ Enrichment complete!")
            st.dataframe(enriched.head(MAX_PREVIEW_ROWS), use_container_width=True)
            st.download_button(
                "📥 Download Enriched CSV",
                data=enriched.to_csv(index=False).encode("utf-8"),
                file_name="enriched_data.csv",
                mime="text/csv",
            )
    except Exception as e:
        st.error("The enrichment step encountered an error (details below).")
        st.exception(e)

