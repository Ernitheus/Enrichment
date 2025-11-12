import os, re, json, time, zipfile
from pathlib import Path

import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import tldextract

# -------------------- CONFIG --------------------
st.set_page_config(page_title="Nonprofit Enrichment Tool", layout="wide")
APP_TITLE = "🚀 Nonprofit Enrichment Tool (IRS + Optional Accurate Domain Finder)"

BMF_DEFAULT_FOLDER = "IRS_EO_BMF"
MAX_PREVIEW_ROWS = 200

MAX_DOMAIN_LOOKUPS     = 60
MAX_SEARCH_CANDIDATES  = 6
MAX_VISIT_CANDIDATES   = 3
REQUEST_DELAY_SEC      = 0.35
HTTP_TIMEOUT_SEC       = 12

# -------------------- session_state init --------------------
for k, v in {"bmf_ready": False, "bmf_data": None, "bmf_name_col": None}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# -------------------- Helpers --------------------
def _extract_one(query, choices):
    choices = list(choices)
    q = query.lower()
    for c in choices:
        if c.lower() == q:
            return c, 100
    for c in choices:
        if q in c.lower():
            return c, 75
    return (choices[0], 0) if choices else (None, 0)

def get_best_name_col(columns):
    preferred = ["name", "organizationname", "orgname", "entityname", "organization_name"]
    for c in preferred:
        if c in columns:
            return c
    match, _ = _extract_one("name", columns)
    return match if match else (columns[0] if columns else None)

def normalize_bmf_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    col_map = {
        "ein": ["ein", "ein number", "einnum"],
        "organizationname": ["organizationname", "orgname", "name"],
        "state": ["state", "state_cd", "st"],
        "city": ["city", "town", "mailingcity"],
        "website": ["website", "url", "web"],
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
    if not folder.exists(): return
    for z in folder.glob("*.zip"):
        try:
            with zipfile.ZipFile(z, "r") as zf:
                zf.extractall(folder)
        except Exception:
            pass

def _list_bmf_files(bases):
    hits = []
    for base in bases:
        if base.exists():
            hits += list(base.glob("*.csv")) + list(base.glob("*.txt")) + list(base.glob("*.tsv"))
    return hits

@st.cache_data(show_spinner=False)
def scan_bmf(bmf_folder_input: str):
    bmf_folder = Path(bmf_folder_input).expanduser().resolve()
    bases = [bmf_folder, Path.cwd()]
    for b in bases: _unzip_in_place(b)
    files = _list_bmf_files(bases)
    if not files:
        return pd.DataFrame(), []
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f, dtype=str, engine="python")
            df.columns = df.columns.str.lower().str.strip()
            dfs.append(df)
        except Exception:
            pass
    if not dfs: return pd.DataFrame(), []
    return pd.concat(dfs, ignore_index=True), [f.name for f in files]

def clean_uploaded(file):
    df = pd.read_csv(file, dtype=str)
    df.columns = df.columns.str.lower().str.strip()
    org_col = get_best_name_col(df.columns)
    df[org_col] = df[org_col].astype(str).str.lower().str.strip()
    return df, org_col

def match_eins(uploaded_df, org_col, bmf_df, bmf_name_col):
    left, right = uploaded_df.copy(), bmf_df.copy()
    left[org_col] = left[org_col].astype(str).str.lower().str.strip()
    right[bmf_name_col] = right[bmf_name_col].astype(str).str.lower().str.strip()
    cols = [c for c in ["ein", "state", "city", "website"] if c in right.columns]
    return left.merge(right[[bmf_name_col, *cols]], left_on=org_col, right_on=bmf_name_col, how="left")

def dedupe(df, org_col):
    out = df.copy()
    if "ein" in out.columns: out = out.drop_duplicates("ein")
    if org_col in out.columns: out = out.drop_duplicates(org_col)
    return out

# -------------------- Domain helpers --------------------
def domain_only(url_or_host) -> str:
    try:
        if url_or_host is None or (isinstance(url_or_host, float) and pd.isna(url_or_host)):
            return ""
        s = str(url_or_host).strip()
        if not s or s.lower() in {"nan", "none", "null"}:
            return ""
        if "://" not in s:
            s = "http://" + s
        ext = tldextract.extract(s)
        if not ext.domain or not ext.suffix:
            return ""
        return f"{ext.domain}.{ext.suffix}".lower()
    except Exception:
        return ""

def guess_domain_from_name(name: str) -> str:
    base = re.sub(r"[^a-z0-9]+", "", name.lower())
    return base + ".org" if base else ""

def http_head_alive(host: str) -> bool:
    if not host: return False
    for scheme in ("https://", "http://"):
        try:
            r = requests.head(scheme+host, timeout=HTTP_TIMEOUT_SEC)
            if r.status_code < 500: return True
        except Exception:
            pass
    return False

def search_duckduckgo_html(query: str) -> list[str]:
    try:
        r = requests.get("https://duckduckgo.com/html/", params={"q": query}, timeout=HTTP_TIMEOUT_SEC)
        soup = BeautifulSoup(r.text, "html.parser")
        hosts = []
        for a in soup.select("a.result__a"):
            h = domain_only(a.get("href", ""))
            if h and h not in hosts:
                hosts.append(h)
        return hosts[:MAX_SEARCH_CANDIDATES]
    except Exception:
        return []

def accurate_domain_for_row(name: str, ein: str, city: str, state: str, hint: str) -> str:
    hint_domain = domain_only(hint)
    if hint_domain and http_head_alive(hint_domain):
        return hint_domain
    q = " ".join(filter(None, [name, ein, city, state]))
    candidates = search_duckduckgo_html(q)
    for c in candidates:
        if http_head_alive(c): return c
    guess = guess_domain_from_name(name)
    if http_head_alive(guess): return guess
    return guess

# -------------------- Streamlit UI --------------------
st.title(APP_TITLE)

st.markdown("### Step 1 — Set folder to scan for IRS data")
bmf_folder_input = st.text_input("📁 Folder path", value=BMF_DEFAULT_FOLDER)

if st.button("📂 Scan BMF files now"):
    with st.spinner("Scanning & loading..."):
        bmf_data, files = scan_bmf(bmf_folder_input)
    if not bmf_data.empty:
        bmf_data = normalize_bmf_columns(bmf_data)
        st.session_state["bmf_data"] = bmf_data
        st.session_state["bmf_name_col"] = get_best_name_col(bmf_data.columns)
        st.session_state["bmf_ready"] = True
        st.success(f"Loaded {len(bmf_data):,} records from {len(files)} file(s)")
    else:
        st.error("No valid BMF data found.")

bmf_ready = st.session_state.get("bmf_ready", False)
if bmf_ready:
    st.info(f"BMF ready • using column: {st.session_state['bmf_name_col']}")
else:
    st.warning("BMF not loaded yet.")

st.markdown("---")
st.markdown("### Step 2 — Upload your organization CSV")
uploaded_file = st.file_uploader("📤 Upload CSV", type=["csv"])
if uploaded_file:
    df, org_col = clean_uploaded(uploaded_file)
    st.dataframe(df.head(MAX_PREVIEW_ROWS))
else:
    org_col = None
    df = None

st.markdown("---")
st.markdown("### Step 3 — Domain Options")
use_accurate = st.checkbox("Enable Accurate Domain Mode (web search, slower)", value=False)
domain_cap = st.number_input("Max rows to search", min_value=10, max_value=500, value=60, step=10)

# -------------------- Enrich --------------------
if st.button("🚀 Enrich Now"):
    if not bmf_ready or df is None:
        st.error("Please upload your org CSV and scan BMF first.")
    else:
        with st.spinner("Matching EINs..."):
            enriched = match_eins(df, org_col, st.session_state["bmf_data"], st.session_state["bmf_name_col"])
        enriched = dedupe(enriched, org_col)
        enriched["WebsiteRaw"] = enriched.get("website", "")
        enriched["WebsiteGuess"] = enriched[org_col].apply(guess_domain_from_name)
        enriched["WebsiteDomain"] = enriched["WebsiteRaw"].map(domain_only)

        if use_accurate:
            work = enriched[enriched["WebsiteDomain"] == ""].head(domain_cap)
            progress = st.progress(0)
            for i, (idx, row) in enumerate(work.iterrows(), start=1):
                best = accurate_domain_for_row(
                    name=row.get(org_col, ""),
                    ein=row.get("ein", ""),
                    city=row.get("city", ""),
                    state=row.get("state", ""),
                    hint=row.get("WebsiteRaw", ""),
                )
                enriched.at[idx, "WebsiteDomain"] = best
                progress.progress(int(i * 100 / len(work)))
                time.sleep(REQUEST_DELAY_SEC)
            progress.empty()

        st.success("✅ Enrichment complete!")
        st.dataframe(enriched.head(MAX_PREVIEW_ROWS))
        st.download_button(
            "📥 Download Enriched CSV",
            enriched.to_csv(index=False).encode("utf-8"),
            "enriched_data.csv",
            "text/csv",
        )
