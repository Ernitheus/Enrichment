# app.py — IRS enrichment + optional accurate domain finder (no API keys) — NaN-safe
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

# Network safety caps/toggles
MAX_DOMAIN_LOOKUPS     = 60
MAX_SEARCH_CANDIDATES  = 6
MAX_VISIT_CANDIDATES   = 3
REQUEST_DELAY_SEC      = 0.35
HTTP_TIMEOUT_SEC       = 12

# -------------------- session_state init --------------------
for k, v in {
    "bmf_ready": False,
    "bmf_data": None,
    "bmf_name_col": None,
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# -------------------- helpers (no external deps) --------------------
def _extract_one(query, choices):
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
    col_map = {
        "ein": ["ein", "ein number", "einnum", "ein_number"],
        "ntee_cd": ["ntee_cd", "ntee", "ntee_code"],
        "revenue_amt": ["revenue_amt", "revenue", "totrevenue", "total_revenue"],
        "income_amt": ["income_amt", "income", "netincome", "net_income"],
        "asset_amt": ["asset_amt", "assets", "totalassets", "total_assets"],
        "organizationname": ["organizationname", "orgname", "name", "entityname"],
        "state": ["state", "state_cd", "st", "stateabbr", "state_abbr"],
        "city": ["city", "town", "locality", "mailingcity", "mailing_city"],
        "website": ["website", "url", "web", "homepage"],
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
            seen.add(rp); out.append(p)
    return out

@st.cache_data(show_spinner=False)
def scan_bmf(bmf_folder_input: str):
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

# -------------------- Simple guess (no network) --------------------
def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()

def _slugify_name(name: str) -> str:
    n = _norm(name).lower()
    n = re.sub(r"[^a-z0-9\s\-&]", "", n)
    n = n.replace("&", "and")
    n = re.sub(r"\s+", " ", n).replace(" ", "")
    return n[:63]

def guess_domain_from_name(name: str) -> str:
    base = _slugify_name(name)
    return (base + ".org") if base else ""

# -------------------- Accurate Domain Mode (no API keys) --------------------
AGGREGATOR_HOSTS = {
    "facebook.com","twitter.com","x.com","linkedin.com","instagram.com",
    "wikipedia.org","guidestar.org","charitynavigator.org",
    "projects.propublica.org","propublica.org",
    "opencorporates.com","findglocal.com","glassdoor.com",
    "mapquest.com","yelp.com","bbb.org","greatnonprofits.org",
    "justia.com","govinfo.gov","irs.gov","google.com","youtube.com","medium.com"
}

def domain_only(url_or_host) -> str:
    """Return root domain (e.g., example.org) from arbitrary input. NaN/None-safe."""
    try:
        if url_or_host is None or (isinstance(url_or_host, float) and pd.isna(url_or_host)):
            return ""
        # handle bytes / non-strings robustly
        if isinstance(url_or_host, bytes):
            try:
                url_or_host = url_or_host.decode("utf-8", errors="ignore")
            except Exception:
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

def http_get(url: str) -> requests.Response | None:
    try:
        return requests.get(url, timeout=HTTP_TIMEOUT_SEC, headers={"User-Agent":"Mozilla/5.0"}, allow_redirects=True)
    except Exception:
        return None

def http_head_alive(host: str) -> bool:
    if not host: return False
    for scheme in ("https://","http://"):
        try:
            r = requests.head(scheme+host, timeout=HTTP_TIMEOUT_SEC, allow_redirects=True)
            if r is not None and r.status_code and r.status_code < 500:
                return True
        except Exception:
            pass
    return False

def search_duckduckgo_html(query: str) -> list[str]:
    try:
        r = requests.get("https://duckduckgo.com/html/", params={"q": query}, timeout=HTTP_TIMEOUT_SEC,
                         headers={"User-Agent":"Mozilla/5.0"})
        soup = BeautifulSoup(r.text, "html.parser")
        hosts = []
        for a in soup.select("a.result__a"):
            host = domain_only(a.get("href",""))
            if host:
                hosts.append(host)
        seen, out = set(), []
        for h in hosts:
            if h not in seen:
                seen.add(h); out.append(h)
        return out
    except Exception:
        return []

def tokens(s: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", (s or "").lower()))

def score_candidate_with_content(host: str, org_name: str, ein: str, city: str, state: str) -> float:
    if not host:
        return 0.0
    score = 0.0
    if host.endswith(".org"): score += 1.2
    if host.endswith(".ngo") or host.endswith(".charity"): score += 1.0
    if not http_head_alive(host):
        return 0.0
    html = None
    for scheme in ("https://","http://"):
        r = http_get(scheme + host)
        if r and r.text and (200 <= r.status_code < 500):
            html = r.text
            break
    if not html:
        return score
    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception:
        return score
    org_tokens = tokens(org_name)
    title = (soup.title.string if soup.title and soup.title.string else "") or ""
    h1 = (soup.h1.get_text(strip=True) if soup.h1 else "")
    combined = " ".join([title, h1]).lower()
    name_hits = len([t for t in org_tokens if t and t in combined])
    score += min(name_hits, 6) * 0.6
    ein_found = bool(re.search(r"\bEIN[^0-9]*([0-9\-\s]{9,12})", html, flags=re.IGNORECASE)) or \
                (bool(ein) and (ein in html))
    if ein_found: score += 1.5
    loc_hits = 0
    if city and city.lower() in html.lower(): loc_hits += 1
    if state and state.lower() in html.lower(): loc_hits += 1
    score += loc_hits * 0.5
    for script in soup.find_all("script", type=lambda t: t and "ld+json" in t.lower()):
        try:
            data = json.loads(script.string or "")
            objs = data if isinstance(data, list) else [data]
            for obj in objs:
                typ = obj.get("@type") if isinstance(obj, dict) else None
                if isinstance(typ, list):
                    is_org = any(t.lower() == "organization" for t in typ if isinstance(t, str))
                else:
                    is_org = isinstance(typ, str) and typ.lower() == "organization"
                if is_org:
                    name = str(obj.get("name","")).lower()
                    url = domain_only(obj.get("url",""))
                    hit = len([t for t in org_tokens if t and t in name])
                    score += min(hit, 5) * 0.4
                    if url and url == host:
                        score += 0.8
        except Exception:
            continue
    return score

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()

def _slugify_name(name: str) -> str:
    n = _norm(name).lower()
    n = re.sub(r"[^a-z0-9\s\-&]", "", n)
    n = n.replace("&", "and")
    n = re.sub(r"\s+", " ", n).replace(" ", "")
    return n[:63]

def accurate_domain_for_row(name: str, ein: str, city: str, state: str, website_hint: str) -> str:
    hint_host = domain_only(website_hint)
    if hint_host and http_head_alive(hint_host):
        return hint_host
    q_parts = [_norm(name)]
    if ein:   q_parts.append(f"EIN {ein}")
    if city:  q_parts.append(city)
    if state: q_parts.append(state)
    candidates = search_duckduckgo_html(" ".join(q_parts))
    candidates = [h for h in candidates if h and h not in AGGREGATOR_HOSTS]
    candidates = candidates[:MAX_SEARCH_CANDIDATES]
    guesses = []
    base = _slugify_name(name)
    if base:
        for sfx in [".org",".com",".net",".ngo",".charity"]:
            guesses.append(base+sfx)
    for g in guesses:
        if g not in candidates:
            candidates.append(g)
    scored = []
    for host in candidates[:MAX_VISIT_CANDIDATES]:
        score = 0.0
        try:
            score = score_candidate_with_content(host, name, ein, city, state)
        except Exception:
            score = 0.0
        scored.append((host, score))
        time.sleep(REQUEST_DELAY_SEC)
    scored.sort(key=lambda x: x[1], reverse=True)
    if scored and scored[0][1] > 0:
        return scored[0][0]
    for g in guesses:
        if http_head_alive(g):
            return g
    return guesses[0] if guesses else ""

# --------------------- UI -----------------------
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
        st.session_state["bmf_ready"] = False
        st.session_state["bmf_data"] = None
        st.session_state["bmf_name_col"] = None
    else:
        bmf_data = normalize_bmf_columns(bmf_data)
        bmf_name_col = get_best_name_col(bmf_data.columns) or "organizationname"
        st.success(f"BMF ready: {len(bmf_data):,} rows from {len(bmf_read_files)} file(s).")
        st.session_state["bmf_data"] = bmf_data
        st.session_state["bmf_name_col"] = bmf_name_col
        st.session_state["bmf_ready"] = True

bmf_ready = bool(
    st.session_state.get("bmf_ready")
    and st.session_state.get("bmf_data") is not None
    and st.session_state.get("bmf_name_col")
)

if bmf_ready:
    st.info(f"BMF loaded • using name column: **{st.session_state.get('bmf_name_col')}**")
else:
    st.warning("BMF not loaded yet. Click **Scan BMF files now** when ready.")

# ---------- Enrichment options ----------
st.markdown("---")
with st.expander("🌐 Domain options", expanded=False):
    use_accurate_domains = st.checkbox(
        "Enable Accurate Domain Mode (web search + homepage validation)",
        value=False,
        help="No API keys needed. Slower but much more accurate. Capped & throttled."
    )
    domain_cap = st.num_

