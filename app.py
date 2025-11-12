# app.py — local IRS enrichment + optional accurate domain finder (no API keys)
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

BMF_DEFAULT_FOLDER = "IRS_EO_BMF"   # we also scan repo root
MAX_PREVIEW_ROWS = 200

# Network safety caps/toggles
MAX_DOMAIN_LOOKUPS     = 60     # max rows to do network domain finding for
MAX_SEARCH_CANDIDATES  = 6      # max domains to consider from search
MAX_VISIT_CANDIDATES   = 3      # max domains to fetch and validate
REQUEST_DELAY_SEC      = 0.35   # throttle between requests
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
    # simple safe matcher
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

def domain_only(url_or_host: str) -> str:
    if not url_or_host:
        return ""
    if "://" not in url_or_host:
        url_or_host = "http://" + url_or_host
    try:
        ext = tldextract.extract(url_or_host)
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
        # de-dupe keep order
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
    """
    Fetch homepage and score based on on-page signals.
    """
    if not host:
        return 0.0
    score = 0.0
    # baseline TLD preference
    if host.endswith(".org"): score += 1.2
    if host.endswith(".ngo") or host.endswith(".charity"): score += 1.0

    # quick liveness
    if not http_head_alive(host):
        return 0.0

    # fetch homepage (https→http fallback)
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
    # page title / h1 match
    title = (soup.title.string if soup.title and soup.title.string else "") or ""
    h1 = (soup.h1.get_text(strip=True) if soup.h1 else "")
    combined = " ".join([title, h1]).lower()
    name_hits = len([t for t in org_tokens if t and t in combined])
    score += min(name_hits, 6) * 0.6  # up to +3.6

    # EIN presence
    ein_found = bool(re.search(r"\bEIN[^0-9]*([0-9\-\s]{9,12})", html, flags=re.IGNORECASE)) or \
                (bool(ein) and (ein in html))
    if ein_found: score += 1.5

    # city/state presence
    loc_hits = 0
    if city and city.lower() in html.lower(): loc_hits += 1
    if state and state.lower() in html.lower(): loc_hits += 1
    score += loc_hits * 0.5

    # schema.org Organization JSON-LD
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

def accurate_domain_for_row(name: str, ein: str, city: str, state: str, website_hint: str) -> str:
    """
    Accurate mode:
      1) use website_hint if live,
      2) search DuckDuckGo for "<name> EIN <ein> <city> <state>",
      3) visit top candidates and score with content signals.
    """
    # 0) use hint if live
    hint_host = domain_only(website_hint)
    if hint_host and http_head_alive(hint_host):
        return hint_host

    # 1) search
    q_parts = [_norm(name)]
    if ein:   q_parts.append(f"EIN {ein}")
    if city:  q_parts.append(city)
    if state: q_parts.append(state)
    candidates = search_duckduckgo_html(" ".join(q_parts))
    # strip aggregators
    candidates = [h for h in candidates if h and h not in AGGREGATOR_HOSTS]
    # keep top N only
    candidates = candidates[:MAX_SEARCH_CANDIDATES]

    # Always try adding smart guesses as well (no fetch yet)
    guesses = []
    base = _slugify_name(name)
    if base:
        for sfx in [".org",".com",".net",".ngo",".charity"]:
            guesses.append(base+sfx)
    for g in guesses:
        if g not in candidates:
            candidates.append(g)

    # visit & score top M candidates
    scored = []
    for host in candidates[:MAX_VISIT_CANDIDATES]:
        score = 0.0
        try:
            score = score_candidate_with_content(host, name, ein, city, state)
        except Exception:
            score = 0.0
        scored.append((host, score))
        time.sleep(REQUEST_DELAY_SEC)

    # choose best
    scored.sort(key=lambda x: x[1], reverse=True)
    if scored and scored[0][1] > 0:
        return scored[0][0]

    # fallback: first live guess
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

# recompute readiness strictly
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
    domain_cap = st.number_input(
        "Max rows to resolve with Accurate Mode (cap)",
        min_value=10, max_value=500, value=MAX_DOMAIN_LOOKUPS, step=10
    )

# ---------- Enrich ----------
if st.button("🚀 Enrich Now"):
    try:
        if uploaded_df is None or not org_col:
            st.error("Please upload your org CSV first.")
        elif not bmf_ready:
            st.error("Please load BMF data first (click **Scan BMF files now**).")
        else:
            bmf_data = st.session_state.get("bmf_data")
            bmf_name_col = st.session_state.get("bmf_name_col")

            if bmf_data is None or not bmf_name_col:
                st.error("BMF data is not available in session. Please scan again.")
            else:
                st.info("🔎 Matching EINs locally...")
                enriched = match_eins(uploaded_df, org_col, bmf_data, bmf_name_col)
                if "ein" in enriched.columns:
                    enriched.rename(columns={"ein": "EIN"}, inplace=True)

                enriched = dedupe(enriched, org_col)

                # Website raw (if present in BMF)
                if "website" in enriched.columns:
                    enriched["WebsiteRaw"] = enriched["website"]
                else:
                    enriched["WebsiteRaw"] = ""

                # Default zero-network guess (fast)
                name_for_guess = org_col if org_col in enriched.columns else (
                    bmf_name_col if bmf_name_col in enriched.columns else None
                )
                if name_for_guess:
                    enriched["WebsiteGuess"] = enriched[name_for_guess].fillna("").apply(guess_domain_from_name)
                else:
                    enriched["WebsiteGuess"] = ""

                # Accurate Domain Mode (optional)
                enriched["WebsiteDomain"] = enriched["WebsiteRaw"].map(domain_only)
                missing_mask = enriched["WebsiteDomain"].isna() | (enriched["WebsiteDomain"] == "")
                work = enriched[missing_mask].copy()

                if use_accurate_domains:
                    if len(work) > domain_cap:
                        st.warning(f"Accurate mode capped at {domain_cap} rows (out of {len(work)} without domains).")
                        work = work.iloc[:domain_cap].copy()

                    city_col  = next((c for c in ["city", "mailing_city"] if c in enriched.columns), None)
                    state_col = next((c for c in ["state","state_cd","st","state_abbr"] if c in enriched.columns), None)

                    st.info(f"Resolving domains accurately for {len(work)} row(s)...")
                    prog = st.progress(0)
                    for i, (idx, row) in enumerate(work.iterrows(), start=1):
                        try:
                            name = row.get(name_for_guess, "") or row.get(bmf_name_col, "") or ""
                            ein  = str(row.get("EIN","") or "")
                            city = (row.get(city_col,"") if city_col else "")
                            state= (row.get(state_col,"") if state_col else "")
                            hint = row.get("WebsiteRaw","") or ""
                            best = accurate_domain_for_row(name=name, ein=ein, city=city, state=state, website_hint=hint)
                            enriched.at[idx, "WebsiteDomain"] = best or enriched.at[idx, "WebsiteGuess"]
                        except Exception:
                            # keep guess if fails
                            enriched.at[idx, "WebsiteDomain"] = enriched.at[idx, "WebsiteGuess"]
                        finally:
                            prog.progress(int(i * 100 / max(1, len(work))))
                            time.sleep(REQUEST_DELAY_SEC)
                    prog.empty()
                else:
                    # If accurate mode is off, fill WebsiteDomain with guess (fast)
                    enriched.loc[missing_mask, "WebsiteDomain"] = enriched.loc[missing_mask, "WebsiteGuess"]

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

