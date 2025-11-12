# app.py
import os, re, zipfile
from pathlib import Path

import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
import tldextract

# -------------------- CONFIG --------------------
st.set_page_config(page_title="Nonprofit Enrichment Tool", layout="wide")
APP_TITLE = "🚀 Nonprofit Enrichment Tool (Local IRS + ProPublica + Domain Finder)"

PROPUBLICA_API_URL = "https://projects.propublica.org/nonprofits/api/v2/organizations/"
BMF_DEFAULT_FOLDER = "IRS_EO_BMF"   # we also scan repo root
DOMAIN_LOOKUP_LIMIT = 100           # cap for responsiveness

# runtime limits / flags
MAX_PROPUBLICA_LOOKUPS = 100        # hard cap per run
MAX_DOMAIN_LOOKUPS     = 100        # hard cap per run
PROPUBLICA_DELAY_SEC   = 0.20       # throttle between ProPublica calls
DEFAULT_SAFE_MODE      = True       # start in safe mode (no external requests)

# -------------------- SAFE FUZZY ----------------
def _extract_one(query, choices):
    try:
        from rapidfuzz import process as rf_process, fuzz as rf_fuzz
        m = rf_process.extractOne(query, choices, scorer=rf_fuzz.WRatio)
        if m:
            return m[0], int(m[1])
    except Exception:
        pass
    return (choices[0], 0) if choices else (None, 0)

# -------------------- UTILITIES -----------------
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
        "state": ["state", "state_cd", "st", "stateabbr", "state_abbr"],
        "city": ["city", "town", "locality", "mailingcity", "mailing_city"],
        "website": ["website", "url", "web", "homepage"],
        # sometimes org name is different in BMF dumps:
        "organizationname": ["organizationname", "orgname", "name", "entityname"]
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
    """Scan + parse BMF files. Called on demand so boot is fast."""
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
        raise ValueError("Could not detect a name column.")
    df[org_col] = df[org_col].astype(str).str.lower().str.strip()
    return df, org_col

def match_eins(uploaded_df, org_col, bmf_df, bmf_name_col):
    left = uploaded_df.copy()
    right = bmf_df.copy()
    left[org_col] = left[org_col].astype(str).str.lower().str.strip()
    right[bmf_name_col] = right[bmf_name_col].astype(str).str.lower().str.strip()
    cols = [c for c in ["ein", "ntee_cd", "revenue_amt", "income_amt", "asset_amt", "state", "city", "website"] if c in right.columns]
    return left.merge(right[[bmf_name_col, *cols]], left_on=org_col, right_on=bmf_name_col, how="left")

def dedupe(df, org_col):
    out = df.copy()
    if "ein" in out.columns:
        out = out.drop_duplicates(subset=["ein"], keep="first")
    if org_col in out.columns:
        out = out.drop_duplicates(subset=[org_col], keep="first")
    return out

# ---------------- DOMAIN FINDER (no API keys) ----------------
AGGREGATOR_HOSTS = {
    "facebook.com","twitter.com","x.com","linkedin.com","instagram.com",
    "wikipedia.org","guidestar.org","charitynavigator.org",
    "projects.propublica.org","propublica.org",
    "opencorporates.com","findglocal.com","glassdoor.com",
    "mapquest.com","yelp.com","bbb.org","greatnonprofits.org",
    "justia.com","govinfo.gov","irs.gov","google.com","youtube.com","medium.com"
}

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()

def _slugify_name(name: str) -> str:
    n = _norm(name).lower()
    n = re.sub(r"[^a-z0-9\s\-&]", "", n)
    n = n.replace("&", "and")
    n = re.sub(r"\s+", " ", n).replace(" ", "")
    return n[:63]

def _domain_only(url_or_host: str) -> str:
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

def _score_candidate(host: str, org_slug: str) -> float:
    if not host:
        return 0.0
    score = 0.0
    hflat = host.replace(".", "")
    if host.endswith(".org"):
        score += 2.0
    if host.endswith(".ngo") or host.endswith(".charity"):
        score += 1.5
    if org_slug and org_slug in hflat:
        score += 1.5
    if host in AGGREGATOR_HOSTS:
        score -= 3.0
    return score

@st.cache_data(show_spinner=False, ttl=60*60)
def _http_head_alive(host: str) -> bool:
    if not host:
        return False
    for scheme in ("https://", "http://"):
        try:
            r = requests.head(scheme + host, timeout=6, allow_redirects=True)
            if r.status_code and r.status_code < 500:
                return True
        except Exception:
            pass
    return False

def _candidate_guesses_from_name(name: str):
    base = _slugify_name(name)
    if not base:
        return []
    return [base + s for s in [".org", ".com", ".net", ".ngo", ".charity"]]

def _search_duckduckgo_html(name: str, ein: str, state: str = "", city: str = "") -> list[str]:
    q_parts = [_norm(name)]
    if ein:
        q_parts.append(f"EIN {ein}")
    if city:
        q_parts.append(city)
    if state:
        q_parts.append(state)
    try:
        r = requests.get(
            "https://duckduckgo.com/html/",
            params={"q": " ".join(q_parts)},
            timeout=20,
            headers={"User-Agent": "Mozilla/5.0"}
        )
        soup = BeautifulSoup(r.text, "html.parser")
        hosts = []
        for a in soup.select("a.result__a"):
            host = _domain_only(a.get("href", ""))
            if host:
                hosts.append(host)
        seen, out = set(), []
        for h in hosts:
            if h not in seen:
                seen.add(h)
                out.append(h)
        return out
    except Exception:
        return []

def find_best_domain(name: str, ein: str = "", state: str = "", city: str = "", fallback_website: str = "") -> str:
    host = _domain_only(fallback_website)
    if host and _http_head_alive(host):
        return host
    org_slug = _slugify_name(name)
    # guesses first
    for g in _candidate_guesses_from_name(name):
        if _http_head_alive(g):
            return g
    # search + score
    candidates = _search_duckduckgo_html(name, ein, state, city)
    candidates += _candidate_guesses_from_name(name)
    scored = sorted(
        ({"host": h, "score": _score_candidate(h, org_slug)} for h in candidates),
        key=lambda x: x["score"],
        reverse=True,
    )
    for item in scored[:8]:
        if _http_head_alive(item["host"]):
            return item["host"]
    return (scored[0]["host"] if scored else "")

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
        st.dataframe(uploaded_df.head(50), use_container_width=True)
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

# ---------- Enrichment options ----------
st.markdown("---")
with st.expander("⚙️ Enrichment options (use if it crashes)", expanded=False):
    safe_mode = st.checkbox(
        "Safe mode (skip external requests)",
        value=DEFAULT_SAFE_MODE,
        help="Disables ProPublica and DuckDuckGo calls. Good for first run / debugging.",
    )
    enable_propublica = st.checkbox("Call ProPublica API", value=not DEFAULT_SAFE_MODE)
    enable_domain_guess = st.checkbox("Domain guess (no network, just smart .org/.com guesses)", value=True)
    enable_domain_liveness = st.checkbox(
        "Check if guessed domains are live (HTTP HEAD)",
        value=False,
        help="Can be slow on large lists.",
    )
    enable_duckduckgo = st.checkbox(
        "DuckDuckGo HTML search for domains",
        value=False,
        help="No API key, but slower and can be flaky. Use for smaller batches.",
    )

# ---------- Enrich ----------
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

            # ---------- ProPublica (optional / capped) ----------
            if safe_mode:
                st.warning("Safe mode ON → skipping ProPublica calls.")
            elif enable_propublica:
                try:
                    ein_list = enriched.get("EIN", pd.Series(dtype=str)).dropna().astype(str).unique().tolist()
                except Exception:
                    ein_list = []
                if ein_list:
                    if len(ein_list) > MAX_PROPUBLICA_LOOKUPS:
                        st.warning(f"ProPublica lookups capped at {MAX_PROPUBLICA_LOOKUPS} (of {len(ein_list)}) for stability.")
                        ein_list = ein_list[:MAX_PROPUBLICA_LOOKUPS]

                    st.info(f"Fetching ProPublica details for {len(ein_list)} EIN(s)...")
                    pro_rows = []
                    prog = st.progress(0)
                    for i, ein in enumerate(ein_list, start=1):
                        try:
                            r = requests.get(f"{PROPUBLICA_API_URL}{ein}.json", timeout=15)
                            if r.status_code == 200:
                                data = r.json()
                                org = (data.get("organization") or {})
                                pro_rows.append({
                                    "EIN": ein,
                                    "Employees": org.get("employee_count", "N/A"),
                                    "Website": org.get("website", "N/A"),
                                    "Mission": org.get("mission", "N/A"),
                                    "990 Link": f"https://projects.propublica.org/nonprofits/organizations/{ein}/full"
                                })
                        except Exception:
                            pass
                        if PROPUBLICA_DELAY_SEC:
                            import time; time.sleep(PROPUBLICA_DELAY_SEC)
                        prog.progress(int(i * 100 / max(1, len(ein_list))))
                    prog.empty()

                    if pro_rows:
                        pro_df = pd.DataFrame(pro_rows)
                        enriched = enriched.merge(pro_df, on="EIN", how="left")
                else:
                    st.info("No EINs found to query in ProPublica.")
            else:
                st.info("ProPublica is disabled (uncheck Safe mode + enable the toggle to fetch).")

            # Always dedupe after merge steps
            enriched = dedupe(enriched, org_col)

            # ---------- Domains (optional / guarded) ----------
            st.info("🌐 Resolving website domains...")
            # Base normalization from existing Website columns if they exist
            site_cols = [c for c in ["Website", "website", "url", "web"] if c in enriched.columns]
            if site_cols:
                base_col = site_cols[0]
                enriched["WebsiteDomain"] = enriched[base_col].map(lambda s: _domain_only(s))
            else:
                enriched["WebsiteDomain"] = ""

            # Determine rows to resolve, apply cap
            missing_mask = enriched["WebsiteDomain"].isna() | (enriched["WebsiteDomain"] == "")
            to_resolve = enriched[missing_mask].copy()
            total_missing = len(to_resolve)

            if total_missing == 0:
                st.success("All rows already have a domain from existing data.")
            else:
                if total_missing > MAX_DOMAIN_LOOKUPS:
                    st.warning(f"Domain resolution capped at {MAX_DOMAIN_LOOKUPS} rows (out of {total_missing}).")
                    to_resolve = to_resolve.iloc[:MAX_DOMAIN_LOOKUPS].copy()

                # Locality hints if present
                city_col  = next((c for c in ["city", "mailing_city"] if c in enriched.columns), None)
                state_col = next((c for c in ["state", "state_cd", "st", "state_abbr"] if c in enriched.columns), None)

                prog = st.progress(0)
                errors = 0

                for i, (idx, row) in enumerate(to_resolve.iterrows(), start=1):
                    try:
                        name = row.get(org_col, "") or row.get(bmf_name_col, "") or ""
                        ein  = str(row.get("EIN", "") or "")
                        city = (row.get(city_col, "") if city_col else "")
                        state= (row.get(state_col, "") if state_col else "")
                        fallback_site = ""
                        if "Website" in row and row["Website"]:
                            fallback_site = row["Website"]
                        elif "website" in row and row["website"]:
                            fallback_site = row["website"]

                        host = _domain_only(fallback_site)

                        # 1) cheap guess (no network unless liveness toggle)
                        if (not host) and enable_domain_guess:
                            for g in _candidate_guesses_from_name(name):
                                if enable_domain_liveness:
                                    if _http_head_alive(g):
                                        host = g; break
                                else:
                                    host = g; break

                        # 2) optional DuckDuckGo HTML search (network)
                        if (not host) and (not safe_mode) and enable_duckduckgo:
                            host = find_best_domain(
                                name=name, ein=ein, state=state, city=city,
                                fallback_website=fallback_site if enable_domain_liveness else ""
                            )

                        enriched.at[idx, "WebsiteDomain"] = host or ""
                    except Exception:
                        errors += 1
                    finally:
                        prog.progress(int(i * 100 / max(1, len(to_resolve))))
                prog.empty()
                if errors:
                    st.warning(f"Domain finder skipped {errors} row(s) due to errors (kept going).")

            st.success("✅ Enrichment complete!")
            st.dataframe(enriched.head(200), use_container_width=True)
            st.download_button(
                "📥 Download Enriched CSV",
                data=enriched.to_csv(index=False).encode("utf-8"),
                file_name="enriched_data.csv",
                mime="text/csv",
            )

    except Exception as e:
        st.error("The enrichment step encountered an error (showing details below).")
        st.exception(e)

