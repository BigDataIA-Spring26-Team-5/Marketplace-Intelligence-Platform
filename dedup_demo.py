"""
UC1 Deduplication Pipeline — Step-by-Step Walkthrough
Run: streamlit run dedup_demo.py
"""
import warnings
warnings.filterwarnings("ignore")

import streamlit as st
import pandas as pd
import re
import json
import time
from pathlib import Path
from collections import Counter
from rapidfuzz import fuzz

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent

# Source configurations — column mapping per dataset
# The pipeline uses 4 standard fields: name_col, brand_col, date_col, ingredients_col
# Each source maps its own column names to these standard fields.
SOURCE_CONFIGS = {
    "USDA FoodData (sample)": {
        "path": BASE_DIR / "archive" / "data_samples" / "usda_fooddata_sample.csv",
        "name_col": "description",
        "brand_col": "brand_owner",
        "brand_fallback_col": "brand_name",
        "date_col": "published_date",
        "ingredients_col": "ingredients",
        "category_col": "food_category",
        "id_col": "fdc_id",
    },
    "USDA FoodData (pipeline)": {
        "path": BASE_DIR / "data" / "usda_products.csv",
        "name_col": "description",
        "brand_col": "brand_owner",
        "brand_fallback_col": "brand_name",
        "date_col": "published_date",
        "ingredients_col": "ingredients",
        "category_col": "food_category",
        "id_col": "fdc_id",
    },
    "Open Food Facts": {
        "path": BASE_DIR / "data" / "off_products.csv",
        "name_col": "product_name",
        "brand_col": "brands",
        "brand_fallback_col": None,
        "date_col": "last_modified_t",
        "ingredients_col": "ingredients_text",
        "category_col": "categories",
        "id_col": "code",
    },
    "FDA Recalls": {
        "path": BASE_DIR / "data" / "fda_recalls.csv",
        "name_col": "product_description",
        "brand_col": "recalling_firm",
        "brand_fallback_col": None,
        "date_col": "recall_initiation_date",
        "ingredients_col": None,
        "category_col": "classification",
        "id_col": "recall_number",
    },
    "Open Prices": {
        "path": BASE_DIR / "data" / "open_prices.csv",
        "name_col": "product_name",
        "brand_col": "product_brands",
        "brand_fallback_col": None,
        "date_col": "date",
        "ingredients_col": None,
        "category_col": "product_categories",
        "id_col": "price_id",
    },
    "Amazon ESCI": {
        "path": BASE_DIR / "data" / "esci_sample.csv",
        "name_col": "product_title",
        "brand_col": "product_brand",
        "brand_fallback_col": None,
        "date_col": None,
        "ingredients_col": "product_description",
        "category_col": None,
        "id_col": "product_id",
    },
}

# Two-layer noise detection:
# Layer 1 (pattern-based): Universal business/legal suffixes — always noise
LEGAL_SUFFIXES = {
    "inc", "llc", "corp", "ltd", "co", "lp", "plc",
    "incorporated", "corporation", "company", "limited",
    "enterprises", "holdings", "group", "international",
    "sales", "distributing", "distribution", "manufacturing",
    "north", "america", "usa", "us",
    "the",
}
#
# Layer 2 (data-driven): Auto-detected from THIS dataset using word frequency.
# Catches domain-specific noise like "foods" in a food dataset, or "pharma" in a drug dataset.
# No human decides this — the data tells us.

# Company name aliases: map lowercased noise-stripped variations → canonical brand name
COMPANY_ALIASES = {
    # General Mills
    "general mills sales": "General Mills",
    "general mills": "General Mills",
    # Kellogg's
    "kellogg co": "Kellogg's",
    "kelloggs": "Kellogg's",
    "kellogg": "Kellogg's",
    # Kraft Heinz
    "kraft foods": "Kraft Heinz",
    "heinz": "Kraft Heinz",
    "kraft": "Kraft Heinz",
    # Conagra
    "conagra brands": "Conagra",
    "conagra foods": "Conagra",
    # Nestlé  (note: "usa" is stripped by LEGAL_SUFFIXES before alias lookup)
    "nestle usa": "Nestlé",
    "nestle": "Nestlé",
    # Unilever
    "unilever home": "Unilever",
    "unilever bestfoods": "Unilever",
    # PepsiCo / Frito-Lay
    "pepsico": "PepsiCo",
    "frito lay": "Frito-Lay",
    "frito lay north america": "Frito-Lay",
    # Coca-Cola
    "coca cola": "Coca-Cola",
    "the coca cola company": "Coca-Cola",
    # Hormel
    "hormel foods": "Hormel",
    # Post
    "post consumer brands": "Post",
    # Campbell's
    "campbells": "Campbell's",
    "campbell soup": "Campbell's",
    # Ferrero
    "ferrero usa": "Ferrero",
    "ferrero": "Ferrero",
    # Mars
    "mars wrigley": "Mars",
    "mars": "Mars",
    # Tyson
    "tyson foods": "Tyson",
    # Dannon
    "the dannon company": "Dannon",
    # Quaker
    "quaker oats": "Quaker",
    # Nabisco / Mondelez
    "nabisco": "Nabisco",
    "mondelez": "Mondelēz",
}

# Allergen keywords for rule-based detection (FDA Big-9 allergens)
ALLERGEN_KEYWORDS = {
    "milk":      ["milk", "cream", "butter", "cheese", "whey", "casein", "lactose"],
    "wheat":     ["wheat", "flour", "gluten", "semolina", "spelt", "barley", "rye"],
    "soy":       ["soy", "soybean", "soy lecithin", "tofu", "edamame"],
    "eggs":      ["egg", "eggs", "albumin", "mayonnaise"],
    "peanuts":   ["peanut", "peanuts", "groundnut"],
    "tree nuts": ["almond", "hazelnut", "walnut", "cashew", "pecan", "pistachio", "macadamia", "brazil nut"],
    "fish":      ["salmon", "tuna", "cod", "tilapia", "anchovy", "sardine"],
    "shellfish": ["shrimp", "crab", "lobster", "clam", "oyster", "scallop"],
    "sesame":    ["sesame", "tahini"],
    "sulfites":  ["sulfite", "sulphite", "sulfur dioxide"],
}

TEXT_COLS = ["description", "brand_owner", "brand_name"]

STEPS = [
    "1. Load Raw Data",
    "2. Identify Duplicates",
    "3. Trim Whitespace",
    "4. Lowercase",
    "5. Remove Noise Words",
    "6. Remove Punctuation",
    "7. Regex — Strip Sizes",
    "8. Blocking & Fuzzy Matching",
    "9. Clustering (Union-Find)",
    "10. Golden Record (DQ Score)",
    "11. LLM Enrichment (Groq)",
    "12. Final Cleaned Data",
]

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def trim_whitespace(text):
    if pd.isna(text):
        return ""
    return str(text).strip()


def to_lower(text):
    return str(text).lower()


def detect_noise_words(df, column="brand_owner", threshold_pct=0.1):
    """Two-layer noise word detection.

    Layer 1 — Pattern-based: Universal business/legal suffixes (LEGAL_SUFFIXES).
    These are ALWAYS noise. "Sales", "Inc", "Corporation" never identify a brand.

    Layer 2 — Data-driven: Scan all values in `column`, count how many UNIQUE
    companies each word appears in. If a word appears in more than `threshold_pct`
    of all companies, it's noise. This catches domain-specific words like "foods"
    in a food dataset.

    Together, they handle both universal patterns AND dataset-specific noise.
    Works at any scale — 200 rows or 20 million rows.
    """
    # Get unique brand values (deduplicated at company level)
    values = df[column].dropna().unique()
    total_companies = len(values)
    if total_companies == 0:
        return [], pd.DataFrame()

    # Tokenize: for each unique company, extract its words
    word_company_count = Counter()
    for val in values:
        words = set(re.findall(r'[a-z]+', str(val).lower()))
        for w in words:
            if len(w) > 1:
                word_company_count[w] += 1

    # Layer 2 threshold
    min_count = max(2, int(total_companies * threshold_pct))

    # Build analysis dataframe for display
    analysis = []
    for word, count in word_company_count.most_common():
        pct = count / total_companies * 100
        freq_noise = count >= min_count
        pattern_noise = word in LEGAL_SUFFIXES

        if pattern_noise and freq_noise:
            reason = f"Layer 1+2: Legal suffix AND appears in {count}/{total_companies} brands ({pct:.0f}%)"
            is_noise = True
        elif pattern_noise:
            reason = f"Layer 1: Legal/business suffix — always noise regardless of frequency"
            is_noise = True
        elif freq_noise:
            reason = f"Layer 2: Appears in {count}/{total_companies} brands ({pct:.0f}%) — too common in this dataset"
            is_noise = True
        else:
            reason = f"Only in {count} brand(s) — identifies the brand"
            is_noise = False

        analysis.append({
            "word": word,
            "appears_in_n_brands": count,
            "pct_of_brands": round(pct, 1),
            "detection_layer": "Layer 1 (pattern)" if pattern_noise else ("Layer 2 (data-driven)" if freq_noise else "—"),
            "is_noise": is_noise,
            "reason": reason,
        })

    analysis_df = pd.DataFrame(analysis)
    noise_words = [r["word"] for r in analysis if r["is_noise"]]

    return noise_words, analysis_df


def remove_noise(text, noise_words):
    """Remove noise words using word-boundary matching to avoid partial matches.
    e.g., 'co' should not match inside 'coca' or 'cola'."""
    for w in noise_words:
        pattern = r'\b' + re.escape(w) + r'\b'
        text = re.sub(pattern, '', text, flags=re.IGNORECASE)
    return " ".join(text.split())


def remove_punct(text):
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return " ".join(text.split())


def strip_sizes(text):
    # Leading size: ".75oz doritos nacho", "1.37oz doritos nacho"
    text = re.sub(r"^\.?\d+(\.\d+)?\s*oz\s+", "", text, flags=re.IGNORECASE)
    # Trailing size with unit: "coca cola bottle, 1.25 liters", "quaker oats 42 oz"
    text = re.sub(
        r"[,\s]+\d+(\.\d+)?\s*(oz|ounces?|liters?|l|ml|mlt|gal|lb|lbs|kg|g|grm|ct|pk|pack)\b.*$",
        "", text, flags=re.IGNORECASE,
    )
    # Trailing short size: "doritos ranch 2.125z"
    text = re.sub(
        r"\s+\d+(\.\d+)?\s*z\s*$",
        "", text, flags=re.IGNORECASE,
    )
    return text.strip()


def extract_size(text):
    """Extract the size/unit token from a product description. Returns the size string or ''."""
    text = str(text)
    parts = []
    m = re.match(r"(\.?\d+(?:\.\d+)?\s*oz)\s+", text, flags=re.IGNORECASE)
    if m:
        parts.append(m.group(1).strip())
    m2 = re.search(
        r"[,\s]+(\d+(?:\.\d+)?\s*(?:oz|ounces?|liters?|l|ml|mlt|gal|lb|lbs|kg|g|grm|ct|pk|pack))\b.*$",
        text, flags=re.IGNORECASE,
    )
    if m2:
        parts.append(m2.group(1).strip())
    if not parts:
        m3 = re.search(r"\s+(\d+(?:\.\d+)?\s*z)\s*$", text, flags=re.IGNORECASE)
        if m3:
            parts.append(m3.group(1).strip())
    return parts[0] if parts else ""


def normalize_company(text):
    """Map brand owner text to a canonical company name via COMPANY_ALIASES.
    Falls back to title-cased original if no alias matches."""
    if not text or pd.isna(text):
        return ""
    cleaned = remove_noise(str(text).lower().strip(), list(LEGAL_SUFFIXES))
    cleaned = remove_punct(cleaned).strip()
    if cleaned in COMPANY_ALIASES:
        return COMPANY_ALIASES[cleaned]
    for alias, canonical in COMPANY_ALIASES.items():
        if alias in cleaned:
            return canonical
    return str(text).strip().title()


def extract_allergens(ingredients_text):
    """Scan ingredient string for allergen keywords (FDA Big-9). Returns comma-separated list."""
    if pd.isna(ingredients_text) or not str(ingredients_text).strip():
        return ""
    text = str(ingredients_text).lower()
    found = [allergen for allergen, keywords in ALLERGEN_KEYWORDS.items()
             if any(kw in text for kw in keywords)]
    return ", ".join(found)


def full_normalize(text, noise_words=None):
    """Run all normalization steps on a text string.
    Order matters: strip sizes BEFORE removing punctuation,
    because punctuation removal turns '1.37oz' into '1 37oz'."""
    if noise_words is None:
        noise_words = []
    text = trim_whitespace(text)
    text = to_lower(text)
    text = strip_sizes(text)       # strip sizes first (needs dots intact)
    text = remove_noise(text, noise_words)
    text = remove_punct(text)
    return text.strip()


def get_brand_at(df, idx, cfg):
    """Get brand for a row by index, using source config with fallback."""
    brand_col = cfg.get("brand_col")
    fallback_col = cfg.get("brand_fallback_col")
    if brand_col and brand_col in df.columns:
        val = str(df.at[idx, brand_col]) if pd.notna(df.at[idx, brand_col]) else ""
        if val.strip():
            return val
    if fallback_col and fallback_col in df.columns:
        val = str(df.at[idx, fallback_col]) if pd.notna(df.at[idx, fallback_col]) else ""
        if val.strip():
            return val
    return ""


def fuzzy_score(name_a, brand_a, name_b, brand_b):
    """Return dict with name, brand, combined, and weighted scores."""
    ns = fuzz.token_sort_ratio(name_a, name_b)
    bs = fuzz.token_sort_ratio(brand_a, brand_b)
    cs = fuzz.token_sort_ratio(f"{name_a} {brand_a}", f"{name_b} {brand_b}")
    ws = ns * 0.5 + bs * 0.2 + cs * 0.3
    return {"name_score": ns, "brand_score": bs, "combined_score": cs, "weighted_score": round(ws, 1)}


class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra

    def clusters(self):
        groups = {}
        for x in self.parent:
            r = self.find(x)
            groups.setdefault(r, []).append(x)
        return groups


def compute_completeness(row):
    count = sum(1 for v in row if pd.notna(v) and str(v).strip() != "")
    return count / len(row)


def compute_freshness(date_str, min_date, max_date):
    try:
        d = pd.to_datetime(date_str)
    except Exception:
        return 0.0
    if max_date == min_date:
        return 1.0
    return (d - min_date).days / (max_date - min_date).days


def compute_dq(row, group_df, cfg):
    comp = compute_completeness(row)
    date_col = cfg.get("date_col")
    fresh = 0.5  # default if no date column
    if date_col and date_col in group_df.columns:
        dates = pd.to_datetime(group_df[date_col], errors="coerce")
        min_d, max_d = dates.min(), dates.max()
        fresh = compute_freshness(row.get(date_col, ""), min_d, max_d)
    ing_col = cfg.get("ingredients_col")
    richness = 0.5  # default if no ingredients column
    if ing_col and ing_col in group_df.columns:
        ing_lens = group_df[ing_col].astype(str).str.len()
        min_l, max_l = ing_lens.min(), ing_lens.max()
        ing_len = len(str(row.get(ing_col, "")))
        richness = (ing_len - min_l) / (max_l - min_l) if max_l > min_l else 1.0
    return round(comp * 0.4 + fresh * 0.35 + richness * 0.25, 4)


def call_groq_llm(description, brand, ingredients):
    """Call Groq Llama 3.3 70B for product enrichment."""
    from dotenv import load_dotenv
    import os
    load_dotenv(BASE_DIR / ".env")
    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key:
        return None

    from groq import Groq
    client = Groq(api_key=api_key)

    prompt = f"""You are a food product data analyst. Given product data, extract clean structured attributes.

Product data:
- Description: {description}
- Brand: {brand}
- Ingredients: {str(ingredients)[:300]}

Extract and return ONLY valid JSON with these fields:
{{
  "clean_name": "standardized product name",
  "clean_brand": "normalized brand name (e.g., General Mills, not GENERAL MILLS INC.)",
  "primary_category": "single best category (e.g., Breakfast Cereal, Snack, Beverage, Dairy, Condiment)",
  "dietary_tags": "comma-separated: gluten-free, vegan, organic, dairy-free, nut-free (only if applicable, empty string if none)",
  "allergens": "comma-separated: milk, eggs, wheat, soy, peanuts, tree nuts (only if found in ingredients, empty string if none)",
  "is_organic": "true or false"
}}"""

    try:
        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": "You are a helpful assistant. Always respond with valid JSON only."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=512,
        )
        text = response.choices[0].message.content.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1]
        if text.endswith("```"):
            text = text.rsplit("```", 1)[0]
        return json.loads(text.strip()), prompt, text.strip()
    except Exception as e:
        return None, prompt, str(e)


# ---------------------------------------------------------------------------
# Streamlit App
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Dedup Pipeline Walkthrough", layout="wide")
st.title("UC1: Deduplication Pipeline — Step by Step")

# --- Navigation ---
if "step" not in st.session_state:
    st.session_state.step = 1

# Sidebar
st.sidebar.title("Pipeline Steps")
for i, name in enumerate(STEPS, 1):
    if i == st.session_state.step:
        st.sidebar.markdown(f"**:arrow_right: {name}**")
    else:
        if st.sidebar.button(name, key=f"sidebar_{i}"):
            st.session_state.step = i
            st.rerun()

st.sidebar.markdown("---")
st.sidebar.info(f"Current: Step {st.session_state.step} of {len(STEPS)}")

# Nav buttons at top
col_prev, col_info, col_next = st.columns([1, 3, 1])
with col_prev:
    if st.session_state.step > 1:
        if st.button(":arrow_left: Previous Step"):
            st.session_state.step -= 1
            st.rerun()
with col_info:
    st.markdown(f"### {STEPS[st.session_state.step - 1]}")
with col_next:
    if st.session_state.step < len(STEPS):
        if st.button("Next Step :arrow_right:"):
            st.session_state.step += 1
            st.rerun()

st.markdown("---")

# Load data once
@st.cache_data
def load_data(path):
    return pd.read_csv(path)

@st.cache_data
def get_noise_words(_df, brand_col, name_col):
    """Detect noise words from the dataset. Cached so it runs once."""
    brand_noise, brand_analysis = pd.DataFrame(), pd.DataFrame()
    if brand_col and brand_col in _df.columns:
        brand_noise, brand_analysis = detect_noise_words(_df, column=brand_col, threshold_pct=0.1)
    else:
        brand_noise = []
    desc_noise, desc_analysis = pd.DataFrame(), pd.DataFrame()
    if name_col and name_col in _df.columns:
        desc_noise, desc_analysis = detect_noise_words(_df, column=name_col, threshold_pct=0.15)
    else:
        desc_noise = []
    if isinstance(brand_noise, pd.DataFrame):
        brand_noise = []
    if isinstance(desc_noise, pd.DataFrame):
        desc_noise = []
    all_noise = sorted(set(brand_noise + desc_noise))
    return all_noise, brand_analysis, desc_analysis

# --- Source Selection (sidebar) ---
st.sidebar.markdown("---")
st.sidebar.markdown("### Data Source")

# Find available sources (file exists)
available_sources = {}
for name, cfg in SOURCE_CONFIGS.items():
    if cfg["path"].exists():
        available_sources[name] = cfg

# Also allow custom CSV upload
upload_option = "Upload Custom CSV"
source_options = list(available_sources.keys()) + [upload_option]

if "selected_source" not in st.session_state:
    st.session_state.selected_source = source_options[0] if source_options else upload_option

selected_source = st.sidebar.selectbox("Select dataset", source_options,
                                        index=source_options.index(st.session_state.selected_source)
                                        if st.session_state.selected_source in source_options else 0)
st.session_state.selected_source = selected_source

if selected_source == upload_option:
    uploaded_file = st.sidebar.file_uploader("Upload a CSV file", type=["csv"])
    if uploaded_file is not None:
        raw_df = pd.read_csv(uploaded_file)
        input_label = uploaded_file.name
        # Auto-detect columns
        cols = list(raw_df.columns)
        st.sidebar.markdown("**Map your columns:**")
        _cfg = {
            "name_col": st.sidebar.selectbox("Product name column", cols, index=0),
            "brand_col": st.sidebar.selectbox("Brand column", ["(none)"] + cols, index=0),
            "brand_fallback_col": None,
            "date_col": st.sidebar.selectbox("Date column", ["(none)"] + cols, index=0),
            "ingredients_col": st.sidebar.selectbox("Ingredients column", ["(none)"] + cols, index=0),
            "category_col": st.sidebar.selectbox("Category column", ["(none)"] + cols, index=0),
            "id_col": st.sidebar.selectbox("ID column", cols, index=0),
        }
        # Convert "(none)" to None
        for k in ["brand_col", "date_col", "ingredients_col", "category_col"]:
            if _cfg[k] == "(none)":
                _cfg[k] = None
    else:
        st.sidebar.warning("Please upload a CSV file.")
        st.stop()
else:
    _cfg = available_sources[selected_source]
    raw_df = load_data(str(_cfg["path"]))
    input_label = _cfg["path"].name

# Show current config
st.sidebar.markdown("**Column mapping:**")
st.sidebar.code(f"Name: {_cfg['name_col']}\nBrand: {_cfg.get('brand_col', 'none')}\n"
                f"Date: {_cfg.get('date_col', 'none')}\nID: {_cfg.get('id_col', 'none')}")

# Compute noise words for this source
_noise_words, _brand_analysis, _desc_analysis = get_noise_words(
    raw_df, _cfg.get("brand_col"), _cfg.get("name_col")
)

# Convenience aliases for column names (used throughout all steps)
NAME_COL = _cfg["name_col"]
BRAND_COL = _cfg.get("brand_col") or ""
BRAND_FB_COL = _cfg.get("brand_fallback_col")
DATE_COL = _cfg.get("date_col")
ING_COL = _cfg.get("ingredients_col")
CAT_COL = _cfg.get("category_col")
ID_COL = _cfg.get("id_col", raw_df.columns[0])

# Determine which text columns exist for this source
TEXT_COLS = [c for c in [NAME_COL, BRAND_COL, BRAND_FB_COL] if c and c in raw_df.columns]

# Create a standardized view with common column names so steps work generically.
# This maps source-specific columns to standard names used in display code.
# The original raw_df is NOT modified — this is just for display/logic convenience.
_col_map = {}
if NAME_COL in raw_df.columns:
    _col_map[NAME_COL] = "description"
if BRAND_COL and BRAND_COL in raw_df.columns:
    _col_map[BRAND_COL] = "brand_owner"
if BRAND_FB_COL and BRAND_FB_COL in raw_df.columns:
    _col_map[BRAND_FB_COL] = "brand_name"
if ID_COL and ID_COL in raw_df.columns:
    _col_map[ID_COL] = "fdc_id"
if DATE_COL and DATE_COL in raw_df.columns:
    _col_map[DATE_COL] = "published_date"
if ING_COL and ING_COL in raw_df.columns:
    _col_map[ING_COL] = "ingredients"
if CAT_COL and CAT_COL in raw_df.columns:
    _col_map[CAT_COL] = "food_category"

# Rename columns to standard names so all display code works unchanged
raw_df = raw_df.rename(columns=_col_map)

# Update TEXT_COLS to use standard names
TEXT_COLS = [c for c in ["description", "brand_owner", "brand_name"] if c in raw_df.columns]

# Override config to use standard names everywhere
_cfg = {
    "name_col": "description",
    "brand_col": "brand_owner" if "brand_owner" in raw_df.columns else None,
    "brand_fallback_col": "brand_name" if "brand_name" in raw_df.columns else None,
    "date_col": "published_date" if "published_date" in raw_df.columns else None,
    "ingredients_col": "ingredients" if "ingredients" in raw_df.columns else None,
    "category_col": "food_category" if "food_category" in raw_df.columns else None,
    "id_col": "fdc_id" if "fdc_id" in raw_df.columns else raw_df.columns[0],
}

# ===================================================================
# STEP 1: Load Raw Data
# ===================================================================
if st.session_state.step == 1:
    st.markdown("### What")
    st.write("Load the raw USDA FoodData sample CSV and inspect it.")

    st.markdown("### Why")
    st.write("Before cleaning, we need to see what we're working with — how many rows, what columns, what the data looks like.")

    st.markdown("### Result")
    c1, c2, c3 = st.columns(3)
    c1.metric("Total Rows", len(raw_df))
    c2.metric("Columns", len(raw_df.columns))
    c3.metric("Unique Products?", raw_df["description"].nunique())

    st.markdown("**Columns:**")
    col_info_df = pd.DataFrame({
        "Column": raw_df.columns,
        "Type": raw_df.dtypes.astype(str).values,
        "Non-Null": raw_df.notna().sum().values,
        "Example": [str(raw_df[c].iloc[0])[:60] for c in raw_df.columns],
    })
    st.dataframe(col_info_df, use_container_width=True)

    st.markdown("**Full Dataset:**")
    st.dataframe(raw_df, use_container_width=True, height=400)

# ===================================================================
# STEP 2: Identify Duplicates
# ===================================================================
elif st.session_state.step == 2:
    st.markdown("### What")
    st.write("Find rows where the same product appears multiple times by grouping on the `description` column.")

    st.markdown("### Why")
    st.write("USDA tracks each package size (box, bottle) as a separate record with its own barcode. "
             "So 'Cheerios Cereal' in a 20g box, 28g box, and 26g box = 3 separate rows for the same cereal.")

    # Find duplicate groups
    desc_counts = raw_df["description"].value_counts()
    dup_descs = desc_counts[desc_counts > 1]

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Rows", len(raw_df))
    c2.metric("Duplicate Groups", len(dup_descs))
    c3.metric("Rows in Duplicate Groups", raw_df[raw_df["description"].isin(dup_descs.index)].shape[0])

    st.markdown("### Duplicate Groups Found")
    dup_summary = []
    for desc, count in dup_descs.items():
        rows = raw_df[raw_df["description"] == desc]
        dup_summary.append({
            "Product": desc,
            "Count": count,
            "fdc_ids": ", ".join(rows["fdc_id"].astype(str).tolist()),
            "Serving Sizes": ", ".join(rows["serving_size"].astype(str).tolist()) if "serving_size" in rows.columns else "N/A",
        })
    st.dataframe(pd.DataFrame(dup_summary), use_container_width=True)

    st.markdown("### Highlighted Duplicates")
    st.write("Rows with the same description are highlighted with the same color:")

    # Color-code duplicate rows
    colors = ["#FFD700", "#87CEEB", "#98FB98", "#DDA0DD", "#F0E68C",
              "#E6E6FA", "#FFDAB9", "#B0E0E6", "#FFB6C1", "#D3D3D3"]
    desc_to_color = {}
    for i, desc in enumerate(dup_descs.index):
        desc_to_color[desc] = colors[i % len(colors)]

    def highlight_dups(row):
        color = desc_to_color.get(row["description"], "")
        if color:
            return [f"background-color: {color}"] * len(row)
        return [""] * len(row)

    display_cols = [c for c in ["fdc_id", "description", "brand_owner", "brand_name", "gtin_upc",
                    "serving_size", "serving_size_unit", "published_date"] if c in raw_df.columns]
    st.dataframe(
        raw_df[display_cols].style.apply(highlight_dups, axis=1),
        use_container_width=True, height=500,
    )

    st.markdown("### Hidden Duplicates")
    st.warning("Some duplicates are NOT visible yet because sizes are baked into the name:\n\n"
               "- `.75OZ DORITOS NACHO` and `1.37OZ DORITOS NACHO` — same product, different bag sizes\n"
               "- `Coca-Cola Bottle, 1.25 Liters` and `Coca-Cola Bottle, 2 Liters` — same Coke, different bottles\n\n"
               "We need text normalization to reveal these hidden duplicates.")

# ===================================================================
# STEP 3: Trim Whitespace
# ===================================================================
elif st.session_state.step == 3:
    st.markdown("### What")
    st.write('Apply `str.strip()` to remove invisible leading/trailing spaces from every text field.')

    st.markdown("### Why")
    st.write('Some rows have invisible trailing spaces. '
             '`"Hormel Foods Corporation "` (with space) won\'t match `"Hormel Foods Corporation"` (without space) '
             "even though they're the same. Computers compare character by character — a trailing space makes them different strings.")

    st.markdown("### How")
    st.code("text.strip()  # removes leading and trailing whitespace", language="python")

    # Apply trim
    trimmed_df = raw_df.copy()
    changes = []
    for col in TEXT_COLS:
        for idx in trimmed_df.index:
            old = str(trimmed_df.at[idx, col]) if pd.notna(trimmed_df.at[idx, col]) else ""
            new = old.strip()
            if old != new:
                changes.append({"Row": idx, "Column": col, "Before": repr(old), "After": repr(new)})
            trimmed_df.at[idx, col] = new

    c1, c2 = st.columns(2)
    c1.metric("Rows Changed", len(set(c["Row"] for c in changes)))
    c2.metric("Total Changes", len(changes))

    if changes:
        st.markdown("### Changes Found")
        st.dataframe(pd.DataFrame(changes), use_container_width=True)
    else:
        st.success("No whitespace issues found — data was already trimmed.")

    st.markdown("### Data After Trimming")
    st.dataframe(trimmed_df[[c for c in ["fdc_id", "description", "brand_owner", "brand_name"] if c in trimmed_df.columns]],
                 use_container_width=True, height=300)

# ===================================================================
# STEP 4: Lowercase
# ===================================================================
elif st.session_state.step == 4:
    st.markdown("### What")
    st.write("Convert `description` and `brand_owner` to lowercase.")

    st.markdown("### Why")
    st.write('`"GENERAL MILLS SALES INC."` and `"General Mills"` are the same brand but computers see '
             "them as completely different strings. `G` is not equal to `g` — they have different character codes. "
             "Lowercasing everything makes them comparable.")

    st.markdown("### How")
    st.code("text.lower()  # 'GENERAL MILLS' → 'general mills'", language="python")

    # Show before/after
    compare = []
    for idx in raw_df.index:
        for col in ["description", "brand_owner"]:
            old = str(raw_df.at[idx, col]).strip() if pd.notna(raw_df.at[idx, col]) else ""
            new = old.lower()
            if old != new:
                compare.append({
                    "fdc_id": raw_df.at[idx, "fdc_id"],
                    "Column": col,
                    "Before": old[:60],
                    "After": new[:60],
                })

    st.metric("Rows with case changes", len(set(c["fdc_id"] for c in compare)))

    st.markdown("### Before vs After (sample)")
    st.dataframe(pd.DataFrame(compare[:30]), use_container_width=True)

    st.info('Now `"Cheerios Cereal"` and `"CHEERIOS CEREAL"` both become `"cheerios cereal"` — identical strings.')

# ===================================================================
# STEP 5: Remove Noise Words
# ===================================================================
elif st.session_state.step == 5:
    st.markdown("### What")
    st.write("Remove common business/filler words that don't identify the actual brand or product.")

    st.markdown("### Why")
    st.write('`"general mills sales inc."` and `"general mills"` are the same company. '
             'The words "sales" and "inc." are legal/business suffixes — they add no identity value. '
             "If we keep them, fuzzy matching scores drop and we miss true duplicates.")

    st.markdown("### How — Two-Layer Detection")
    st.write("We use **two layers** working together to catch all noise words:")

    st.markdown("**Layer 1 — Pattern-based (universal):**")
    st.write("Business/legal suffixes that are ALWAYS noise in ANY dataset. "
             '"Sales", "Inc", "Corporation", "LLC" never identify a brand — they describe '
             "the business structure. These are caught even if they appear in only 1 company.")
    st.code(f"LEGAL_SUFFIXES = {sorted(LEGAL_SUFFIXES)}", language="python")

    st.markdown("**Layer 2 — Data-driven (automatic):**")
    st.write("Scan all brand names, count how many DIFFERENT companies each word appears in. "
             "If a word appears in > 10% of all companies → it's noise for THIS dataset. "
             'This catches domain-specific words like "foods" in a food dataset.')
    st.code("""
1. Tokenize every brand_owner value into individual words
2. For each word, count how many DIFFERENT companies it appears in
3. If a word appears in > 10% of all companies → noise (Layer 2)
4. If it's a legal suffix → noise (Layer 1)
5. Otherwise → meaningful, keep it

This scales to any dataset size. No manual review needed.
""", language="text")

    st.markdown("### Word Frequency Analysis (brand_owner)")
    st.write(f"Total unique companies in dataset: **{raw_df['brand_owner'].nunique()}**")

    # Show the auto-detected analysis
    c1, c2 = st.columns(2)
    noise_only = _brand_analysis[_brand_analysis["is_noise"] == True] if len(_brand_analysis) > 0 else pd.DataFrame()
    meaningful = _brand_analysis[_brand_analysis["is_noise"] == False].head(15) if len(_brand_analysis) > 0 else pd.DataFrame()

    with c1:
        st.markdown("**Noise Words (detected):**")
        if len(noise_only) > 0:
            display_cols = [c for c in ["word", "appears_in_n_brands", "pct_of_brands", "detection_layer", "reason"]
                           if c in noise_only.columns]
            st.dataframe(noise_only[display_cols], use_container_width=True)
        else:
            st.write("No noise words detected at current threshold.")

    with c2:
        st.markdown("**Meaningful Words (kept):**")
        if len(meaningful) > 0:
            st.dataframe(meaningful[["word", "appears_in_n_brands", "pct_of_brands", "reason"]],
                         use_container_width=True)

    st.markdown(f"### Auto-Detected Noise Words: `{_noise_words}`")
    st.write(f"**{len(_noise_words)} words** detected as noise automatically.")

    st.markdown("### Step-by-Step Removal (Examples)")
    # Generate examples dynamically from actual data
    brand_values = raw_df["brand_owner"].dropna().unique()
    for brand in sorted(brand_values):
        original = str(brand).lower()
        cleaned = remove_noise(original, _noise_words)
        if cleaned != original and cleaned.strip():
            # Find which noise words were removed
            removed = [w for w in _noise_words if re.search(r'\b' + re.escape(w) + r'\b', original)]
            if removed:
                with st.expander(f'"{brand}"'):
                    current = original
                    for w in removed:
                        prev = current
                        pattern = r'\b' + re.escape(w) + r'\b'
                        current = re.sub(pattern, '', current, flags=re.IGNORECASE)
                        current = " ".join(current.split())
                        st.write(f'Remove **"{w}"** (appears in {_brand_analysis[_brand_analysis["word"]==w]["appears_in_n_brands"].values[0] if len(_brand_analysis[_brand_analysis["word"]==w]) > 0 else "?"} brands): `"{prev}"` → `"{current}"`')
                    st.success(f'Final: `"{current}"`')

    # Show before/after for all brands
    st.markdown("### All Brand Names — Before vs After")
    brand_compare = []
    for brand in sorted(raw_df["brand_owner"].dropna().unique()):
        cleaned = remove_noise(str(brand).lower(), _noise_words)
        if cleaned != str(brand).lower():
            brand_compare.append({"Before": str(brand), "After Noise Removal": cleaned})
    if brand_compare:
        st.dataframe(pd.DataFrame(brand_compare), use_container_width=True)
    else:
        st.info("No brand names changed — noise words may not be present in this dataset's brand names.")

    # --- Company Aliases ---
    st.markdown("---")
    st.markdown("### Company Name Aliases")
    st.write("Even after removing noise words, brand owner names still vary: "
             '`"General Mills Sales"` and `"General Mills"` are the same company. '
             "A lookup dictionary maps known variations to canonical names.")
    st.write("**Alias dictionary (excerpt):**")
    alias_sample = {k: v for k, v in list(COMPANY_ALIASES.items())[:12]}
    st.json(alias_sample)

    alias_rows = []
    for brand in sorted(raw_df["brand_owner"].dropna().unique()):
        after_noise = remove_noise(str(brand).lower(), _noise_words)
        canonical = normalize_company(str(brand))
        changed = canonical.lower() != str(brand).strip().lower()
        alias_rows.append({
            "brand_owner (raw)": str(brand),
            "after noise removal": after_noise,
            "canonical name": canonical,
            "alias matched": "YES" if changed else "—",
        })

    alias_df = pd.DataFrame(alias_rows)

    def highlight_alias(row):
        if row["alias matched"] == "YES":
            return ["background-color: #90EE90"] * len(row)
        return [""] * len(row)

    st.write(f"**{alias_df[alias_df['alias matched'] == 'YES'].shape[0]} brands** resolved to canonical names:")
    st.dataframe(alias_df.style.apply(highlight_alias, axis=1), use_container_width=True)

# ===================================================================
# STEP 6: Remove Punctuation
# ===================================================================
elif st.session_state.step == 6:
    st.markdown("### What")
    st.write("Replace every non-letter, non-number character with a space. Then collapse multiple spaces into one.")

    st.markdown("### Why")
    st.write('After removing noise words, we\'re left with dangling punctuation: `"ferrero ,"`. '
             'Also `"coca-cola"` has a hyphen that prevents matching with `"coca cola"`. '
             "Punctuation adds no identity value for matching products.")

    st.markdown("### How")
    st.code('re.sub(r"[^a-z0-9\\s]", " ", text)  # replace non-alphanumeric with space\n'
            '" ".join(text.split())                 # collapse multiple spaces',
            language="python")

    # Show before/after
    examples_data = []
    for idx in raw_df.index:
        for col in ["description", "brand_owner"]:
            val = str(raw_df.at[idx, col]).strip() if pd.notna(raw_df.at[idx, col]) else ""
            lowered = remove_noise(val.lower(), _noise_words)
            cleaned = remove_punct(lowered)
            if lowered != cleaned:
                examples_data.append({
                    "fdc_id": raw_df.at[idx, "fdc_id"],
                    "Column": col,
                    "Before (after noise removal)": lowered[:60],
                    "After (punctuation removed)": cleaned[:60],
                    "Characters Removed": "".join(c for c in lowered if not c.isalnum() and c != " "),
                })

    st.metric("Fields with punctuation removed", len(examples_data))
    st.dataframe(pd.DataFrame(examples_data).drop_duplicates(subset=["Before (after noise removal)"]),
                 use_container_width=True)

    st.markdown("### Key Examples")
    punct_examples = [
        ('ferrero ,', 'ferrero', 'Dangling comma from noise word removal'),
        ('coca-cola bottle', 'coca cola bottle', 'Hyphen prevents exact matching'),
        ('post consumer brands,', 'post consumer brands', 'Trailing comma'),
        ('the coca-cola company-0049000000016', 'coca cola company 0049000000016', 'Hyphens and special chars'),
    ]
    for before, after, reason in punct_examples:
        st.write(f'`"{before}"` → `"{after}"` — *{reason}*')

# ===================================================================
# STEP 7: Regex — Strip Sizes
# ===================================================================
elif st.session_state.step == 7:
    st.markdown("### What")
    st.write("Remove package size/weight patterns from product descriptions using regex.")

    st.markdown("### Why")
    st.write('`.75OZ DORITOS NACHO` and `1.37OZ DORITOS NACHO` are the same Doritos — just different bag sizes. '
             '`Coca-Cola Bottle, 1.25 Liters` and `Coca-Cola Bottle, 2 Liters` are the same Coke. '
             "The size is packaging info, not product identity.")

    st.markdown("### Regex Patterns Used")

    st.markdown("**Pattern 1 — Leading size (at start of text):**")
    st.code(r'^\d+(\.\d+)?\s*oz\s+', language="regex")
    st.write("Meaning: start of string → digits → optional decimal → 'oz' → space")
    st.write('`".75oz doritos nacho"` → matched: **`.75oz `** → Result: `"doritos nacho"`')
    st.write('`"1.37oz doritos nacho"` → matched: **`1.37oz `** → Result: `"doritos nacho"`')

    st.markdown("**Pattern 2 — Trailing size with unit:**")
    st.code(r',?\s*\d+(\.\d+)?\s*(oz|liters?|l|ml|mlt|gal|lb|kg|g|grm|ct|pk|pack)\b.*$', language="regex")
    st.write("Meaning: optional comma → digits → unit word → everything after")
    st.write('`"coca cola bottle 1.25 liters"` → matched: **` 1.25 liters`** → Result: `"coca cola bottle"`')
    st.write('`"coca cola bottle 2 liters"` → matched: **` 2 liters`** → Result: `"coca cola bottle"`')

    st.markdown("**Pattern 3 — Trailing short size:**")
    st.code(r'\s+\d+(\.\d+)?\s*[a-z]{1,2}$', language="regex")
    st.write('`"doritos ranch 2.125z"` → matched: **` 2.125z`** → Result: `"doritos ranch"`')

    # Apply to all descriptions
    st.markdown("### Before vs After — All Descriptions")
    regex_changes = []
    for idx in raw_df.index:
        desc = str(raw_df.at[idx, "description"]).strip() if pd.notna(raw_df.at[idx, "description"]) else ""
        # Apply lowercase + noise first, then strip sizes BEFORE punctuation
        # (because punctuation removal turns "1.25" into "1 25" breaking the regex)
        after_noise = remove_noise(desc.lower(), _noise_words)
        stripped = strip_sizes(after_noise)
        final = remove_punct(stripped)
        if after_noise != stripped:
            regex_changes.append({
                "fdc_id": raw_df.at[idx, "fdc_id"],
                "Original": desc[:50],
                "After Noise Removal": after_noise[:50],
                "After Size Strip": remove_punct(stripped)[:50],
                "What Was Removed": after_noise.replace(stripped, "").strip(),
            })

    st.metric("Rows with sizes stripped", len(regex_changes))
    st.dataframe(pd.DataFrame(regex_changes), use_container_width=True)

    st.markdown("### Hidden Duplicates Now Revealed")
    # Show groups that become identical after size stripping
    norm_descs = {}
    for idx in raw_df.index:
        desc = str(raw_df.at[idx, "description"]).strip()
        # Correct order: sizes stripped before punctuation removal
        n = remove_punct(strip_sizes(remove_noise(desc.lower(), _noise_words)))
        norm_descs.setdefault(n, []).append({"fdc_id": raw_df.at[idx, "fdc_id"], "original": desc})

    new_dups = {k: v for k, v in norm_descs.items() if len(v) > 1}
    # Find groups that are NEW (weren't visible as duplicates before)
    raw_desc_counts = raw_df["description"].value_counts()
    for norm_key, items in list(new_dups.items()):
        orig_descs = set(i["original"] for i in items)
        if len(orig_descs) == 1:
            pass  # was already a visible duplicate
        else:
            st.success(f'**"{norm_key}"** — {len(items)} rows now match:')
            for item in items:
                st.write(f'  - fdc_id `{item["fdc_id"]}`: `"{item["original"]}"`')

    # --- Sizes Column ---
    st.markdown("---")
    st.markdown("### Extracted Sizes Column")
    st.write("The size stripped from each description is saved as a `size_label` column. "
             "At cluster level (Step 10), all size variants for the same product are aggregated into a list.")

    size_rows = []
    for idx in raw_df.index:
        desc = str(raw_df.at[idx, "description"]) if pd.notna(raw_df.at[idx, "description"]) else ""
        size_label = extract_size(desc)
        sv = raw_df.at[idx, "serving_size"]
        svu = raw_df.at[idx, "serving_size_unit"] if pd.notna(raw_df.at[idx, "serving_size_unit"]) else ""
        serving = f"{sv} {svu}".strip() if pd.notna(sv) else ""
        size_rows.append({
            "fdc_id": raw_df.at[idx, "fdc_id"],
            "description": desc[:60],
            "size_label (from desc)": size_label,
            "serving_size": serving,
        })

    size_df = pd.DataFrame(size_rows)
    n_with_size = size_df[size_df["size_label (from desc)"] != ""].shape[0]
    st.metric("Rows with extracted size", f"{n_with_size} / {len(size_df)}")
    st.dataframe(size_df, use_container_width=True)

    # --- Allergens Column ---
    st.markdown("---")
    st.markdown("### Allergens Column (Rule-Based)")
    st.write("The `ingredients` field is scanned against a predefined keyword dictionary "
             "covering the **FDA Big-9 allergens**. No LLM needed.")

    st.write("**Allergen keyword dictionary:**")
    st.json(ALLERGEN_KEYWORDS)

    allergen_rows = []
    for idx in raw_df.index:
        desc = str(raw_df.at[idx, "description"]) if pd.notna(raw_df.at[idx, "description"]) else ""
        ing = raw_df.at[idx, "ingredients"] if pd.notna(raw_df.at[idx, "ingredients"]) else ""
        allergens_found = extract_allergens(ing)
        allergen_rows.append({
            "fdc_id": raw_df.at[idx, "fdc_id"],
            "description": desc[:50],
            "ingredients (truncated)": str(ing)[:80],
            "allergens_found": allergens_found if allergens_found else "none",
        })

    allergen_df = pd.DataFrame(allergen_rows)
    n_with_allergens = allergen_df[allergen_df["allergens_found"] != "none"].shape[0]

    c1, c2 = st.columns(2)
    c1.metric("Rows with allergens detected", n_with_allergens)
    c2.metric("Rows with no allergens", len(allergen_df) - n_with_allergens)

    st.dataframe(allergen_df, use_container_width=True)

    # Breakdown by allergen type
    st.markdown("**Breakdown by allergen type:**")
    breakdown = {}
    for allergen in ALLERGEN_KEYWORDS:
        count = allergen_df["allergens_found"].str.contains(allergen, na=False).sum()
        breakdown[allergen] = count
    breakdown_df = pd.DataFrame([{"Allergen": k, "Products Affected": v}
                                  for k, v in sorted(breakdown.items(), key=lambda x: -x[1])])
    st.dataframe(breakdown_df, use_container_width=True)

# ===================================================================
# STEP 8: Blocking & Fuzzy Matching
# ===================================================================
elif st.session_state.step == 8:
    st.markdown("### What")
    st.write("Group rows by normalized (description + brand) into blocks, then score similarity within each block.")

    st.markdown("### Why — Blocking")
    total_pairs = len(raw_df) * (len(raw_df) - 1) // 2
    st.write(f"With {len(raw_df)} rows, comparing every pair = **{total_pairs:,} comparisons**. "
             "With blocking, we only compare within groups — maybe 50 comparisons total. "
             "At 450K rows, this saves billions of comparisons.")

    st.markdown("### How — Fuzzy Scoring Formula")
    st.code("weighted_score = name_score × 0.5 + brand_score × 0.2 + combined_score × 0.3\n"
            "Threshold: >= 85 = MATCH", language="text")
    st.write("`token_sort_ratio` splits text into words, sorts them alphabetically, then compares. "
             'So "General Mills" vs "Mills General" still scores 100.')

    # Build blocks
    block_data = {}
    for idx in raw_df.index:
        desc = full_normalize(str(raw_df.at[idx, _cfg["name_col"]]) if pd.notna(raw_df.at[idx, _cfg["name_col"]]) else "", _noise_words)
        brand = full_normalize(get_brand_at(raw_df, idx, _cfg), _noise_words)
        key = (desc, brand)
        block_data.setdefault(key, []).append(idx)

    multi_blocks = {k: v for k, v in block_data.items() if len(v) > 1}
    single_blocks = {k: v for k, v in block_data.items() if len(v) == 1}

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Blocks", len(block_data))
    c2.metric("Blocks with Duplicates", len(multi_blocks))
    c3.metric("Singleton Blocks (no dups)", len(single_blocks))

    st.markdown("### Blocks with Duplicates — Pairwise Scores")
    for (desc_key, brand_key), indices in sorted(multi_blocks.items(), key=lambda x: -len(x[1])):
        with st.expander(f'Block: "{desc_key}" + "{brand_key}" ({len(indices)} rows)'):
            block_df = raw_df.loc[indices, [c for c in ["fdc_id", "description", "brand_owner", "gtin_upc",
                                             "serving_size", "published_date"] if c in raw_df.columns]]
            st.write("**Rows in this block:**")
            st.dataframe(block_df, use_container_width=True)

            # Pairwise scores
            if len(indices) > 1:
                st.write("**Pairwise Similarity Scores:**")
                pair_rows = []
                for i in range(len(indices)):
                    for j in range(i + 1, len(indices)):
                        a, b = indices[i], indices[j]
                        na = full_normalize(str(raw_df.at[a, _cfg["name_col"]]), _noise_words)
                        ba = full_normalize(get_brand_at(raw_df, a, _cfg), _noise_words)
                        nb = full_normalize(str(raw_df.at[b, _cfg["name_col"]]), _noise_words)
                        bb = full_normalize(get_brand_at(raw_df, b, _cfg), _noise_words)
                        scores = fuzzy_score(na, ba, nb, bb)
                        pair_rows.append({
                            "Row A (fdc_id)": raw_df.at[a, "fdc_id"],
                            "Row B (fdc_id)": raw_df.at[b, "fdc_id"],
                            "Name Score": scores["name_score"],
                            "Brand Score": scores["brand_score"],
                            "Combined Score": scores["combined_score"],
                            "Weighted Score": scores["weighted_score"],
                            "Match?": "YES" if scores["weighted_score"] >= 85 else "NO",
                        })

                pair_df = pd.DataFrame(pair_rows)

                def color_match(row):
                    color = "background-color: #90EE90" if row["Match?"] == "YES" else "background-color: #FFB6C1"
                    return [color] * len(row)

                st.dataframe(pair_df.style.apply(color_match, axis=1), use_container_width=True)

# ===================================================================
# STEP 9: Clustering (Union-Find)
# ===================================================================
elif st.session_state.step == 9:
    st.markdown("### What")
    st.write("Link all matching pairs into clusters using Union-Find (transitive closure).")

    st.markdown("### Why")
    st.write("If Row A matches Row B (score 94) and Row B matches Row C (score 91), "
             "then A, B, C are all the same product — even if A and C were never directly compared. "
             "Union-Find handles this automatically.")

    st.markdown("### How — Union-Find Algorithm")
    st.code("""
class UnionFind:
    def find(x):    # find the root/leader of x's group
    def union(a,b): # merge a's group and b's group together

# Process each matching pair:
for (row_a, row_b) in matching_pairs:
    union_find.union(row_a, row_b)

# Extract clusters:
clusters = union_find.clusters()  # {leader: [member1, member2, ...]}
""", language="python")

    # Build actual clusters
    uf = UnionFind()
    all_indices = list(raw_df.index)
    for idx in all_indices:
        uf.find(idx)  # register all

    block_data = {}
    for idx in raw_df.index:
        desc = full_normalize(str(raw_df.at[idx, _cfg["name_col"]]) if pd.notna(raw_df.at[idx, _cfg["name_col"]]) else "", _noise_words)
        brand = full_normalize(get_brand_at(raw_df, idx, _cfg), _noise_words)
        block_data.setdefault((desc, brand), []).append(idx)

    merge_log = []
    for (desc_key, brand_key), indices in block_data.items():
        if len(indices) < 2:
            continue
        for i in range(len(indices)):
            for j in range(i + 1, len(indices)):
                a, b = indices[i], indices[j]
                na = full_normalize(str(raw_df.at[a, _cfg["name_col"]]), _noise_words)
                ba = full_normalize(get_brand_at(raw_df, a, _cfg), _noise_words)
                nb = full_normalize(str(raw_df.at[b, _cfg["name_col"]]), _noise_words)
                bb = full_normalize(get_brand_at(raw_df, b, _cfg), _noise_words)
                scores = fuzzy_score(na, ba, nb, bb)
                if scores["weighted_score"] >= 85:
                    before_a = uf.find(a)
                    before_b = uf.find(b)
                    uf.union(a, b)
                    merge_log.append({
                        "fdc_id A": raw_df.at[a, "fdc_id"],
                        "fdc_id B": raw_df.at[b, "fdc_id"],
                        "Score": scores["weighted_score"],
                        "Action": "MERGE" if before_a != before_b else "Already same cluster",
                    })

    clusters = uf.clusters()
    multi_clusters = {k: v for k, v in clusters.items() if len(v) > 1}

    c1, c2, c3 = st.columns(3)
    c1.metric("Total Clusters", len(clusters))
    c2.metric("Multi-row Clusters", len(multi_clusters))
    c3.metric("Rows to Deduplicate", sum(len(v) - 1 for v in multi_clusters.values()))

    st.markdown("### Merge Log")
    st.write("Each merge step in the Union-Find process:")
    st.dataframe(pd.DataFrame(merge_log), use_container_width=True)

    st.markdown("### Final Clusters")
    for leader, members in sorted(multi_clusters.items(), key=lambda x: -len(x[1])):
        desc = raw_df.at[members[0], "description"]
        with st.expander(f'Cluster: "{desc}" ({len(members)} rows)'):
            # Show Union-Find steps
            st.write("**Union-Find merge steps:**")
            sets_display = [f"{{{raw_df.at[m, 'fdc_id']}}}" for m in members]
            st.code(f"Start:  {' '.join(sets_display)}")
            merged = [members[0]]
            for m in members[1:]:
                merged.append(m)
                merged_ids = ", ".join(str(raw_df.at[x, "fdc_id"]) for x in merged)
                st.code(f"Union → {{{merged_ids}}}")
            st.write("**Rows in cluster:**")
            st.dataframe(
                raw_df.loc[members, [c for c in ["fdc_id", "description", "brand_owner", "brand_name",
                                     "serving_size", "published_date"] if c in raw_df.columns]],
                use_container_width=True,
            )

# ===================================================================
# STEP 10: Golden Record Selection (DQ Scoring)
# ===================================================================
elif st.session_state.step == 10:
    st.markdown("### What")
    st.write("From each cluster, pick the ONE best row — the 'golden record' — based on a Data Quality score.")

    st.markdown("### Why")
    st.write("You can't keep all 3 Cheerios rows. You need one source of truth. "
             "Pick the row with the highest data quality — most complete, most recent, richest ingredients.")

    st.markdown("### DQ Score Formula")
    st.code("""
DQ Score = Completeness × 0.4 + Freshness × 0.35 + Ingredient Richness × 0.25

Completeness  = (non-null fields) / (total fields)     — more fields filled = better
Freshness     = normalized published_date (0=oldest, 1=newest within cluster)
Ingredient Richness = normalized ingredient text length (0=shortest, 1=longest)
""", language="text")

    # Build clusters
    uf = UnionFind()
    for idx in raw_df.index:
        uf.find(idx)

    block_data = {}
    for idx in raw_df.index:
        desc = full_normalize(str(raw_df.at[idx, _cfg["name_col"]]) if pd.notna(raw_df.at[idx, _cfg["name_col"]]) else "", _noise_words)
        brand = full_normalize(get_brand_at(raw_df, idx, _cfg), _noise_words)
        block_data.setdefault((desc, brand), []).append(idx)

    for indices in block_data.values():
        if len(indices) < 2:
            continue
        for i in range(len(indices)):
            for j in range(i + 1, len(indices)):
                a, b = indices[i], indices[j]
                na = full_normalize(str(raw_df.at[a, _cfg["name_col"]]), _noise_words)
                ba = full_normalize(get_brand_at(raw_df, a, _cfg), _noise_words)
                nb = full_normalize(str(raw_df.at[b, _cfg["name_col"]]), _noise_words)
                bb = full_normalize(get_brand_at(raw_df, b, _cfg), _noise_words)
                if fuzzy_score(na, ba, nb, bb)["weighted_score"] >= 85:
                    uf.union(a, b)

    clusters = uf.clusters()
    multi_clusters = {k: v for k, v in clusters.items() if len(v) > 1}

    # Show DQ breakdown per cluster
    golden_records = []
    merge_summary = []

    for leader, members in sorted(multi_clusters.items(), key=lambda x: -len(x[1])):
        group_df = raw_df.loc[members]
        desc = group_df.iloc[0]["description"]

        with st.expander(f'Cluster: "{desc}" ({len(members)} rows)'):
            score_rows = []
            for m in members:
                row = raw_df.loc[m]
                dq = compute_dq(row, group_df, _cfg)
                comp = compute_completeness(row)
                # Freshness
                fresh = 0.5
                pub_date = ""
                if "published_date" in group_df.columns:
                    dates = pd.to_datetime(group_df["published_date"], errors="coerce")
                    min_d, max_d = dates.min(), dates.max()
                    fresh = compute_freshness(row.get("published_date", ""), min_d, max_d)
                    pub_date = row.get("published_date", "")
                # Richness
                ing_len = 0
                richness = 0.5
                if "ingredients" in group_df.columns:
                    ing_lens = group_df["ingredients"].astype(str).str.len()
                    min_l, max_l = ing_lens.min(), ing_lens.max()
                    ing_len = len(str(row.get("ingredients", "")))
                    richness = (ing_len - min_l) / (max_l - min_l) if max_l > min_l else 1.0
                score_rows.append({
                    "fdc_id": row.get("fdc_id", row.name),
                    "Fields Filled": f"{int(comp * len(row))}/{len(row)}",
                    "Completeness": round(comp, 3),
                    "Published Date": pub_date,
                    "Freshness": round(fresh, 3),
                    "Ingredients Len": ing_len,
                    "Richness": round(richness, 3),
                    "DQ Score": dq,
                })

            score_df = pd.DataFrame(score_rows)
            winner_idx = score_df["DQ Score"].idxmax()

            st.write("**Component 1 — Completeness (40% weight):** Count non-null fields per row")
            st.write("**Component 2 — Freshness (35% weight):** Normalize dates (oldest=0, newest=1)")
            st.write("**Component 3 — Ingredient Richness (25% weight):** Normalize ingredient text length")

            def highlight_winner(row):
                if row.name == winner_idx:
                    return ["background-color: #90EE90"] * len(row)
                return ["background-color: #FFB6C1"] * len(row)

            st.dataframe(score_df.style.apply(highlight_winner, axis=1), use_container_width=True)

            winner_fdc = score_df.loc[winner_idx, "fdc_id"]
            dropped_fdc = [str(r["fdc_id"]) for _, r in score_df.iterrows() if r["fdc_id"] != winner_fdc]
            st.success(f"**Winner:** fdc_id `{winner_fdc}` (DQ Score: {score_df.loc[winner_idx, 'DQ Score']})")
            st.write(f"**Dropped:** fdc_ids `{', '.join(dropped_fdc)}`")

            # Sizes aggregation
            all_sizes = []
            all_serving = []
            for m in members:
                sz = extract_size(str(raw_df.at[m, "description"]))
                if sz:
                    all_sizes.append(sz)
                sv = raw_df.at[m, "serving_size"]
                svu = raw_df.at[m, "serving_size_unit"] if pd.notna(raw_df.at[m, "serving_size_unit"]) else ""
                if pd.notna(sv):
                    all_serving.append(f"{sv} {svu}".strip())
            all_sizes = list(dict.fromkeys(all_sizes))          # dedup, preserve order
            all_serving = list(dict.fromkeys(all_serving))
            st.write(f"**Sizes (from descriptions):** `{all_sizes}`")
            st.write(f"**Serving sizes (from serving_size column):** `{all_serving}`")

            # Collect winner index in original df
            winner_orig_idx = members[score_df["DQ Score"].values.tolist().index(score_df.loc[winner_idx, "DQ Score"])]
            golden_records.append(winner_orig_idx)
            # Store sizes for Step 12
            if "cluster_sizes" not in st.session_state:
                st.session_state["cluster_sizes"] = {}
                st.session_state["cluster_serving"] = {}
            st.session_state["cluster_sizes"][winner_orig_idx] = all_sizes
            st.session_state["cluster_serving"][winner_orig_idx] = all_serving
            merge_summary.append({
                "Product": desc,
                "Original Rows": len(members),
                "Winner fdc_id": winner_fdc,
                "Winner DQ Score": score_df.loc[winner_idx, "DQ Score"],
                "Dropped fdc_ids": ", ".join(dropped_fdc),
            })

    # Add singletons
    single_clusters = {k: v for k, v in clusters.items() if len(v) == 1}
    for leader, members in single_clusters.items():
        golden_records.append(members[0])

    st.markdown("---")
    st.markdown("### Summary of All Merges")
    st.dataframe(pd.DataFrame(merge_summary), use_container_width=True)

    deduped_df = raw_df.loc[golden_records].copy()
    c1, c2, c3 = st.columns(3)
    c1.metric("Original Rows", len(raw_df))
    c2.metric("After Dedup", len(deduped_df))
    c3.metric("Rows Removed", len(raw_df) - len(deduped_df))

    st.session_state["deduped_df"] = deduped_df
    st.session_state["merge_summary"] = merge_summary

# ===================================================================
# STEP 11: LLM Enrichment (Groq Llama 3 70B) with Optimizations
# ===================================================================
elif st.session_state.step == 11:
    st.markdown("### What")
    st.write("Enrich products with structured attributes (category, allergens, dietary tags). "
             "But instead of sending every product to an LLM, we use **4 optimization layers** "
             "to minimize LLM calls. LLM is only used as a last resort for ambiguous products.")

    # Get deduped data
    deduped_df = st.session_state.get("deduped_df", raw_df).copy()
    merge_summary = st.session_state.get("merge_summary", [])
    rows_removed = sum(m["Original Rows"] - 1 for m in merge_summary) if merge_summary else 0
    n_deduped = len(raw_df) - rows_removed

    # =================================================================
    # LAYER 0: Dedup savings
    # =================================================================
    st.markdown("---")
    st.markdown("## Layer 0: Deduplication (already done)")
    st.write("We already reduced the dataset in Steps 1-10.")
    c1, c2, c3 = st.columns(3)
    c1.metric("Original Rows", len(raw_df))
    c2.metric("After Dedup", n_deduped)
    c3.metric("Saved", f"{rows_removed} calls")

    # =================================================================
    # LAYER 1: Rule-based category mapping
    # =================================================================
    st.markdown("---")
    st.markdown("## Layer 1: Rule-Based Enrichment (0 LLM calls)")
    st.write("USDA already gives us `food_category` (21 standard categories). "
             "We can map these directly to our target `primary_category` without any LLM.")

    # Category mapping rules
    CATEGORY_MAP = {
        # Cereal
        "Processed Cereal Products": "Breakfast Cereal",
        "Cereal": "Breakfast Cereal",
        "Hot Cereal": "Breakfast Cereal",
        "Cold Cereal": "Breakfast Cereal",
        # Beverages
        "Non Alcoholic Beverages - Ready to Drink": "Beverage",
        "Non Alcoholic Beverages  Ready to Drink": "Beverage",
        "Soda": "Beverage",
        "Tea Bags": "Beverage",
        "Energy, Protein & Muscle Recovery Drinks": "Beverage",
        # Snacks
        "Snacks": "Snack",
        "Chips, Pretzels & Snacks": "Snack",
        "Snack, Energy & Granola Bars": "Snack",
        "Wholesome Snacks": "Snack",
        "Popcorn, Peanuts, Seeds & Related Snacks": "Snack",
        # Cookies/Bakery
        "Cookies & Biscuits": "Bakery",
        "Biscuits/Cookies": "Bakery",
        "Crackers & Biscotti": "Bakery",
        "Cakes, Cupcakes, Snack Cakes": "Bakery",
        "Croissants, Sweet Rolls, Muffins & Other Pastries": "Bakery",
        "Breads & Buns": "Bakery",
        "Dough Based Products / Meals": "Bakery",
        # Candy/Chocolate
        "Candy": "Candy",
        "Chocolate": "Candy",
        "Chewing Gum & Mints": "Candy",
        # Condiments/Spreads
        "Sauces/Spreads/Dips/Condiments": "Condiment",
        "Ketchup, Mustard, BBQ & Cheese Sauce": "Condiment",
        "Dips & Salsa": "Condiment",
        "Syrups & Molasses": "Condiment",
        "Nut & Seed Butters": "Spread",
        "Butter & Spread": "Spread",
        "Jam, Jelly & Fruit Spreads": "Spread",
        # Dairy
        "Cheese": "Dairy",
        "Yogurt": "Dairy",
        "Ice Cream & Frozen Yogurt": "Frozen Dairy",
        # Prepared/Canned
        "Prepared Soups": "Prepared Food",
        "Pasta Dinners": "Prepared Food",
        "Vegetable Based Products / Meals": "Prepared Food",
        "Pizza": "Prepared Food",
        "Canned & Bottled Beans": "Canned/Preserved",
        "Canned Vegetables": "Canned/Preserved",
        "Tomatoes": "Canned/Preserved",
        "Pickles, Olives, Peppers & Relishes": "Canned/Preserved",
        # Frozen
        "Frozen Vegetables": "Frozen",
        "Frozen Prepared Sides": "Frozen",
        "Frozen Fruit & Fruit Juice Concentrates": "Frozen",
        # Pantry
        "Pasta by Shape & Type": "Pantry",
        "Grains/Flour": "Pantry",
        "Flours & Corn Meal": "Pantry",
        "Seasoning Mixes, Salts, Marinades & Tenderizers": "Pantry",
        "Dry Mixes": "Pantry",
        # Meat
        "Pepperoni, Salami & Cold Cuts": "Meat",
        # Produce
        "Pre-Packaged Fruit & Vegetables": "Produce",
    }

    st.write("**Mapping rules:**")
    map_df = pd.DataFrame([{"USDA food_category": k, "Mapped primary_category": v}
                           for k, v in sorted(CATEGORY_MAP.items())])
    st.dataframe(map_df, use_container_width=True, height=250)

    # Apply rules
    deduped_df["rule_category"] = deduped_df["food_category"].map(CATEGORY_MAP)
    rule_hit = deduped_df["rule_category"].notna()
    rule_miss = ~rule_hit

    # Allergen extraction from ingredients using keyword search (uses top-level ALLERGEN_KEYWORDS)
    deduped_df["rule_allergens"] = deduped_df["ingredients"].apply(extract_allergens)

    # Check organic
    deduped_df["rule_is_organic"] = deduped_df["description"].str.lower().str.contains("organic", na=False)

    st.write("**Allergen keywords** (FDA Big-9 — scan ingredients text for these):")
    st.json(ALLERGEN_KEYWORDS)

    st.markdown("### Rule-Based Results")
    n_rule_hit = rule_hit.sum()

    c1, c2, c3 = st.columns(3)
    c1.metric("Handled by Rules", n_rule_hit)
    c2.metric("Still Need LLM", rule_miss.sum())
    c3.metric("LLM Calls Saved", n_rule_hit, delta=f"of {n_deduped}")

    # Show sample of rule-based enrichment
    rule_sample = deduped_df[rule_hit][["fdc_id", "description", "food_category",
                                        "rule_category", "rule_allergens", "rule_is_organic"]].head(10)
    st.dataframe(rule_sample, use_container_width=True)

    # =================================================================
    # LAYER 2: Cache (simulated)
    # =================================================================
    st.markdown("---")
    st.markdown("## Layer 2: Cache Lookup (0 LLM calls)")
    st.write("If we've enriched a product before (in a previous pipeline run), "
             "we store the result in a cache. Next time we see the same product, "
             "we just look it up — no LLM call needed.")
    st.write("**In this demo:** First run, so cache is empty. "
             "But after running once, all enriched products go into the cache.")

    st.code("""
# How cache works:
cache_key = normalize(description + brand)
if cache_key in cache:
    return cache[cache_key]    # instant, free
else:
    result = call_llm(...)     # expensive
    cache[cache_key] = result  # save for next time
    return result
""", language="python")

    # Simulate: assume 70% of remaining products would be cache hits on re-runs
    n_remaining_after_rules = int(rule_miss.sum())
    n_cache_hits = 0  # first run = 0 cache hits
    n_after_cache = n_remaining_after_rules - n_cache_hits

    st.info(f"**First run:** 0 cache hits (cache is empty). "
            f"**Re-runs:** Up to {n_remaining_after_rules} products would be cache hits = 0 LLM calls.")

    # =================================================================
    # LAYER 3: Batching
    # =================================================================
    st.markdown("---")
    st.markdown("## Layer 3: Batching (fewer LLM calls)")
    st.write("Instead of 1 LLM call per product, we send multiple products in one call. "
             "The LLM processes 5-10 products at once and returns a JSON array.")

    BATCH_SIZE = 5
    n_need_llm = n_after_cache
    n_batched_calls = max(1, -(-n_need_llm // BATCH_SIZE))  # ceiling division

    st.code(f"""
Products needing LLM:  {n_need_llm}
Batch size:            {BATCH_SIZE} products per call
LLM calls needed:     {n_need_llm} / {BATCH_SIZE} = {n_batched_calls} calls
""", language="text")

    # =================================================================
    # PROGRESSIVE SAVINGS SUMMARY
    # =================================================================
    st.markdown("---")
    st.markdown("## Progressive Savings Summary")

    savings_data = [
        {"Layer": "No optimization", "Products": len(raw_df), "LLM Calls": len(raw_df),
         "Calls Saved": 0, "Technique": "1 call per row"},
        {"Layer": "0. Dedup", "Products": n_deduped, "LLM Calls": n_deduped,
         "Calls Saved": rows_removed, "Technique": "Remove duplicate rows"},
        {"Layer": "1. + Rules", "Products": n_deduped, "LLM Calls": n_remaining_after_rules,
         "Calls Saved": len(raw_df) - n_remaining_after_rules, "Technique": "Map food_category + keyword allergens"},
        {"Layer": "2. + Cache", "Products": n_deduped, "LLM Calls": n_after_cache,
         "Calls Saved": len(raw_df) - n_after_cache, "Technique": "Lookup previously enriched products"},
        {"Layer": "3. + Batching", "Products": n_deduped, "LLM Calls": n_batched_calls,
         "Calls Saved": len(raw_df) - n_batched_calls, "Technique": f"Send {BATCH_SIZE} products per call"},
    ]
    savings_df = pd.DataFrame(savings_data)

    def color_savings(row):
        pct = row["Calls Saved"] / len(raw_df) * 100
        if pct > 80:
            return ["background-color: #90EE90"] * len(row)
        elif pct > 50:
            return ["background-color: #FFFFAA"] * len(row)
        return [""] * len(row)

    st.dataframe(savings_df.style.apply(color_savings, axis=1), use_container_width=True)

    # Scale projections
    st.markdown("### At Production Scale (454K USDA records)")
    scale_factor = 454678 / len(raw_df)
    scale_data = []
    for s in savings_data:
        scale_calls = int(s["LLM Calls"] * scale_factor)
        if s["Layer"] == "2. + Cache":
            # On re-runs, cache hits ~70% of remaining
            scale_calls = int(scale_calls * 0.3)
        time_mins = scale_calls * (60 / 25) / 60  # at 25 calls/min
        cost = scale_calls * 0.01
        scale_data.append({
            "Layer": s["Layer"],
            "LLM Calls": f"{scale_calls:,}",
            "Time (at 25 calls/min)": f"{time_mins:.1f} hours" if time_mins > 1 else f"{time_mins*60:.0f} minutes",
            "Est. Cost ($0.01/call)": f"${cost:,.0f}",
        })
    st.dataframe(pd.DataFrame(scale_data), use_container_width=True)

    # =================================================================
    # LAYER 4: LLM Enrichment (only for remaining products)
    # =================================================================
    st.markdown("---")
    st.markdown("## Layer 4: LLM Enrichment (only what's left)")
    st.write(f"After rules handled {n_rule_hit} products, only **{n_need_llm} products** need the LLM. "
             f"With batching, that's just **{n_batched_calls} API calls**.")

    # Show which products need LLM
    llm_needed = deduped_df[rule_miss]
    if len(llm_needed) > 0:
        st.write("**Products that need LLM** (no rule could handle them):")
        st.dataframe(llm_needed[["fdc_id", "description", "food_category"]].head(10), use_container_width=True)

    st.markdown("### Run LLM on Sample")
    n_llm_available = max(0, len(llm_needed))
    if n_llm_available == 0:
        st.success("**All products were handled by rules! No LLM calls needed.**")
        st.write("But you can still try LLM enrichment on a few products to compare rule-based vs LLM results:")
        n_enrich = st.slider("Products to enrich with LLM (for comparison)", 1, min(10, len(deduped_df)), 3)
        llm_sample_source = deduped_df
    else:
        n_enrich = st.slider("Products to enrich with LLM", 1, min(10, n_llm_available), min(3, n_llm_available))
        llm_sample_source = llm_needed

    if st.button("Run LLM Enrichment"):
        sample = llm_sample_source.head(n_enrich)
        enriched_results = []

        progress_bar = st.progress(0)
        status = st.empty()

        for i, (idx, row) in enumerate(sample.iterrows()):
            desc = str(row.get("description", ""))
            brand = str(row.get("brand_owner", ""))
            ingredients = str(row.get("ingredients", ""))

            status.write(f"Enriching [{i+1}/{n_enrich}]: **{desc[:50]}** ...")

            result = call_groq_llm(desc, brand, ingredients)
            if result is not None:
                parsed, prompt, raw_response = result
                if isinstance(parsed, dict):
                    enriched_results.append({
                        "fdc_id": row["fdc_id"],
                        "original_description": desc[:50],
                        **parsed,
                    })
                    with st.expander(f'Product: "{desc[:50]}"'):
                        st.markdown("**Prompt sent to LLM:**")
                        st.code(prompt, language="text")
                        st.markdown("**Raw LLM Response:**")
                        st.code(raw_response, language="json")
                        st.markdown("**Extracted Fields:**")
                        st.json(parsed)
                else:
                    st.warning(f"Failed for {desc[:50]}: {raw_response}")
            else:
                st.error(f"API call failed for {desc[:50]}")

            progress_bar.progress((i + 1) / n_enrich)
            if i < n_enrich - 1:
                time.sleep(2.5)

        status.write("Done!")

        if enriched_results:
            st.markdown("### LLM Enriched Results")
            st.dataframe(pd.DataFrame(enriched_results), use_container_width=True)

    # =================================================================
    # FINAL: Combined output
    # =================================================================
    st.markdown("---")
    st.markdown("## Final: Combined Enrichment (Rules + LLM)")
    st.write("Products handled by rules get rule-based attributes. "
             "Products handled by LLM get LLM-extracted attributes. "
             "Everything combined into one clean catalog.")

    # Show the rule-enriched data as a preview
    final_preview = deduped_df[["fdc_id", "description", "food_category",
                                 "rule_category", "rule_allergens", "rule_is_organic"]].copy()
    final_preview.columns = ["fdc_id", "description", "food_category",
                             "primary_category", "allergens", "is_organic"]
    final_preview["enriched_by"] = deduped_df["rule_category"].apply(
        lambda x: "Rules" if pd.notna(x) else "Needs LLM")

    st.dataframe(final_preview, use_container_width=True, height=300)

    st.markdown("### Download")
    csv_final = final_preview.to_csv(index=False)
    st.download_button("Download Enriched + Deduped CSV", csv_final,
                       "enriched_deduped_products.csv", "text/csv")

# ===================================================================
# STEP 12: Final Cleaned Data
# ===================================================================
elif st.session_state.step == 12:
    st.markdown("### Final Cleaned & Enriched Dataset")
    st.write("This is the end result of the entire pipeline — deduplicated, normalized, "
             "and enriched with structured attributes.")

    # Rebuild deduped_df if not in session state (user may jump directly here)
    deduped_df = st.session_state.get("deduped_df", None)
    if deduped_df is None:
        # Rebuild from scratch
        uf = UnionFind()
        for idx in raw_df.index:
            uf.find(idx)
        block_data = {}
        for idx in raw_df.index:
            desc = full_normalize(str(raw_df.at[idx, _cfg["name_col"]]) if pd.notna(raw_df.at[idx, _cfg["name_col"]]) else "", _noise_words)
            brand = full_normalize(get_brand_at(raw_df, idx, _cfg), _noise_words)
            block_data.setdefault((desc, brand), []).append(idx)
        for indices in block_data.values():
            if len(indices) < 2:
                continue
            for i in range(len(indices)):
                for j in range(i + 1, len(indices)):
                    a, b = indices[i], indices[j]
                    na = full_normalize(str(raw_df.at[a, _cfg["name_col"]]), _noise_words)
                    ba = full_normalize(get_brand_at(raw_df, a, _cfg), _noise_words)
                    nb = full_normalize(str(raw_df.at[b, _cfg["name_col"]]), _noise_words)
                    bb = full_normalize(get_brand_at(raw_df, b, _cfg), _noise_words)
                    if fuzzy_score(na, ba, nb, bb)["weighted_score"] >= 85:
                        uf.union(a, b)
        clusters = uf.clusters()
        multi_clusters = {k: v for k, v in clusters.items() if len(v) > 1}
        golden_records = []
        for leader, members in multi_clusters.items():
            group_df = raw_df.loc[members]
            best_idx = None
            best_dq = -1
            for m in members:
                dq = compute_dq(raw_df.loc[m], group_df, _cfg)
                if dq > best_dq:
                    best_dq = dq
                    best_idx = m
            golden_records.append(best_idx)
        for leader, members in clusters.items():
            if len(members) == 1:
                golden_records.append(members[0])
        deduped_df = raw_df.loc[golden_records].copy()

    # Apply rule-based enrichment
    CATEGORY_MAP = {
        "Processed Cereal Products": "Breakfast Cereal", "Cereal": "Breakfast Cereal",
        "Hot Cereal": "Breakfast Cereal", "Cold Cereal": "Breakfast Cereal",
        "Non Alcoholic Beverages - Ready to Drink": "Beverage",
        "Non Alcoholic Beverages  Ready to Drink": "Beverage",
        "Soda": "Beverage", "Tea Bags": "Beverage",
        "Energy, Protein & Muscle Recovery Drinks": "Beverage",
        "Snacks": "Snack", "Chips, Pretzels & Snacks": "Snack",
        "Snack, Energy & Granola Bars": "Snack", "Wholesome Snacks": "Snack",
        "Popcorn, Peanuts, Seeds & Related Snacks": "Snack",
        "Cookies & Biscuits": "Bakery", "Biscuits/Cookies": "Bakery",
        "Crackers & Biscotti": "Bakery", "Cakes, Cupcakes, Snack Cakes": "Bakery",
        "Croissants, Sweet Rolls, Muffins & Other Pastries": "Bakery",
        "Breads & Buns": "Bakery", "Dough Based Products / Meals": "Bakery",
        "Candy": "Candy", "Chocolate": "Candy", "Chewing Gum & Mints": "Candy",
        "Sauces/Spreads/Dips/Condiments": "Condiment",
        "Ketchup, Mustard, BBQ & Cheese Sauce": "Condiment",
        "Dips & Salsa": "Condiment", "Syrups & Molasses": "Condiment",
        "Nut & Seed Butters": "Spread", "Butter & Spread": "Spread",
        "Jam, Jelly & Fruit Spreads": "Spread",
        "Cheese": "Dairy", "Yogurt": "Dairy",
        "Ice Cream & Frozen Yogurt": "Frozen Dairy",
        "Prepared Soups": "Prepared Food", "Pasta Dinners": "Prepared Food",
        "Vegetable Based Products / Meals": "Prepared Food", "Pizza": "Prepared Food",
        "Canned & Bottled Beans": "Canned/Preserved", "Canned Vegetables": "Canned/Preserved",
        "Tomatoes": "Canned/Preserved", "Pickles, Olives, Peppers & Relishes": "Canned/Preserved",
        "Frozen Vegetables": "Frozen", "Frozen Prepared Sides": "Frozen",
        "Frozen Fruit & Fruit Juice Concentrates": "Frozen",
        "Pasta by Shape & Type": "Pantry", "Grains/Flour": "Pantry",
        "Flours & Corn Meal": "Pantry",
        "Seasoning Mixes, Salts, Marinades & Tenderizers": "Pantry", "Dry Mixes": "Pantry",
        "Pepperoni, Salami & Cold Cuts": "Meat",
        "Pre-Packaged Fruit & Vegetables": "Produce",
    }
    def clean_description(desc):
        """Clean the raw description: strip sizes, title case, remove noise patterns."""
        text = str(desc).strip()
        # Strip leading sizes: ".75OZ DORITOS NACHO" -> "DORITOS NACHO"
        text = re.sub(r'^\.?\d+(\.\d+)?\s*oz\s+', '', text, flags=re.IGNORECASE)
        # Strip trailing sizes: "HEINZ TOMATO KETCHUP, 20 OZ" -> "HEINZ TOMATO KETCHUP"
        text = re.sub(r'[,\s]+\d+(\.\d+)?\s*(oz|ounces?|liters?|l|ml|mlt|gal|lb|lbs|kg|g|grm|ct|pk|pack)\b.*$', '', text, flags=re.IGNORECASE)
        # Strip trailing short sizes: "Doritos Ranch 2.125z" -> "Doritos Ranch"
        text = re.sub(r'\s+\d+(\.\d+)?\s*z\s*$', '', text, flags=re.IGNORECASE)
        # Clean up: remove duplicate product name patterns like "OREO MUFFINS, OREO"
        text = re.sub(r',\s*\w+$', '', text) if text.count(',') == 1 and len(text.split(',')[-1].strip().split()) == 1 else text
        return text.strip().title()

    final_df = deduped_df.copy()
    final_df["clean_description"] = final_df["description"].apply(clean_description)
    final_df["primary_category"] = final_df["food_category"].map(CATEGORY_MAP).fillna("")
    final_df["allergens"] = final_df["ingredients"].apply(extract_allergens)
    final_df["is_organic"] = final_df["description"].str.lower().str.contains("organic", na=False)
    final_df["canonical_brand"] = final_df["brand_owner"].apply(normalize_company)

    # Sizes: use cluster aggregation from Step 10 session state if available, else per-row extraction
    cluster_sizes_map = st.session_state.get("cluster_sizes", {})
    cluster_serving_map = st.session_state.get("cluster_serving", {})
    final_df["sizes"] = final_df.index.map(
        lambda i: ", ".join(cluster_sizes_map[i]) if i in cluster_sizes_map
        else extract_size(str(raw_df.at[i, "description"]) if i in raw_df.index else "")
    )
    final_df["serving_sizes"] = final_df.index.map(
        lambda i: ", ".join(cluster_serving_map[i]) if i in cluster_serving_map
        else (f"{raw_df.at[i, 'serving_size']} {raw_df.at[i, 'serving_size_unit']}".strip()
              if i in raw_df.index and pd.notna(raw_df.at[i, "serving_size"]) else "")
    )

    # Before vs After metrics
    st.markdown("### Pipeline Summary")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Original Rows", len(raw_df))
    c2.metric("After Dedup", len(final_df))
    c3.metric("Duplicates Removed", len(raw_df) - len(final_df))
    c4.metric("Reduction", f"{(len(raw_df) - len(final_df)) / len(raw_df) * 100:.1f}%")

    st.markdown("### What Each Step Did")
    steps_summary = pd.DataFrame([
        {"Step": "1-2. Load & Identify", "Action": "Found duplicate groups", "Impact": f"{raw_df['description'].value_counts().gt(1).sum()} duplicate groups found"},
        {"Step": "3. Trim Whitespace", "Action": "Removed invisible spaces", "Impact": "Text fields cleaned"},
        {"Step": "4. Lowercase", "Action": "Standardized case", "Impact": "All text comparable"},
        {"Step": "5. Noise Words", "Action": "Removed business suffixes (auto-detected)", "Impact": f"{len(_noise_words)} noise words removed"},
        {"Step": "6. Punctuation", "Action": "Removed special characters", "Impact": "Clean alphanumeric text"},
        {"Step": "7. Regex Sizes", "Action": "Stripped package sizes from names", "Impact": "Hidden duplicates revealed"},
        {"Step": "8. Blocking", "Action": "Grouped candidate duplicates", "Impact": "Avoided O(n^2) comparisons"},
        {"Step": "9. Clustering", "Action": "Union-Find transitive closure", "Impact": "Confirmed duplicate clusters"},
        {"Step": "10. Golden Record", "Action": "DQ score picked best row per cluster", "Impact": f"{len(raw_df) - len(final_df)} rows removed"},
        {"Step": "11. Enrichment", "Action": "Rule-based category + allergen extraction", "Impact": "0 LLM calls needed"},
    ])
    st.dataframe(steps_summary, use_container_width=True)

    st.markdown("### Description Cleaning (sizes removed)")
    desc_changes = final_df[final_df["description"] != final_df["clean_description"]][
        ["fdc_id", "description", "clean_description"]].copy()
    desc_changes.columns = ["fdc_id", "Original (raw)", "Cleaned (sizes removed)"]
    if len(desc_changes) > 0:
        st.write(f"**{len(desc_changes)} descriptions cleaned:**")
        st.dataframe(desc_changes, use_container_width=True)
    else:
        st.write("No descriptions needed size removal.")

    st.markdown("### Final Cleaned Dataset")
    display_cols = ["fdc_id", "clean_description", "canonical_brand", "brand_owner", "brand_name",
                    "primary_category", "allergens", "is_organic", "sizes", "serving_sizes",
                    "food_category", "serving_size", "serving_size_unit", "published_date"]
    available_cols = [c for c in display_cols if c in final_df.columns]
    st.dataframe(final_df[available_cols], use_container_width=True, height=500)

    st.markdown("### Before vs After (side by side)")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Before (raw data)**")
        st.write(f"{len(raw_df)} rows, no categories, no allergens")
        st.dataframe(raw_df[["fdc_id", "description", "brand_owner", "food_category"]].head(15),
                     use_container_width=True)
    with c2:
        st.markdown("**After (cleaned + enriched)**")
        st.write(f"{len(final_df)} rows, with categories and allergens")
        st.dataframe(final_df[["fdc_id", "clean_description", "primary_category", "allergens"]].head(15),
                     use_container_width=True)

    st.markdown("### Download")
    # Build a clean export with best column order
    export_cols = ["fdc_id", "clean_description", "canonical_brand", "brand_owner", "brand_name",
                   "primary_category", "allergens", "is_organic", "sizes", "serving_sizes",
                   "gtin_upc", "ingredients", "food_category",
                   "serving_size", "serving_size_unit", "published_date",
                   "market_country", "data_source", "description"]
    export_df = final_df[[c for c in export_cols if c in final_df.columns]].copy()
    export_df = export_df.rename(columns={"clean_description": "product_name", "description": "original_description"})

    c1, c2 = st.columns(2)
    with c1:
        csv_final = export_df.to_csv(index=False)
        st.download_button("Download Final Cleaned CSV", csv_final,
                           "usda_final_cleaned.csv", "text/csv")
    with c2:
        csv_raw = raw_df.to_csv(index=False)
        st.download_button("Download Original Raw CSV (for comparison)", csv_raw,
                           "usda_raw_original.csv", "text/csv")

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
st.caption(f"Step {st.session_state.step} of {len(STEPS)} | "
           f"Input: {input_label} ({len(raw_df)} rows)")
