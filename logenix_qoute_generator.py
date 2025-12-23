from flask import Flask, request, render_template
import pandas as pd
import os
from datetime import datetime, date

app = Flask(__name__)

EXCEL_FILE = "queries.xlsx"
PRICES_FILE = "prices_updated.xlsx"
SHOW_LIMIT = 4  # show max 4 quote boxes


# -------------------------
# ROUTES (CURRENTLY 3)
# -------------------------

ROUTES = [
    {
        "id": "R1",
        "title": "Route 1",
        "path": "Karachi Port → Chaman Border (Afghanistan/Pakistan) → Torghundi Border (Turkmenistan/Afghanistan) → Ashgabat Port (Turkmenistan).",
        "origin_keywords": ["karachi port", "karachi"],
        "destination_city_keywords": ["ashgabat", "ashgabat port"],
        "destination_country_keywords": ["turkmenistan"],
    },
    {
        "id": "R2",
        "title": "Route 2",
        "path": "Karachi Port → Peshawar → Torkham Border (Afghanistan/Pakistan) → Kabul → Shir Khan Border (Tajikistan/Afghanistan) → Dushanbe (Tajikistan).",
        "origin_keywords": ["karachi port", "karachi"],
        "destination_city_keywords": ["dushanbe", "dushambe"],  # handle spelling variant
        "destination_country_keywords": ["tajikistan"],
    },
    {
        "id": "R3",
        "title": "Route 3",
        "path": "Karachi Port → Peshawar → Torkham Border (Afghanistan/Pakistan) → Kabul → Hairatan Border (Uzbekistan/Afghanistan) → Tashkent → Almaty (Kazakhstan).",
        "origin_keywords": ["karachi port", "karachi"],
        "destination_city_keywords": ["almaty"],
        "destination_country_keywords": ["kazakhstan"],
    },
]


def norm_text(x) -> str:
    if x is None or pd.isna(x):
        return ""
    return str(x).strip().lower()


def route_match_score(origin: str, destination: str, route: dict) -> int:
    """
    Scoring:
      - origin must match (otherwise score = 0)
      - destination city match => +3
      - destination country match => +1
    Best route = highest score.
    """
    o = norm_text(origin)
    d = norm_text(destination)

    origin_ok = any(k in o for k in route.get("origin_keywords", []))
    if not origin_ok:
        return 0

    score = 1  # baseline if origin matched (means we provide from that origin)
    if any(k in d for k in route.get("destination_city_keywords", [])):
        score += 3
    if any(k in d for k in route.get("destination_country_keywords", [])):
        score += 1

    # If destination didn't match at all, treat as not a route match
    # (so we don't show routes just because origin matches)
    if score == 1:
        return 0

    return score


def get_matching_routes(origin: str, destination: str):
    """
    Returns:
      matched_routes: list of route dicts (with score)
      best_route_id: str|None
    """
    scored = []
    for r in ROUTES:
        s = route_match_score(origin, destination, r)
        if s > 0:
            rr = dict(r)
            rr["score"] = s
            scored.append(rr)

    if not scored:
        return [], None

    scored.sort(key=lambda x: x["score"], reverse=True)
    best_route_id = scored[0]["id"]
    return scored, best_route_id


# -------------------------
# DROPDOWN / AUTOCOMPLETE LISTS
# -------------------------

COUNTRIES = [
    "Pakistan", "United Arab Emirates", "Saudi Arabia", "Qatar", "Oman",
    "Kuwait", "Bahrain", "Turkey", "China", "India", "Afghanistan",
    "Uzbekistan", "Kazakhstan", "Turkmenistan", "Kyrgyzstan", "Tajikistan",
    "USA", "UK", "Germany", "France", "Italy", "Spain", "Netherlands",
    "Malaysia", "Indonesia", "Singapore", "Japan", "South Korea", "Australia",

    "Karachi Port",
    "Ladkrabang, Bangkok.",
    "Aqaba Port",
    "Shanghai/Taicang/Ningbo Port",
    "Malaysia",
    "Mersin Port",
    "Abu-Dhabi",
    "Jabel Ali Port",
    "India",
    "Bandar Abbas Port",
    "Nava Sheva Port",
    "Jizzakh",
    "Yokohama Port",
    "Bahrain Port",
    "Qingdao port",
    "Dekhkanabad",
    "Ras Al Khaimah",
    "Shanghai",
    "Taicang",
    "Shanghai/Qingdao Port",
    "Daegu",
    "Nhava Sheva port/Mundra port",
    "Muscat",
    "Taijin Port",
    "Abu Dhabi",
    "Conrad, USA",
    "Dubai",
    "Germany",
    "Bandar Abbas",
    "Klang port",
    "Jebel Ali",
    "UAE",
    "ICD Ludhiana",
    "Korea",
    "Jebel ALi Port",
    "Shenzhen",
    "Ningbo",
    "Yiwu",
    "Czech Republic",
    "Fujairah",
    "Vizag (Visakhapatnam) Port",
    "Yiwu City",
    "Yiwu City/Ningbo",
    "Nhava Sheva/Mundra Port",
    "Klaipeda Port",
    "Qingdao/LYG port",
    "Jebel Ali/Bandar Abbas Port Port",
    "Tashkent",
    "Aveiro",
    "Islam Qila/Herat",
    "Islam Qila",
    "Herat",
    "Chennai Port",
    "Karachi/Bandar Abbas Port",
    "Chittagong port",
    "Bandar Abbas Port/Herat Custom",
    "Herat customs",
    "LYG/Qingdao Port",
    "Tbilisi Port",
    "Karachi/Mersin/Poti Port",
    "Towrgondi",
    "Poti Port",
    "Karachi/Chittagong/Nava Sheva Port",
    "Bandar Abbas/LYG/Qingdao port",
    "Dar es Salaam/Mombasa port",
    "Mombasa port",
    "Belfast Port",
    "Rotterdam",
    "Umm Qasr/Dammam/Jebel Ali /Latakia/Beirut/Aqaba Port"
]

BASE_COMMODITIES = [
    "Food Item",
    "Pharmaceutical Products",
    "Automobile Parts",
    "Solar Modules",
    "CT Scan Machine",
    "General Cargo",
    "Paper Product",
    "Tea",
    "Cement",
    "Medicines",
    "Buffalo Meat",
    "Basalt Product",
    "Sausages",
    "Agrochemical",
    "Electronic Items",
    "Calcium Hypochlorite 65%",
    "Potassium Chloride",
    "Spare Parts",
    "Tea & Animal Nurtition Feed",
    "Equipments",
    "Potassium Nitrate",
    "Technical Salt",
    "Rice",
    "Machinery",
    "Chemicals",
    "Herbal Medicins",
    "Hardware",
    "Tires",
    "Used Textile Machinery",
    "Soap Noodles",
    "Vehicles",
    "Lubricants",
    "Spandex Yarn",
    "Medical Equipment",
    "Empty Container",
    "Liquid OIl",
    "FIber Cabels",
    "Electrical Equipment",
    "ALu ALu Foil",
    "Medical Diluents and Machines",
    "Veterinary / Livestock Farming Equipment",
    "Multipurpose Tents",
    "Composite Rod",
    "Armored Vehicle",
    "Steel Bloom",
    "Battery",
    "Surgical Disposable Item"
]


# -------------------------
# EXCEL HELPERS (PRICES)
# -------------------------

def safe_str(x) -> str:
    if x is None or pd.isna(x):
        return "-"
    s = str(x).strip()
    return s if s else "-"


def fmt_any(x):
    if x is None or pd.isna(x):
        return "-"
    if isinstance(x, (datetime, pd.Timestamp)):
        return x.strftime("%d-%b-%Y")
    s = str(x).strip()
    return s if s else "-"


def parse_date_any(v):
    if v is None or pd.isna(v):
        return None
    if isinstance(v, (datetime, pd.Timestamp)):
        return v.date()
    s = str(v).strip()
    if not s:
        return None
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        return None
    return dt.date()


def parse_price_to_float(v):
    if v is None or pd.isna(v):
        return None
    s = str(v).strip()
    if not s:
        return None
    s = s.replace(",", "").replace("$", "").strip()
    try:
        return float(s)
    except Exception:
        return None


def decide_ft_from_container(container_type: str):
    ct = norm_text(container_type)
    if "20" in ct:
        return "20ft"
    if "40" in ct:
        return "40ft"
    return "40ft"


def flatten_multiindex_columns(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df.columns, pd.MultiIndex):
        return df

    new_cols = []
    for a, b in df.columns:
        a = "" if (a is None or str(a).startswith("Unnamed")) else str(a).strip()
        b = "" if (b is None or str(b).startswith("Unnamed")) else str(b).strip()
        if a and b:
            new_cols.append(f"{a} {b}".strip())
        elif b:
            new_cols.append(b.strip())
        else:
            new_cols.append(a.strip())
    df.columns = new_cols
    return df


def looks_like_bad_header(df: pd.DataFrame) -> bool:
    cols = [str(c) for c in df.columns]
    suspicious_prefix = any(c.lower().startswith("pol ") or c.lower().startswith("pod ") for c in cols)
    suspicious_time = any("00:00:00" in c for c in cols)
    return suspicious_prefix or suspicious_time


def load_prices_df():
    if not os.path.exists(PRICES_FILE):
        return None

    # Prefer single header for updated sheet
    try:
        df = pd.read_excel(PRICES_FILE)
        if df is not None and not df.empty:
            if "POL" in df.columns and "POD" in df.columns and "Commodity" in df.columns:
                return df
    except Exception:
        pass

    # Fallback for older formats
    try:
        df = pd.read_excel(PRICES_FILE, header=[0, 1])
        df = flatten_multiindex_columns(df)
        if looks_like_bad_header(df):
            return None
        return df
    except Exception:
        return None


def find_col(df: pd.DataFrame, needle: str):
    n = needle.strip().lower()
    for c in df.columns:
        if n in str(c).lower():
            return c
    return None


def pick_ocean_freight_column(df: pd.DataFrame, ft: str):
    candidates = [
        f"Ocean Freight ({ft})",
        f"Ocean Freight {ft}",
        f"Ocean Freight/{ft}",
        f"Ocean Freight {ft.replace('ft','')}",
    ]
    for cand in candidates:
        c = find_col(df, cand)
        if c:
            return c
    return find_col(df, "Ocean Freight")


def pick_exworks_column(df: pd.DataFrame, ft: str):
    candidates = [
        f"Ex-works ({ft})",
        f"Ex works ({ft})",
        f"Ex-works {ft}",
        f"Ex works {ft}",
        "Ex-works",
        "Ex works"
    ]
    for cand in candidates:
        c = find_col(df, cand)
        if c:
            return c
    return None


def get_commodities():
    commodities = list(BASE_COMMODITIES)
    if os.path.exists(EXCEL_FILE):
        try:
            df = pd.read_excel(EXCEL_FILE)
            if "commodity" in df.columns:
                existing = df["commodity"].dropna().astype(str).str.strip().unique()
                for c in existing:
                    if c and c not in commodities:
                        commodities.append(c)
        except Exception:
            pass
    return commodities


def save_to_excel(record):
    df_new = pd.DataFrame([record])
    if os.path.exists(EXCEL_FILE):
        df_existing = pd.read_excel(EXCEL_FILE)
        df_final = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_final = df_new
    df_final.to_excel(EXCEL_FILE, index=False)


def get_strict_quotes(origin, destination, commodity, container_type, limit=4):
    df = load_prices_df()
    if df is None or df.empty:
        return [], None, "Could not load prices_updated.xlsx properly. Please confirm the file exists and headers are correct."

    pol_col = "POL" if "POL" in df.columns else find_col(df, "POL")
    pod_col = "POD" if "POD" in df.columns else find_col(df, "POD")
    com_col = "Commodity" if "Commodity" in df.columns else find_col(df, "Commodity")
    validity_col = "Rates Validity" if "Rates Validity" in df.columns else find_col(df, "Rates Validity")

    if not pol_col or not pod_col or not com_col or not validity_col:
        return [], None, "Missing required columns in prices_updated.xlsx (need POL, POD, Commodity, Rates Validity)."

    user_pol = norm_text(origin)
    user_pod = norm_text(destination)
    user_com = norm_text(commodity)

    df_match = df[
        (df[pol_col].astype(str).str.strip().str.lower() == user_pol) &
        (df[pod_col].astype(str).str.strip().str.lower() == user_pod) &
        (df[com_col].astype(str).str.strip().str.lower() == user_com)
    ].copy()

    if df_match.empty:
        return [], None, None

    today = date.today()
    df_match["_validity_date"] = df_match[validity_col].apply(parse_date_any)
    df_match["_is_valid"] = df_match["_validity_date"].apply(lambda d: (d is not None and d >= today))

    ft = decide_ft_from_container(container_type)
    ocean_col = pick_ocean_freight_column(df_match, ft)
    exworks_col = pick_exworks_column(df_match, ft)

    if ocean_col and ocean_col in df_match.columns:
        df_match["_ocean_price"] = df_match[ocean_col].apply(parse_price_to_float)
    else:
        df_match["_ocean_price"] = None

    df_match["_valid_sort"] = df_match["_is_valid"].apply(lambda x: 1 if x else 0)
    df_match = df_match.sort_values(
        by=["_valid_sort", "_validity_date"],
        ascending=[False, False],
        na_position="last"
    ).head(limit)

    best_idx = None
    valid_rows = df_match[(df_match["_is_valid"] == True) & (df_match["_ocean_price"].notna())]
    if not valid_rows.empty:
        best_idx = valid_rows.sort_values("_ocean_price", ascending=True).index[0]

    display_cols = [c for c in df_match.columns if not str(c).startswith("_")]

    results = []
    for idx, row in df_match.iterrows():
        vd = row.get("_validity_date")
        if vd is None:
            validity_label = "Validity: Unknown"
            validity_kind = "unknown"
        elif row.get("_is_valid"):
            validity_label = f"Validity: Valid (until {vd.strftime('%d/%m/%Y')})"
            validity_kind = "valid"
        else:
            validity_label = f"Validity: Expired (until {vd.strftime('%d/%m/%Y')})"
            validity_kind = "expired"

        results.append({
            "is_best": (best_idx is not None and idx == best_idx),
            "title": f"{safe_str(row.get(pol_col))} ➜ {safe_str(row.get(pod_col))}",
            "validity_label": validity_label,
            "validity_kind": validity_kind,
            "ocean_col": ocean_col or "Ocean Freight",
            "ocean_val": fmt_any(row.get(ocean_col)) if ocean_col else "-",
            "exworks_col": exworks_col,
            "exworks_val": fmt_any(row.get(exworks_col)) if exworks_col else "-",
            "fields": [(col, fmt_any(row.get(col))) for col in display_cols]
        })

    best_text = None
    if best_idx is not None:
        raw_best_price = df_match.at[best_idx, "_ocean_price"]
        best_price_num = parse_price_to_float(raw_best_price)
        if best_price_num is not None:
            best_text = f"Best Option available (valid rate + lowest {ft} Ocean Freight): {best_price_num:.2f}"

    return results, best_text, None


# -------------------------
# ROUTES
# -------------------------

@app.route("/", methods=["GET"])
def index():
    return render_template(
        "form.html",
        countries=COUNTRIES,
        commodities=get_commodities(),
        submitted=False,
        data=None,
        routes=[],
        best_route_id=None,
        rates=[],
        best_text=None,
        error_msg=None
    )


@app.route("/submit", methods=["POST"])
def submit():
    data = {
        "quote_id": f"QUOTE-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        "company_name": request.form["company_name"],
        "shipping_from": request.form["shipping_from"],
        "destination": request.form["destination"],
        "commodity": request.form["commodity"],
        "weight_tons": request.form["weight_tons"],
        "container_type": request.form["container_type"],
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }

    save_to_excel(data)

    # Routes suggestion
    matched_routes, best_route_id = get_matching_routes(
        origin=data["shipping_from"],
        destination=data["destination"]
    )

    # Quotes
    rates, best_text, error_msg = get_strict_quotes(
        origin=data["shipping_from"],
        destination=data["destination"],
        commodity=data["commodity"],
        container_type=data["container_type"],
        limit=SHOW_LIMIT
    )

    return render_template(
        "form.html",
        countries=COUNTRIES,
        commodities=get_commodities(),
        submitted=True,
        data=data,
        routes=matched_routes,
        best_route_id=best_route_id,
        rates=rates,
        best_text=best_text,
        error_msg=error_msg
    )


if __name__ == "__main__":
    app.run(debug=True)
