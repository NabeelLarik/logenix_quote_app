from flask import Flask, request, render_template
import pandas as pd
import os
from datetime import datetime, date, timedelta

app = Flask(__name__)

EXCEL_FILE = "queries.xlsx"
PRICES_FILE = "prices_updated.xlsx"

# Recently expired window (days). Change if you want (e.g., 14).
RECENTLY_EXPIRED_DAYS = 7

# Locations / countries list for dropdown + autocomplete
COUNTRIES = [
    # Base countries
    "Pakistan", "United Arab Emirates", "Saudi Arabia", "Qatar", "Oman",
    "Kuwait", "Bahrain", "Turkey", "China", "India", "Afghanistan",
    "Uzbekistan", "Kazakhstan", "Turkmenistan", "Kyrgyzstan", "Tajikistan",
    "USA", "UK", "Germany", "France", "Italy", "Spain", "Netherlands",
    "Malaysia", "Indonesia", "Singapore", "Japan", "South Korea", "Australia",

    # Ports / specific locations
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
    "Karachi Port ",
    "Fujairah",
    "Dubai ",
    "Vizag (Visakhapatnam) Port",
    "Yiwu City",
    "Yiwu City/Ningbo",
    "Nhava Sheva/Mundra Port",
    "Klaipeda Port",
    "Qingdao/LYG port",
    "Jebel Ali/Bandar Abbas Port Port",
    "Tashkent",
    "Mersin Port",
    "Aveiro",
    "Islam Qila/Herat",
    "Islam Qila",
    "Herat",
    "Chennai Port",
    "bandar Abbas Port ",
    "Karachi/Bandar Abbas Port",
    "Chittagong port",
    "Tashkent ",
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

# Base commodity list (user-provided)
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
# Helpers
# -------------------------

def norm_text(x) -> str:
    if x is None:
        return ""
    return str(x).strip().lower()


def safe_str(x) -> str:
    if x is None or pd.isna(x):
        return "-"
    s = str(x).strip()
    return s if s else "-"


def fmt_any(value):
    """Pretty formatting for values shown in UI."""
    if value is None or pd.isna(value):
        return "-"
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.strftime("%d-%b-%y")  # 04-Nov-25
    # if date-like string keep as-is
    return str(value).strip() if str(value).strip() else "-"


def parse_validity_to_date(v):
    """
    Try to parse Rates Validity into a python date.
    Handles datetime, pandas Timestamp, and strings.
    """
    if v is None or pd.isna(v):
        return None
    if isinstance(v, (datetime, pd.Timestamp)):
        return v.date()

    s = str(v).strip()
    if not s or s == "-" or s.lower() == "nan":
        return None

    # try pandas to_datetime on strings
    try:
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            return None
        return dt.date()
    except Exception:
        return None


def parse_price_to_float(v):
    """Convert '$850.00' / '850' to float for best-option comparisons."""
    if v is None or pd.isna(v):
        return None
    s = str(v).strip()
    if not s or s == "-" or s.lower() == "nan":
        return None
    s = s.replace(",", "").replace("$", "").strip()
    try:
        return float(s)
    except Exception:
        return None


def flatten_columns(cols):
    """
    Input: MultiIndex columns like ('Ocean Freight','20ft')
    Output: 'Ocean Freight 20ft'
    """
    flat = []
    for a, b in cols:
        a = "" if (a is None or str(a).startswith("Unnamed")) else str(a).strip()
        b = "" if (b is None or str(b).startswith("Unnamed")) else str(b).strip()
        if a and b:
            flat.append(f"{a} {b}".strip())
        elif b:
            flat.append(b.strip())
        else:
            flat.append(a.strip())
    return flat


def load_prices_df():
    """
    prices_updated.xlsx has 2 header rows -> read header=[0,1]
    and flatten.
    """
    if not os.path.exists(PRICES_FILE):
        return None

    df = pd.read_excel(PRICES_FILE, header=[0, 1])
    df.columns = flatten_columns(df.columns)
    return df


def find_col(df, needle: str):
    """
    Find a column containing needle (case-insensitive).
    Returns column name or None.
    """
    n = needle.strip().lower()
    for c in df.columns:
        if n in str(c).lower():
            return c
    return None


def get_commodities():
    """
    Base commodity list + unique commodities from queries.xlsx
    """
    commodities = list(BASE_COMMODITIES)

    if os.path.exists(EXCEL_FILE):
        try:
            df = pd.read_excel(EXCEL_FILE)
            if "commodity" in df.columns:
                existing = (
                    df["commodity"]
                    .dropna()
                    .astype(str)
                    .str.strip()
                    .unique()
                )
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


def decide_ft_from_container(container_type: str):
    """
    Decide whether to compare Ocean Freight 20ft or 40ft.
    Defaults to 40ft if unclear.
    """
    ct = norm_text(container_type)
    if "20" in ct:
        return "20ft"
    if "40" in ct:
        return "40ft"
    return "40ft"


def get_smart_rates(origin, destination, commodity, container_type, limit=4):
    """
    Smart matching with VALIDITY as top priority:
      - Filter rows where Rates Validity is:
          valid today or future, OR expired within RECENTLY_EXPIRED_DAYS.
      - Then sort primarily by:
          1) validity_status (valid first)
          2) validity_date (later expiry is better)
          3) match score (POD, Commodity, Container)
          4) price (lowest is better)
    Show all columns in UI.

    Returns: (rates_list, best_text)
    """
    df = load_prices_df()
    if df is None or df.empty:
        return [], None

    # Columns (sheet uses "Ocean Freight Rates Validity" etc.)
    pol_col = find_col(df, "POL") or "POL"
    pod_col = find_col(df, "POD") or "POD"
    com_col = find_col(df, "Commodity")  # likely "Ocean Freight Commodity"
    ct_col = find_col(df, "Type of Container")  # likely "Ocean Freight Type of Container"
    validity_col = find_col(df, "Rates Validity")  # likely "Ocean Freight Rates Validity"
    of20_col = find_col(df, "Ocean Freight 20ft") or "Ocean Freight 20ft"
    of40_col = find_col(df, "Ocean Freight 40ft") or "Ocean Freight 40ft"

    if pol_col not in df.columns:
        return [], None

    user_pol = norm_text(origin)
    user_pod = norm_text(destination)
    user_com = norm_text(commodity)
    user_ct = norm_text(container_type)

    # Required filter: POL match
    pol_series = df[pol_col].astype(str).str.strip().str.lower()
    base = df[pol_series == user_pol].copy()

    if base.empty:
        return [], None

    # Validity filtering: valid today/future OR recently expired
    today = date.today()
    cutoff = today - timedelta(days=RECENTLY_EXPIRED_DAYS)

    if validity_col in base.columns:
        base["_validity_date"] = base[validity_col].apply(parse_validity_to_date)
    else:
        base["_validity_date"] = None

    # Keep only valid or recently expired
    def keep_row(vd):
        if vd is None:
            return False
        return vd >= cutoff

    base = base[base["_validity_date"].apply(keep_row)]
    if base.empty:
        return [], None

    # validity_status: 2 = valid (>=today), 1 = recently expired (cutoff..today-1)
    def validity_status(vd):
        if vd is None:
            return 0
        if vd >= today:
            return 2
        if vd >= cutoff:
            return 1
        return 0

    base["_validity_status"] = base["_validity_date"].apply(validity_status)

    # Match scoring (secondary)
    def score_row(row):
        score = 0
        if pod_col in base.columns and user_pod and norm_text(row.get(pod_col, "")) == user_pod:
            score += 3
        if com_col in base.columns and user_com and norm_text(row.get(com_col, "")) == user_com:
            score += 2
        if ct_col in base.columns and user_ct and norm_text(row.get(ct_col, "")) == user_ct:
            score += 1
        return score

    base["_match_score"] = base.apply(score_row, axis=1)

    # Choose price column for "Best Option"
    ft = decide_ft_from_container(container_type)
    chosen_price_col = of20_col if ft == "20ft" else of40_col

    if chosen_price_col in base.columns:
        base["_price_num"] = base[chosen_price_col].apply(parse_price_to_float)
    else:
        base["_price_num"] = None

    base["_has_price"] = base["_price_num"].apply(lambda x: 0 if x is None else 1)

    # SORT: validity first (status + date), then matching, then price
    base_sorted = base.sort_values(
        by=["_validity_status", "_validity_date", "_match_score", "_has_price", "_price_num"],
        ascending=[False, False, False, False, True],
        na_position="last"
    ).head(limit)

    # Determine best option among shown rows: lowest price among valid first, else among recent-expired
    best_idx = None
    best_price = None

    shown = base_sorted.copy()

    # Prefer valid rows for best option
    valid_rows = shown[shown["_validity_status"] == 2].dropna(subset=["_price_num"])
    recent_rows = shown[shown["_validity_status"] == 1].dropna(subset=["_price_num"])

    if not valid_rows.empty:
        best_row = valid_rows.sort_values("_price_num", ascending=True).iloc[0]
        best_idx = best_row.name
        best_price = best_row["_price_num"]
    elif not recent_rows.empty:
        best_row = recent_rows.sort_values("_price_num", ascending=True).iloc[0]
        best_idx = best_row.name
        best_price = best_row["_price_num"]

    # Build results: show ALL columns in each card
    results = []
    all_cols = [c for c in df.columns]  # original flattened headers

    for idx, row in shown.iterrows():
        # prepare label for validity
        vd = row.get("_validity_date", None)
        vstat = row.get("_validity_status", 0)
        if vd is None:
            validity_label = "Unknown"
        elif vstat == 2:
            validity_label = f"Valid (until {vd.strftime('%d/%m/%Y')})"
        else:
            validity_label = f"Recently expired (until {vd.strftime('%d/%m/%Y')})"

        fields = []
        for col in all_cols:
            fields.append((col, fmt_any(row.get(col, None))))

        results.append({
            "is_best": (idx == best_idx),
            "title": f"{safe_str(row.get(pol_col))} ➜ {safe_str(row.get(pod_col))}",
            "validity_label": validity_label,
            "chosen_ft": ft,
            "chosen_price_col": chosen_price_col,
            "fields": fields
        })

    best_text = None
    if best_price is not None:
        best_text = f"Best Option selected (lowest {ft} Ocean Freight based on validity-prioritized results): {best_price:.2f}"

    return results, best_text


# -------------------------
# Routes
# -------------------------

@app.route("/", methods=["GET"])
def index():
    return render_template(
        "form.html",
        countries=COUNTRIES,
        commodities=get_commodities(),
        submitted=False,
        data=None,
        rates=[],
        best_text=None
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

    rates, best_text = get_smart_rates(
        origin=data["shipping_from"],
        destination=data["destination"],
        commodity=data["commodity"],
        container_type=data["container_type"],
        limit=4
    )

    return render_template(
        "form.html",
        countries=COUNTRIES,
        commodities=get_commodities(),
        submitted=True,
        data=data,
        rates=rates,
        best_text=best_text
    )


if __name__ == "__main__":
    app.run(debug=True)