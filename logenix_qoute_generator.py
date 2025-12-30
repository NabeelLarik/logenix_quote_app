from flask import Flask, request, render_template
import pandas as pd
import os
import re
from datetime import datetime, date

app = Flask(__name__)

EXCEL_FILE = "queries.xlsx"
PRICES_FILE = "prices_updated.xlsx"
SHOW_LIMIT = 4  # max 4 quote boxes


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
        "destination_city_keywords": ["dushanbe", "dushambe"],
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
    "Almaty",
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
# UTILS
# -------------------------
def norm_text(x) -> str:
    if x is None or pd.isna(x):
        return ""
    return str(x).strip().lower()


def canon(s: str) -> str:
    if s is None:
        return ""
    s = str(s).replace("\u00A0", " ")
    s = s.replace("–", "-").replace("—", "-")
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def fmt_money(v):
    try:
        return f"${float(v):,.2f}"
    except Exception:
        return None


def fmt_date_like(x):
    if x is None or pd.isna(x):
        return None
    if isinstance(x, (datetime, pd.Timestamp)):
        return x.strftime("%d-%b-%Y")
    s = str(x).strip()
    if not s:
        return None
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        return None
    return dt.strftime("%d-%b-%Y")


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
    """
    Parses numbers from:
      - numeric cells
      - '$850.00', '850', 'USD 850', '850 approx'
    """
    if v is None or pd.isna(v):
        return None

    if isinstance(v, (int, float)) and not isinstance(v, bool):
        try:
            return float(v)
        except Exception:
            return None

    s = str(v).strip()
    if not s:
        return None

    s = s.replace("\u00A0", " ").replace(",", "").replace("$", "").strip()
    m = re.search(r"(-?\d+(\.\d+)?)", s)
    if not m:
        return None

    try:
        return float(m.group(1))
    except Exception:
        return None


# -------------------------
# ROUTES MATCHING
# -------------------------
def route_match_score(origin: str, destination: str, route: dict) -> int:
    o = norm_text(origin)
    d = norm_text(destination)

    origin_ok = any(k in o for k in route.get("origin_keywords", []))
    if not origin_ok:
        return 0

    score = 1
    if any(k in d for k in route.get("destination_city_keywords", [])):
        score += 3
    if any(k in d for k in route.get("destination_country_keywords", [])):
        score += 1

    if score == 1:
        return 0
    return score


def get_matching_routes(origin: str, destination: str):
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
    return scored, scored[0]["id"]


# -------------------------
# EXCEL HELPERS
# -------------------------
def load_prices_df():
    if not os.path.exists(PRICES_FILE):
        return None
    try:
        return pd.read_excel(PRICES_FILE)
    except Exception:
        return None


def save_to_excel(record):
    df_new = pd.DataFrame([record])
    if os.path.exists(EXCEL_FILE):
        df_existing = pd.read_excel(EXCEL_FILE)
        df_final = pd.concat([df_existing, df_new], ignore_index=True)
    else:
        df_final = df_new
    df_final.to_excel(EXCEL_FILE, index=False)


def get_commodities():
    """
    Base list + new commodities typed by users (read from queries.xlsx).
    """
    commodities = list(BASE_COMMODITIES)

    if os.path.exists(EXCEL_FILE):
        try:
            df = pd.read_excel(EXCEL_FILE)
            com_col = next((c for c in df.columns if c.lower() == "commodity"), None)
            if com_col:
                existing = df[com_col].dropna().astype(str).str.strip().unique()
                for c in existing:
                    if c and c not in commodities:
                        commodities.append(c)
        except Exception:
            pass

    return commodities


# -------------------------
# GRAND TOTAL LOGIC
# -------------------------
def is_charges_column(col_name: str) -> bool:
    return canon(col_name).endswith("_charges")


def compute_grand_total(row: pd.Series, columns: list[str]):
    total = 0.0
    found_any = False
    for col in columns:
        if not is_charges_column(col):
            continue
        num = parse_price_to_float(row.get(col))
        if num is None:
            continue
        total += num
        found_any = True
    return total, found_any


def compute_grand_totals_for_df(df: pd.DataFrame, display_cols: list[str]):
    """
    ✅ FIX for Pylance: no df.at[idx, col] per-row assignment.
    Returns two lists aligned with df.index:
      - totals (float)
      - has_any (bool)
    """
    totals: list[float] = []
    has_any: list[bool] = []
    for _, row in df.iterrows():
        t, ok = compute_grand_total(row, display_cols)
        totals.append(float(t))
        has_any.append(bool(ok))
    return totals, has_any


# -------------------------
# QUOTE SEARCH (STRICT + VALIDITY PRIORITY)
# -------------------------
def get_strict_quotes(origin, destination, commodity, limit=4):
    """
    Priority:
      1) Rates Validity (valid first, then newest)
      2) POL exact match (case-insensitive)
      3) POD exact match
      4) Commodity exact match
    """
    df = load_prices_df()
    if df is None or df.empty:
        return [], None, "Could not load prices_updated.xlsx properly. Please confirm the file exists and headers are correct."

    required = ["POL", "POD", "Commodity", "Rates Validity"]
    for r in required:
        if r not in df.columns:
            return [], None, f"Missing required column in prices_updated.xlsx: {r}"

    user_pol = norm_text(origin)
    user_pod = norm_text(destination)
    user_com = norm_text(commodity)

    df_match = df[
        (df["POL"].astype(str).str.strip().str.lower() == user_pol) &
        (df["POD"].astype(str).str.strip().str.lower() == user_pod) &
        (df["Commodity"].astype(str).str.strip().str.lower() == user_com)
    ].copy()

    if df_match.empty:
        return [], None, None

    today = date.today()
    df_match["_validity_date"] = df_match["Rates Validity"].apply(parse_date_any)
    df_match["_is_valid"] = df_match["_validity_date"].apply(lambda d: (d is not None and d >= today))
    df_match["_valid_sort"] = df_match["_is_valid"].apply(lambda x: 1 if x else 0)

    # Sort by validity then latest date
    df_match = df_match.sort_values(
        by=["_valid_sort", "_validity_date"],
        ascending=[False, False],
        na_position="last"
    ).head(limit)

    # Best option: among valid rows choose lowest Grand Total (if any total exists), else first valid, else first
    display_cols = [c for c in df_match.columns if not str(c).startswith("_")]

    totals, has_any = compute_grand_totals_for_df(df_match, display_cols)
    df_match["_grand_total_num"] = totals
    df_match["_grand_total_has"] = has_any

    valid_rows = df_match[df_match["_is_valid"] == True]
    if not valid_rows.empty:
        valid_with_total = valid_rows[valid_rows["_grand_total_has"] == True]
        if not valid_with_total.empty:
            best_idx = valid_with_total.sort_values("_grand_total_num", ascending=True).index[0]
        else:
            best_idx = valid_rows.index[0]
    else:
        best_idx = df_match.index[0]

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

        fields = []
        for col in display_cols:
            raw = row.get(col)

            # Empty -> N/A in red in UI
            if raw is None or pd.isna(raw) or str(raw).strip() == "":
                fields.append({"key": str(col), "val": None, "is_na": True, "is_grand_total": False})
                continue

            # Date formatting
            if "date" in canon(col) or "validity" in canon(col):
                d = fmt_date_like(raw)
                fields.append({"key": str(col), "val": d if d else None, "is_na": (d is None), "is_grand_total": False})
                continue

            # Charges formatting
            if is_charges_column(col):
                num = parse_price_to_float(raw)
                if num is not None:
                    fields.append({"key": str(col), "val": fmt_money(num), "is_na": False, "is_grand_total": False})
                else:
                    fields.append({"key": str(col), "val": str(raw).strip(), "is_na": False, "is_grand_total": False})
                continue

            fields.append({"key": str(col), "val": str(raw).strip(), "is_na": False, "is_grand_total": False})

        # Add Grand Total as LAST field so it always shows
        total_num, found_any = compute_grand_total(row, display_cols)
        grand_total_str = fmt_money(total_num) if found_any else "N/A"

        fields.append({
            "key": "Grand Total",
            "val": grand_total_str,
            "is_na": (grand_total_str == "N/A"),
            "is_grand_total": True
        })

        results.append({
            "is_best": (idx == best_idx),
            "title": f"{str(row.get('POL')).strip()} ➜ {str(row.get('POD')).strip()}",
            "validity_label": validity_label,
            "validity_kind": validity_kind,
            "fields": fields
        })

    best_text = "Best Option available based on rate validity and match."
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

    matched_routes, best_route_id = get_matching_routes(
        origin=data["shipping_from"],
        destination=data["destination"]
    )

    rates, best_text, error_msg = get_strict_quotes(
        origin=data["shipping_from"],
        destination=data["destination"],
        commodity=data["commodity"],
        limit=SHOW_LIMIT
    )

    # Form fields will be empty after submit (no values are re-injected into inputs)
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