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
# BASIC UTILS
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


def safe_str(x) -> str:
    if x is None or pd.isna(x):
        return ""
    return str(x).strip()


def fmt_date_like(x):
    if x is None or pd.isna(x):
        return None
    if isinstance(x, (datetime, pd.Timestamp)):
        return x.strftime("%d-%b-%Y")
    # try parse date-like strings
    s = str(x).strip()
    if not s:
        return None
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        return None
    return dt.strftime("%d-%b-%Y")


def fmt_money(v):
    if v is None:
        return None
    try:
        return f"${float(v):,.2f}"
    except Exception:
        return None


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
    Strong numeric parser:
      - numeric cells
      - '$850.00', 'USD 850', '850 (approx)', etc
      - extracts FIRST numeric pattern
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
# ROUTE MATCHING
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
# DROPDOWNS
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
# PRICES FILE LOADING
# -------------------------

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

    try:
        df = pd.read_excel(PRICES_FILE)
        if df is not None and not df.empty:
            return df
    except Exception:
        pass

    try:
        df = pd.read_excel(PRICES_FILE, header=[0, 1])
        df = flatten_multiindex_columns(df)
        if looks_like_bad_header(df):
            return None
        return df
    except Exception:
        return None


def find_col(df: pd.DataFrame, needle: str):
    n = canon(needle)
    for c in df.columns:
        if n in canon(c):
            return c
    return None


def pick_ocean_freight_column(df: pd.DataFrame, ft: str):
    candidates = [
        f"Ocean Freight ({ft})",
        f"Ocean Freight {ft}",
        f"Ocean Freight/{ft}",
        f"Ocean Freight - {ft}",
        f"Ocean Freight {ft.replace('ft','')}",
        f"Ocean Freight ({ft.replace('ft','')})",
    ]
    for cand in candidates:
        c = find_col(df, cand)
        if c:
            return c
    return None


def pick_exworks_column(df: pd.DataFrame, ft: str):
    candidates = [
        f"Ex-works ({ft})",
        f"Ex works ({ft})",
        f"Ex-works {ft}",
        f"Ex works {ft}",
        f"Exworks ({ft})",
        f"Exworks {ft}",
    ]
    for cand in candidates:
        c = find_col(df, cand)
        if c:
            return c
    return None


def pick_switch_bl_column(df: pd.DataFrame):
    candidates = [
        "Switch BL",
        "Switch B/L",
        "Switch BL Cost",
        "Switch B/L Cost",
        "Switch Bill",
        "Switch Bill Cost",
    ]
    for cand in candidates:
        c = find_col(df, cand)
        if c:
            return c

    for c in df.columns:
        cc = canon(c)
        if "switch" in cc and ("bl" in cc or "b/l" in cc):
            return c
    return None


def decide_ft_from_container(container_type: str):
    ct = norm_text(container_type)
    if "20" in ct:
        return "20ft"
    if "40" in ct:
        return "40ft"
    return "40ft"


# -------------------------
# COMMODITIES (BASE + FROM queries.xlsx)
# -------------------------

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


# -------------------------
# COST COLUMN DETECTION (for Grand Total)
# -------------------------

COST_KEYWORDS = [
    "charge", "charges",
    "price", "purchase",
    "freight",
    "ex-works", "ex works", "exworks",
    "switch bl", "switch b/l",
    "insurance", "guarantee",
    "custom", "clearance",
    "scanning", "examination",
    "tax", "cess",
    "wharfage", "storage",
    "profit",
    "bonded",
    "manifestation",
    "soc",
    "nlc",
    "overweight",
    "truck", "trucking", "halting",
    "miscellaneous",
    "other_origin",
]

EXCLUDE_COST_EXACT = {
    "weight",  # NOT a cost
}
# anything containing 'weight' but NOT 'overweight' should be excluded
def is_weight_non_cost(col_name: str) -> bool:
    c = canon(col_name)
    if c in EXCLUDE_COST_EXACT:
        return True
    if "weight" in c and "overweight" not in c:
        return True
    return False


def is_cost_column(col_name: str) -> bool:
    c = canon(col_name)
    if is_weight_non_cost(col_name):
        return False
    # also avoid common non-cost ids/dates
    if any(k in c for k in ["date", "validity", "customer", "vendor", "carrier", "pol", "pod", "commodity", "inquiry", "s.no", "sno", "serial", "route"]):
        # carrier is not a cost, keep it excluded from total
        return False
    return any(k in c for k in COST_KEYWORDS)


# -------------------------
# QUOTE SEARCH + DISPLAY PACKING
# -------------------------

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

    ocean_20_col = pick_ocean_freight_column(df_match, "20ft")
    ocean_40_col = pick_ocean_freight_column(df_match, "40ft")
    ex_20_col = pick_exworks_column(df_match, "20ft")
    ex_40_col = pick_exworks_column(df_match, "40ft")
    switch_col = pick_switch_bl_column(df_match)

    # best based on selected container size (20/40)
    ft_best = decide_ft_from_container(container_type)
    ocean_best_col = ocean_40_col if ft_best == "40ft" else ocean_20_col
    if ocean_best_col and ocean_best_col in df_match.columns:
        df_match["_ocean_price"] = df_match[ocean_best_col].apply(parse_price_to_float)
    else:
        df_match["_ocean_price"] = None

    # sort valid first, then latest validity
    df_match["_valid_sort"] = df_match["_is_valid"].apply(lambda x: 1 if x else 0)
    df_match = df_match.sort_values(
        by=["_valid_sort", "_validity_date"],
        ascending=[False, False],
        na_position="last"
    ).head(limit)

    # best row among VALID with numeric ocean price
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

        # show raw core prices (only if parseable)
        switch_val = parse_price_to_float(row.get(switch_col)) if switch_col else None
        ex20 = parse_price_to_float(row.get(ex_20_col)) if ex_20_col else None
        ex40 = parse_price_to_float(row.get(ex_40_col)) if ex_40_col else None
        oc20 = parse_price_to_float(row.get(ocean_20_col)) if ocean_20_col else None
        oc40 = parse_price_to_float(row.get(ocean_40_col)) if ocean_40_col else None

        price_lines = []
        if switch_val is not None:
            price_lines.append(("Switch BL", fmt_money(switch_val)))
        if ex20 is not None:
            price_lines.append(("Ex-Works (20ft)", fmt_money(ex20)))
        if ex40 is not None:
            price_lines.append(("Ex-Works (40ft)", fmt_money(ex40)))
        if oc20 is not None:
            price_lines.append(("Ocean Freight (20ft)", fmt_money(oc20)))
        if oc40 is not None:
            price_lines.append(("Ocean Freight (40ft)", fmt_money(oc40)))

        # requested totals for switch+container prices
        totals = []
        if switch_val is not None and ex20 is not None:
            totals.append(("Total Price: Switch BL + Ex-Works (20ft)", fmt_money(switch_val + ex20)))
        if switch_val is not None and ex40 is not None:
            totals.append(("Total Price: Switch BL + Ex-Works (40ft)", fmt_money(switch_val + ex40)))
        if switch_val is not None and oc20 is not None:
            totals.append(("Total Price: Switch BL + Ocean Freight (20ft)", fmt_money(switch_val + oc20)))
        if switch_val is not None and oc40 is not None:
            totals.append(("Total Price: Switch BL + Ocean Freight (40ft)", fmt_money(switch_val + oc40)))

        # GRAND TOTAL: sum all cost-like columns available in this row
        grand_total = 0.0
        grand_has_any = False
        grand_components = []
        for col in display_cols:
            if not is_cost_column(col):
                continue
            val = parse_price_to_float(row.get(col))
            if val is None:
                continue
            grand_has_any = True
            grand_total += val
            # show component too
            grand_components.append((str(col), fmt_money(val)))

        if grand_has_any:
            totals.append(("Grand Total (all prices found)", fmt_money(grand_total)))

        # ALL FIELDS, with N/A red if empty
        fields = []
        for col in display_cols:
            raw = row.get(col)

            # empty?
            if raw is None or pd.isna(raw) or str(raw).strip() == "":
                fields.append({"key": str(col), "val": None, "is_na": True})
                continue

            # nice formatting for dates
            if "date" in canon(col) or "validity" in canon(col):
                d = fmt_date_like(raw)
                if d is None:
                    fields.append({"key": str(col), "val": None, "is_na": True})
                else:
                    fields.append({"key": str(col), "val": d, "is_na": False})
                continue

            # show money formatting for cost columns when parseable
            if is_cost_column(col):
                num = parse_price_to_float(raw)
                if num is not None:
                    fields.append({"key": str(col), "val": fmt_money(num), "is_na": False})
                else:
                    fields.append({"key": str(col), "val": safe_str(raw), "is_na": False})
                continue

            # default string
            fields.append({"key": str(col), "val": safe_str(raw), "is_na": False})

        results.append({
            "is_best": (best_idx is not None and idx == best_idx),
            "title": f"{safe_str(row.get(pol_col))} ➜ {safe_str(row.get(pod_col))}",
            "validity_label": validity_label,
            "validity_kind": validity_kind,
            "price_lines": price_lines,
            "totals": totals,
            "fields": fields,
        })

    best_text = None
    if best_idx is not None:
        best_price_num = df_match.at[best_idx, "_ocean_price"]
        best_price_num = parse_price_to_float(best_price_num)
        if best_price_num is not None:
            best_text = f"Best Option available (valid rate + lowest {ft_best} Ocean Freight): {fmt_money(best_price_num)}"

    return results, best_text, None


# -------------------------
# APP ROUTES
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
        container_type=data["container_type"],
        limit=SHOW_LIMIT
    )

    # form should be empty after submit (template has no value= bindings)
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