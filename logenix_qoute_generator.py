from __future__ import annotations

from flask import Flask, request, render_template
import pandas as pd
import os
import re
import json
from datetime import datetime, date
from typing import Optional, Tuple, List, Dict, Any

app = Flask(__name__)

EXCEL_FILE = "queries.xlsx"
PRICES_FILE = "prices_updated.xlsx"
ROUTES_HISTORY_FILE = "routes_history.xlsx"
ROUTES_JSON_FILE = "routes.json"

SHOW_LIMIT = 4  # max 4 quote boxes


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
    "Karachi/Chittagong/Nava Sheva Port",
    "Bandar Abbas/LYG/Qingdao port",
    "Dar es Salaam/Mombasa port",
    "Mombasa port",
    "Belfast Port",
    "Rotterdam",
    "Umm Qasr/Dammam/Jebel Ali /Latakia/Beirut/Aqaba Port",
    "Almaty"
]

BASE_COMMODITIES = [
    "Food Item", "Pharmaceutical Products", "Automobile Parts", "Solar Modules",
    "CT Scan Machine", "General Cargo", "Paper Product", "Tea", "Cement",
    "Medicines", "Buffalo Meat", "Basalt Product", "Sausages", "Agrochemical",
    "Electronic Items", "Calcium Hypochlorite 65%", "Potassium Chloride",
    "Spare Parts", "Tea & Animal Nurtition Feed", "Equipments", "Potassium Nitrate",
    "Technical Salt", "Rice", "Machinery", "Chemicals", "Herbal Medicins",
    "Hardware", "Tires", "Used Textile Machinery", "Soap Noodles", "Vehicles",
    "Lubricants", "Spandex Yarn", "Medical Equipment", "Empty Container",
    "Liquid OIl", "FIber Cabels", "Electrical Equipment", "ALu ALu Foil",
    "Medical Diluents and Machines", "Veterinary / Livestock Farming Equipment",
    "Multipurpose Tents", "Composite Rod", "Armored Vehicle", "Steel Bloom",
    "Battery", "Surgical Disposable Item"
]

SALESPERSONS = ["Sulaiman", "Ahmed", "Dawood"]

CARGO_TYPES = [
    "General Cargo", "Containerized Cargo", "Bulk Cargo (Dry Bulk)", "Liquid Bulk Cargo",
    "Break Bulk Cargo", "Project Cargo", "Perishable Cargo", "DG Dangerous / Hazardous Cargo",
    "Roll-on/Roll-off (RoRo) Cargo", "Temperature-Controlled (Reefer) Cargo",
]

CONTAINER_TYPES = [
    "Dry Container (General Purpose)",
    "High Cube Container",
    "Reefer Container",
    "Open Top Container",
    "Flat Rack Container",
    "Tank Container",
    "Open Side Container",
    "Ventilated Container",
    "Insulated Container",
]

CONTAINER_SIZES = [
    "20ft",
    "40ft",
]

PACKAGING_TYPES = [
    "Loose Cargo", "Palletized (Stackable)", "Palletized (non-stackable)", "Floor-Loaded",
    "Carton Packed", "Crated", "Drummed", "Bagged / Sacked", "Jumbo Bags (FIBC)",
    "Baled", "Bundled", "Coiled / Rolled", "IBC Packed", "Unitized", "Shrink-Wrapped",
    "Breakbulk Packed", "Stackable", "Non-Stackable", "Top-Load Only", "Fragile",
    "Overweight", "Out of Gauge (OOG)",
]


# -------------------------
# UTILS
# -------------------------
def canon(s: Any) -> str:
    if s is None:
        return ""
    s = str(s).replace("\u00A0", " ")
    s = s.replace("–", "-").replace("—", "-")
    s = s.strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def norm_text(x) -> str:
    if x is None or pd.isna(x):
        return ""
    return str(x).strip().lower()


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


def parse_percent_to_float(text: str):
    if text is None:
        return None
    s = str(text).strip().lower()
    if not s or s == "none":
        return None
    s = s.replace("%", "").strip()
    m = re.search(r"(-?\d+(\.\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def clean_container_size_label(val: str) -> str:
    s = canon(val)
    if not s:
        return ""
    if "20" in s:
        return "20ft"
    if "40" in s:
        return "40ft"
    return str(val).strip()


def reverse_path(path: str) -> str:
    if not path:
        return ""
    parts = [p.strip() for p in path.split("→")]
    parts = [p for p in parts if p]
    if len(parts) <= 1:
        return path
    parts.reverse()
    return " → ".join(parts)


# -------------------------
# ROUTES (load from routes.json)
# -------------------------
def load_routes_json() -> List[Dict[str, Any]]:
    if not os.path.exists(ROUTES_JSON_FILE):
        return []
    try:
        with open(ROUTES_JSON_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            out: List[Dict[str, Any]] = []
            for r in data:
                if isinstance(r, dict) and r.get("id"):
                    out.append(r)
            return out
        return []
    except Exception:
        return []


def route_base_match(pol: str, pod: str, route: dict) -> Tuple[bool, bool]:
    """
    Match ONLY on POL/POD keywords.
    Reverse support kept.
    """
    pol_s = canon(pol)
    pod_s = canon(pod)

    pol_ok = any(canon(k) and canon(k) in pol_s for k in route.get("pol_keywords", []))
    pod_ok = any(canon(k) and canon(k) in pod_s for k in route.get("pod_keywords", []))
    if pol_ok and pod_ok:
        return True, False

    pol_ok_rev = any(canon(k) and canon(k) in pod_s for k in route.get("pol_keywords", []))
    pod_ok_rev = any(canon(k) and canon(k) in pol_s for k in route.get("pod_keywords", []))
    if pol_ok_rev and pod_ok_rev:
        return True, True

    return False, False


def transit_time_key(route: Dict[str, Any]) -> Tuple[int, int]:
    tt = route.get("transit_time_days") or {}

    def safe_int(v: Any) -> int:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return 10**9
        try:
            return int(v)
        except Exception:
            try:
                return int(str(v).strip())
            except Exception:
                return 10**9

    mn = safe_int(tt.get("min"))
    mx = safe_int(tt.get("max"))
    return (mn, mx)

def get_matching_routes(pol: str, pod: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    routes_src = load_routes_json()
    matched: List[Dict[str, Any]] = []

    for r in routes_src:
        ok, is_reverse = route_base_match(pol, pod, r)
        if not ok:
            continue

        rr = dict(r)
        rr["is_recent"] = False
        rr["is_custom"] = False
        rr["is_reverse"] = bool(is_reverse)
        rr["path"] = reverse_path(rr.get("path", "")) if is_reverse else rr.get("path", "")
        rr["route_status"] = (rr.get("route_status") or "open").strip().lower()
        rr["_tt_key"] = transit_time_key(rr)
        matched.append(rr)

    if not matched:
        return [], None

    matched.sort(key=lambda x: x.get("_tt_key", (10**9, 10**9)))
    best_id = matched[0].get("id")
    return matched, best_id

# -------------------------
# ROUTE HISTORY (CUSTOM ROUTES)
# -------------------------
def load_routes_history_df() -> pd.DataFrame:
    if not os.path.exists(ROUTES_HISTORY_FILE):
        return pd.DataFrame(columns=["pol", "pod", "route_text", "created_at"])
    try:
        df = pd.read_excel(ROUTES_HISTORY_FILE)
        for c in ["pol", "pod", "route_text", "created_at"]:
            if c not in df.columns:
                df[c] = ""
        return df
    except Exception:
        return pd.DataFrame(columns=["pol", "pod", "route_text", "created_at"])


def save_route_history(pol: str, pod: str, route_text: str):
    pol_n = canon(pol)
    pod_n = canon(pod)
    rt = str(route_text).strip()
    if not rt:
        return

    df = load_routes_history_df()

    exists = False
    if not df.empty:
        mask = (
            df["pol"].astype(str).apply(canon).eq(pol_n)
            & df["pod"].astype(str).apply(canon).eq(pod_n)
            & df["route_text"].astype(str).apply(canon).eq(canon(rt))
        )
        exists = bool(mask.any())

    if not exists:
        df_new = pd.DataFrame([{
            "pol": pol.strip(),
            "pod": pod.strip(),
            "route_text": rt,
            "created_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        }])
        df = pd.concat([df, df_new], ignore_index=True)

        if len(df) > 5000:
            df = df.tail(5000)

        df.to_excel(ROUTES_HISTORY_FILE, index=False)


def get_recent_routes(pol: str, pod: str, limit: int = 5) -> List[Dict[str, Any]]:
    df = load_routes_history_df()
    if df.empty:
        return []

    pol_n = canon(pol)
    pod_n = canon(pod)

    mask = (
        df["pol"].astype(str).apply(canon).eq(pol_n)
        & df["pod"].astype(str).apply(canon).eq(pod_n)
    )
    dff = df[mask].copy()
    if dff.empty:
        return []

    if "created_at" in dff.columns:
        dff["_dt"] = pd.to_datetime(dff["created_at"], errors="coerce")
        dff = dff.sort_values("_dt", ascending=False, na_position="last")

    dff = dff.head(limit)

    out = []
    for i, row in dff.iterrows():
        out.append({
            "id": f"HR-{i}",
            "title": "Recent Used Route",
            "path": str(row.get("route_text", "")).strip(),
            "is_recent": True,
            "is_custom": True,
            "is_reverse": False,
            "route_status": "open",
            "transit_time_days": None,
            "_tt_key": (10**9, 10**9)
        })
    return out


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


def save_to_excel(record: Dict[str, Any]):
    df_new = pd.DataFrame([record])
    if os.path.exists(EXCEL_FILE):
        try:
            df_existing = pd.read_excel(EXCEL_FILE)
            df_final = pd.concat([df_existing, df_new], ignore_index=True)
        except Exception:
            df_final = df_new
    else:
        df_final = df_new
    df_final.to_excel(EXCEL_FILE, index=False)


def get_commodities():
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


def get_salespersons():
    persons = list(SALESPERSONS)
    if os.path.exists(EXCEL_FILE):
        try:
            df = pd.read_excel(EXCEL_FILE)
            col = next((c for c in df.columns if c.lower() == "salesperson_name"), None)
            if col:
                existing = df[col].dropna().astype(str).str.strip().unique()
                for p in existing:
                    if p and p not in persons:
                        persons.append(p)
        except Exception:
            pass
    return persons


def get_cargo_types():
    types = list(CARGO_TYPES)
    if os.path.exists(EXCEL_FILE):
        try:
            df = pd.read_excel(EXCEL_FILE)
            col = next((c for c in df.columns if c.lower() == "cargo_type"), None)
            if col:
                existing = df[col].dropna().astype(str).str.strip().unique()
                for t in existing:
                    if t and t not in types:
                        types.append(t)
        except Exception:
            pass
    return types


def get_packaging_types():
    types = list(PACKAGING_TYPES)
    if os.path.exists(EXCEL_FILE):
        try:
            df = pd.read_excel(EXCEL_FILE)
            col = next((c for c in df.columns if c.lower() == "packaging_type"), None)
            if col:
                existing = df[col].dropna().astype(str).str.strip().unique()
                for t in existing:
                    if t and t not in types:
                        types.append(t)
        except Exception:
            pass
    return types


# -------------------------
# GRAND TOTAL LOGIC
# -------------------------
def is_charges_column(col_name: str) -> bool:
    return canon(col_name).endswith("_charges")


def compute_grand_total(row: pd.Series, columns: List[str]) -> Tuple[float, bool]:
    total = 0.0
    found_any = False
    for col in columns:
        if not is_charges_column(col):
            continue
        num = parse_price_to_float(row.get(col))
        if num is None:
            continue
        total += float(num)
        found_any = True
    return float(total), bool(found_any)


def compute_grand_totals_for_df(df: pd.DataFrame, columns: List[str]) -> Tuple[List[float], List[bool]]:
    totals: List[float] = []
    has_any: List[bool] = []
    for _, row in df.iterrows():
        t, ok = compute_grand_total(row, columns)
        totals.append(float(t))
        has_any.append(bool(ok))
    return totals, has_any


# -------------------------
# QUOTE SEARCH
# -------------------------
def get_strict_quotes(origin, destination, commodity, container_size_label: str, limit=4):
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

    df_match = df_match.sort_values(
        by=["_valid_sort", "_validity_date"],
        ascending=[False, False],
        na_position="last"
    ).head(limit)

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
    csl = (container_size_label or "").strip()

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

            if raw is None or pd.isna(raw) or str(raw).strip() == "":
                fields.append({"key": str(col), "val": None, "is_na": True, "is_grand_total": False})
                continue

            if "date" in canon(col) or "validity" in canon(col):
                d = fmt_date_like(raw)
                fields.append({"key": str(col), "val": d if d else None, "is_na": (d is None), "is_grand_total": False})
                continue

            if is_charges_column(col):
                num = parse_price_to_float(raw)
                if num is not None:
                    fields.append({"key": str(col), "val": fmt_money(num), "is_na": False, "is_grand_total": False})
                else:
                    fields.append({"key": str(col), "val": str(raw).strip(), "is_na": False, "is_grand_total": False})
                continue

            fields.append({"key": str(col), "val": str(raw).strip(), "is_na": False, "is_grand_total": False})

        total_num, found_any = compute_grand_total(row, display_cols)
        grand_total_str = fmt_money(total_num) if found_any else "N/A"

        fields.append({
            "key": "Grand Total",
            "val": grand_total_str,
            "is_na": (grand_total_str == "N/A"),
            "is_grand_total": True
        })

        pol_disp = str(row.get("POL", "")).strip()
        pod_disp = str(row.get("POD", "")).strip()
        title_prefix = f"{csl} " if csl else ""
        title = f"{title_prefix}Shipping from {pol_disp} to {pod_disp}"

        results.append({
            "is_best": (idx == best_idx),
            "title": title,
            "validity_label": validity_label,
            "validity_kind": validity_kind,
            "fields": fields
        })

    best_text = "Best Option available based on rate validity and match."
    return results, best_text, None


# -------------------------
# TEMPLATE HELPERS
# -------------------------
def build_display_items_for_submitted(data: Dict[str, Any]) -> List[Dict[str, str]]:
    items: List[Dict[str, str]] = []

    def add(label: str, key: str):
        v = data.get(key, "")
        if v is None:
            return
        s = str(v).strip()
        if s == "":
            return
        items.append({"label": label, "value": s})

    add("Quote ID", "quote_id")
    add("Company", "company_name")
    add("Salesperson Name", "salesperson_name")
    add("Container Ownership", "container_ownership")
    add("Incoterm for Origin", "incoterm_origin")
    add("Incoterm for Destination", "incoterm_destination")

    add("Pick Up Point 1 - Factory / Warehouse address", "shipping_from_1_address")
    add("Pick Up Point 1 - Port of Loading", "shipping_from_1_port")
    add("Pick Up Point 1 - City", "shipping_from_1_city")
    add("Pick Up Point 1 - Country", "shipping_from_1_country")

    add("Pick Up Point 2 - Factory / Warehouse address", "shipping_from_2_address")
    add("Pick Up Point 2 - Port of Loading", "shipping_from_2_port")
    add("Pick Up Point 2 - City", "shipping_from_2_city")
    add("Pick Up Point 2 - Country", "shipping_from_2_country")

    add("Pick Up Point 3 - Factory / Warehouse address", "shipping_from_3_address")
    add("Pick Up Point 3 - Port of Loading", "shipping_from_3_port")
    add("Pick Up Point 3 - City", "shipping_from_3_city")
    add("Pick Up Point 3 - Country", "shipping_from_3_country")

    add("Pick Up Point 4 - Factory / Warehouse address", "shipping_from_4_address")
    add("Pick Up Point 4 - Port of Loading", "shipping_from_4_port")
    add("Pick Up Point 4 - City", "shipping_from_4_city")
    add("Pick Up Point 4 - Country", "shipping_from_4_country")

    add("Delivery Point 1 - Factory / Warehouse address", "destination_1_address")
    add("Delivery Point 1 - Port of Loading", "destination_1_port")
    add("Delivery Point 1 - City", "destination_1_city")
    add("Delivery Point 1 - Country", "destination_1_country")

    add("Delivery Point 2 - Factory / Warehouse address", "destination_2_address")
    add("Delivery Point 2 - Port of Loading", "destination_2_port")
    add("Delivery Point 2 - City", "destination_2_city")
    add("Delivery Point 2 - Country", "destination_2_country")

    add("Delivery Point 3 - Factory / Warehouse address", "destination_3_address")
    add("Delivery Point 3 - Port of Loading", "destination_3_port")
    add("Delivery Point 3 - City", "destination_3_city")
    add("Delivery Point 3 - Country", "destination_3_country")

    add("Delivery Point 4 - Factory / Warehouse address", "destination_4_address")
    add("Delivery Point 4 - Port of Loading", "destination_4_port")
    add("Delivery Point 4 - City", "destination_4_city")
    add("Delivery Point 4 - Country", "destination_4_country")

    add("Transit Border 1", "transit_border_1")
    add("Transit Border 2", "transit_border_2")
    add("Transit Border 3", "transit_border_3")
    add("Transit Border 4", "transit_border_4")

    add("Selected Route ID", "selected_route_id")
    add("Selected Route", "selected_route_text")
    add("Route Status", "selected_route_status")
    add("Transit Time (Days)", "selected_route_transit_days")
    add("Custom Route", "custom_route_text")

    add("Cargo Type", "cargo_type")
    add("Packaging Type", "packaging_type")
    add("Free Days to Return Container", "free_days_return")

    add("Lifting / Labor required?", "lifting_labor_required")
    add("Who is responsible for offloading?", "offloading_responsible")
    add("Who is responsible for Final Customs?", "final_customs_responsible")

    add("Reloading Required", "reloading_required")
    add("Reloading Count", "reloading_count")
    add("Reloading Places", "reloading_places")

    add("Commodity", "commodity")
    add("CBM", "cbm")
    add("Weight", "weight_tons")

    add("Type of Container", "container_type")
    add("Container Size Summary", "container_size")
    add("Total Number of Containers", "num_containers")
    add("20ft Containers", "size_20ft_count")
    add("40ft Containers", "size_40ft_count")

    add("Width (ft)", "width_ft")
    add("Height (ft)", "height_ft")
    add("Temperature (°C)", "temperature_c")

    add("Cargo Value", "cargo_value")
    add("Insurance Rate", "insurance_rate")
    add("Insurance Amount", "insurance_amount")

    add("Miscellaneous Cost", "misc_cost")
    add("Special Cost Option", "special_cost_option")
    add("Reason", "reason")
    add("Special Cost", "special_cost")

    add("Shipment Type", "shipment_type")
    add("Timestamp", "timestamp")

    return items


def empty_form_data() -> Dict[str, Any]:
    return {}


# -------------------------
# ROUTES
# -------------------------
@app.route("/", methods=["GET"])
def index():
    return render_template(
        "form.html",
        countries=COUNTRIES,
        commodities=get_commodities(),
        salespersons=get_salespersons(),
        cargo_types=get_cargo_types(),
        container_types=CONTAINER_TYPES,
        container_sizes=CONTAINER_SIZES,
        packaging_types=get_packaging_types(),

        stage="input",
        form_data=empty_form_data(),
        submitted=False,
        submitted_items=[],

        routes=[],
        best_route_id=None,
        selected_route_id=None,
        route_error_msg=None,

        rates=[],
        best_text=None,
        error_msg=None
    )


@app.route("/submit", methods=["POST"])
def submit():
    action = request.form.get("_action", "next").strip().lower()

    # -------------------------
    # read POL/POD (up to 4 each)
    # -------------------------
    # -------------------------
    # read Pick Up / Delivery Point Details (up to 4 each)
    # Match routes ONLY on City (POL city vs POD city)
    # -------------------------
    pols: List[Dict[str, str]] = []
    pods: List[Dict[str, str]] = []

    for i in range(1, 5):
        pols.append({
            "address": request.form.get(f"shipping_from_{i}_address", "").strip(),
            "port": request.form.get(f"shipping_from_{i}_port", "").strip(),
            "city": request.form.get(f"shipping_from_{i}_city", "").strip(),
            "country": request.form.get(f"shipping_from_{i}_country", "").strip(),
        })
        pods.append({
            "address": request.form.get(f"destination_{i}_address", "").strip(),
            "port": request.form.get(f"destination_{i}_port", "").strip(),
            "city": request.form.get(f"destination_{i}_city", "").strip(),
            "country": request.form.get(f"destination_{i}_country", "").strip(),
        })

    shipping_from_1_city = pols[0]["city"] if len(pols) > 0 else ""
    destination_1_city = pods[0]["city"] if len(pods) > 0 else ""

    # -------------------------
    # Basic fields
    # -------------------------
    company_name = request.form.get("company_name", "").strip()
    salesperson_name = request.form.get("salesperson_name", "").strip()
    container_ownership = request.form.get("container_ownership", "").strip()
    incoterm_origin = request.form.get("incoterm_origin", "").strip()
    incoterm_destination = request.form.get("incoterm_destination", "").strip()

    shipment_type = request.form.get("shipment_type", "").strip()

    lifting_labor_required = request.form.get("lifting_labor_required", "").strip()
    offloading_responsible = request.form.get("offloading_responsible", "").strip()
    final_customs_responsible = request.form.get("final_customs_responsible", "").strip()

    # Transit borders (OPTIONAL)
    transit_border_1 = request.form.get("transit_border_1", "").strip()
    transit_border_2 = request.form.get("transit_border_2", "").strip()
    transit_border_3 = request.form.get("transit_border_3", "").strip()
    transit_border_4 = request.form.get("transit_border_4", "").strip()

    cargo_type = request.form.get("cargo_type", "").strip()
    packaging_type = request.form.get("packaging_type", "").strip()

    free_days_return_raw = request.form.get("free_days_return", "").strip()
    try:
        free_days_return = int(free_days_return_raw)
    except Exception:
        free_days_return = ""

    # Reloading
    reloading_required = request.form.get("reloading_required", "").strip()
    reloading_count_raw = request.form.get("reloading_count", "").strip()

    reloading_count = 0
    reloading_places_list: List[str] = []

    if reloading_required.lower() == "yes":
        try:
            reloading_count = int(reloading_count_raw)
        except Exception:
            reloading_count = 0

        if reloading_count < 0:
            reloading_count = 0
        if reloading_count > 5:
            reloading_count = 5

        for i in range(1, reloading_count + 1):
            place = request.form.get(f"reloading_place_{i}", "").strip()
            if place:
                reloading_places_list.append(place)

    reloading_places = "; ".join(reloading_places_list)

    # Weight
    weight_choice = request.form.get("weight_choice", "").strip()
    weight_other = request.form.get("weight_other", "").strip()
    if weight_choice == "Other":
        weight_final = weight_other if weight_other else ""
    else:
        weight_final = weight_choice

    # Container fields
    container_type = request.form.get("container_type", "").strip()

    size_20ft_selected = request.form.get("size_20ft_selected", "").strip().lower()
    size_40ft_selected = request.form.get("size_40ft_selected", "").strip().lower()

    size_20ft_count_raw = request.form.get("size_20ft_count", "").strip()
    size_40ft_count_raw = request.form.get("size_40ft_count", "").strip()

    def to_int_or_zero(x: str) -> int:
        try:
            v = int(x)
            return v if v > 0 else 0
        except Exception:
            return 0

    size_20ft_count = to_int_or_zero(size_20ft_count_raw)
    size_40ft_count = to_int_or_zero(size_40ft_count_raw)

    total_containers = size_20ft_count + size_40ft_count

    size_labels: List[str] = []
    if size_20ft_count > 0:
        size_labels.append("20ft")
    if size_40ft_count > 0:
        size_labels.append("40ft")
    container_size_summary = " & ".join(size_labels) if size_labels else ""

    num_containers = total_containers

    # Dimensions / Temperature
    width_ft = request.form.get("width_ft", "").strip()
    height_ft = request.form.get("height_ft", "").strip()
    temperature_c = request.form.get("temperature_c", "").strip()

    # Commodity and costs
    commodity = request.form.get("commodity", "").strip()
        # CBM (Cubic Meter) - single package CBM
    cbm_raw = request.form.get("cbm", "").strip()
    cbm_value: Any = ""
    if cbm_raw:
        try:
            cbm_value = float(cbm_raw)
        except Exception:
            cbm_value = cbm_raw  # keep raw if user typed something unexpected

    cargo_value_raw = request.form.get("cargo_value", "").strip()
    cargo_value_num = parse_price_to_float(cargo_value_raw)
    cargo_value_saved = cargo_value_num if cargo_value_num is not None else (cargo_value_raw if cargo_value_raw else "")

    insurance_rate_raw = request.form.get("insurance_rate", "").strip()
    insurance_rate_num = parse_percent_to_float(insurance_rate_raw)

    insurance_amount_num = None
    if cargo_value_num is not None and insurance_rate_num is not None:
        insurance_amount_num = (insurance_rate_num / 100.0) * cargo_value_num

    insurance_rate_saved = insurance_rate_raw if insurance_rate_raw else ""
    if insurance_rate_raw.strip().lower() == "none":
        insurance_rate_saved = "none"

    insurance_amount_saved = fmt_money(insurance_amount_num) if insurance_amount_num is not None else ""

    misc_cost_raw = request.form.get("misc_cost", "").strip()
    misc_cost_saved = misc_cost_raw

    special_cost_option = request.form.get("special_cost_option", "").strip()
    reason = ""
    special_cost = ""
    if special_cost_option.lower() == "yes":
        reason = request.form.get("reason", "").strip()
        special_cost = request.form.get("special_cost", "").strip()

    # -------------------------
    # build form_data for repopulation
    # -------------------------
    form_data: Dict[str, Any] = {
        "company_name": company_name,
        "salesperson_name": salesperson_name,
        "container_ownership": container_ownership,
        "incoterm_origin": incoterm_origin,
        "incoterm_destination": incoterm_destination,

        "shipping_from_1_address": pols[0]["address"],
        "shipping_from_1_port": pols[0]["port"],
        "shipping_from_1_city": pols[0]["city"],
        "shipping_from_1_country": pols[0]["country"],

        "shipping_from_2_address": pols[1]["address"],
        "shipping_from_2_port": pols[1]["port"],
        "shipping_from_2_city": pols[1]["city"],
        "shipping_from_2_country": pols[1]["country"],

        "shipping_from_3_address": pols[2]["address"],
        "shipping_from_3_port": pols[2]["port"],
        "shipping_from_3_city": pols[2]["city"],
        "shipping_from_3_country": pols[2]["country"],

        "shipping_from_4_address": pols[3]["address"],
        "shipping_from_4_port": pols[3]["port"],
        "shipping_from_4_city": pols[3]["city"],
        "shipping_from_4_country": pols[3]["country"],

        "destination_1_address": pods[0]["address"],
        "destination_1_port": pods[0]["port"],
        "destination_1_city": pods[0]["city"],
        "destination_1_country": pods[0]["country"],

        "destination_2_address": pods[1]["address"],
        "destination_2_port": pods[1]["port"],
        "destination_2_city": pods[1]["city"],
        "destination_2_country": pods[1]["country"],

        "destination_3_address": pods[2]["address"],
        "destination_3_port": pods[2]["port"],
        "destination_3_city": pods[2]["city"],
        "destination_3_country": pods[2]["country"],

        "destination_4_address": pods[3]["address"],
        "destination_4_port": pods[3]["port"],
        "destination_4_city": pods[3]["city"],
        "destination_4_country": pods[3]["country"],

        "transit_border_1": transit_border_1,
        "transit_border_2": transit_border_2,
        "transit_border_3": transit_border_3,
        "transit_border_4": transit_border_4,

        "cargo_type": cargo_type,
        "packaging_type": packaging_type,
        "free_days_return": free_days_return_raw,

        "lifting_labor_required": lifting_labor_required,
        "offloading_responsible": offloading_responsible,
        "final_customs_responsible": final_customs_responsible,

        "reloading_required": reloading_required,
        "reloading_count": reloading_count_raw,
        "reloading_places": reloading_places_list,

        "commodity": commodity,
        "cbm": cbm_raw,
        "weight_choice": weight_choice,
        "weight_other": weight_other,

        "container_type": container_type,
        "container_size": container_size_summary,
        "num_containers": str(num_containers) if num_containers else "",
        "size_20ft_selected": "yes" if size_20ft_selected == "yes" or size_20ft_count > 0 else "",
        "size_20ft_count": size_20ft_count_raw,
        "size_40ft_selected": "yes" if size_40ft_selected == "yes" or size_40ft_count > 0 else "",
        "size_40ft_count": size_40ft_count_raw,

        "width_ft": width_ft,
        "height_ft": height_ft,
        "temperature_c": temperature_c,

        "cargo_value": cargo_value_raw,
        "insurance_rate": insurance_rate_raw,
        "misc_cost": misc_cost_saved,

        "special_cost_option": special_cost_option,
        "reason": reason,
        "special_cost": special_cost,

        "shipment_type": shipment_type,
    }

    # -------------------------
    # ROUTE MATCHING (POL/POD only)
    # -------------------------
    matched_routes, best_route_id = get_matching_routes(
        pol=shipping_from_1_city,
        pod=destination_1_city
    )

    recent_routes = get_recent_routes(shipping_from_1_city, destination_1_city, limit=5)
    all_routes = matched_routes + recent_routes

    best_route_id_all: Optional[str] = None
    if all_routes:
        all_routes_sorted = sorted(all_routes, key=lambda x: x.get("_tt_key", (10**9, 10**9)))
        best_route_id_all = all_routes_sorted[0].get("id")

    if action == "next":
        return render_template(
            "form.html",
            countries=COUNTRIES,
            commodities=get_commodities(),
            salespersons=get_salespersons(),
            cargo_types=get_cargo_types(),
            container_types=CONTAINER_TYPES,
            container_sizes=CONTAINER_SIZES,
            packaging_types=get_packaging_types(),

            stage="routes",
            form_data=form_data,
            submitted=False,
            submitted_items=[],

            routes=all_routes,
            best_route_id=best_route_id_all,
            selected_route_id=None,
            route_error_msg=None,

            rates=[],
            best_text=None,
            error_msg=None
        )

    # -------------------------
    # GENERATE step
    # -------------------------
    selected_route_id = request.form.get("selected_route_id", "").strip()
    own_route_text = request.form.get("own_route_text", "").strip()
    confirm_closed = request.form.get("confirm_closed_route", "").strip().lower()

    route_error_msg = None
    selected_route_text = None
    selected_route_status = ""
    selected_route_transit_days = ""

    if all_routes:
        if not selected_route_id:
            route_error_msg = "Please select one route or choose 'My own route'."
        elif selected_route_id == "OWN":
            if not own_route_text:
                route_error_msg = "Please type your own route."
            else:
                pol_ok = canon(shipping_from_1_city) in canon(own_route_text)
                pod_ok = canon(destination_1_city) in canon(own_route_text)
                if not (pol_ok and pod_ok):
                    route_error_msg = "Your custom route must contain Pick Up City and Delivery City."
                else:
                    selected_route_text = own_route_text.strip()
                    selected_route_status = "open"
                    selected_route_transit_days = ""
                    save_route_history(shipping_from_1_city, destination_1_city, selected_route_text)
        else:
            chosen = next((r for r in all_routes if str(r.get("id")) == selected_route_id), None)
            if not chosen:
                route_error_msg = "Selected route not found. Please choose again."
            else:
                selected_route_text = str(chosen.get("path", "")).strip()
                selected_route_status = (chosen.get("route_status") or "open").strip().lower()
                tt = chosen.get("transit_time_days") or {}
                if isinstance(tt, dict) and tt.get("min") is not None and tt.get("max") is not None:
                    selected_route_transit_days = f"{tt.get('min')}-{tt.get('max')}"
                else:
                    selected_route_transit_days = ""

                if selected_route_status == "closed" and confirm_closed != "yes":
                    route_error_msg = "Your selected route is CLOSED. Please confirm you want to proceed with a closed route."
    else:
        if selected_route_id == "OWN":
            if not own_route_text:
                route_error_msg = "No routes found for now. Please type your own route."
            else:
                pol_ok = canon(shipping_from_1_city) in canon(own_route_text)
                pod_ok = canon(destination_1_city) in canon(own_route_text)
                if not (pol_ok and pod_ok):
                    route_error_msg = "Your custom route must contain Pick Up Point and Point of Delivery (POL and POD)."
                else:
                    selected_route_text = own_route_text.strip()
                    selected_route_status = "open"
                    selected_route_transit_days = ""
                    save_route_history(shipping_from_1_city, destination_1_city, selected_route_text)
        else:
            route_error_msg = "No routes found for now. Please choose 'My own route' and type your route."

    # Validate mandatory dynamic fields
    ct = canon(container_type)
    is_open_or_flat = ("open top" in ct) or ("flat rack" in ct)
    is_reefer = ("reefer" in ct)

    if is_open_or_flat:
        if not width_ft or not height_ft:
            route_error_msg = route_error_msg or "Width and Height are required for Open Top / Flat Rack."
    if is_reefer:
        if not temperature_c:
            route_error_msg = route_error_msg or "Temperature is required for Reefer."

    data: Dict[str, Any] = {
        "quote_id": f"QUOTE-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        "company_name": company_name,
        "salesperson_name": salesperson_name,
        "container_ownership": container_ownership,
        "incoterm_origin": incoterm_origin,
        "incoterm_destination": incoterm_destination,

        "shipping_from_1_address": pols[0]["address"],
        "shipping_from_1_port": pols[0]["port"],
        "shipping_from_1_city": pols[0]["city"],
        "shipping_from_1_country": pols[0]["country"],

        "shipping_from_2_address": pols[1]["address"],
        "shipping_from_2_port": pols[1]["port"],
        "shipping_from_2_city": pols[1]["city"],
        "shipping_from_2_country": pols[1]["country"],

        "shipping_from_3_address": pols[2]["address"],
        "shipping_from_3_port": pols[2]["port"],
        "shipping_from_3_city": pols[2]["city"],
        "shipping_from_3_country": pols[2]["country"],

        "shipping_from_4_address": pols[3]["address"],
        "shipping_from_4_port": pols[3]["port"],
        "shipping_from_4_city": pols[3]["city"],
        "shipping_from_4_country": pols[3]["country"],

        "destination_1_address": pods[0]["address"],
        "destination_1_port": pods[0]["port"],
        "destination_1_city": pods[0]["city"],
        "destination_1_country": pods[0]["country"],

        "destination_2_address": pods[1]["address"],
        "destination_2_port": pods[1]["port"],
        "destination_2_city": pods[1]["city"],
        "destination_2_country": pods[1]["country"],

        "destination_3_address": pods[2]["address"],
        "destination_3_port": pods[2]["port"],
        "destination_3_city": pods[2]["city"],
        "destination_3_country": pods[2]["country"],

        "destination_4_address": pods[3]["address"],
        "destination_4_port": pods[3]["port"],
        "destination_4_city": pods[3]["city"],
        "destination_4_country": pods[3]["country"],


        "transit_border_1": transit_border_1,
        "transit_border_2": transit_border_2,
        "transit_border_3": transit_border_3,
        "transit_border_4": transit_border_4,

        "selected_route_id": selected_route_id,
        "selected_route_text": selected_route_text if selected_route_text else "",
        "selected_route_status": selected_route_status,
        "selected_route_transit_days": selected_route_transit_days,
        "custom_route_text": own_route_text if selected_route_id == "OWN" else "",

        "cargo_type": cargo_type,
        "packaging_type": packaging_type,
        "free_days_return": free_days_return,

        "lifting_labor_required": lifting_labor_required,
        "offloading_responsible": offloading_responsible,
        "final_customs_responsible": final_customs_responsible,

        "reloading_required": reloading_required,
        "reloading_count": reloading_count if reloading_required.lower() == "yes" else 0,
        "reloading_places": reloading_places if reloading_required.lower() == "yes" else "",

        "commodity": commodity,
        "cbm": cbm_value,
        "weight_tons": weight_final,

        "container_type": container_type,
        "container_size": container_size_summary,
        "num_containers": num_containers,
        "size_20ft_selected": "Yes" if size_20ft_count > 0 else "",
        "size_20ft_count": size_20ft_count,
        "size_40ft_selected": "Yes" if size_40ft_count > 0 else "",
        "size_40ft_count": size_40ft_count,

        "width_ft": width_ft,
        "height_ft": height_ft,
        "temperature_c": temperature_c,

        "cargo_value": cargo_value_saved,
        "insurance_rate": insurance_rate_saved,
        "insurance_amount": insurance_amount_saved,
        "misc_cost": misc_cost_saved,

        "special_cost_option": special_cost_option,
        "reason": reason,
        "special_cost": special_cost,

        "shipment_type": shipment_type,

        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }

    if route_error_msg:
        return render_template(
            "form.html",
            countries=COUNTRIES,
            commodities=get_commodities(),
            salespersons=get_salespersons(),
            cargo_types=get_cargo_types(),
            container_types=CONTAINER_TYPES,
            container_sizes=CONTAINER_SIZES,
            packaging_types=get_packaging_types(),

            stage="routes",
            form_data=form_data,
            submitted=False,
            submitted_items=[],

            routes=all_routes,
            best_route_id=best_route_id_all,
            selected_route_id=selected_route_id if selected_route_id else None,
            route_error_msg=route_error_msg,

            rates=[],
            best_text=None,
            error_msg=None
        )

    save_to_excel(data)

    container_size_label_for_title = container_size_summary if container_size_summary else ""
    rates, best_text, error_msg = get_strict_quotes(
        origin=shipping_from_1_city,
        destination=destination_1_city,
        commodity=commodity,
        container_size_label=container_size_label_for_title,
        limit=SHOW_LIMIT
    )
    
    submitted_items = build_display_items_for_submitted(data)

    return render_template(
        "form.html",
        countries=COUNTRIES,
        commodities=get_commodities(),
        salespersons=get_salespersons(),
        cargo_types=get_cargo_types(),
        container_types=CONTAINER_TYPES,
        container_sizes=CONTAINER_SIZES,
        packaging_types=get_packaging_types(),

        stage="result",
        form_data=empty_form_data(),
        submitted=True,
        submitted_items=submitted_items,

        routes=[],
        best_route_id=None,
        selected_route_id=None,
        route_error_msg=None,

        rates=rates,
        best_text=best_text,
        error_msg=error_msg
    )


if __name__ == "__main__":
    app.run(debug=True)