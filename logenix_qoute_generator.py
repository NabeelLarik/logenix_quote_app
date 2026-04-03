from __future__ import annotations

from flask import Flask, request, render_template
import pandas as pd
import os
import re
import json
import requests
import io
from datetime import datetime, date
from typing import Optional, Tuple, List, Dict, Any

app = Flask(__name__)

# -------------------------
# ONEDRIVE CONFIG
# -------------------------
TENANT_ID = os.getenv("TENANT_ID", "").strip()
CLIENT_ID = os.getenv("CLIENT_ID", "").strip()
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "").strip()

# The OneDrive owner account where the files exist
ONEDRIVE_USER_EMAIL = os.getenv("ONEDRIVE_USER_EMAIL", "nahmed@logenix.pk").strip()

ONEDRIVE_PRICES_PATH = os.getenv(
    "ONEDRIVE_PRICES_PATH",
    "Automation Documents/Logenix/prices_updated.xlsx"
).strip()

ONEDRIVE_QUERIES_PATH = os.getenv(
    "ONEDRIVE_QUERIES_PATH",
    "Automation Documents/Logenix/queries.xlsx"
).strip()
ROUTES_HISTORY_FILE = "routes_history.xlsx"
ROUTES_JSON_FILE = "routes.json"

SHOW_LIMIT = 1  # max 4 quote boxes


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
    "2x20ft",
]

PACKAGING_TYPES = [
    "Loose Cargo", "Palletized (Stackable)", "Palletized (non-stackable)", "Floor-Loaded",
    "Carton Packed", "Crated", "Drummed", "Bagged / Sacked", "Jumbo Bags (FIBC)",
    "Baled", "Bundled", "Coiled / Rolled", "IBC Packed", "Unitized", "Shrink-Wrapped",
    "Breakbulk Packed", "Stackable", "Non-Stackable", "Top-Load Only", "Fragile",
    "Overweight", "Out of Gauge (OOG)",
]

# -------------------------
# ONEDRIVE GRAPH HELPERS
# -------------------------
def get_access_token() -> str:
    if not TENANT_ID or not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("TENANT_ID / CLIENT_ID / CLIENT_SECRET are missing.")

    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"

    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default",
        "grant_type": "client_credentials",
    }

    r = requests.post(url, data=data, timeout=60)
    r.raise_for_status()
    return r.json()["access_token"]


def _graph_drive_content_url(file_path: str) -> str:
    safe_path = file_path.lstrip("/")
    return f"https://graph.microsoft.com/v1.0/users/{ONEDRIVE_USER_EMAIL}/drive/root:/{safe_path}:/content"


def download_excel_from_onedrive(file_path: str) -> bytes:
    token = get_access_token()
    url = _graph_drive_content_url(file_path)

    headers = {"Authorization": f"Bearer {token}"}

    r = requests.get(url, headers=headers, timeout=120)
    r.raise_for_status()
    return r.content


def upload_excel_to_onedrive(file_path: str, content: bytes):
    token = get_access_token()
    url = _graph_drive_content_url(file_path)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    }

    r = requests.put(url, headers=headers, data=content, timeout=120)
    r.raise_for_status()


def read_queries_df_from_onedrive() -> pd.DataFrame:
    try:
        content = download_excel_from_onedrive(ONEDRIVE_QUERIES_PATH)
        return pd.read_excel(io.BytesIO(content), sheet_name=0)
    except Exception:
        return pd.DataFrame()

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


def find_col_case_insensitive(df: pd.DataFrame, target: str) -> Optional[str]:
    """
    Finds a column in df by case-insensitive comparison.
    Returns actual column name or None.
    """
    t = canon(target)
    for c in df.columns:
        if canon(c) == t:
            return c
    return None


def validity_status_and_text(v) -> Tuple[str, Optional[str], Optional[date]]:
    """
    Returns:
      status: "na" | "expired" | "valid" | "unknown"
      text: formatted date string if available (e.g., 12-Jan-2026)
      parsed_date: python date or None
    """
    if v is None or pd.isna(v) or str(v).strip() == "":
        return "na", None, None

    d = parse_date_any(v)
    if d is None:
        s = str(v).strip()
        return "unknown", s, None

    today = date.today()
    status = "valid" if d >= today else "expired"
    return status, d.strftime("%d-%b-%Y"), d
def validity_status_from_text(text: Any) -> str:
    """
    Safe helper for already-formatted validity text like '12-Apr-2026'.
    Returns: 'valid' | 'expired' | 'na'
    """
    if text is None:
        return "na"
    s = str(text).strip()
    if not s:
        return "na"

    d = parse_date_any(s)
    if d is None:
        return "na"

    return "valid" if d >= date.today() else "expired"



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
    
def parse_money_allow_empty(v) -> float:
    """
    Parses money-like input like: '200', '200$', '$200', '200.5', '200.50 USD'
    Returns 0.0 if empty/invalid.
    """
    num = parse_price_to_float(v)
    return float(num) if num is not None else 0.0


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

def extract_route_id(value: Any) -> str:
    """
    Extract route id like R2, R58 from:
      - 'R2'
      - 'R2 Karachi to Kabul via Chaman'
      - '["R2 Karachi...", "R7 Bandar..."]'
    Returns uppercase route id or "".
    """
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    m = re.search(r"\bR\d+\b", s, flags=re.IGNORECASE)
    return m.group(0).upper() if m else ""


def route_cell_matches_selected(cell_value: Any, selected_route_id: str, selected_route_text: str = "") -> bool:
    """
    Match the Excel 'routes' cell against the selected UI route.

    Supports cells like:
      - R2 Karachi to Kabul via Chaman
      - ['R2 Karachi to Kabul via Chaman', 'R7 Bandar Abbas to Herat via Islam Qala']
      - R2 Karachi to Kabul via Chaman, R7 Bandar Abbas to Herat via Islam Qala
      - R2 Karachi to Kabul via Chaman; R7 Bandar Abbas to Herat via Islam Qala
      - multi-line values

    Main match is by route id (R2, R7, etc.) so titles can vary slightly.
    """
    selected_id = extract_route_id(selected_route_id or selected_route_text)
    if not selected_id:
        return False

    if cell_value is None or pd.isna(cell_value):
        return False

    raw = str(cell_value).strip()
    if not raw:
        return False

    # Direct whole-token route-id match first
    if re.search(rf"(?<![A-Z0-9]){re.escape(selected_id)}(?![A-Z0-9])", raw, flags=re.IGNORECASE):
        return True

    # If the cell is a stringified Python/JSON-like list, split it loosely
    parts = re.split(r"[\n;,]+", raw)
    for part in parts:
        if re.search(rf"(?<![A-Z0-9]){re.escape(selected_id)}(?![A-Z0-9])", part.strip(), flags=re.IGNORECASE):
            return True

    return False

# -------------------------
# ROUTE STATUS HELPERS
# -------------------------
def normalize_route_status(val: Any) -> str:
    s = canon(val)
    if s in {"open", "closed", "not sure", "not used"}:
        return s
    return "open"


def route_status_rank(val: Any) -> int:
    """
    Lower rank = better route for sorting.
    Business order:
      open      -> best
      not sure  -> usable but uncertain
      not used  -> lower preference
      closed    -> last
    """
    s = normalize_route_status(val)
    if s == "open":
        return 0
    if s == "not sure":
        return 1
    if s == "not used":
        return 2
    if s == "closed":
        return 3
    return 4


def route_requires_confirmation(val: Any) -> bool:
    """
    Require user confirmation for risky route statuses.
    """
    s = normalize_route_status(val)
    return s in {"closed", "not sure", "not used"}


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


def route_base_match(
    pol: str,
    pod: str,
    route: dict,
    origin_city: str = "",
    destination_city: str = "",
    transit_borders: Optional[List[str]] = None
) -> Tuple[bool, bool, int]:
    """
    Match using:
      - pol_keywords
      - pod_keywords
      - origin_city_keywords
      - destination_city_keywords
      - must_borders (optional score boost if user provided matching borders)

    Returns:
      (matched, is_reverse, score)

    Rules:
      - POL + POD match is mandatory
      - origin/destination city improve route score if user provided them
      - transit border match improves route score if user provided borders
    """
    transit_borders = transit_borders or []

    pol_s = canon(pol)
    pod_s = canon(pod)
    org_s = canon(origin_city)
    dst_s = canon(destination_city)
    user_borders = [canon(x) for x in transit_borders if canon(x)]

    pol_keywords = route.get("pol_keywords", []) or []
    pod_keywords = route.get("pod_keywords", []) or []
    origin_city_keywords = route.get("origin_city_keywords", []) or []
    destination_city_keywords = route.get("destination_city_keywords", []) or []
    must_borders = route.get("must_borders", []) or []

    def any_match(text: str, keywords: List[str]) -> bool:
        return any(canon(k) and canon(k) in text for k in keywords)

    def border_score(route_borders: List[str], user_border_values: List[str]) -> int:
        if not route_borders or not user_border_values:
            return 0
        score = 0
        rb = [canon(x) for x in route_borders if canon(x)]
        for ub in user_border_values:
            if any(r and (r in ub or ub in r) for r in rb):
                score += 20
        return score

    # -------------------------
    # Forward direction
    # -------------------------
    pol_ok = any_match(pol_s, pol_keywords)
    pod_ok = any_match(pod_s, pod_keywords)

    origin_ok = False
    dest_ok = False

    if org_s and origin_city_keywords:
        origin_ok = any_match(org_s, origin_city_keywords)

    if dst_s and destination_city_keywords:
        dest_ok = any_match(dst_s, destination_city_keywords)

    forward_score = 0
    if pol_ok:
        forward_score += 100
    if pod_ok:
        forward_score += 100
    if origin_ok:
        forward_score += 35
    if dest_ok:
        forward_score += 35
    forward_score += border_score(must_borders, user_borders)

    forward_matched = pol_ok and pod_ok

    # -------------------------
    # Reverse direction
    # -------------------------
    pol_ok_rev = any_match(pod_s, pol_keywords)
    pod_ok_rev = any_match(pol_s, pod_keywords)

    origin_ok_rev = False
    dest_ok_rev = False

    if org_s and destination_city_keywords:
        origin_ok_rev = any_match(org_s, destination_city_keywords)

    if dst_s and origin_city_keywords:
        dest_ok_rev = any_match(dst_s, origin_city_keywords)

    reverse_score = 0
    if pol_ok_rev:
        reverse_score += 100
    if pod_ok_rev:
        reverse_score += 100
    if origin_ok_rev:
        reverse_score += 35
    if dest_ok_rev:
        reverse_score += 35
    reverse_score += border_score(must_borders, user_borders)

    reverse_matched = pol_ok_rev and pod_ok_rev

    if forward_matched and reverse_matched:
        if reverse_score > forward_score:
            return True, True, reverse_score
        return True, False, forward_score

    if forward_matched:
        return True, False, forward_score

    if reverse_matched:
        return True, True, reverse_score

    return False, False, 0


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

def get_matching_routes(
    pol: str,
    pod: str,
    origin_city: str = "",
    destination_city: str = "",
    transit_borders: Optional[List[str]] = None
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    routes_src = load_routes_json()
    matched: List[Dict[str, Any]] = []

    transit_borders = transit_borders or []

    for r in routes_src:
        ok, is_reverse, match_score = route_base_match(
            pol=pol,
            pod=pod,
            route=r,
            origin_city=origin_city,
            destination_city=destination_city,
            transit_borders=transit_borders
        )
        if not ok:
            continue

        rr = dict(r)
        rr["is_recent"] = False
        rr["is_custom"] = False
        rr["is_reverse"] = bool(is_reverse)
        rr["path"] = reverse_path(rr.get("path", "")) if is_reverse else rr.get("path", "")
        rr["route_status"] = normalize_route_status(rr.get("route_status"))
        rr["_tt_key"] = transit_time_key(rr)
        rr["_match_score"] = int(match_score)

        matched.append(rr)

    if not matched:
        return [], None
    matched.sort(
        key=lambda x: (
            route_status_rank(x.get("route_status", "")),
            -int(x.get("_match_score", 0)),
            x.get("_tt_key", (10**9, 10**9))
        )
    )
    best_id = matched[0].get("id")
    return matched, best_id

# -------------------------
# ROUTE HISTORY (DISABLED)
# -------------------------
def load_routes_history_df() -> pd.DataFrame:
    return pd.DataFrame(columns=["pol", "pod", "route_text", "created_at"])


def save_route_history(pol: str, pod: str, route_text: str):
    return


def get_recent_routes(pol: str, pod: str, limit: int = 5) -> List[Dict[str, Any]]:
    return []


# -------------------------
# EXCEL HELPERS
# -------------------------
def load_prices_df():
    try:
        content = download_excel_from_onedrive(ONEDRIVE_PRICES_PATH)

        # IMPORTANT: your file has 2 sheets → we use FIRST sheet (prices)
        return pd.read_excel(io.BytesIO(content), sheet_name=0)

    except Exception as e:
        print("Error loading prices from OneDrive:", e)
        return None


def save_to_excel(record: Dict[str, Any]):
    try:
        # download existing file
        content = download_excel_from_onedrive(ONEDRIVE_QUERIES_PATH)
        df_existing = pd.read_excel(io.BytesIO(content))

    except Exception:
        # file doesn't exist yet
        df_existing = pd.DataFrame()

    df_new = pd.DataFrame([record])
    df_final = pd.concat([df_existing, df_new], ignore_index=True)

    # save to memory
    buffer = io.BytesIO()
    df_final.to_excel(buffer, index=False)
    buffer.seek(0)

    # upload back to OneDrive
    upload_excel_to_onedrive(ONEDRIVE_QUERIES_PATH, buffer.read())


def get_commodities():
    commodities = list(BASE_COMMODITIES)
    try:
        df = read_queries_df_from_onedrive()
        if not df.empty:
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
    try:
        df = read_queries_df_from_onedrive()
        if not df.empty:
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
    try:
        df = read_queries_df_from_onedrive()
        if not df.empty:
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
    try:
        df = read_queries_df_from_onedrive()
        if not df.empty:
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
    """
    Treat normal *_charges as charges AND also treat these special cost columns as charges
    so they get validity + remove button + included in grand total.
    """
    c = canon(col_name)
    if c.endswith("_charges"):
        return True

    # ✅ NEW: special columns that behave like charges
    if c in {"labor_lifting_cost", "offloading_cost"}:
        return True

    return False



def compute_grand_total(row: pd.Series, columns: List[str]) -> Tuple[float, bool]:
    total = 0.0
    found_any = False

    # These trucking columns are calculated separately from the selected single row,
    # so do NOT include them in the generic best-row grand-total calculation.
    trucking_sheet_cols = {
        canon("trucking_charges_20ft"),
        canon("trucking_charges_40ft"),
        canon("trucking_charges_2x20ft"),
    }

    for col in columns:
        if not is_charges_column(col):
            continue
        if canon(col) in trucking_sheet_cols:
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



def _money_to_float(val) -> float:
    """Parse $1,200.00 / 1200 / '  $200 ' into float. Returns 0.0 if empty/bad."""
    if val is None:
        return 0.0
    s = str(val).strip()
    if not s:
        return 0.0
    # keep digits, dot, minus
    s2 = re.sub(r"[^0-9.\-]", "", s)
    try:
        return float(s2) if s2 else 0.0
    except Exception:
        return 0.0

def _fmt_money(n: float) -> str:
    return f"${n:,.2f}"

def _parse_date_any(d):
    """Parse many formats like 22/03/2026, 10-Jan-26, 2026-03-22."""
    if d is None:
        return None
    s = str(d).strip()
    if not s:
        return None
    dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        return None
    return dt.date()

from typing import Any

def _normalize_container_size(s: Any) -> str:
    """Normalize variations like '20ft ', ' 40FT', '2x20ft'. Accepts Any/None safely."""
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    t = str(s).strip().lower().replace(" ", "")
    t = t.replace("feet", "ft")
    if t in ("20", "20ft", "20f"):
        return "20ft"
    if t in ("40", "40ft", "40f"):
        return "40ft"
    if t in ("2x20", "2x20ft", "2*20ft", "2×20ft"):
        return "2x20ft"
    return str(s).strip()

def _get_adjacent_validity_column(df: pd.DataFrame, base_col: str) -> str | None:
    """
    Your sheet has many 'validity' columns. We pick the validity column that is
    immediately to the right of the given base column.
    """
    cols = list(df.columns)
    if base_col not in cols:
        return None
    i = cols.index(base_col)
    if i + 1 >= len(cols):
        return None
    nxt = cols[i + 1]
    # in excel it is "validity" (duplicated) -> pandas makes validity, validity.1, validity.2 ...
    if str(nxt).lower().startswith("validity"):
        return nxt
    return None

def compute_trucking_total_and_validity(
    matched_df: pd.DataFrame,
    qty_20: int,
    qty_40: int,
    qty_2x20: int,
    today: date | None = None
):
    """
    NEW trucking logic:
    - Read ALL trucking charges from ONE selected row only
    - Expected columns in the same row:
        trucking_charges_20ft      + next validity column
        trucking_charges_40ft      + next validity column
        trucking_charges_2x20ft    + next validity column
    - Multiply each by selected quantity
    - Show ONE validity:
        * if any selected size has valid date -> show soonest valid date
        * else if all selected are expired -> show soonest expired date
        * else blank

    Returns:
      total, validity_text, expired_sizes, used_dates, used_rates, missing_sizes
    """
    if today is None:
        today = date.today()

    wanted = []
    if qty_20 and qty_20 > 0:
        wanted.append(("20ft", qty_20, "trucking_charges_20ft"))
    if qty_40 and qty_40 > 0:
        wanted.append(("40ft", qty_40, "trucking_charges_40ft"))
    if qty_2x20 and qty_2x20 > 0:
        wanted.append(("2x20ft", qty_2x20, "trucking_charges_2x20ft"))

    if matched_df is None or matched_df.empty:
        return 0.0, "", [], [], {}, [sz for (sz, _, _) in wanted]

    # Use ONLY the first row of the passed dataframe
    row = matched_df.iloc[0]

    total = 0.0
    used_dates: List[Tuple[str, str, Optional[date]]] = []
    expired_sizes: List[str] = []
    used_rates: Dict[str, float] = {}
    missing_sizes: List[str] = []

    df_cols = list(matched_df.columns)

    for size_label, qty, charge_col_name in wanted:
        charge_col = find_col_case_insensitive(matched_df, charge_col_name)

        if not charge_col:
            used_rates[size_label] = 0.0
            used_dates.append((size_label, "unknown", None))
            missing_sizes.append(size_label)
            continue

        raw_rate = row.get(charge_col)
        rate = _money_to_float(raw_rate)

        if rate <= 0:
            used_rates[size_label] = 0.0
            used_dates.append((size_label, "unknown", None))
            missing_sizes.append(size_label)
            continue

        validity_col = None
        try:
            idx = df_cols.index(charge_col)
            if idx + 1 < len(df_cols) and str(df_cols[idx + 1]).strip().lower().startswith("validity"):
                validity_col = df_cols[idx + 1]
        except Exception:
            validity_col = None

        vdate = _parse_date_any(row.get(validity_col)) if validity_col else None

        if vdate is None:
            status = "unknown"
        elif vdate >= today:
            status = "valid"
        else:
            status = "expired"

        used_rates[size_label] = float(rate)
        total += float(rate) * float(qty)
        used_dates.append((size_label, status, vdate))

        if status == "expired":
            expired_sizes.append(size_label)

    valid_dates = [d for (_, st, d) in used_dates if st == "valid" and d is not None]
    expired_dates = [d for (_, st, d) in used_dates if st == "expired" and d is not None]

    show_date = None
    if valid_dates:
        show_date = min(valid_dates)
    elif expired_dates:
        show_date = min(expired_dates)

    validity_str = show_date.strftime("%d-%b-%Y") if show_date else ""

    return total, validity_str, expired_sizes, used_dates, used_rates, missing_sizes

# -------------------------
# QUOTE SEARCH
# -------------------------
def get_strict_quotes(
    pol_port: str,
    pod_port: str,
    incoterm_origin: str,
    incoterm_destination: str,

    origin_address: str,
    origin_city: str,
    origin_country: str,

    dest_address: str,
    dest_city: str,
    dest_country: str,

    container_size_label: str,

    # ✅ NEW: selected route match
    selected_route_id: str = "",
    selected_route_text: str = "",

    # ✅ NEW: container mix counts
    size_20ft_count: int = 0,
    size_40ft_count: int = 0,
    size_2x20ft_count: int = 0,

    special_cost_lines: Optional[List[Dict[str, Any]]] = None,


    # ✅ NEW: container ownership rules
    container_ownership: str = "",
    soc_clearance_cost_value: str = "",
    lifting_labor_required: str = "",
    offloading_responsible: str = "",

    insurance_amount_num: Optional[float] = None,
    misc_cost_value: str = "",

    limit: int = 1
):
    df = load_prices_df()
    if df is None or df.empty:
        return [], None, "Could not load prices_updated.xlsx properly. Please confirm the file exists and headers are correct."

    def col_exists(name: str) -> bool:
        return name in df.columns

    POL_COL = "POL"
    POD_COL = "POD"

    ORG_ADDR_COL = "wareshouse_address" if col_exists("wareshouse_address") else None
    ORG_CITY_COL = "city" if col_exists("city") else None
    ORG_COUNTRY_COL = "country" if col_exists("country") else None

    DST_ADDR_COL = "wareshouse_address.1" if col_exists("wareshouse_address.1") else None
    DST_CITY_COL = "city.1" if col_exists("city.1") else None
    DST_COUNTRY_COL = "country.1" if col_exists("country.1") else None

    if DST_ADDR_COL is None and col_exists("pod_wareshouse_address"):
        DST_ADDR_COL = "pod_wareshouse_address"
    if DST_CITY_COL is None and col_exists("pod_city"):
        DST_CITY_COL = "pod_city"
    if DST_COUNTRY_COL is None and col_exists("pod_country"):
        DST_COUNTRY_COL = "pod_country"

    if POL_COL not in df.columns or POD_COL not in df.columns:
        return [], None, "Missing required columns in prices_updated.xlsx: POL and/or POD"


    io = canon(incoterm_origin)
    idst = canon(incoterm_destination)

    origin_fields_open = (io.startswith("exw") or ("fca" in io) or ("fot" in io))
    dest_fields_required = (idst.startswith("dap") or idst.startswith("dpu") or idst.startswith("ddp") or idst.startswith("ddu"))
    dest_fields_optional = (idst.startswith("cpt") or idst.startswith("cip"))

    pol_key = canon(pol_port)
    pod_key = canon(pod_port)

    df["_pol_key"] = df[POL_COL].apply(canon)
    df["_pod_key"] = df[POD_COL].apply(canon)

    df_match = df[(df["_pol_key"] == pol_key) & (df["_pod_key"] == pod_key)].copy()

    if df_match.empty:
        return [], None, f"No matching rates found for POL='{pol_port}' and POD='{pod_port}'."

    # -------------------------
    # ✅ NEW: Route filter using Excel column 'routes'
    # The selected UI route must also match the row's routes cell
    # -------------------------
    routes_col = find_col_case_insensitive(df_match, "routes")

    selected_route_id_clean = extract_route_id(selected_route_id)
    selected_route_text_clean = (selected_route_text or "").strip()

    if selected_route_id_clean:
        if routes_col is None:
            return [], None, "Selected route was provided, but column 'routes' was not found in prices_updated.xlsx."

        # ✅ IMPORTANT:
        # Some Excel groups have the route text only on the first row and blanks below.
        # Forward-fill so sibling rows (like 20ft / 40ft trucking rows) still belong to the same selected route.
        df_match = df_match.copy()
        df_match[routes_col] = df_match[routes_col].ffill()

        df_match = df_match[
            df_match[routes_col].apply(
                lambda x: route_cell_matches_selected(
                    cell_value=x,
                    selected_route_id=selected_route_id_clean,
                    selected_route_text=selected_route_text_clean
                )
            )
        ].copy()

        if df_match.empty:
            return [], None, (
                f"No matching rates found for POL='{pol_port}', POD='{pod_port}' "
                f"and selected route='{selected_route_id_clean}'."
            )

    # ✅ Keep a relaxed copy for trucking BEFORE strict origin/destination address filters.
    # Trucking rows for 20ft / 40ft may exist in the same POL/POD/route group
    # but may not repeat all city/country/address values row-by-row.
    trucking_df = df_match.copy()

    # ✅ Ensure df_best is always defined
    df_best = df_match.head(1).copy()

    # -------------------------
    # Ocean dropdown options (valid rows only)
    # -------------------------
    ship_line_col = find_col_case_insensitive(df_match, "Shipping Line Name")
    of20_col = find_col_case_insensitive(df_match, "Ocean Freight (20ft)_charges")
    of40_col = find_col_case_insensitive(df_match, "Ocean Freight (40ft)_charges")

    # Ocean validity: in your sheet it's the validity column right after Ocean Freight (40ft)_charges
    validity_col = None
    if of40_col:
        cols_list = list(df_match.columns)
        try:
            idx40 = cols_list.index(of40_col)
            if idx40 + 1 < len(cols_list) and "validity" in canon(cols_list[idx40 + 1]):
                validity_col = cols_list[idx40 + 1]
        except Exception:
            validity_col = None

    # fallback only if not found (but adjacency is the main method)
    if not validity_col:
        validity_col = (
            find_col_case_insensitive(df_match, "Rates Validity")
            or find_col_case_insensitive(df_match, "Validity")
            or find_col_case_insensitive(df_match, "validity")
        )

    ocean_freight_options: List[Dict[str, Any]] = []
    today_opt = date.today()

    if ship_line_col and (of20_col or of40_col) and validity_col:
        for _, rr in df_match.iterrows():
            line_name = str(rr.get(ship_line_col, "")).strip()
            if not line_name:
                continue

            vd = parse_date_any(rr.get(validity_col))
            if vd is None or vd < today_opt:
                continue

            n20 = parse_price_to_float(rr.get(of20_col)) if of20_col else None
            n40 = parse_price_to_float(rr.get(of40_col)) if of40_col else None

            ocean_freight_options.append({
                "line": line_name,
                "validity": vd.strftime("%d-%b-%Y"),
                "amt20": fmt_money(n20) if n20 is not None else "N/A",
                "amt40": fmt_money(n40) if n40 is not None else "N/A",
                "amt20_num": float(n20) if n20 is not None else 0.0,
                "amt40_num": float(n40) if n40 is not None else 0.0,
            })

        seen = set()
        dedup = []
        for o in ocean_freight_options:
            k = (canon(o["line"]), o["validity"], o["amt20"], o["amt40"])
            if k in seen:
                continue
            seen.add(k)
            dedup.append(o)
        ocean_freight_options = sorted(dedup, key=lambda x: canon(x["line"]))

    # -------------------------
    # Address matching helper
    # -------------------------
    addr_warning_notes: List[str] = []

    def address_soft_match(user_addr: str, sheet_addr: Any) -> bool:
        ua = canon(user_addr)
        sa = canon(sheet_addr)
        if not ua or not sa:
            return False
        if ua in sa or sa in ua:
            return True
        ua_tokens = {t for t in ua.split() if len(t) >= 3}
        sa_tokens = {t for t in sa.split() if len(t) >= 3}
        return len(ua_tokens.intersection(sa_tokens)) >= 2

    # -------------------------
    # ORIGIN filters (only if origin fields open)
    # -------------------------
    if origin_fields_open:
        if origin_city and ORG_CITY_COL:
            oc = canon(origin_city)
            df_match = df_match[df_match[ORG_CITY_COL].apply(canon) == oc]

        if origin_country and ORG_COUNTRY_COL:
            oco = canon(origin_country)
            df_match = df_match[df_match[ORG_COUNTRY_COL].apply(canon) == oco]

        if df_match.empty:
            return [], None, "No match after applying Origin City/Country filters."

        if origin_address and ORG_ADDR_COL:
            any_addr = any(address_soft_match(origin_address, r.get(ORG_ADDR_COL)) for _, r in df_match.iterrows())
            if not any_addr:
                addr_warning_notes.append("⚠ Origin address not exact match, but POL/City/Country matched.")

    # -------------------------
    # DESTINATION filters
    # -------------------------
    if dest_fields_required or dest_fields_optional:
        use_city = True if dest_fields_required else bool(dest_city.strip())
        use_country = True if dest_fields_required else bool(dest_country.strip())

        if use_city and dest_city and DST_CITY_COL:
            dc = canon(dest_city)
            df_match = df_match[df_match[DST_CITY_COL].apply(canon) == dc]

        if use_country and dest_country and DST_COUNTRY_COL:
            dco = canon(dest_country)
            df_match = df_match[df_match[DST_COUNTRY_COL].apply(canon) == dco]

        if df_match.empty:
            return [], None, "No match after applying Destination City/Country filters."

        if dest_address and DST_ADDR_COL:
            any_addr = any(address_soft_match(dest_address, r.get(DST_ADDR_COL)) for _, r in df_match.iterrows())
            if not any_addr:
                addr_warning_notes.append("⚠ Destination address not exact match, but POD/City/Country matched.")
                
                
 
    # -------------------------
    # ✅ BEST ROW selection (NO global Rates Validity)
    # We select the lowest computed grand total among all matched rows.
    # -------------------------
    display_cols = [c for c in df_match.columns if not str(c).startswith("_")]

    totals, has_any = compute_grand_totals_for_df(df_match, display_cols)
    df_match["_grand_total_num"] = totals
    df_match["_grand_total_has"] = has_any

    any_with_total = df_match[df_match["_grand_total_has"] == True]
    if not any_with_total.empty:
        best_idx = any_with_total.sort_values("_grand_total_num").index[0]
    else:
        best_idx = df_match.index[0]

    df_best = df_match.loc[[best_idx]].copy()

# ✅ ALWAYS set df_best (this was the bug)
    #df_best = df_match.loc[[best_idx]].copy()

     # -------------------------
    # ✅ Trucking (CALCULATED from ONE selected row only)
    # Uses the already-selected best row (df_best), not multiple rows.
    # -------------------------
    trucking_total_calc = 0.0
    trucking_validity_text = ""
    trucking_expired_sizes: List[str] = []
    trucking_used_dates: List[Tuple[str, str, Optional[date]]] = []
    trucking_used_rates: Dict[str, float] = {}
    trucking_missing_sizes: List[str] = []

    try:
        (
            trucking_total_calc,
            trucking_validity_text,
            trucking_expired_sizes,
            trucking_used_dates,
            trucking_used_rates,
            trucking_missing_sizes,
        ) = compute_trucking_total_and_validity(
            matched_df=df_best,
            qty_20=size_20ft_count,
            qty_40=size_40ft_count,
            qty_2x20=size_2x20ft_count,
        )
    except Exception:
        trucking_total_calc = 0.0
        trucking_validity_text = ""
        trucking_expired_sizes = []
        trucking_used_dates = []
        trucking_used_rates = {}
        trucking_missing_sizes = []
    # -------------------------
    # Container ownership flags
    # -------------------------
    own_c = canon(container_ownership)
    is_soc_customer = (own_c == canon("SOC - Customer Owned"))
    is_soc_logenix = (own_c == canon("SOC - Logenix Owned"))
    is_coc = (own_c == canon("COC"))

    # helper to add a specific charges column from sheet + its validity (next validity column)
    def add_sheet_charge_row_if_present(table_rows: List[Dict[str, Any]], row: pd.Series, charge_col_name: str) -> bool:
        actual = find_col_case_insensitive(pd.DataFrame(columns=display_cols), charge_col_name) or charge_col_name
        actual = next((c for c in display_cols if canon(c) == canon(charge_col_name)), None)
        if not actual:
            return False

        raw_val = row.get(actual)
        if raw_val is None or pd.isna(raw_val) or str(raw_val).strip() == "":
            return False

        validity_text = ""
        validity_status = "na"
        idx = display_cols.index(actual)
        if idx + 1 < len(display_cols) and "validity" in canon(display_cols[idx + 1]):
            raw_validity = row.get(display_cols[idx + 1])
            validity_status, validity_fmt, _ = validity_status_and_text(raw_validity)
            validity_text = validity_fmt or ""

        num = parse_price_to_float(raw_val)
        cost_text = fmt_money(num) if num is not None else str(raw_val).strip()

        if any(canon(r.get("name", "")) == canon(actual) for r in table_rows):
            return True

        table_rows.append({
            "name": str(actual),
            "cost": cost_text,
            "validity": validity_text,
            "validity_status": validity_status,
            "can_remove": True,
            "include_in_total": True,
            "cost_num": float(num) if num is not None else 0.0,
            "is_grand_total": False,

            "can_change": False,
            "change_type": "",
            "ocean_size": "",
            "grand_kind": "",
            "options": []
        })
        return True

    # -------------------------
    # BUILD RESULT TABLE
    # -------------------------
    results: List[Dict[str, Any]] = []
    special_cost_lines = special_cost_lines or []
    
    for _, row in df_best.iterrows():
        validity_label = "Validity: As per individual charge validity column."
        validity_kind = "na"

        table_rows: List[Dict[str, Any]] = []

        def _row_exists(name: str) -> bool:
            return any(canon(r.get("name", "")) == canon(name) for r in table_rows)

        # ---- Shipping Line selector row
        if ship_line_col:
            line_val = str(row.get(ship_line_col, "")).strip()
            if line_val and not _row_exists("Shipping Line Name"):
                table_rows.append({
                    "name": "Shipping Line Name",
                    "cost": line_val,
                    "validity": "",
                    "can_remove": False,
                    "include_in_total": False,
                    "cost_num": 0.0,
                    "is_grand_total": False,

                    "can_change": True,
                    "change_type": "ocean_freight_line",
                    "ocean_size": "",
                    "grand_kind": "",
                    "options": ocean_freight_options
                })

        # -------------------------
        # IMPORTANT:
        # For container ownership rules, we SKIP these columns in the generic loop,
        # and add them explicitly only when applicable:
        # - SOC_Purchase_Price_charges => only for SOC - Logenix Owned
        # - COC_charges => only for COC
        # For SOC - Customer Owned => neither should be included.
        # -------------------------
        # Also skip Labor/Offloading here because we add them explicitly below
# based on user selections (Yes/No, Logenix/Customer).
        SKIP_SPECIAL = {
            canon("SOC_Purchase_Price_charges"),
            canon("COC_charges"),
            canon("Labor_lifting_cost"),
            canon("offloading_cost"),

            # ✅ New trucking columns from sheet must not render directly
            # because trucking is shown as one calculated row later.
            canon("trucking_charges_20ft"),
            canon("trucking_charges_40ft"),
            canon("trucking_charges_2x20ft"),
        }


        i = 0
        while i < len(display_cols):
            col = display_cols[i]
            raw = row.get(col)

            if raw is None or pd.isna(raw) or str(raw).strip() == "":
                i += 1
                continue

            col_c = canon(col)

            # Skip non-charges validity columns
            if ("validity" in col_c) and (not is_charges_column(col)):
                i += 1
                continue

            # ✅ skip SOC/COC special columns here; handled below based on ownership
            if canon(col) in SKIP_SPECIAL:
                # also skip its validity if next col is validity
                if i + 1 < len(display_cols) and "validity" in canon(display_cols[i + 1]):
                    i += 2
                else:
                    i += 1
                continue

            # ---- CHARGES
            if is_charges_column(col):
                is_ocean20 = (canon(col) == canon("Ocean Freight (20ft)_charges"))
                if is_ocean20 and (i + 1 < len(display_cols)) and (canon(display_cols[i + 1]) == canon("Ocean Freight (40ft)_charges")):
                    raw20 = row.get(col)
                    raw40 = row.get(display_cols[i + 1])

                    num20 = parse_price_to_float(raw20)
                    num40 = parse_price_to_float(raw40)

                    cost20 = fmt_money(num20) if num20 is not None else str(raw20).strip()
                    cost40 = fmt_money(num40) if num40 is not None else str(raw40).strip()
                    
                    validity_text = ""
                    validity_status = "na"
                    adv = 2
                    if i + 2 < len(display_cols) and "validity" in canon(display_cols[i + 2]):
                        raw_validity = row.get(display_cols[i + 2])
                        validity_status, validity_fmt, _ = validity_status_and_text(raw_validity)
                        validity_text = validity_fmt or ""
                        adv = 3

                    table_rows.append({
                        "name": "Ocean Freight (20ft)_charges",
                        "cost": cost20,
                        "validity": validity_text,
                        "validity_status": validity_status,
                        "can_remove": True,
                        "include_in_total": True,
                        "cost_num": float(num20) if num20 is not None else 0.0,
                        "is_grand_total": False,

                        "can_change": False,
                        "change_type": "",
                        "ocean_size": "20",
                        "grand_kind": "",
                        "options": []
                    })
                    table_rows.append({
                        "name": "Ocean Freight (40ft)_charges",
                        "cost": cost40,
                        "validity": validity_text,
                        "validity_status": validity_status,
                        "can_remove": True,
                        "include_in_total": True,
                        "cost_num": float(num40) if num40 is not None else 0.0,
                        "is_grand_total": False,

                        "can_change": False,
                        "change_type": "",
                        "ocean_size": "40",
                        "grand_kind": "",
                        "options": []
                    })

                    i += adv
                    continue

                num = parse_price_to_float(raw)
                cost_text = fmt_money(num) if num is not None else str(raw).strip()

                validity_text = ""
                validity_status = "na"
                if i + 1 < len(display_cols) and "validity" in canon(display_cols[i + 1]):
                    raw_validity = row.get(display_cols[i + 1])
                    validity_status, validity_fmt, _ = validity_status_and_text(raw_validity)
                    validity_text = validity_fmt or ""
                    i += 2
                else:
                    i += 1

                table_rows.append({
                    "name": str(col),
                    "cost": cost_text,
                    "validity": validity_text,
                    "validity_status": validity_status,
                    "can_remove": True,
                    "include_in_total": True,
                    "cost_num": float(num) if num is not None else 0.0,
                    "is_grand_total": False,

                    "can_change": False,
                    "change_type": "",
                    "ocean_size": "",
                    "grand_kind": "",
                    "options": []
                })
                continue

            # ---- Non-charges info
            if "date" in col_c or "validity" in col_c:
                cost_text = fmt_date_like(raw) or str(raw).strip()
            else:
                cost_text = str(raw).strip()

                table_rows.append({
                "name": str(col),
                "cost": cost_text,
                "validity": "",
                "validity_status": "na",
                "can_remove": False,
                "include_in_total": False,
                "cost_num": 0.0,
                "is_grand_total": False,

                "can_change": False,
                "change_type": "",
                "ocean_size": "",
                "grand_kind": "",
                "options": []
            })
            i += 1

        # -------------------------
        # ✅ Apply Container Ownership logic
        # -------------------------
        # SOC - Customer Owned => do NOT include any container purchase/COC charges
        # SOC - Logenix Owned => include SOC_Purchase_Price_charges + user's clearance charges
        # COC => include COC_charges (+ validity)
        if is_soc_logenix:
            # include SOC purchase price from sheet
            add_sheet_charge_row_if_present(table_rows, row, "SOC_Purchase_Price_charges")

            # include user custom clearance charges (removable)
            clearance_num = parse_money_allow_empty(soc_clearance_cost_value)
            if clearance_num > 0 and not _row_exists("SOC Custom Clearance_charges"):
                table_rows.append({
                    "name": "SOC Custom Clearance_charges",
                    "cost": fmt_money(clearance_num) or "$0.00",
                    "validity": "",
                    "can_remove": True,
                    "include_in_total": True,
                    "cost_num": float(clearance_num),
                    "is_grand_total": False,

                    "can_change": False,
                    "change_type": "",
                    "ocean_size": "",
                    "grand_kind": "",
                    "options": []
                })

        elif is_coc:
            add_sheet_charge_row_if_present(table_rows, row, "COC_charges")

        # -------------------------
        # ✅ Lifting / Labor Required Logic
        # -------------------------
        lifting_c = canon(lifting_labor_required)
        is_lifting_yes = (lifting_c == canon("Yes"))

        if is_lifting_yes:

            labor_col = next(
                (c for c in display_cols if canon(c) == canon("Labor_lifting_cost")),
                None
            )

            labor_validity_text = ""
            labor_cost_text = "N/A"
            labor_cost_num = 0.0

            if labor_col:
                raw_labor = row.get(labor_col)

                # validity next column
                idx_labor = display_cols.index(labor_col)
                if idx_labor + 1 < len(display_cols) and "validity" in canon(display_cols[idx_labor + 1]):
                    labor_validity_text = fmt_date_like(row.get(display_cols[idx_labor + 1])) or ""

                if raw_labor is not None and not pd.isna(raw_labor) and str(raw_labor).strip() != "":
                    parsed_labor = parse_price_to_float(raw_labor)
                    if parsed_labor is not None:
                        labor_cost_text = fmt_money(parsed_labor)
                        labor_cost_num = float(parsed_labor)
                    else:
                        labor_cost_text = str(raw_labor).strip()

            # prevent duplicate
            if not any(canon(r.get("name", "")) == canon("Labor_lifting_cost") for r in table_rows):
                table_rows.append({
                    "name": "Labor_lifting_cost",
                    "cost": labor_cost_text,
                     "validity": labor_validity_text,
                    "validity_status": validity_status_from_text(labor_validity_text),
                     "can_remove": True,

                    # Always included (N/A contributes 0 but removable)
                    "include_in_total": True,
                    "cost_num": labor_cost_num,
                    "is_grand_total": False,


                    "can_change": False,
                    "change_type": "",
                    "ocean_size": "",
                    "grand_kind": "",
                    "options": []
                })

        # -------------------------
        # ✅ Offloading Responsible Logic
        # -------------------------
        off_c = canon(offloading_responsible)
        is_offloading_logenix = (off_c == canon("Logenix"))

        if is_offloading_logenix:
            off_col = next(
                (c for c in display_cols if canon(c) == canon("offloading_cost")),
                None
            )

            off_validity_text = ""
            off_cost_text = "N/A"
            off_cost_num = 0.0

            if off_col:
                raw_off = row.get(off_col)

                # validity is expected right after offloading_cost (named validity like others)
                idx_off = display_cols.index(off_col)
                if idx_off + 1 < len(display_cols) and "validity" in canon(display_cols[idx_off + 1]):
                    off_validity_text = fmt_date_like(row.get(display_cols[idx_off + 1])) or ""

                # cost value
                if raw_off is not None and not pd.isna(raw_off) and str(raw_off).strip() != "":
                    parsed_off = parse_price_to_float(raw_off)
                    if parsed_off is not None:
                        off_cost_text = fmt_money(parsed_off) or str(raw_off).strip()
                        off_cost_num = float(parsed_off)
                    else:
                        off_cost_text = str(raw_off).strip()

            # add once, no duplicates
            if not any(canon(r.get("name", "")) == canon("offloading_cost") for r in table_rows):
                table_rows.append({
                    "name": "offloading_cost",
                    "cost": off_cost_text,
                    "validity": off_validity_text,
                    "validity_status": validity_status_from_text(off_validity_text),
                    "can_remove": True,

                    # Always included (N/A contributes 0 but removable)
                    "include_in_total": True,
                    "cost_num": off_cost_num,
                    "is_grand_total": False,


                    "can_change": False,
                    "change_type": "",
                    "ocean_size": "",
                    "grand_kind": "",
                    "options": []
                })
                


# ✅ Add CALCULATED Trucking Charges row (always show if user selected any containers)
        user_selected_any_trucking = (
            (size_20ft_count > 0) or (size_40ft_count > 0) or (size_2x20ft_count > 0)
        )

        if user_selected_any_trucking and not _row_exists("trucking_charges"):
            notes: List[str] = []
            for sz in (trucking_expired_sizes or []):
                notes.append(f"{sz} trucking is expired but included.")
            for sz in (trucking_missing_sizes or []):
                notes.append(f"{sz} trucking rate not found in selected row (shown as N/A).")
            trucking_note_text = " ".join(notes).strip()

            def _truck_rate_text(sz: str) -> str:
                r = float(trucking_used_rates.get(sz, 0.0) or 0.0)
                return (fmt_money(r) or "N/A") if r > 0 else "N/A"

            break_parts: List[str] = []
            if size_20ft_count > 0:
                break_parts.append(f"20ft {size_20ft_count}×{_truck_rate_text('20ft')}")
            if size_40ft_count > 0:
                break_parts.append(f"40ft {size_40ft_count}×{_truck_rate_text('40ft')}")
            if size_2x20ft_count > 0:
                break_parts.append(f"2x20ft {size_2x20ft_count}×{_truck_rate_text('2x20ft')}")

            trucking_breakdown_text = f"  ({', '.join(break_parts)})" if break_parts else ""

            cost_display = (fmt_money(trucking_total_calc) or "$0.00") if float(trucking_total_calc) > 0 else "N/A"

            table_rows.append({
                "name": "trucking_charges",
                "cost": f"{cost_display}{trucking_breakdown_text}",
                "validity": trucking_validity_text or "",
                 "validity_status": validity_status_from_text(trucking_validity_text),
                "can_remove": True,
                "include_in_total": True,
                "cost_num": float(trucking_total_calc) if float(trucking_total_calc) > 0 else 0.0,
                "is_grand_total": False,

                "can_change": False,
                "change_type": "",
                "ocean_size": "",
                "grand_kind": "",
                "options": [],

                "note": trucking_note_text
            })
        # -------------------------
        # Extras (insurance/misc/special) — ONCE ONLY
        # -------------------------
        extra_total = 0.0
        
        if insurance_amount_num is not None and float(insurance_amount_num) > 0:
            if not _row_exists("Insurance (Calculated)"):
                table_rows.append({
                    "name": "Insurance (Calculated)",
                    "cost": fmt_money(insurance_amount_num) or "$0.00",
                    "validity": "",
                    "validity_status": "na",
                    "can_remove": True,
                    "include_in_total": True,
                    "cost_num": float(insurance_amount_num),
                    "is_grand_total": False,

                    "can_change": False,
                    "change_type": "",
                    "ocean_size": "",
                    "grand_kind": "",
                    "options": []
                })
            extra_total += float(insurance_amount_num)

        misc_num = parse_money_allow_empty(misc_cost_value)
        if misc_num > 0:
            if not _row_exists("Miscellaneous Cost"):
                table_rows.append({
                    "name": "Miscellaneous Cost",
                    "cost": fmt_money(misc_num) or "$0.00",
                    "validity": "",
                    "validity_status": "na",
                    "can_remove": True,
                    "include_in_total": True,
                    "cost_num": float(misc_num),
                    "is_grand_total": False,

                    "can_change": False,
                    "change_type": "",
                    "ocean_size": "",
                    "grand_kind": "",
                    "options": []
                })
            extra_total += float(misc_num)

        special_total = 0.0
        for it in (special_cost_lines or []):
            label = (it.get("reason") or "").strip() or "Special Cost"
            amt = float(it.get("cost_num") or 0.0)
            if amt <= 0:
                continue
            row_name = f"Special Cost — {label}"
            if not _row_exists(row_name):
                table_rows.append({
                    "name": row_name,
                    "cost": fmt_money(amt) or "$0.00",
                    "validity": "",
                    "can_remove": True,
                    "include_in_total": True,
                    "cost_num": amt,
                    "is_grand_total": False,

                    "can_change": False,
                    "change_type": "",
                    "ocean_size": "",
                    "grand_kind": "",
                    "options": []
                })
            special_total += amt
        extra_total += special_total

        # -------------------------
        # Two Grand Totals (20 / 40)
        # -------------------------
        base_total_sheet = 0.0
        for rr in table_rows:
            if rr.get("include_in_total") and not rr.get("is_grand_total"):
                base_total_sheet += float(rr.get("cost_num") or 0.0)

        ocean20 = 0.0
        ocean40 = 0.0
        for rr in table_rows:
            if canon(rr.get("name", "")) == canon("Ocean Freight (20ft)_charges"):
                ocean20 = float(rr.get("cost_num") or 0.0)
            if canon(rr.get("name", "")) == canon("Ocean Freight (40ft)_charges"):
                ocean40 = float(rr.get("cost_num") or 0.0)

        # For 20ft: exclude ocean40
        gt20_num = float(base_total_sheet) - float(ocean40)

        # For 40ft: exclude ocean20
        gt40_num = float(base_total_sheet) - float(ocean20)

        table_rows.append({
            "name": "Grand total with 20ft container",
            "cost": fmt_money(gt20_num) or "$0.00",
            "validity": "",
            "validity_status": "na",
            "can_remove": False,
            "include_in_total": False,
            "cost_num": float(gt20_num),
            "is_grand_total": True,

            "can_change": False,
            "change_type": "",
            "ocean_size": "",
            "grand_kind": "20",
            "options": []
        })
        table_rows.append({
            "name": "Grand total with 40ft container",
            "cost": fmt_money(gt40_num) or "$0.00",
            "validity": "",
            "validity_status": "na",
            "can_remove": False,
            "include_in_total": False,
            "cost_num": float(gt40_num),
            "is_grand_total": True,

            "can_change": False,
            "change_type": "",
            "ocean_size": "",
            "grand_kind": "40",
            "options": []
        })

        results.append({
            "validity_label": validity_label,
            "validity_kind": validity_kind,
            "addr_warning": " ".join(addr_warning_notes).strip(),
            "table_rows": table_rows
        })

    best_text = "Best Option available based on rate validity and match."
    return results[: max(1, int(limit or 1))], best_text, None



# if results:
#     results = [next((x for x in results if x.get("is_best")), results[0])]
#     return results, best_text, None



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
    add("SOC Custom Clearance Charges", "soc_clearance_charges")
    add("Incoterm for Origin", "incoterm_origin")
    add("Incoterm for Destination", "incoterm_destination")
    add("Port of Load", "port_of_loading")
    add("Port of Destination", "port_of_destination")


    add("Pick Up Point 1 - Factory / Warehouse address", "shipping_from_1_address")
    add("Pick Up Point 1 - City", "shipping_from_1_city")
    add("Pick Up Point 1 - Country", "shipping_from_1_country")

    add("Pick Up Point 2 - Factory / Warehouse address", "shipping_from_2_address")
    add("Pick Up Point 2 - City", "shipping_from_2_city")
    add("Pick Up Point 2 - Country", "shipping_from_2_country")

    add("Pick Up Point 3 - Factory / Warehouse address", "shipping_from_3_address")
    add("Pick Up Point 3 - City", "shipping_from_3_city")
    add("Pick Up Point 3 - Country", "shipping_from_3_country")

    add("Pick Up Point 4 - Factory / Warehouse address", "shipping_from_4_address")
    add("Pick Up Point 4 - City", "shipping_from_4_city")
    add("Pick Up Point 4 - Country", "shipping_from_4_country")

    add("Delivery Point 1 - Factory / Warehouse address", "destination_1_address")
    add("Delivery Point 1 - City", "destination_1_city")
    add("Delivery Point 1 - Country", "destination_1_country")

    add("Delivery Point 2 - Factory / Warehouse address", "destination_2_address")
    add("Delivery Point 2 - City", "destination_2_city")
    add("Delivery Point 2 - Country", "destination_2_country")

    add("Delivery Point 3 - Factory / Warehouse address", "destination_3_address")
    add("Delivery Point 3 - City", "destination_3_city")
    add("Delivery Point 3 - Country", "destination_3_country")

    add("Delivery Point 4 - Factory / Warehouse address", "destination_4_address")
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
    add("MSDS Available?", "msds_available")
    add("DG Class Number", "dg_class_number")
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
    add("Open Top: In-cage / Out-of-cage", "open_top_cage_option")
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
    for i in range(1, 11):
        add(f"Special Reason {i}", f"special_reason_{i}")
        add(f"Special Cost {i}", f"special_cost_{i}")

# ✅ add total only ONCE (outside loop)
    add("Special Costs Total", "special_cost_total")

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

from flask import jsonify

def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())

def build_routes_for_pol_pod(pol: str, pod: str):
    """
    Returns: (routes_list, best_route_id, route_error_msg)

    Expected route dict keys (flexible):
    - id
    - title
    - path
    - pol / pod   (or port_of_loading / port_of_destination)
    - route_status (open/closed)
    - transit_time_days: {min,max} OR transit_min/transit_max
    """
    pol_n = _norm(pol)
    pod_n = _norm(pod)

    routes = []
    best_route_id = None
    route_error_msg = ""

    # You already have ROUTES_JSON_FILE = "routes.json"
    if not os.path.exists(ROUTES_JSON_FILE):
        return [], None, "routes.json not found"

    try:
        with open(ROUTES_JSON_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        return [], None, f"Failed to read routes.json: {e}"

    # data can be list or dict wrapper
    all_routes = data.get("routes") if isinstance(data, dict) else data
    if not isinstance(all_routes, list):
        return [], None, "Invalid routes.json format"

    for r in all_routes:
        if not isinstance(r, dict):
            continue

        r_pol = _norm(r.get("pol") or r.get("port_of_loading") or "")
        r_pod = _norm(r.get("pod") or r.get("port_of_destination") or "")

        # Strict match (same behavior as old stage routes should have)
        if r_pol == pol_n and r_pod == pod_n:
            routes.append(r)

    if not routes:
        return [], None, ""

    # pick "best" route:
    # 1) prefer open
    # 2) then prefer smallest transit min
    def _transit_min(rr):
        t = rr.get("transit_time_days") or {}
        if isinstance(t, dict) and t.get("min") is not None:
            return t.get("min")
        if rr.get("transit_min") is not None:
            return rr.get("transit_min")
        return 10**9

    routes_sorted = sorted(
        routes,
        key=lambda rr: (
            route_status_rank(rr.get("route_status", "")),
            _transit_min(rr)
        )
    )
    best_route_id = routes_sorted[0].get("id")

    return routes, best_route_id, route_error_msg


@app.post("/api/routes")
def api_routes():
    pol = (request.form.get("port_of_loading") or "").strip()
    pod = (request.form.get("port_of_destination") or "").strip()
    origin_city = (request.form.get("origin_city") or "").strip()
    destination_city = (request.form.get("destination_city") or "").strip()

    transit_borders = [
        (request.form.get("transit_border_1") or "").strip(),
        (request.form.get("transit_border_2") or "").strip(),
        (request.form.get("transit_border_3") or "").strip(),
        (request.form.get("transit_border_4") or "").strip(),
    ]
    if not pol or not pod:
        return jsonify({"ok": False, "routes": [], "best_route_id": None, "route_error_msg": ""}), 200

    routes, best_route_id = get_matching_routes(
        pol=pol,
        pod=pod,
        origin_city=origin_city,
        destination_city=destination_city,
        transit_borders=transit_borders
    )
    route_error_msg = ""

    # Normalize payload for frontend
    payload = []
    for r in routes:
        t = r.get("transit_time_days") if isinstance(r.get("transit_time_days"), dict) else {}
        payload.append({
            "id": str(r.get("id", "")),
            "title": r.get("title", "") or f"Route {r.get('id','')}",
            "path": r.get("path", "") or "",
            "route_status": (r.get("route_status") or "open").lower(),
            "is_recent": bool(r.get("is_recent", False)),
            "transit_min": t.get("min") if isinstance(t, dict) else r.get("transit_min"),
            "transit_max": t.get("max") if isinstance(t, dict) else r.get("transit_max"),
        })

    return jsonify({
        "ok": True,
        "routes": payload,
        "best_route_id": str(best_route_id) if best_route_id is not None else None,
        "route_error_msg": route_error_msg or ""
    }), 200

@app.route("/submit", methods=["POST"])
def submit():
    action = request.form.get("_action", "").strip().lower()
    if action not in ("next", "generate"):
    # if frontend didn't send _action properly, assume generate if route is present
        action = "generate" if (request.form.get("selected_route_id") or "").strip() else "next"
    route_error_msg: Optional[str] = None  # ✅ FIX: always defined
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
        "city": request.form.get(f"shipping_from_{i}_city", "").strip(),
        "country": request.form.get(f"shipping_from_{i}_country", "").strip(),
    })
        pods.append({
        "address": request.form.get(f"destination_{i}_address", "").strip(),
        "city": request.form.get(f"destination_{i}_city", "").strip(),
        "country": request.form.get(f"destination_{i}_country", "").strip(),
    })

    # -------------------------
    # Basic fields
    # -------------------------
    company_name = request.form.get("company_name", "").strip()
    salesperson_name = request.form.get("salesperson_name", "").strip()
    container_ownership = request.form.get("container_ownership", "").strip()
    soc_clearance_charges_raw = request.form.get("soc_clearance_charges", "").strip()
    soc_clearance_charges_saved = soc_clearance_charges_raw
    incoterm_origin = request.form.get("incoterm_origin", "").strip()
    incoterm_destination = request.form.get("incoterm_destination", "").strip()
    port_of_loading = request.form.get("port_of_loading", "").strip()
    port_of_destination = request.form.get("port_of_destination", "").strip()
# -------------------------
# INCOTERM SMART FALLBACK FOR ROUTE MATCH
# If address fields are locked by Incoterm, city might be empty.
# Use POL/POD as fallback so routing & pricing still work.
# -------------------------
    inc_origin_code = canon(incoterm_origin).split("-")[0].strip().upper()
    inc_dest_code = canon(incoterm_destination).split("-")[0].strip().upper()

# Origin address OPEN only for EXW/FCA/FOT, else treated as locked
    origin_open = inc_origin_code in {"EXW", "FCA", "FOT"}

# Delivery address REQUIRED for DAP/DPU/DDP/DDU, OPTIONAL for CPT/CIP, else locked
    delivery_required = inc_dest_code in {"DAP", "DPU", "DDP", "DDU"}
    delivery_optional = inc_dest_code in {"CPT", "CIP"}

    shipping_from_1_city = pols[0]["city"] if len(pols) > 0 else ""
    destination_1_city = pods[0]["city"] if len(pods) > 0 else ""
    if (not origin_open) and (not shipping_from_1_city):
        shipping_from_1_city = port_of_loading
    
    if (not delivery_required) and (not delivery_optional) and (not destination_1_city):
            destination_1_city = port_of_destination


    shipment_type = request.form.get("shipment_type", "").strip()

    lifting_labor_required = request.form.get("lifting_labor_required", "").strip()
    offloading_responsible = request.form.get("offloading_responsible", "").strip()
    final_customs_responsible = request.form.get("final_customs_responsible", "").strip()

    # Transit borders (OPTIONAL)
    transit_border_1 = request.form.get("transit_border_1", "").strip()
    transit_border_2 = request.form.get("transit_border_2", "").strip()
    transit_border_3 = request.form.get("transit_border_3", "").strip()
    transit_border_4 = request.form.get("transit_border_4", "").strip()


    # keep as number if possible, else keep raw string (same pattern as cbm handling)

    cargo_type = request.form.get("cargo_type", "").strip()
    packaging_type = request.form.get("packaging_type", "").strip()
        # -------------------------
# DG Fields (only when cargo_type is DG)
# -------------------------
    msds_available = request.form.get("msds_available", "").strip()
    dg_class_number = request.form.get("dg_class_number", "").strip()
    is_dg = canon(cargo_type) == canon("DG Dangerous / Hazardous Cargo")


    if not is_dg:
        msds_available = ""
        dg_class_number = ""
    else:
    # If DG, MSDS should be Yes/No (optional but recommended)
        msds_c = canon(msds_available)
        if msds_c not in {canon("Yes"), canon("No")}:
        # keep it blank if weird, frontend usually sends correctly
            msds_available = ""

    # DG Class only allowed if MSDS is Yes
        if canon(msds_available) != canon("Yes"):
            dg_class_number = ""

    # Strict requirement: if MSDS is Yes, DG Class must be provided
        if canon(msds_available) == canon("Yes") and not dg_class_number:
            route_error_msg = "DG Class Number is required when MSDS Available is Yes."

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
    # New: Open Top Cage Option (only for Open Top Container)
    open_top_cage_option = request.form.get("open_top_cage_option", "").strip()

    ct_canon = canon(container_type)
    is_open_top_container = (ct_canon == canon("Open Top Container"))

# If not Open Top -> clear it (so Excel doesn't get garbage)
    if not is_open_top_container:
        open_top_cage_option = ""
    else:
    # Only allow one of the two expected values
        opt_c = canon(open_top_cage_option)
        allowed = {canon("In-cage"), canon("Out-of-cage")}
        if opt_c not in allowed:
            open_top_cage_option = ""

    size_20ft_selected = request.form.get("size_20ft_selected", "").strip().lower()
    size_40ft_selected = request.form.get("size_40ft_selected", "").strip().lower()
    size_2x20ft_selected = request.form.get("size_2x20ft_selected", "").strip().lower()

    size_20ft_count_raw = request.form.get("size_20ft_count", "").strip()
    size_40ft_count_raw = request.form.get("size_40ft_count", "").strip()
    size_2x20ft_count_raw = request.form.get("size_2x20ft_count", "").strip()


    def to_int_or_zero(x: str) -> int:
        try:
            v = int(x)
            return v if v > 0 else 0
        except Exception:
            return 0

    size_20ft_count = to_int_or_zero(size_20ft_count_raw)
    size_40ft_count = to_int_or_zero(size_40ft_count_raw)
    size_2x20ft_count = to_int_or_zero(size_2x20ft_count_raw)

    total_containers = size_20ft_count + size_40ft_count + size_2x20ft_count
    if total_containers <= 0:
        # fallback safety (frontend already checks this)
        total_containers = 0


    size_labels: List[str] = []
    if size_20ft_count > 0:
        size_labels.append("20ft")
    if size_40ft_count > 0:
        size_labels.append("40ft")
    if size_2x20ft_count > 0:
        size_labels.append("2x20ft")
    container_size_summary = " & ".join(size_labels) if size_labels else ""

    num_containers = total_containers

    # Dimensions / Temperature
    width_ft = request.form.get("width_ft", "").strip()
    height_ft = request.form.get("height_ft", "").strip()
    temperature_c = request.form.get("temperature_c", "").strip()
    # Normalize common text (optional)
    if canon(temperature_c) in {"room temp", "room temperature", "ambient", "ambient temperature"}:
        temperature_c = "room temperature"


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
    special_cost_items: List[Dict[str, Any]] = []
    special_cost_total = 0.0
    for i in range(1, 11):
        r = request.form.get(f"special_reason_{i}", "").strip()
        c_raw = request.form.get(f"special_cost_{i}", "").strip()
        if r or c_raw:
            c_val = parse_money_allow_empty(c_raw)
            special_cost_items.append({
            "reason": r,
            "cost_raw": c_raw,
            "cost_num": c_val
        })
            special_cost_total += c_val


    # -------------------------
    # build form_data for repopulation
    # -------------------------
    form_data: Dict[str, Any] = {
        "company_name": company_name,
        "salesperson_name": salesperson_name,
        "container_ownership": container_ownership,
        "soc_clearance_charges": soc_clearance_charges_saved,
        "incoterm_origin": incoterm_origin,
        "incoterm_destination": incoterm_destination,

        "shipping_from_1_address": pols[0]["address"],
        "shipping_from_1_city": pols[0]["city"],
        "shipping_from_1_country": pols[0]["country"],

        "shipping_from_2_address": pols[1]["address"],
        "shipping_from_2_city": pols[1]["city"],
        "shipping_from_2_country": pols[1]["country"],

        "shipping_from_3_address": pols[2]["address"],
        "shipping_from_3_city": pols[2]["city"],
        "shipping_from_3_country": pols[2]["country"],

        "shipping_from_4_address": pols[3]["address"],
        "shipping_from_4_city": pols[3]["city"],
        "shipping_from_4_country": pols[3]["country"],

        "destination_1_address": pods[0]["address"],   
        "destination_1_city": pods[0]["city"],
        "destination_1_country": pods[0]["country"],

        "destination_2_address": pods[1]["address"],
        "destination_2_city": pods[1]["city"],
        "destination_2_country": pods[1]["country"],

        "destination_3_address": pods[2]["address"],
        "destination_3_city": pods[2]["city"],
        "destination_3_country": pods[2]["country"],

        "destination_4_address": pods[3]["address"],
        "destination_4_city": pods[3]["city"],
        "destination_4_country": pods[3]["country"],

        "port_of_loading": port_of_loading,
        "port_of_destination": port_of_destination,


        "transit_border_1": transit_border_1,
        "transit_border_2": transit_border_2,
        "transit_border_3": transit_border_3,
        "transit_border_4": transit_border_4,


        "cargo_type": cargo_type,
        "packaging_type": packaging_type,
        "msds_available": msds_available,
        "dg_class_number": dg_class_number,
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
        "open_top_cage_option": open_top_cage_option,
        "container_size": container_size_summary,
        "num_containers": str(num_containers) if num_containers else "",
               "size_20ft_selected": "yes" if size_20ft_selected == "yes" or size_20ft_count > 0 else "",
        "size_20ft_count": size_20ft_count_raw,

        "size_40ft_selected": "yes" if size_40ft_selected == "yes" or size_40ft_count > 0 else "",
        "size_40ft_count": size_40ft_count_raw,

        "size_2x20ft_selected": "yes" if size_2x20ft_selected == "yes" or size_2x20ft_count > 0 else "",
        "size_2x20ft_count": size_2x20ft_count_raw,


        "width_ft": width_ft,
        "height_ft": height_ft,
        "temperature_c": temperature_c,

        "cargo_value": cargo_value_raw,
        "insurance_rate": insurance_rate_raw,
        "misc_cost": misc_cost_saved,

        "special_cost_option": special_cost_option,
        "shipment_type": shipment_type,
    }
    for i in range(1, 11):
        form_data[f"special_reason_{i}"] = request.form.get(f"special_reason_{i}", "").strip()
        form_data[f"special_cost_{i}"] = request.form.get(f"special_cost_{i}", "").strip()
        form_data["special_reasons"] = [it.get("reason", "") for it in special_cost_items]
        form_data["special_costs"] = [it.get("cost_raw", "") for it in special_cost_items]


    # -------------------------
    # ROUTE MATCHING (POL/POD only)
    # -------------------------
    matched_routes, best_route_id = get_matching_routes(
        pol=port_of_loading,
        pod=port_of_destination,
        origin_city=shipping_from_1_city,
        destination_city=destination_1_city,
         transit_borders=[
            transit_border_1,
            transit_border_2,
            transit_border_3,
            transit_border_4,
        ]
    )

    recent_routes = get_recent_routes(port_of_loading, port_of_destination, limit=5)
    all_routes = matched_routes + recent_routes

    best_route_id_all: Optional[str] = None
    if all_routes:
        all_routes_sorted = sorted(
            all_routes,
            key=lambda x: (
                route_status_rank(x.get("route_status", "")),
                -int(x.get("_match_score", 0)),
                x.get("_tt_key", (10**9, 10**9))
            )
        )
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
    selected_route_id = (request.form.get("selected_route_id", "") or request.form.get("selected_route", "")).strip()

    own_route_text = (
        request.form.get("own_route_text", "")
            or request.form.get("custom_route_text", "")
            or request.form.get("selected_route_text", "")
    ).strip()
    confirm_closed = (request.form.get("confirm_closed_route", "") or request.form.get("confirm_closed", "")).strip().lower()

    # ✅ Extra hard validation: user must pick a route or OWN
    if not selected_route_id:
        route_error_msg = "Please select one route or choose 'My own route'."
    elif selected_route_id == "OWN" and not own_route_text:
        route_error_msg = "Please type your own route."

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
                pol_ok = canon(port_of_loading) in canon(own_route_text)
                pod_ok = canon(port_of_destination) in canon(own_route_text)
                if not (pol_ok and pod_ok):
                    route_error_msg = "Your custom route must contain Pick Up City and Delivery City."
                else:
                    selected_route_text = own_route_text.strip()
                    selected_route_status = "open"
                    selected_route_transit_days = ""
                    save_route_history(port_of_loading, port_of_destination, selected_route_text)
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

                if route_requires_confirmation(selected_route_status) and confirm_closed != "yes":
                    if selected_route_status == "closed":
                        route_error_msg = "Your selected route is CLOSED. Please confirm you want to proceed with this closed route."
                    elif selected_route_status == "not sure":
                        route_error_msg = "Your selected route is marked NOT SURE. Please confirm you want to proceed with this uncertain route."
                    elif selected_route_status == "not used":
                        route_error_msg = "Your selected route is marked NOT USED. Please confirm you want to proceed with this route."
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
                    save_route_history(port_of_loading, port_of_destination, selected_route_text)
        else:
            route_error_msg = "No routes found for now. Please choose 'My own route' and type your route."

    # Validate mandatory dynamic fields
    ct = canon(container_type)
    is_open_or_flat = ("open top" in ct) or ("flat rack" in ct)
    is_reefer = ("reefer" in ct)
    is_open_top_exact = (ct == canon("Open Top Container"))

    if is_open_or_flat:
        if not width_ft or not height_ft:
            route_error_msg = "Width and Height are required for Open Top / Flat Rack."

# ✅ New: Open Top requires In-cage / Out-of-cage
    if is_open_top_exact:
        if canon(open_top_cage_option) not in {canon("In-cage"), canon("Out-of-cage")}:
            route_error_msg = "Please select In-cage or Out-of-cage for Open Top Container."

    if is_reefer:
        if not temperature_c or not str(temperature_c).strip():
            route_error_msg = "Temperature is required for Reefer."


    data: Dict[str, Any] = {
        "quote_id": f"QUOTE-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        "company_name": company_name,
        "salesperson_name": salesperson_name,
        "container_ownership": container_ownership,
        "soc_clearance_charges": soc_clearance_charges_saved,
        "incoterm_origin": incoterm_origin,
        "incoterm_destination": incoterm_destination,

        "shipping_from_1_address": pols[0]["address"],
        "shipping_from_1_city": pols[0]["city"],
        "shipping_from_1_country": pols[0]["country"],

        "shipping_from_2_address": pols[1]["address"],
        "shipping_from_2_city": pols[1]["city"],
        "shipping_from_2_country": pols[1]["country"],

        "shipping_from_3_address": pols[2]["address"],
        "shipping_from_3_city": pols[2]["city"],
        "shipping_from_3_country": pols[2]["country"],

        "shipping_from_4_address": pols[3]["address"],
        "shipping_from_4_city": pols[3]["city"],
        "shipping_from_4_country": pols[3]["country"],

        "destination_1_address": pods[0]["address"],
        "destination_1_city": pods[0]["city"],
        "destination_1_country": pods[0]["country"],

        "destination_2_address": pods[1]["address"],
        "destination_2_city": pods[1]["city"],
        "destination_2_country": pods[1]["country"],

        "destination_3_address": pods[2]["address"],
        "destination_3_city": pods[2]["city"],
        "destination_3_country": pods[2]["country"],

        "destination_4_address": pods[3]["address"],
        "destination_4_city": pods[3]["city"],
        "destination_4_country": pods[3]["country"],

        "port_of_loading": port_of_loading,
        "port_of_destination": port_of_destination,



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
        "msds_available": msds_available,
        "dg_class_number": dg_class_number,
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
        "open_top_cage_option": open_top_cage_option,
        "container_size": container_size_summary,
        "num_containers": num_containers,
        "size_20ft_selected": "Yes" if size_20ft_count > 0 else "",
        "size_20ft_count": size_20ft_count,

        "size_40ft_selected": "Yes" if size_40ft_count > 0 else "",
        "size_40ft_count": size_40ft_count,

        "size_2x20ft_selected": "Yes" if size_2x20ft_count > 0 else "",
        "size_2x20ft_count": size_2x20ft_count,


        "width_ft": width_ft,
        "height_ft": height_ft,
        "temperature_c": temperature_c,

        "cargo_value": cargo_value_saved,
        "insurance_rate": insurance_rate_saved,
        "insurance_amount": insurance_amount_saved,
        "misc_cost": misc_cost_saved,

        "special_cost_option": special_cost_option,
        "special_cost_total": float(special_cost_total),
        "shipment_type": shipment_type,

        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }
    for i in range(1, 11):
        data[f"special_reason_{i}"] = request.form.get(f"special_reason_{i}", "").strip()
        data[f"special_cost_{i}"] = request.form.get(f"special_cost_{i}", "").strip()


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
    
    rates, best_text, error_msg = get_strict_quotes(
    pol_port=port_of_loading,
    pod_port=port_of_destination,

    incoterm_origin=incoterm_origin,
    incoterm_destination=incoterm_destination,

    origin_address=pols[0]["address"],
    origin_city=pols[0]["city"],
    origin_country=pols[0]["country"],

    dest_address=pods[0]["address"],
    dest_city=pods[0]["city"],
    dest_country=pods[0]["country"],

    container_size_label=(container_size_summary if container_size_summary else ""),
    
    # ✅ NEW: selected route filter
    selected_route_id=selected_route_id,
    selected_route_text=selected_route_text if selected_route_text else "",

    size_20ft_count=size_20ft_count,
    size_40ft_count=size_40ft_count,
    size_2x20ft_count=size_2x20ft_count,

    container_ownership=container_ownership,
    soc_clearance_cost_value=soc_clearance_charges_saved,
    lifting_labor_required=lifting_labor_required,
    offloading_responsible=offloading_responsible,

    special_cost_lines=special_cost_items,

    # ✅ NEW correct extras
    insurance_amount_num=insurance_amount_num,
    misc_cost_value=misc_cost_saved,

    limit=1
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
    try:
        _ = download_excel_from_onedrive(ONEDRIVE_PRICES_PATH)
        print("[OK] prices_updated.xlsx reachable on OneDrive")
    except Exception as e:
        print("[ERROR] OneDrive prices file check failed:", e)

    try:
        _ = download_excel_from_onedrive(ONEDRIVE_QUERIES_PATH)
        print("[OK] queries.xlsx reachable on OneDrive")
    except Exception as e:
        print("[ERROR] OneDrive queries file check failed:", e)

    app.run(debug=True)