from __future__ import annotations

from flask import Flask, request, render_template
import pandas as pd
import os
import re
import json
import requests
import io
import time
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


def upload_excel_to_onedrive(file_path: str, content: bytes, retries: int = 3, retry_delay: float = 1.5):
    token = get_access_token()
    url = _graph_drive_content_url(file_path)

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    }

    last_err = None

    for attempt in range(retries):
        try:
            r = requests.put(url, headers=headers, data=content, timeout=120)
            r.raise_for_status()
            return
        except requests.exceptions.HTTPError as e:
            last_err = e
            status = e.response.status_code if e.response is not None else None

            # Common temporary Graph/OneDrive conflicts
            if status in (409, 423, 429, 500, 502, 503, 504):
                if attempt < retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
            raise

    if last_err:
        raise last_err


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

def normalize_location_key(s: Any) -> str:
    """
    Normalizes ports / cities / place text so:
    'Shanghai' matches 'Shanghai Port'
    'Karachi' matches 'Karachi Port'
    'Port of Karachi' matches 'Karachi Port'
    """
    t = canon(s)
    if not t:
        return ""

    t = t.replace("/", " ").replace(",", " ")
    t = re.sub(
        r"\b(port of discharge|port of destination|port of loading|port of load|"
        r"seaport|sea port|dry port|port|harbor|harbour|terminal|pod|pol)\b",
        " ",
        t,
        flags=re.IGNORECASE,
    )
    t = re.sub(r"\s+", " ", t).strip()
    return t


def flexible_text_match(user_value: Any, sheet_value: Any) -> bool:
    """
    Flexible match for city/country/address-like text.
    Accept exact match OR one-side-contained match.
    """
    u = canon(user_value)
    s = canon(sheet_value)

    if not u or not s:
        return False

    return u == s or u in s or s in u


def flexible_location_match(user_value: Any, sheet_value: Any) -> bool:
    """
    Flexible match for POL/POD/port-like values.
    """
    u = normalize_location_key(user_value)
    s = normalize_location_key(sheet_value)

    if not u or not s:
        return False

    return u == s or u in s or s in u

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

        routes = data.get("routes") if isinstance(data, dict) else data
        if not isinstance(routes, list):
            return []

        out: List[Dict[str, Any]] = []
        for r in routes:
            if isinstance(r, dict) and r.get("id"):
                out.append(r)
        return out
    except Exception:
        return []


def normalize_route_type(val: Any) -> str:
    s = canon(val)
    allowed = {
        "pickup_to_pol_to_pod_to_final",
        "pol_to_pod_to_final",
        "pol_to_pod",
        "city_to_country_to_city",
        "city_to_pol_to_pod",
        "pol_to_city",
        "city_to_city",
        "city_to_pol_to_pod_to_city",
        "city_to_pol",
        "pol_to_pod_to_city",
        "city_to_pod_to_city",
    }
    return s if s in allowed else "city_to_city"


def normalize_route_modes(route: Dict[str, Any]) -> List[str]:
    raw = route.get("modes")
    if isinstance(raw, list):
        vals = [canon(x) for x in raw if canon(x)]
    elif raw:
        vals = [canon(raw)]
    else:
        vals = []

    cleaned: List[str] = []
    for v in vals:
        if v in {"land", "sea", "rail"} and v not in cleaned:
            cleaned.append(v)

    if cleaned:
        return cleaned

    # fallback by route type
    rt = normalize_route_type(route.get("route_type"))

    if rt == "pol_to_pod":
        return ["sea"]

    if rt == "pol_to_city":
        return ["land"]

    if rt == "pol_to_pod_to_city":
        return ["land", "sea"]

    if rt == "city_to_city":
        return ["land"]

    if rt == "city_to_country_to_city":
        return ["land"]

    if rt == "city_to_pol":
        return ["land"]

    if rt == "city_to_pol_to_pod":
        return ["land", "sea"]

    if rt == "city_to_pol_to_pod_to_city":
        return ["land", "sea"]

    if rt == "pickup_to_pol_to_pod_to_final":
        return ["land", "sea"]

    if rt == "pol_to_pod_to_final":
        return ["land", "sea"]

    return ["sea"]


def route_mode_label(route: Dict[str, Any]) -> str:
    modes = normalize_route_modes(route)
    if not modes:
        return ""
    return " + ".join(m.title() for m in modes)


def route_status_label(route: Dict[str, Any]) -> str:
    status = normalize_route_status(route.get("route_status"))
    if status == "open":
        return "Open"
    if status == "not sure":
        return "Not Sure"
    if status == "not used":
        return "Not Used"
    if status == "closed":
        return "Closed"
    return "Open"


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

def route_specificity_rank(route: Dict[str, Any]) -> int:
    """
    Lower is better.
    Prefer exact/specific routes over grouped/generic titles like:
    'Shanghai / Ningbo / Qingdao ...'
    """
    title = str(route.get("title", "") or "")
    path = str(route.get("path", "") or "")

    rank = 0

    # Penalize grouped titles/paths
    if " / " in title:
        rank += 2
    if " / " in path.split("→")[0]:
        rank += 1

    # Prefer routes with fewer POL keywords
    pol_keywords = route.get("pol_keywords") or []
    if isinstance(pol_keywords, list):
        if len(pol_keywords) >= 5:
            rank += 2
        elif len(pol_keywords) >= 3:
            rank += 1

    return rank

def _norm_kw_value(val: Any, is_port: bool = False) -> str:
    if is_port:
        return normalize_location_key(val)
    return canon(val)


def _value_matches_keywords(value: Any, keywords: List[str], is_port: bool = False) -> bool:
    v = _norm_kw_value(value, is_port=is_port)
    if not v:
        return False

    for kw in (keywords or []):
        k = _norm_kw_value(kw, is_port=is_port)
        if not k:
            continue
        if v == k:
            return True
    return False


def _path_segments(route: Dict[str, Any]) -> List[str]:
    raw = str(route.get("path", "") or "").strip()
    if not raw:
        return []
    parts = [p.strip() for p in raw.split("→")]
    return [canon(p) for p in parts if canon(p)]


def _segment_matches_keywords(segment: str, keywords: List[str], is_port: bool = False) -> bool:
    if not segment:
        return False

    seg_cmp = normalize_location_key(segment) if is_port else canon(segment)
    if not seg_cmp:
        return False

    for kw in (keywords or []):
        kw_cmp = _norm_kw_value(kw, is_port=is_port)
        if not kw_cmp:
            continue

        if seg_cmp == kw_cmp:
            return True

        if re.search(rf"(?<![a-z0-9]){re.escape(kw_cmp)}(?![a-z0-9])", seg_cmp):
            return True

    return False


def _any_segment_matches_text(segments: List[str], value: str, is_port: bool = False) -> bool:
    if not value:
        return False
    return any(_segment_matches_keywords(seg, [value], is_port=is_port) for seg in segments)


def _find_segment_index(
    segments: List[str],
    keywords: List[str],
    is_port: bool = False,
    start_idx: int = 0
) -> Optional[int]:
    if not segments:
        return None
    for i in range(max(0, start_idx), len(segments)):
        if _segment_matches_keywords(segments[i], keywords, is_port=is_port):
            return i
    return None


def _route_structured_cities(route: Dict[str, Any], side_key: str) -> List[str]:
    box = route.get(side_key) or {}
    vals = box.get("cities") if isinstance(box, dict) else []
    if not isinstance(vals, list):
        return []
    return [str(x).strip() for x in vals if str(x).strip()]


def _route_structured_countries(route: Dict[str, Any], side_key: str) -> List[str]:
    box = route.get(side_key) or {}
    vals = box.get("countries") if isinstance(box, dict) else []
    if not isinstance(vals, list):
        return []
    return [str(x).strip() for x in vals if str(x).strip()]


def _route_matches_origin_country_strict(route: Dict[str, Any], origin_country: str) -> bool:
    c = canon(origin_country)
    if not c:
        return False

    structured = _route_structured_countries(route, "origin_city_country")
    if structured:
        return any(canon(x) == c for x in structured)

    return _value_matches_keywords(
        origin_country,
        route.get("origin_country_keywords", []) or [],
        is_port=False
    )


def _route_matches_origin_city_strict(route: Dict[str, Any], origin_city: str) -> bool:
    c = canon(origin_city)
    if not c:
        return False

    if _value_matches_keywords(origin_city, route.get("origin_city_keywords", []) or [], is_port=False):
        return True

    structured = _route_structured_cities(route, "origin_city_country")
    if structured and any(canon(x) == c for x in structured):
        return True

    segments = _path_segments(route)
    if segments and _segment_matches_keywords(segments[0], [origin_city], is_port=False):
        return True

    return False


def _route_matches_pol_strict(route: Dict[str, Any], pol: str) -> bool:
    if not normalize_location_key(pol):
        return False
    return _value_matches_keywords(pol, route.get("pol_keywords", []) or [], is_port=True)


def _route_matches_pod_strict(route: Dict[str, Any], pod: str) -> bool:
    if not normalize_location_key(pod):
        return False
    return _value_matches_keywords(pod, route.get("pod_keywords", []) or [], is_port=True)


def _route_matches_destination_city_strict(route: Dict[str, Any], destination_city: str) -> bool:
    c = canon(destination_city)
    if not c:
        return False

    if _value_matches_keywords(destination_city, route.get("destination_city_keywords", []) or [], is_port=False):
        return True

    structured = _route_structured_cities(route, "destination_city_country")
    if structured and any(canon(x) == c for x in structured):
        return True

    return False


def _route_matches_destination_country_strict(route: Dict[str, Any], destination_country: str) -> bool:
    c = canon(destination_country)
    if not c:
        return False

    structured = _route_structured_countries(route, "destination_city_country")
    if structured:
        return any(canon(x) == c for x in structured)

    return _value_matches_keywords(
        destination_country,
        route.get("destination_country_keywords", []) or [],
        is_port=False
    )


def _first_segment_matches_origin_city(route: Dict[str, Any], origin_city: str) -> bool:
    segments = _path_segments(route)
    if not segments:
        return False
    return _segment_matches_keywords(segments[0], [origin_city], is_port=False)


def _first_segment_matches_pol(route: Dict[str, Any], pol: str) -> bool:
    segments = _path_segments(route)
    if not segments:
        return False
    first_seg = segments[0]
    return (
        _segment_matches_keywords(first_seg, [pol], is_port=True)
        or _segment_matches_keywords(first_seg, route.get("pol_keywords", []) or [], is_port=True)
    )


def _last_segment_matches_location_text(route: Dict[str, Any], value: str, is_port: bool = False) -> bool:
    segments = _path_segments(route)
    if not segments:
        return False

    last_seg = segments[-1]
    if _segment_matches_keywords(last_seg, [value], is_port=is_port):
        return True

    # If end is a city that represents the POD/POL location
    structured_dest_cities = _route_structured_cities(route, "destination_city_country")
    if structured_dest_cities and _segment_matches_keywords(last_seg, structured_dest_cities, is_port=False):
        return True

    return False


def _last_segment_matches_destination_city(route: Dict[str, Any], destination_city: str) -> bool:
    segments = _path_segments(route)
    if not segments:
        return False

    last_seg = segments[-1]

    if _segment_matches_keywords(last_seg, [destination_city], is_port=False):
        return True

    structured = _route_structured_cities(route, "destination_city_country")
    if structured and _segment_matches_keywords(last_seg, structured, is_port=False):
        return any(canon(x) == canon(destination_city) for x in structured)

    return False


def _last_segment_matches_destination_country(route: Dict[str, Any], destination_country: str) -> bool:
    if not _route_matches_destination_country_strict(route, destination_country):
        return False

    segments = _path_segments(route)
    if not segments:
        return False

    last_seg = segments[-1]

    if _segment_matches_keywords(last_seg, [destination_country], is_port=False):
        return True

    structured_cities = _route_structured_cities(route, "destination_city_country")
    if structured_cities and _segment_matches_keywords(last_seg, structured_cities, is_port=False):
        return True

    return False


def _ordered_waypoint_match(route: Dict[str, Any], value: str, is_port: bool = False) -> bool:
    segments = _path_segments(route)
    if not segments or not value:
        return False
    return _any_segment_matches_text(segments, value, is_port=is_port)


def _kw_score_exact(value: Any, keywords: List[str], pts: int, is_port: bool = False) -> int:
    return pts if _value_matches_keywords(value, keywords, is_port=is_port) else 0


def route_base_match(
    pol: str,
    pod: str,
    route: dict,
    origin_city: str = "",
    origin_country: str = "",
    destination_city: str = "",
    destination_country: str = "",
    transit_borders: Optional[List[str]] = None,
    allow_reverse: bool = False
) -> Tuple[bool, bool, int]:
    transit_borders = transit_borders or []
    segments = _path_segments(route)

    user_has_origin_city = bool(canon(origin_city))
    user_has_origin_country = bool(canon(origin_country))
    user_has_pol = bool(normalize_location_key(pol))
    user_has_pod = bool(normalize_location_key(pod))
    user_has_destination_city = bool(canon(destination_city))
    user_has_destination_country = bool(canon(destination_country))

    # -------------------------
    # HARD START MATCH
    # -------------------------
    start_ok = False
    score = 0

    if user_has_origin_country:
        if not _route_matches_origin_country_strict(route, origin_country):
            return False, False, 0
        score += 40

    if user_has_origin_city:
        if not _route_matches_origin_city_strict(route, origin_city):
            return False, False, 0
        if not _first_segment_matches_origin_city(route, origin_city):
            return False, False, 0
        start_ok = True
        score += 120

    elif user_has_pol and not user_has_origin_country:
        # pure POL start
        if not _route_matches_pol_strict(route, pol):
            return False, False, 0
        if not _first_segment_matches_pol(route, pol):
            return False, False, 0
        start_ok = True
        score += 120

    elif user_has_origin_country:
        # country-only start
        start_ok = True
        score += 20

    else:
        return False, False, 0

    # -------------------------
    # HARD END MATCH
    # Priority:
    # destination city > destination country > pod > pol-as-end
    # -------------------------
    end_ok = False

    if user_has_destination_city:
        if not _route_matches_destination_city_strict(route, destination_city):
            return False, False, 0
        if not _last_segment_matches_destination_city(route, destination_city):
            return False, False, 0
        end_ok = True
        score += 120

    elif user_has_destination_country:
        if not _route_matches_destination_country_strict(route, destination_country):
            return False, False, 0
        if not _last_segment_matches_destination_country(route, destination_country):
            return False, False, 0
        end_ok = True
        score += 90

    elif user_has_pod:
        # no destination given -> POD acts as endpoint
        if not _last_segment_matches_location_text(route, pod, is_port=True):
            return False, False, 0
        end_ok = True
        score += 80

    elif user_has_pol and (user_has_origin_city or user_has_origin_country):
        # origin -> POL query, POL acts as endpoint
        if not _last_segment_matches_location_text(route, pol, is_port=True):
            return False, False, 0
        end_ok = True
        score += 80

    else:
        end_ok = True

    if not start_ok or not end_ok:
        return False, False, 0

    # -------------------------
    # OPTIONAL WAYPOINTS / ALTERNATIVES
    # POL and POD are soft when destination is also provided.
    # This lets alternative routes still appear if start and end match.
    # -------------------------
    if user_has_pol and (user_has_origin_city or user_has_origin_country) and (user_has_destination_city or user_has_destination_country):
        if _route_matches_pol_strict(route, pol) or _ordered_waypoint_match(route, pol, is_port=True):
            score += 35
        else:
            score += 5  # alternative route without same POL

    if user_has_pod and (user_has_destination_city or user_has_destination_country):
        if _route_matches_pod_strict(route, pod) or _ordered_waypoint_match(route, pod, is_port=True):
            score += 35
        else:
            score += 5  # alternative route without same POD

    # transit borders: optional boost
    border_keywords = (
        route.get("must_borders")
        or route.get("border_keywords")
        or route.get("transit_border_keywords")
        or []
    )
    if transit_borders and border_keywords:
        for b in transit_borders:
            if _value_matches_keywords(b, border_keywords, is_port=False):
                score += 20
                break

    return True, False, int(score)


def get_matching_routes(
    pol: str,
    pod: str,
    origin_city: str = "",
    origin_country: str = "",
    destination_city: str = "",
    destination_country: str = "",
    transit_borders: Optional[List[str]] = None
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    routes_src = load_routes_json()
    transit_borders = transit_borders or []

    matched: List[Dict[str, Any]] = []

    for r in routes_src:
        ok, is_reverse, match_score = route_base_match(
            pol=pol,
            pod=pod,
            route=r,
            origin_city=origin_city,
            origin_country=origin_country,
            destination_city=destination_city,
            destination_country=destination_country,
            transit_borders=transit_borders,
            allow_reverse=False,
        )
        if not ok:
            continue

        rr = dict(r)
        rr["is_recent"] = False
        rr["is_custom"] = False
        rr["is_reverse"] = False
        rr["route_type"] = normalize_route_type(rr.get("route_type"))
        rr["modes"] = normalize_route_modes(rr)
        rr["mode_label"] = route_mode_label(rr)
        rr["status_label"] = route_status_label(rr)
        rr["path"] = rr.get("path", "")
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
            route_specificity_rank(x),
            x.get("_tt_key", (10**9, 10**9))
        )
    )

    best_id = matched[0].get("id")
    for rr in matched:
        rr["is_best"] = (rr.get("id") == best_id)

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

# -------------------------
# PRICING SHEET SECTION HELPERS
# -------------------------
SECTION_MARKERS = [
    "Basic_Details_Section",
    "Ocean_Freight_Section",
    "Railway_Shipment_Section",
    "Land_Shipment_Section",
    "Airway_Section",
]

SECTION_END_MARKERS = [
    "Airway_Section_Ends",
]

SHIPMENT_MODE_TO_SECTIONS = {
    "Ocean Freight Shipment": ["Ocean_Freight_Section"],
    "Railway Shipment": ["Railway_Shipment_Section"],
    "Land Shipment": ["Land_Shipment_Section"],
    "Airway": ["Airway_Section"],

    # Existing Ocean + Land button already in frontend
    "Ocean Freight + Land Shipment": ["Ocean_Freight_Section", "Land_Shipment_Section"],

    # Alias support if you later rename the frontend label/value
    "Ocean Freight Shipment + Land Shipment": ["Ocean_Freight_Section", "Land_Shipment_Section"],

    # New 2-way combinations
    "Ocean Freight Shipment + Railway Shipment": ["Ocean_Freight_Section", "Railway_Shipment_Section"],
    "Railway Shipment + Land Shipment": ["Railway_Shipment_Section", "Land_Shipment_Section"],
    "Land Shipment + Airway": ["Land_Shipment_Section", "Airway_Section"],

    # New 3-way combination
    "Ocean Freight Shipment + Railway Shipment + Land Shipment": [
        "Ocean_Freight_Section",
        "Railway_Shipment_Section",
        "Land_Shipment_Section",
    ],
}


def is_blank_or_unnamed_column(col_name: Any) -> bool:
    if col_name is None:
        return True
    s = str(col_name).strip()
    if not s:
        return True
    return canon(s).startswith("unnamed:")


def is_section_marker_column(col_name: Any) -> bool:
    c = canon(col_name)
    return c in {canon(x) for x in SECTION_MARKERS} or c in {canon(x) for x in SECTION_END_MARKERS}


def find_section_marker_positions(df: pd.DataFrame) -> Dict[str, int]:
    """
    Finds section marker columns by canonical header name.
    Handles accidental spaces in headers, for example:
      Railway_Shipment_Section
      Railway_Shipment_Section 
       Railway_charges_20ft
    """
    positions: Dict[str, int] = {}
    cols = list(df.columns)

    for marker in SECTION_MARKERS + SECTION_END_MARKERS:
        marker_c = canon(marker)
        for idx, col in enumerate(cols):
            if canon(col) == marker_c:
                positions[marker] = idx
                break

    return positions


def get_section_columns(df: pd.DataFrame, section_name: str) -> List[str]:
    """
    Returns columns for a specific mode section.

    Expected current structure:
      Basic_Details_Section starts at A and ends before Ocean_Freight_Section.
      Ocean_Freight_Section ends before Railway_Shipment_Section.
      Railway_Shipment_Section ends before Land_Shipment_Section.
      Land_Shipment_Section ends before Airway_Section.
      Airway_Section ends before Airway_Section_Ends, if that marker exists.
      routes remains at the end.
    """
    cols = list(df.columns)
    positions = find_section_marker_positions(df)

    if section_name not in positions:
        return []

    start_idx = positions[section_name]
    possible_ends: List[int] = []

    # End before the next mode section marker.
    for marker in SECTION_MARKERS:
        idx = positions.get(marker)
        if idx is not None and idx > start_idx:
            possible_ends.append(idx)

    # Airway has an explicit ending marker.
    if canon(section_name) == canon("Airway_Section"):
        airway_end_idx = positions.get("Airway_Section_Ends")
        if airway_end_idx is not None and airway_end_idx > start_idx:
            possible_ends.append(airway_end_idx)

    # Always stop before routes if no later section/end marker is found.
    for idx, c in enumerate(cols):
        if canon(c) == canon("routes") and idx > start_idx:
            possible_ends.append(idx)
            break

    end_idx = min(possible_ends) if possible_ends else len(cols)

    return [
        c for c in cols[start_idx:end_idx]
        if not is_blank_or_unnamed_column(c)
    ]


def get_basic_section_columns(df: pd.DataFrame) -> List[str]:
    """
    Basic section starts at Basic_Details_Section and ends before Ocean_Freight_Section.
    This section is used for matching POL/POD, address, container, commodity, weight,
    and the one shared validity column.
    """
    cols = list(df.columns)
    positions = find_section_marker_positions(df)

    start_idx = positions.get("Basic_Details_Section", 0)
    end_idx = positions.get("Ocean_Freight_Section", len(cols))

    if end_idx <= start_idx:
        return cols[:end_idx] if end_idx > 0 else cols

    return [
        c for c in cols[start_idx:end_idx]
        if not is_blank_or_unnamed_column(c)
    ]


def get_route_columns(df: pd.DataFrame) -> List[str]:
    """
    routes column remains at the end of the sheet like before.
    Always keep it available for selected-route matching.
    """
    return [c for c in df.columns if canon(c) == canon("routes")]


def get_validity_column_from_basic_section(df: pd.DataFrame) -> Optional[str]:
    """
    New rule:
    There is only one validity column in Basic_Details_Section.
    That validity applies to all selected mode sections and all charge rows.
    """
    basic_cols = get_basic_section_columns(df)

    for c in basic_cols:
        if canon(c) == canon("validity"):
            return c

    for c in basic_cols:
        if "validity" in canon(c):
            return c

    return None


def select_pricing_columns_for_shipment_mode(
    df: pd.DataFrame,
    shipment_mode: str
) -> Tuple[pd.DataFrame, Optional[str], List[str]]:
    """
    Keeps only:
      - Basic_Details_Section columns
      - selected shipment mode section columns
      - routes column

    Examples:
      Ocean Freight Shipment                                  => Basic + Ocean + routes
      Railway Shipment                                        => Basic + Railway + routes
      Land Shipment                                           => Basic + Land + routes
      Airway                                                  => Basic + Airway + routes
      Ocean Freight + Land Shipment                           => Basic + Ocean + Land + routes
      Ocean Freight Shipment + Railway Shipment               => Basic + Ocean + Railway + routes
      Railway Shipment + Land Shipment                        => Basic + Railway + Land + routes
      Land Shipment + Airway                                  => Basic + Land + Airway + routes
      Ocean Freight Shipment + Railway Shipment + Land Shipment => Basic + Ocean + Railway + Land + routes
    """
    mode = (shipment_mode or "").strip()

    if not mode:
        return df.copy(), "Please select the shipment mode.", []

    selected_sections = SHIPMENT_MODE_TO_SECTIONS.get(mode)
    if not selected_sections:
        return df.copy(), f"Unsupported shipment mode selected: {mode}", []

    basic_cols = get_basic_section_columns(df)

    if not basic_cols:
        return df.copy(), "Basic_Details_Section columns were not found in prices_updated.xlsx.", []

    keep_cols: List[str] = []

    for c in basic_cols:
        if c not in keep_cols:
            keep_cols.append(c)

    missing_sections: List[str] = []

    for sec in selected_sections:
        sec_cols = get_section_columns(df, sec)
        if not sec_cols:
            missing_sections.append(sec)
            continue

        for c in sec_cols:
            if c not in keep_cols:
                keep_cols.append(c)

    for c in get_route_columns(df):
        if c not in keep_cols:
            keep_cols.append(c)

    if missing_sections:
        return df.copy(), (
            "Selected shipment mode section was not found in prices_updated.xlsx: "
            + ", ".join(missing_sections)
        ), []

    if not keep_cols:
        return df.copy(), "No pricing columns found for selected shipment mode.", []

    return df.loc[:, keep_cols].copy(), None, keep_cols

def save_to_excel(record: Dict[str, Any]) -> Tuple[bool, str]:
    try:
        try:
            # download existing file
            content = download_excel_from_onedrive(ONEDRIVE_QUERIES_PATH)
            df_existing = pd.read_excel(io.BytesIO(content))
        except Exception:
            # file doesn't exist yet or cannot be read
            df_existing = pd.DataFrame()

        df_new = pd.DataFrame([record])
        df_final = pd.concat([df_existing, df_new], ignore_index=True)

        # save to memory
        buffer = io.BytesIO()
        df_final.to_excel(buffer, index=False)
        buffer.seek(0)

        # upload back to OneDrive
        upload_excel_to_onedrive(ONEDRIVE_QUERIES_PATH, buffer.read())
        return True, ""

    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        if status == 409:
            return False, "Could not save query to queries.xlsx because the file is busy or locked in OneDrive."
        return False, f"Could not save query to queries.xlsx (HTTP {status})."

    except Exception as e:
        return False, f"Could not save query to queries.xlsx: {str(e)}"

def add_generated_quote_prices_to_record(
    record: Dict[str, Any],
    rates: Optional[List[Dict[str, Any]]]
) -> Dict[str, Any]:
    """
    Adds all generated quote line items and grand totals into the same record
    that is saved to queries.xlsx.

    This saves:
      - A JSON copy of all generated quote rows
      - Separate Excel columns for each generated row
      - Separate Excel columns for grand totals
    """
    rates = rates or []

    quote_export: List[Dict[str, Any]] = []

    # Default summary columns, so queries.xlsx always has stable columns
    record["generated_quote_count"] = len(rates)
    record["generated_grand_total_per_20ft_container"] = ""
    record["generated_grand_total_per_40ft_container"] = ""
    record["generated_grand_total_shipment_cost"] = ""
    record["generated_grand_total_per_20ft_container_num"] = ""
    record["generated_grand_total_per_40ft_container_num"] = ""
    record["generated_grand_total_shipment_cost_num"] = ""

    line_no = 1

    for quote_idx, quote in enumerate(rates, start=1):
        title = str(quote.get("title", "") or "").strip()
        match_note = str(quote.get("match_note", "") or "").strip()
        table_rows = quote.get("table_rows") or []

        record[f"generated_quote_{quote_idx}_title"] = title
        record[f"generated_quote_{quote_idx}_match_note"] = match_note

        for row in table_rows:
            if not isinstance(row, dict):
                continue

            name = str(row.get("name", "") or "").strip()
            cost = str(row.get("cost", "") or "").strip()
            validity = str(row.get("validity", "") or "").strip()
            validity_status = str(row.get("validity_status", "") or "").strip()

            cost_num = row.get("cost_num", "")
            per20_num = row.get("per20_num", "")
            per40_num = row.get("per40_num", "")
            ship20_num = row.get("ship20_num", "")
            ship40_num = row.get("ship40_num", "")
            ship_common_num = row.get("ship_common_num", "")
            unit_count = row.get("unit_count", "")

            include_in_total = bool(row.get("include_in_total", False))
            is_grand_total = bool(row.get("is_grand_total", False))
            is_red_text = bool(row.get("is_red_text", False))
            grand_mode = str(row.get("grand_mode", "") or "").strip()
            grand_key = str(row.get("grand_key", "") or "").strip()

            # Store all generated rows in JSON too
            quote_export.append({
                "quote_index": quote_idx,
                "quote_title": title,
                "line_no": line_no,
                "name": name,
                "cost": cost,
                "validity": validity,
                "validity_status": validity_status,
                "include_in_total": include_in_total,
                "is_grand_total": is_grand_total,
                "is_display_only_red_row": is_red_text,
                "grand_mode": grand_mode,
                "grand_key": grand_key,
                "cost_num": cost_num,
                "per20_num": per20_num,
                "per40_num": per40_num,
                "ship20_num": ship20_num,
                "ship40_num": ship40_num,
                "ship_common_num": ship_common_num,
                "unit_count": unit_count,
            })

            # Store each generated row as normal Excel columns too
            prefix = f"generated_price_{line_no:02d}"
            record[f"{prefix}_quote_index"] = quote_idx
            record[f"{prefix}_name"] = name
            record[f"{prefix}_cost"] = cost
            record[f"{prefix}_validity"] = validity
            record[f"{prefix}_validity_status"] = validity_status
            record[f"{prefix}_include_in_total"] = "Yes" if include_in_total else "No"
            record[f"{prefix}_is_grand_total"] = "Yes" if is_grand_total else "No"
            record[f"{prefix}_is_display_only"] = "Yes" if is_red_text else "No"
            record[f"{prefix}_cost_num"] = cost_num
            record[f"{prefix}_per20_num"] = per20_num
            record[f"{prefix}_per40_num"] = per40_num
            record[f"{prefix}_ship20_num"] = ship20_num
            record[f"{prefix}_ship40_num"] = ship40_num
            record[f"{prefix}_ship_common_num"] = ship_common_num
            record[f"{prefix}_unit_count"] = unit_count

            # Easy-to-read grand total columns
            name_c = canon(name)

            if is_grand_total:
                parsed_total = parse_price_to_float(cost)

                if name_c == canon("Grand total per 20ft container"):
                    record["generated_grand_total_per_20ft_container"] = cost
                    record["generated_grand_total_per_20ft_container_num"] = (
                        float(parsed_total) if parsed_total is not None else ""
                    )

                elif name_c == canon("Grand total per 40ft container"):
                    record["generated_grand_total_per_40ft_container"] = cost
                    record["generated_grand_total_per_40ft_container_num"] = (
                        float(parsed_total) if parsed_total is not None else ""
                    )

                elif name_c == canon("Grand total shipment cost"):
                    record["generated_grand_total_shipment_cost"] = cost
                    record["generated_grand_total_shipment_cost_num"] = (
                        float(parsed_total) if parsed_total is not None else ""
                    )

            line_no += 1

    record["generated_quote_prices_json"] = json.dumps(
        quote_export,
        ensure_ascii=False,
        default=str
    )

    return record

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
# GRAND TOTAL / SIZE-AWARE LOGIC
# -------------------------
def is_charges_column(col_name: str) -> bool:
    c = canon(col_name)

    if not c:
        return False

    if is_blank_or_unnamed_column(col_name):
        return False

    if is_section_marker_column(col_name):
        return False

    if c in {
        canon("routes"),
        canon("validity"),
        canon("Airway_Section_Ends"),
    }:
        return False

    # UI-only / display-only items: never include in quote totals
    if c in {
        canon("incurrence_charges"),
        canon("switch_bl_charges"),
    }:
        return False

    if "_charges" in c:
        return True

    if c.endswith("_cost_20ft") or c.endswith("_cost_40ft"):
        return True

    return False


def charge_size_bucket(col_name: str) -> str:
    """
    Returns:
      '20' | '40' | '2x20' | 'common'

    Supports both naming styles, for example:
      - Ocean Freight (20ft)_charges
      - Ocean Freight (40ft)_charges
      - Switch_BL_charges
      - Labor_lifting_cost_40ft
      - trucking_charges_2x20ft
    """
    c = canon(col_name)

    # 2x20 first
    if (
        c.endswith("_2x20ft")
        or "(2x20ft)" in c
        or " 2x20ft" in c
    ):
        return "2x20"

    # 20ft patterns
    if (
        c.endswith("_20ft")
        or "(20ft)" in c
        or "_20ft_" in c
        or c.endswith("(20ft)_charges")
        or c.endswith("(20ft)_cost")
    ):
        return "20"

    # 40ft patterns
    if (
        c.endswith("_40ft")
        or "(40ft)" in c
        or "_40ft_" in c
        or c.endswith("(40ft)_charges")
        or c.endswith("(40ft)_cost")
    ):
        return "40"

    return "common"


def strip_size_suffix(col_name: str) -> str:
    c = str(col_name).strip()
    c_low = c.lower()

    # normalize common size tokens used in your Excel headers
    replacements = [
        "(2x20ft)",
        "(20ft)",
        "(40ft)",
        "_2x20ft",
        "_20ft",
        "_40ft",
        " 2x20ft",
        " 20ft",
        " 40ft",
    ]

    out = c
    for token in replacements:
        out = re.sub(re.escape(token), "", out, flags=re.IGNORECASE)

    # cleanup duplicated underscores / spaces
    out = re.sub(r"__+", "_", out)
    out = re.sub(r"\s{2,}", " ", out)
    out = out.strip(" _-")
    return out


def is_trucking_charge_column(col_name: str) -> bool:
    c = canon(col_name)
    return c in {
        canon("trucking_charges_20ft"),
        canon("trucking_charges_40ft"),
        canon("trucking_charges_2x20ft"),
    }


def get_selected_container_units(
    size_20ft_count: int,
    size_40ft_count: int,
    size_2x20ft_count: int
) -> Dict[str, int]:
    total_20_units = int(size_20ft_count or 0) + (int(size_2x20ft_count or 0) * 2)
    total_40_units = int(size_40ft_count or 0)

    return {
        "single_20_count": int(size_20ft_count or 0),
        "count_2x20": int(size_2x20ft_count or 0),
        "total_20_units": total_20_units,
        "total_40_units": total_40_units,
    }

def build_sized_cost_display(rate: float, units: int) -> str:
    """
    Keep for shipment-total calculations where needed.
    """
    rate = float(rate or 0.0)
    units = int(units or 0)

    if units <= 0:
        return "N/A"

    total = rate * units
    return fmt_money(total) or "$0.00"


def build_rate_display(rate: float) -> str:
    """
    Show the exact picked Excel charge/rate in the quote row.
    Do NOT multiply by quantity for visible line items.
    """
    return fmt_money(float(rate or 0.0)) or "$0.00"


def build_flat_cost_display(amount: float) -> str:
    return fmt_money(float(amount or 0.0)) or "$0.00"


def compute_selected_shipment_total_for_row(
    row: pd.Series,
    columns: List[str],
    total_20_units: int,
    total_40_units: int
) -> Tuple[float, bool]:
    """
    Size-aware best-row selector.
    - 20ft columns => multiplied by total 20ft units
    - 40ft columns => multiplied by total 40ft units
    - common columns => counted once
    - trucking columns => excluded (handled separately)
    """
    total = 0.0
    found_any = False

    for col in columns:
        if not is_charges_column(col):
            continue
        if is_trucking_charge_column(col):
            continue

        num = parse_price_to_float(row.get(col))
        if num is None:
            continue

        bucket = charge_size_bucket(col)

        if bucket == "20":
            if total_20_units > 0:
                total += float(num) * float(total_20_units)
                found_any = True

        elif bucket == "40":
            if total_40_units > 0:
                total += float(num) * float(total_40_units)
                found_any = True

        elif bucket == "2x20":
            # no normal charge columns should use this pattern except trucking
            continue

        else:
            total += float(num)
            found_any = True

    return float(total), bool(found_any)


def compute_selected_shipment_totals_for_df(
    df: pd.DataFrame,
    columns: List[str],
    total_20_units: int,
    total_40_units: int
) -> Tuple[List[float], List[bool]]:
    totals: List[float] = []
    has_any: List[bool] = []

    for _, row in df.iterrows():
        t, ok = compute_selected_shipment_total_for_row(
            row=row,
            columns=columns,
            total_20_units=total_20_units,
            total_40_units=total_40_units
        )
        totals.append(float(t))
        has_any.append(bool(ok))

    return totals, has_any
def compute_trucking_plan_and_totals(
    matched_df: pd.DataFrame,
    single_20_count: int,
    pair_20_count: int,
    total_40_units: int,
    per20_mode: str,
    today: date | None = None,
    global_validity_col: Optional[str] = None
):
    """
    Trucking is still handled the same way as before, but now:
    - trucking columns are only available when the selected pricing section contains them
    - validity comes from the one Basic_Details_Section validity column
    """
    if today is None:
        today = date.today()

    if matched_df is None or matched_df.empty:
        return {
            "shipment_total_20": 0.0,
            "shipment_total_40": 0.0,
            "per_unit_20": 0.0,
            "per_unit_40": 0.0,
            "rows": [],
            "notes": ["Trucking row not found in selected quote row/group."],
        }

    def _validity_for_row(rr: pd.Series) -> Tuple[str, str]:
        validity_text = ""
        validity_status = "na"

        if global_validity_col and global_validity_col in matched_df.columns:
            validity_status, validity_fmt, _ = validity_status_and_text(rr.get(global_validity_col))
            validity_text = validity_fmt or ""
            return validity_text, validity_status

        fallback_validity_col = (
            find_col_case_insensitive(matched_df, "validity")
            or find_col_case_insensitive(matched_df, "Rates Validity")
            or find_col_case_insensitive(matched_df, "Validity")
        )

        if fallback_validity_col:
            validity_status, validity_fmt, _ = validity_status_and_text(rr.get(fallback_validity_col))
            validity_text = validity_fmt or ""

        return validity_text, validity_status

    def _get_rate_and_validity(base_col_name: str):
        actual = find_col_case_insensitive(matched_df, base_col_name)
        if not actual:
            return None, "", "na"

        # Search every row in the matched group until we find a positive rate
        for _, rr in matched_df.iterrows():
            raw_rate = rr.get(actual)
            rate = parse_price_to_float(raw_rate)
            if rate is None or float(rate) <= 0:
                continue

            validity_text, validity_status = _validity_for_row(rr)
            return float(rate), validity_text, validity_status

        return None, "", "na"

    rows: List[Dict[str, Any]] = []
    notes: List[str] = []

    shipment_total_20 = 0.0
    shipment_total_40 = 0.0

    # 2x20ft trucking
    if pair_20_count > 0:
        rate, validity_text, validity_status = _get_rate_and_validity("trucking_charges_2x20ft")
        if rate is not None:
            shipment_total = float(rate) * float(pair_20_count)
            shipment_total_20 += shipment_total
            rows.append({
                "name": "trucking_charges_2x20ft",
                "cost": build_rate_display(rate),
                "validity": validity_text,
                "validity_status": validity_status,
                "can_remove": True,
                "include_in_total": True,
                "cost_num": float(shipment_total),
                "is_grand_total": False,
                "change_type": "",
                "options": [],
                "per20_num": 0.0,
                "per40_num": 0.0,
                "ship20_num": float(shipment_total),
                "ship40_num": 0.0,
                "ship_common_num": 0.0,
                "grand_mode": "",
                "grand_key": "",
                "unit_count": pair_20_count,
            })
        else:
            notes.append("2x20ft trucking rate not found.")

    # single 20ft trucking
    if single_20_count > 0:
        rate, validity_text, validity_status = _get_rate_and_validity("trucking_charges_20ft")
        if rate is not None:
            shipment_total = float(rate) * float(single_20_count)
            shipment_total_20 += shipment_total
            rows.append({
                "name": "trucking_charges_20ft",
                "cost": build_rate_display(rate),
                "validity": validity_text,
                "validity_status": validity_status,
                "can_remove": True,
                "include_in_total": True,
                "cost_num": float(shipment_total),
                "is_grand_total": False,
                "change_type": "",
                "options": [],
                "per20_num": float(rate) if per20_mode == "single20" else 0.0,
                "per40_num": 0.0,
                "ship20_num": float(shipment_total),
                "ship40_num": 0.0,
                "ship_common_num": 0.0,
                "grand_mode": "",
                "grand_key": "",
                "unit_count": single_20_count,
            })
        else:
            notes.append("Single 20ft trucking rate not found.")

    # 40ft trucking
    if total_40_units > 0:
        rate, validity_text, validity_status = _get_rate_and_validity("trucking_charges_40ft")
        if rate is not None:
            shipment_total = float(rate) * float(total_40_units)
            shipment_total_40 += shipment_total
            rows.append({
                "name": "trucking_charges_40ft",
                "cost": build_rate_display(rate),
                "validity": validity_text,
                "validity_status": validity_status,
                "can_remove": True,
                "include_in_total": True,
                "cost_num": float(shipment_total),
                "is_grand_total": False,
                "change_type": "",
                "options": [],
                "per20_num": 0.0,
                "per40_num": float(rate),
                "ship20_num": 0.0,
                "ship40_num": float(shipment_total),
                "ship_common_num": 0.0,
                "grand_mode": "",
                "grand_key": "",
                "unit_count": total_40_units,
            })
        else:
            notes.append("40ft trucking rate not found.")

    per_unit_20 = 0.0
    if per20_mode == "single20":
        per_unit_20 = next((float(r.get("per20_num") or 0.0) for r in rows if canon(r.get("name")) == canon("trucking_charges_20ft")), 0.0)
    elif per20_mode == "pair20":
        per_unit_20 = next((float(r.get("per20_num") or 0.0) for r in rows if canon(r.get("name")) == canon("trucking_charges_2x20ft")), 0.0)

    per_unit_40 = next((float(r.get("per40_num") or 0.0) for r in rows if canon(r.get("name")) == canon("trucking_charges_40ft")), 0.0)

    return {
        "shipment_total_20": float(shipment_total_20),
        "shipment_total_40": float(shipment_total_40),
        "per_unit_20": float(per_unit_20),
        "per_unit_40": float(per_unit_40),
        "rows": rows,
        "notes": notes,
    }

# -------------------------
# QUOTE SEARCH
# -------------------------
def get_strict_quotes(
    pol_port: str,
    pod_port: str,
    incoterm_origin: str,
    incoterm_destination: str,
    shipment_mode: str = "",

    origin_address: str = "",
    origin_city: str = "",
    origin_country: str = "",

    dest_address: str = "",
    dest_city: str = "",
    dest_country: str = "",

    container_size_label: str = "",

    selected_route_type: str = "",
    selected_route_mode_label: str = "",

    selected_route_id: str = "",
    selected_route_text: str = "",

    size_20ft_count: int = 0,
    size_40ft_count: int = 0,
    size_2x20ft_count: int = 0,

    special_cost_lines: Optional[List[Dict[str, Any]]] = None,

    container_ownership: str = "",
    soc_clearance_cost_20ft_value: str = "",
    soc_clearance_cost_40ft_value: str = "",
    soc_selling_price_20ft_value: str = "",
    soc_selling_price_40ft_value: str = "",
    lifting_labor_required: str = "",
    offloading_responsible: str = "",

    insurance_amount_num: Optional[float] = None,
    misc_cost_value: str = "",
    incurrence_charges_value: str = "",

    limit: int = 1
) -> Tuple[List[Dict[str, Any]], Optional[str], Optional[str]]:
    
    df = load_prices_df()
    if df is None or df.empty:
        return [], None, "Could not load prices_updated.xlsx properly. Please confirm the file exists and headers are correct."

    # -------------------------
    # NEW: Keep only Basic + selected shipment mode section + routes
    # -------------------------
    global_validity_col = get_validity_column_from_basic_section(df)

    df, section_error_msg, selected_pricing_columns = select_pricing_columns_for_shipment_mode(
        df=df,
        shipment_mode=shipment_mode
    )

    if section_error_msg:
        return [], None, section_error_msg

    if df is None or df.empty:
        return [], None, "No pricing data found for the selected shipment mode."

    if global_validity_col and global_validity_col not in df.columns:
        global_validity_col = find_col_case_insensitive(df, "validity")

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
    selected_route_type_c = canon(selected_route_type)
    selected_route_mode_c = canon(selected_route_mode_label)

    is_land_only_route = ("land" in selected_route_mode_c and "sea" not in selected_route_mode_c and "rail" not in selected_route_mode_c)

    # Route-specific destination behavior:
    # For routes where final delivery may be in a different country than POD,
    # do NOT force strict destination city/country filtering on prices rows.
    skip_destination_strict_filter = selected_route_type_c in {
        "pickup_to_pol_to_pod_to_final",
        "pol_to_pod_to_final",
        "pol_to_pod_to_city",
        "city_to_pol_to_pod_to_city",
    }

    skip_destination_strict_filter = skip_destination_strict_filter or selected_route_type_c in {
        "pol_to_pod",
        "city_to_pol_to_pod",
    }

    # POL -> City and City -> City routes should not be forced through POD-country logic
    skip_destination_strict_filter = skip_destination_strict_filter or selected_route_type_c in {
        "pol_to_city",
        "city_to_city",
        "city_to_country_to_city",
        "city_to_pol",
    }

    # For land-only city corridor routes, do not require sea POL/POD style quote logic.
    # We still try POL/POD if present, but we must not force ocean-like destination filtering.
    if is_land_only_route:
        skip_destination_strict_filter = True

    pol_key = normalize_location_key(pol_port)
    pod_key = normalize_location_key(pod_port)

    df["_pol_key"] = df[POL_COL].apply(normalize_location_key)
    df["_pod_key"] = df[POD_COL].apply(normalize_location_key)

    pol_mask = df[POL_COL].apply(lambda x: flexible_location_match(pol_port, x))
    pod_mask = df[POD_COL].apply(lambda x: flexible_location_match(pod_port, x))

    # Base match used for shipping-line options:
    # user wants ALL shipping lines where only POL + POD match
    if selected_route_type_c in {"pol_to_city", "city_to_city", "city_to_country_to_city", "city_to_pol"}:
        # For inland routes, do not force POD matching
        if pol_key:
            df_pol_pod = df[pol_mask].copy()
        else:
            df_pol_pod = df.copy()
    else:
        df_pol_pod = df[pol_mask & pod_mask].copy()

    if df_pol_pod.empty:
        if selected_route_type_c in {"pol_to_city", "city_to_city", "city_to_country_to_city", "city_to_pol"}:
            return [], None, f"No matching rates found for POL='{pol_port}' and the selected inland route type."
        return [], None, f"No matching rates found for POL='{pol_port}' and POD='{pod_port}'."

    # Working dataframe for strict quote selection
    df_match = df_pol_pod.copy()

    # IMPORTANT:
    # Some sheets are grouped, and sibling rows may leave repeated text fields blank.
    # Forward-fill shared identifying fields so strict matching does not wrongly drop
    # valid rows that belong to the same POL/POD/route/shipping-line group.
    ffill_cols = [
        POL_COL,
        POD_COL,
        ORG_ADDR_COL,
        ORG_CITY_COL,
        ORG_COUNTRY_COL,
        DST_ADDR_COL,
        DST_CITY_COL,
        DST_COUNTRY_COL,
        find_col_case_insensitive(df_match, "Shipping Line Name"),
        find_col_case_insensitive(df_match, "routes"),
    ]

    for c in ffill_cols:
        if c and c in df_match.columns:
            df_match[c] = df_match[c].ffill()

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

    # Keep a route-level backup BEFORE strict origin/destination filters.
    # If exact city/country matching becomes too strict, we will fall back to this.
    route_level_df = df_match.copy()

    # ✅ Keep a relaxed copy for trucking BEFORE strict origin/destination address filters.
    # Trucking rows for 20ft / 40ft may exist in the same POL/POD/route group
    # but may not repeat all city/country/address values row-by-row.
    trucking_df = df_match.copy()

    # ✅ Ensure df_best is always defined
    df_best = df_match.head(1).copy()

    # -------------------------
    # Ocean dropdown options (valid rows only)
    # -------------------------
    # -------------------------
    # Ocean dropdown options (ALL POL/POD-matching lines, including expired)
    # -------------------------
    # IMPORTANT:
    # Build these options from df_pol_pod, not from strictly filtered df_match.
    # User wants all shipping lines where POL + POD match, even if expired.
    ocean_src_df = df_pol_pod.copy()

    ship_line_col = find_col_case_insensitive(ocean_src_df, "Shipping Line Name")
    of20_col = find_col_case_insensitive(ocean_src_df, "Ocean Freight (20ft)_charges")
    of40_col = find_col_case_insensitive(ocean_src_df, "Ocean Freight (40ft)_charges")

    # New rule: use the single Basic_Details_Section validity column for all charges/options.
    validity_col = global_validity_col
    if validity_col and validity_col not in ocean_src_df.columns:
        validity_col = find_col_case_insensitive(ocean_src_df, "validity")

    ocean_freight_options: List[Dict[str, Any]] = []

    if ship_line_col and (of20_col or of40_col):
        # Forward-fill common grouped columns if sheet has merged/grouped style rows
        ocean_src_df = ocean_src_df.copy()
        ocean_src_df[ship_line_col] = ocean_src_df[ship_line_col].ffill()

        if of20_col:
            ocean_src_df[of20_col] = ocean_src_df[of20_col]
        if of40_col:
            ocean_src_df[of40_col] = ocean_src_df[of40_col]

        for _, rr in ocean_src_df.iterrows():
            line_name = str(rr.get(ship_line_col, "")).strip()
            if not line_name:
                continue

            n20 = parse_price_to_float(rr.get(of20_col)) if of20_col else None
            n40 = parse_price_to_float(rr.get(of40_col)) if of40_col else None

            # Skip rows that have no ocean amounts at all
            if n20 is None and n40 is None:
                continue

            validity_text = ""
            validity_status = "na"

            if validity_col:
                validity_status, validity_fmt, _ = validity_status_and_text(rr.get(validity_col))
                validity_text = validity_fmt or ""

            ocean_freight_options.append({
                "line": line_name,
                "validity": validity_text,
                "validity_status": validity_status,
                "amt20": fmt_money(n20) if n20 is not None else "N/A",
                "amt40": fmt_money(n40) if n40 is not None else "N/A",
                "amt20_num": float(n20) if n20 is not None else 0.0,
                "amt40_num": float(n40) if n40 is not None else 0.0,
            })

        # Deduplicate identical line/rate/validity entries
        seen = set()
        dedup = []
        for o in ocean_freight_options:
            k = (
                canon(o["line"]),
                o["validity"],
                o["validity_status"],
                o["amt20"],
                o["amt40"]
            )
            if k in seen:
                continue
            seen.add(k)
            dedup.append(o)

        # Sort by line first, then validity status
        def _opt_rank(opt: Dict[str, Any]) -> Tuple[str, int, str]:
            vs = canon(opt.get("validity_status", "na"))
            if vs == "valid":
                rank = 0
            elif vs == "expired":
                rank = 1
            elif vs == "unknown":
                rank = 2
            else:
                rank = 3
            return (canon(opt.get("line", "")), rank, opt.get("validity", ""))

        ocean_freight_options = sorted(dedup, key=_opt_rank)

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
        df_origin_try = df_match.copy()

        if origin_city and ORG_CITY_COL:
            df_origin_try = df_origin_try[
                df_origin_try[ORG_CITY_COL].apply(lambda x: flexible_text_match(origin_city, x))
            ]

        if origin_country and ORG_COUNTRY_COL:
            df_origin_try = df_origin_try[
                df_origin_try[ORG_COUNTRY_COL].apply(lambda x: flexible_text_match(origin_country, x))
            ]

        # If strict origin city/country filters become too strict, do NOT kill the quote.
        # Fall back to the already matched POL/POD/route rows.
        if not df_origin_try.empty:
            df_match = df_origin_try
        else:
            addr_warning_notes.append(
                "⚠ Origin city/country exact match not found. Using POL/POD/route matched rows instead."
            )

        if origin_address and ORG_ADDR_COL:
            any_addr = any(address_soft_match(origin_address, r.get(ORG_ADDR_COL)) for _, r in df_match.iterrows())
            if not any_addr:
                addr_warning_notes.append("⚠ Origin address not exact match, but POL/City/Country matched.")

        # -------------------------
    # DESTINATION filters
    # -------------------------
    if (dest_fields_required or dest_fields_optional) and (not skip_destination_strict_filter):
        df_dest_try = df_match.copy()

        use_city = True if dest_fields_required else bool(dest_city.strip())
        use_country = True if dest_fields_required else bool(dest_country.strip())

        if use_city and dest_city and DST_CITY_COL:
            df_dest_try = df_dest_try[
                df_dest_try[DST_CITY_COL].apply(lambda x: flexible_text_match(dest_city, x))
            ]

        if use_country and dest_country and DST_COUNTRY_COL:
            df_dest_try = df_dest_try[
                df_dest_try[DST_COUNTRY_COL].apply(lambda x: flexible_text_match(dest_country, x))
            ]

        if not df_dest_try.empty:
            df_match = df_dest_try
        else:
            addr_warning_notes.append(
                "⚠ Destination city/country exact match not found. Using POL/POD/route matched rows instead."
            )

        if dest_address and DST_ADDR_COL:
            any_addr = any(address_soft_match(dest_address, r.get(DST_ADDR_COL)) for _, r in df_match.iterrows())
            if not any_addr:
                addr_warning_notes.append("⚠ Destination address not exact match, but POD/City/Country matched.")
    elif skip_destination_strict_filter:
        addr_warning_notes.append(
            "⚠ Route type allows final delivery beyond POD country, so destination strict filtering was skipped."
        )
                
                
 
        # -------------------------
    # ✅ BEST ROW selection (NEW size-aware logic)
    # -------------------------
    display_cols = [c for c in df_match.columns if not str(c).startswith("_")]

    units_info = get_selected_container_units(
        size_20ft_count=size_20ft_count,
        size_40ft_count=size_40ft_count,
        size_2x20ft_count=size_2x20ft_count,
    )
    total_20_units = int(units_info["total_20_units"])
    total_40_units = int(units_info["total_40_units"])

    single_20_count = int(units_info["single_20_count"])
    pair_20_count = int(units_info["count_2x20"])

    # BUSINESS RULE:
    # Normal 20ft-family charge columns (all 20ft charges except trucking)
    # must be multiplied by the ACTUAL PHYSICAL 20ft container count:
    #
    #   total_20_units = single_20_count + (pair_20_count * 2)
    #
    # Examples:
    # - only 2x20ft qty 1  => total_20_units = 2
    # - 20ft qty 1 + 2x20ft qty 1 => total_20_units = 3
    #
    # Trucking is handled separately:
    # - single 20ft trucking uses trucking_charges_20ft * single_20_count
    # - 2x20ft trucking uses trucking_charges_2x20ft * pair_20_count
    effective_20_charge_units = total_20_units

    # For the visible "Grand total per 20ft container" row:
    # - if a single 20ft exists, show the single-20 trucking configuration
    # - otherwise show the 2x20ft-pair trucking configuration
    per20_mode = ""
    if single_20_count > 0:
        per20_mode = "single20"
    elif pair_20_count > 0:
        per20_mode = "pair20"
        
    totals, has_any = compute_selected_shipment_totals_for_df(
        df=df_match,
        columns=display_cols,
        total_20_units=total_20_units,
        total_40_units=total_40_units
    )
    df_match["_grand_total_num"] = totals
    df_match["_grand_total_has"] = has_any

    any_with_total = df_match[df_match["_grand_total_has"] == True]
    if not any_with_total.empty:
        best_idx = any_with_total.sort_values("_grand_total_num").index[0]
    else:
        best_idx = df_match.index[0]

    df_best = df_match.loc[[best_idx]].copy()

       # Build a relaxed grouped source for trucking.
    # We already saved trucking_df before strict address filtering for this purpose.
    trucking_src = trucking_df.copy()

    # Forward-fill grouped columns so sibling rows stay in the same quote group
    trucking_ffill_cols = [
        find_col_case_insensitive(trucking_src, "Shipping Line Name"),
        find_col_case_insensitive(trucking_src, "routes"),
        ORG_ADDR_COL,
        ORG_CITY_COL,
        ORG_COUNTRY_COL,
        DST_ADDR_COL,
        DST_CITY_COL,
        DST_COUNTRY_COL,
    ]
    for c in trucking_ffill_cols:
        if c and c in trucking_src.columns:
            trucking_src[c] = trucking_src[c].ffill()

    # Restrict trucking rows to the same selected shipping line as df_best
    selected_line_val = ""
    if ship_line_col and not df_best.empty:
        selected_line_val = str(df_best.iloc[0].get(ship_line_col, "")).strip()

    if selected_line_val and ship_line_col and ship_line_col in trucking_src.columns:
        trucking_src = trucking_src[
            trucking_src[ship_line_col].apply(lambda x: canon(x) == canon(selected_line_val))
        ].copy()

    # Keep the selected route restriction too
    if selected_route_id_clean and routes_col and routes_col in trucking_src.columns:
        trucking_src = trucking_src[
            trucking_src[routes_col].apply(
                lambda x: route_cell_matches_selected(
                    cell_value=x,
                    selected_route_id=selected_route_id_clean,
                    selected_route_text=selected_route_text_clean
                )
            )
        ].copy()

    # Fallback safety
    if trucking_src.empty:
        trucking_src = df_best.copy()

    trucking_plan = compute_trucking_plan_and_totals(
        matched_df=trucking_src,
        single_20_count=single_20_count,
        pair_20_count=pair_20_count,
        total_40_units=total_40_units,
        per20_mode=per20_mode,
        global_validity_col=global_validity_col,
    )

    own_c = canon(container_ownership)
    is_soc_customer = (own_c == canon("SOC - Customer Owned"))
    is_soc_logenix = (own_c == canon("SOC - Logenix Owned"))
    is_coc = (own_c == canon("COC"))

    def _make_base_row_dict() -> Dict[str, Any]:
        return {
            "validity": "",
            "validity_status": "na",
            "can_remove": True,
            "include_in_total": True,
            "cost_num": 0.0,
            "is_grand_total": False,
            "can_change": False,
            "change_type": "",
            "ocean_size": "",
            "grand_kind": "",
            "grand_mode": "",
            "grand_key": "",
            "options": [],
            "per20_num": 0.0,
            "per40_num": 0.0,
            "ship20_num": 0.0,
            "ship40_num": 0.0,
            "ship_common_num": 0.0,
            "unit_count": 0,
            "is_red_text": False,
        }

    def _row_exists(table_rows: List[Dict[str, Any]], name: str) -> bool:
        return any(canon(r.get("name", "")) == canon(name) for r in table_rows)

    def _get_global_validity_for_row(row: pd.Series) -> Tuple[str, str]:
        """
        New rule:
        Use only the Basic_Details_Section validity column for every quote row.
        """
        if global_validity_col and global_validity_col in row.index:
            validity_status, validity_fmt, _ = validity_status_and_text(row.get(global_validity_col))
            return validity_fmt or "", validity_status

        fallback_col = None
        for c in row.index:
            if canon(c) == canon("validity") or "validity" in canon(c):
                fallback_col = c
                break

        if fallback_col:
            validity_status, validity_fmt, _ = validity_status_and_text(row.get(fallback_col))
            return validity_fmt or "", validity_status

        return "", "na"

    
    def _append_info_row(
        table_rows: List[Dict[str, Any]],
        col_name: str,
        raw_val: Any
    ):
        rr = _make_base_row_dict()
        rr.update({
            "name": str(col_name),
            "can_remove": False,
            "include_in_total": False,
            "cost_num": 0.0,
            "cost": "",
        })

        col_canon = canon(col_name)

        # format date-like fields nicely
        if "date" in col_canon or "validity" in col_canon:
            rr["cost"] = fmt_date_like(raw_val) or str(raw_val).strip()
        else:
            rr["cost"] = str(raw_val).strip()

        table_rows.append(rr)

    def _append_common_charge_row(
        table_rows: List[Dict[str, Any]],
        row: pd.Series,
        col_name: str,
        label: Optional[str] = None
    ):
        actual = next((c for c in display_cols if canon(c) == canon(col_name)), None)
        if not actual:
            return

        raw_val = row.get(actual)
        num = parse_price_to_float(raw_val)
        if num is None:
            return

        validity_text, validity_status = _get_global_validity_for_row(row)

        rr = _make_base_row_dict()
        rr.update({
            "name": label or str(actual),
            "cost": build_flat_cost_display(float(num)),
            "validity": validity_text,
            "validity_status": validity_status,
            "cost_num": float(num),

            # Common charges must behave like per-container charges
            # for each active size family.
            "ship_common_num": 0.0,
            "ship20_num": 0.0,
            "ship40_num": 0.0,
        })

        if total_20_units > 0:
            rr["per20_num"] = float(num)

        if total_40_units > 0:
            rr["per40_num"] = float(num)

        table_rows.append(rr)


    def _append_flat_extra_row(
        table_rows: List[Dict[str, Any]],
        name: str,
        amount: float,
        validity_text: str = "",
        validity_status: str = "na"
    ):
        amt = float(amount or 0.0)
        if amt <= 0:
            return

        rr = _make_base_row_dict()
        rr.update({
            "name": name,
            "cost": build_flat_cost_display(amt),
            "validity": validity_text,
            "validity_status": validity_status,
            "cost_num": amt,

            # Insurance / Misc / Special Cost must be part of
            # per-container totals for active size families.
            "ship_common_num": 0.0,
            "ship20_num": 0.0,
            "ship40_num": 0.0,
        })

        if total_20_units > 0:
            rr["per20_num"] = amt

        if total_40_units > 0:
            rr["per40_num"] = amt

        table_rows.append(rr)

    def _append_sized_manual_adjustment_row(
        table_rows: List[Dict[str, Any]],
        name: str,
        amount: float,
        size_bucket: str,
        subtract: bool = False,
        validity_text: str = "",
        validity_status: str = "na"
    ):
        amt = float(amount or 0.0)
        if amt <= 0:
            return

        signed_amt = -amt if subtract else amt

        rr = _make_base_row_dict()
        rr.update({
            "name": name,
            "cost": build_flat_cost_display(signed_amt),
            "validity": validity_text,
            "validity_status": validity_status,
        })

        if size_bucket == "20":
            if total_20_units <= 0:
                return
            shipment_total = float(signed_amt) * float(total_20_units)
            rr["cost_num"] = shipment_total
            rr["per20_num"] = float(signed_amt)
            rr["ship20_num"] = shipment_total
            rr["unit_count"] = total_20_units

        elif size_bucket == "40":
            if total_40_units <= 0:
                return
            shipment_total = float(signed_amt) * float(total_40_units)
            rr["cost_num"] = shipment_total
            rr["per40_num"] = float(signed_amt)
            rr["ship40_num"] = shipment_total
            rr["unit_count"] = total_40_units

        else:
            return

        table_rows.append(rr)

    def _append_display_only_red_row(
        table_rows: List[Dict[str, Any]],
        name: str,
        amount: float,
        validity_text: str = ""
    ):
        amt = float(amount or 0.0)
        if amt <= 0:
            return

        rr = _make_base_row_dict()
        rr.update({
            "name": name,
            "cost": build_flat_cost_display(amt),
            "validity": validity_text,
            "validity_status": "na",
            "can_remove": False,
            "include_in_total": False,
            "cost_num": 0.0,
            "per20_num": 0.0,
            "per40_num": 0.0,
            "ship20_num": 0.0,
            "ship40_num": 0.0,
            "ship_common_num": 0.0,
            "is_red_text": True,
        })
        table_rows.append(rr)
    def _append_display_only_red_excel_row(
        table_rows: List[Dict[str, Any]],
        row: pd.Series,
        col_name: str,
        label: Optional[str] = None
    ):
        actual = next((c for c in display_cols if canon(c) == canon(col_name)), None)
        if not actual:
            return

        raw_val = row.get(actual)
        num = parse_price_to_float(raw_val)
        if num is None:
            return

        validity_text, validity_status = _get_global_validity_for_row(row)

        rr = _make_base_row_dict()
        rr.update({
            "name": label or str(actual),
            "cost": build_flat_cost_display(float(num)),
            "validity": validity_text,
            "validity_status": validity_status,
            "can_remove": False,
            "include_in_total": False,
            "cost_num": 0.0,
            "per20_num": 0.0,
            "per40_num": 0.0,
            "ship20_num": 0.0,
            "ship40_num": 0.0,
            "ship_common_num": 0.0,
            "is_red_text": True,
        })
        table_rows.append(rr)
    def _append_sized_charge_row(
        table_rows: List[Dict[str, Any]],
        row: pd.Series,
        col_name: str,
        label: Optional[str] = None
    ):
        actual = next((c for c in display_cols if canon(c) == canon(col_name)), None)
        if not actual:
            return

        bucket = charge_size_bucket(actual)
        raw_val = row.get(actual)
        num = parse_price_to_float(raw_val)
        if num is None:
            return

        if bucket == "20" and total_20_units <= 0:
            return
        if bucket == "40" and total_40_units <= 0:
            return

        validity_text, validity_status = _get_global_validity_for_row(row)

        rr = _make_base_row_dict()
        rr["name"] = label or str(actual)
        rr["validity"] = validity_text
        rr["validity_status"] = validity_status

        if bucket == "20":
            # 20ft-family charges must use ACTUAL PHYSICAL 20ft quantity
            # single 20ft + (2 * 2x20ft pairs)
            shipment_units = effective_20_charge_units
            shipment_total = float(num) * float(shipment_units)

            # SHOW exact Excel rate in row, not multiplied amount
            rr["cost"] = build_rate_display(float(num))
            rr["cost_num"] = float(shipment_total)
            rr["per20_num"] = float(num)
            rr["ship20_num"] = float(shipment_total)
            rr["unit_count"] = shipment_units
        elif bucket == "40":
            shipment_total = float(num) * float(total_40_units)

            # SHOW exact Excel rate in row, not multiplied amount
            rr["cost"] = build_rate_display(float(num))
            rr["cost_num"] = float(shipment_total)
            rr["per40_num"] = float(num)
            rr["ship40_num"] = float(shipment_total)
            rr["unit_count"] = total_40_units

        table_rows.append(rr)
    results: List[Dict[str, Any]] = []
    special_cost_lines = special_cost_lines or []

    for _, row in df_best.iterrows():
        validity_label = "Validity: As per individual charge validity column."
        validity_kind = "na"
        table_rows: List[Dict[str, Any]] = []

        # ---- Shipping Line selector row
        if ship_line_col:
            line_val = str(row.get(ship_line_col, "")).strip()
            if line_val:
                rr = _make_base_row_dict()
                rr.update({
                    "name": "Shipping Line Name",
                    "cost": line_val,
                    "can_remove": False,
                    "include_in_total": False,
                    "cost_num": 0.0,
                    "can_change": True,
                    "change_type": "ocean_freight_line",
                    "selected_line": line_val,
                    "options": ocean_freight_options,
                })
                table_rows.append(rr)

        # -------------------------
        # Generic scan of all columns
        # Future-proof:
        # - show non-charge columns too
        # - size-aware for _20ft / _40ft
        # - do not hardcode all possible future columns
        # -------------------------
        skip_cols = {
            canon("SOC_Purchase_Price_charges_20ft"),
            canon("SOC_Purchase_Price_charges_40ft"),
            canon("COC_charges_20ft"),
            canon("COC_charges_40ft"),
            canon("Labor_lifting_cost_20ft"),
            canon("Labor_lifting_cost_40ft"),
            canon("offloading_cost_20ft"),
            canon("offloading_cost_40ft"),
            canon("trucking_charges_20ft"),
            canon("trucking_charges_40ft"),
            canon("trucking_charges_2x20ft"),

            # display-only rows
            canon("Switch_BL_charges"),
        }

        i = 0
        while i < len(display_cols):
            col = display_cols[i]
            raw = row.get(col)
            col_c = canon(col)

            if raw is None or pd.isna(raw) or str(raw).strip() == "":
                i += 1
                continue

            if col_c in {"routes"}:
                i += 1
                continue

            # Do not show blank / Unnamed separator columns.
            if is_blank_or_unnamed_column(col):
                i += 1
                continue

            # Do not show section marker columns like Basic_Details_Section,
            # Ocean_Freight_Section, Airway_Section_Ends, etc.
            if is_section_marker_column(col):
                i += 1
                continue

            # Validity is now one Basic_Details_Section column applied to all rows,
            # so do not show it as a normal info row.
            if "validity" in col_c and not is_charges_column(col):
                i += 1
                continue

            if col_c in skip_cols:
                i += 1
                continue

            bucket = charge_size_bucket(col)

            # -------------------------
            # NON-CHARGE COLUMNS
            # -------------------------
            if not is_charges_column(col):
                if bucket == "20" and total_20_units <= 0:
                    i += 1
                    continue

                if bucket == "40" and total_40_units <= 0:
                    i += 1
                    continue

                if bucket == "20":
                    base_name = strip_size_suffix(col)
                    next_col = display_cols[i + 1] if (i + 1) < len(display_cols) else None
                    next_bucket = charge_size_bucket(next_col) if next_col else ""

                    if next_col and next_bucket == "40" and strip_size_suffix(next_col) == base_name:
                        if total_20_units > 0:
                            _append_info_row(table_rows, col, raw)

                        if total_40_units > 0:
                            raw40 = row.get(next_col)
                            if raw40 is not None and not pd.isna(raw40) and str(raw40).strip() != "":
                                _append_info_row(table_rows, next_col, raw40)

                        i += 2
                        continue

                _append_info_row(table_rows, col, raw)
                i += 1
                continue

            # -------------------------
            # CHARGE COLUMNS
            # -------------------------
            if bucket == "20":
                base_name = strip_size_suffix(col)
                next_col = display_cols[i + 1] if (i + 1) < len(display_cols) else None
                next_bucket = charge_size_bucket(next_col) if next_col else ""

                if next_col and next_bucket == "40" and strip_size_suffix(next_col) == base_name:
                    validity_col_local = (
                        display_cols[i + 2]
                        if (i + 2) < len(display_cols) and "validity" in canon(display_cols[i + 2])
                        else None
                    )

                    if total_20_units > 0:
                        raw20 = row.get(col)
                        num20 = parse_price_to_float(raw20)
                        if num20 is not None:
                            shipment_units = effective_20_charge_units
                            rr = _make_base_row_dict()
                            rr.update({
                                "name": str(col),
                                "cost": build_rate_display(float(num20)),
                                "cost_num": float(num20) * float(shipment_units),
                                "per20_num": float(num20),
                                "ship20_num": float(num20) * float(shipment_units),
                                "unit_count": shipment_units,
                            })
                            rr["validity"], rr["validity_status"] = _get_global_validity_for_row(row)
                            table_rows.append(rr)

                    if total_40_units > 0:
                        raw40 = row.get(next_col)
                        num40 = parse_price_to_float(raw40)
                        if num40 is not None:
                            rr = _make_base_row_dict()
                            rr.update({
                                "name": str(next_col),
                                "cost": build_rate_display(float(num40)),
                                "cost_num": float(num40) * float(total_40_units),
                                "per40_num": float(num40),
                                "ship40_num": float(num40) * float(total_40_units),
                                "unit_count": total_40_units,
                            })
                            rr["validity"], rr["validity_status"] = _get_global_validity_for_row(row)
                            table_rows.append(rr)

                    i += 3 if validity_col_local else 2
                    continue

                if total_20_units > 0:
                    _append_sized_charge_row(table_rows, row, col)
                i += 1
                continue

            if bucket == "40":
                if total_40_units > 0:
                    _append_sized_charge_row(table_rows, row, col)
                i += 1
                continue

            _append_common_charge_row(table_rows, row, col)
            i += 1

        # -------------------------
        # Final safety filter
        # -------------------------
        filtered_rows: List[Dict[str, Any]] = []
        for rr in table_rows:
            nm = canon(rr.get("name", ""))

            if ("_40ft" in nm or "(40ft)" in nm) and total_40_units <= 0:
                continue

            if ("_20ft" in nm or "(20ft)" in nm) and total_20_units <= 0:
                continue

            filtered_rows.append(rr)

        table_rows = filtered_rows

        # -------------------------
        # Ownership-specific charges
        # -------------------------
        if is_soc_logenix:
            if total_20_units > 0:
                _append_sized_charge_row(table_rows, row, "SOC_Purchase_Price_charges_20ft")
            if total_40_units > 0:
                _append_sized_charge_row(table_rows, row, "SOC_Purchase_Price_charges_40ft")

            clearance_20_num = parse_money_allow_empty(soc_clearance_cost_20ft_value)
            if clearance_20_num > 0 and not _row_exists(table_rows, "SOC Container Custom Clearance Charges 20ft"):
                _append_sized_manual_adjustment_row(
                    table_rows=table_rows,
                    name="SOC Container Custom Clearance Charges 20ft",
                    amount=clearance_20_num,
                    size_bucket="20",
                    subtract=False,
                )

            clearance_40_num = parse_money_allow_empty(soc_clearance_cost_40ft_value)
            if clearance_40_num > 0 and not _row_exists(table_rows, "SOC Container Custom Clearance Charges 40ft"):
                _append_sized_manual_adjustment_row(
                    table_rows=table_rows,
                    name="SOC Container Custom Clearance Charges 40ft",
                    amount=clearance_40_num,
                    size_bucket="40",
                    subtract=False,
                )

            selling_20_num = parse_money_allow_empty(soc_selling_price_20ft_value)
            if selling_20_num > 0 and not _row_exists(table_rows, "SOC Container Selling Price 20ft"):
                _append_sized_manual_adjustment_row(
                    table_rows=table_rows,
                    name="SOC Container Selling Price 20ft",
                    amount=selling_20_num,
                    size_bucket="20",
                    subtract=True,
                )

            selling_40_num = parse_money_allow_empty(soc_selling_price_40ft_value)
            if selling_40_num > 0 and not _row_exists(table_rows, "SOC Container Selling Price 40ft"):
                _append_sized_manual_adjustment_row(
                    table_rows=table_rows,
                    name="SOC Container Selling Price 40ft",
                    amount=selling_40_num,
                    size_bucket="40",
                    subtract=True,
                )

        elif is_coc:
            if total_20_units > 0:
                _append_sized_charge_row(table_rows, row, "COC_charges_20ft")
            if total_40_units > 0:
                _append_sized_charge_row(table_rows, row, "COC_charges_40ft")

        # -------------------------
        # Lifting / Labor required?
        # -------------------------
        if canon(lifting_labor_required) == canon("Yes"):
            if total_20_units > 0:
                _append_sized_charge_row(table_rows, row, "Labor_lifting_cost_20ft")
            if total_40_units > 0:
                _append_sized_charge_row(table_rows, row, "Labor_lifting_cost_40ft")

        # -------------------------
        # Offloading responsibility
        # -------------------------
        if canon(offloading_responsible) == canon("Logenix"):
            if total_20_units > 0:
                _append_sized_charge_row(table_rows, row, "offloading_cost_20ft")
            if total_40_units > 0:
                _append_sized_charge_row(table_rows, row, "offloading_cost_40ft")

        # -------------------------
        # Trucking rows
        # -------------------------
        for tr in trucking_plan["rows"]:
            table_rows.append(tr)

        # -------------------------
        # Extras
        # -------------------------
        # Insurance must be DISPLAY ONLY in red, below totals, not included in totals
        insurance_display_num = 0.0
        if insurance_amount_num is not None and float(insurance_amount_num) > 0:
            insurance_display_num = float(insurance_amount_num)

        misc_num = parse_money_allow_empty(misc_cost_value)
        if misc_num > 0:
            _append_flat_extra_row(
                table_rows=table_rows,
                name="Miscellaneous Cost",
                amount=float(misc_num),
            )

        for it in (special_cost_lines or []):
            label = (it.get("reason") or "").strip() or "Special Cost"
            amt = float(it.get("cost_num") or 0.0)
            if amt <= 0:
                continue

            _append_flat_extra_row(
                table_rows=table_rows,
                name=f"Special Cost — {label}",
                amount=amt,
            )

        incurrence_num = parse_money_allow_empty(incurrence_charges_value)

        # -------------------------
        # Grand totals
        # -------------------------
        total_per_20 = 0.0
        total_per_40 = 0.0

        single20_truck_rate = 0.0
        pair20_truck_rate = 0.0

        for rr in table_rows:
            if rr.get("is_grand_total"):
                continue
            if not rr.get("include_in_total"):
                continue

            row_name_c = canon(rr.get("name", ""))

            total_per_20 += float(rr.get("per20_num") or 0.0)
            total_per_40 += float(rr.get("per40_num") or 0.0)

            if row_name_c == canon("trucking_charges_20ft"):
                single20_truck_rate = (
                    parse_price_to_float(rr.get("cost")) or float(rr.get("per20_num") or 0.0)
                )

            elif row_name_c == canon("trucking_charges_2x20ft"):
                pair20_truck_rate = (
                    parse_price_to_float(rr.get("cost")) or 0.0
                )

        # 20ft-side shipment rule:
        # base per20 total excluding single 20ft trucking
        # multiplied by total physical 20ft containers
        # then add trucking separately
        base_20_without_single_truck = float(total_per_20)
        if single_20_count > 0:
            base_20_without_single_truck -= float(single20_truck_rate)

        total_shipment_20 = 0.0
        if total_20_units > 0:
            total_shipment_20 = (
                float(base_20_without_single_truck) * float(total_20_units)
                + float(single20_truck_rate) * float(single_20_count)
                + float(pair20_truck_rate) * float(pair_20_count)
            )

        # 40ft-side shipment rule:
        # per 40ft total × selected 40ft quantity
        total_shipment_40 = 0.0
        if total_40_units > 0:
            total_shipment_40 = float(total_per_40) * float(total_40_units)

        total_shipment = float(total_shipment_20) + float(total_shipment_40)

        if total_20_units > 0:
            rr = _make_base_row_dict()
            rr.update({
                "name": "Grand total per 20ft container",
                "cost": build_flat_cost_display(total_per_20),
                "can_remove": False,
                "include_in_total": False,
                "is_grand_total": True,
                "grand_mode": "per_unit",
                "grand_key": "20",
            })
            table_rows.append(rr)

        if total_40_units > 0:
            rr = _make_base_row_dict()
            rr.update({
                "name": "Grand total per 40ft container",
                "cost": build_flat_cost_display(total_per_40),
                "can_remove": False,
                "include_in_total": False,
                "is_grand_total": True,
                "grand_mode": "per_unit",
                "grand_key": "40",
            })
            table_rows.append(rr)

        rr = _make_base_row_dict()
        rr.update({
            "name": "Grand total shipment cost",
            "cost": build_flat_cost_display(total_shipment),
            "can_remove": False,
            "include_in_total": False,
            "is_grand_total": True,
            "grand_mode": "shipment",
            "grand_key": "shipment",
        })
        table_rows.append(rr)

        # -------------------------
        # Display-only rows (RED, below totals)
        # -------------------------
        if insurance_display_num > 0:
            _append_display_only_red_row(
                table_rows=table_rows,
                name="Insurance (Calculated)",
                amount=float(insurance_display_num),
                validity_text=""
            )

        _append_display_only_red_excel_row(
            table_rows=table_rows,
            row=row,
            col_name="Switch_BL_charges",
            label="Switch_BL_charges"
        )

        if incurrence_num > 0:
            _append_display_only_red_row(
                table_rows=table_rows,
                name="Incurrence Charges (Not Included in Totals)",
                amount=float(incurrence_num),
                validity_text=""
            )

        results.append({
            "title": "Matched Quote",
            "match_note": " ".join(addr_warning_notes).strip(),
            "table_rows": table_rows
        })

    best_text = "Best Option available based on rate validity and match."
    return results[: max(1, int(limit or 1))], best_text, None
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
    add("Shipment Mode", "shipment_mode")
    add("Company", "company_name")
    add("Salesperson Name", "salesperson_name")
    add("Container Ownership", "container_ownership")
    add("SOC Container Custom Clearance Charges 20ft", "soc_clearance_charges_20ft")
    add("SOC Container Custom Clearance Charges 40ft", "soc_clearance_charges_40ft")
    add("SOC Container Selling Price 20ft", "soc_selling_price_20ft")
    add("SOC Container Selling Price 40ft", "soc_selling_price_40ft")
    add("Incoterm for Origin", "incoterm_origin")
    add("Incoterm for Destination", "incoterm_destination")
    add("Port of Load", "port_of_loading")
    add("Port of Discharge", "port_of_destination")

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
    add("Route Type", "selected_route_type")
    add("Route Mode", "selected_route_mode_label")
    add("Transit Time (Days)", "selected_route_transit_days")

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
    add("Incurrence Charges", "incurrence_charges")
    add("Special Cost Option", "special_cost_option")
    for i in range(1, 11):
        add(f"Special Reason {i}", f"special_reason_{i}")
        add(f"Special Cost {i}", f"special_cost_{i}")

    add("Special Costs Total", "special_cost_total")

    add("Generated Grand Total per 20ft Container", "generated_grand_total_per_20ft_container")
    add("Generated Grand Total per 40ft Container", "generated_grand_total_per_40ft_container")
    add("Generated Grand Total Shipment Cost", "generated_grand_total_shipment_cost")

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
    Legacy helper kept only for compatibility.
    Route matching now uses get_matching_routes(...) with route_type-aware logic.
    """
    routes, best_route_id = get_matching_routes(pol=pol, pod=pod)
    return routes, best_route_id, ""


@app.post("/api/routes")
def api_routes():
    pol = (request.form.get("port_of_loading") or "").strip()
    pod = (request.form.get("port_of_destination") or "").strip()

    origin_city = (request.form.get("origin_city") or "").strip()
    origin_country = (request.form.get("origin_country") or "").strip()

    destination_city = (request.form.get("destination_city") or "").strip()
    destination_country = (request.form.get("destination_country") or "").strip()

    transit_borders = [
        (request.form.get("transit_border_1") or "").strip(),
        (request.form.get("transit_border_2") or "").strip(),
        (request.form.get("transit_border_3") or "").strip(),
        (request.form.get("transit_border_4") or "").strip(),
    ]

    if not pol and not origin_city and not origin_country:
        return jsonify({"ok": False, "routes": [], "best_route_id": None, "route_error_msg": ""}), 200

    routes, best_route_id = get_matching_routes(
        pol=pol,
        pod=pod,
        origin_city=origin_city,
        origin_country=origin_country,
        destination_city=destination_city,
        destination_country=destination_country,
        transit_borders=transit_borders
    )

    payload = []
    for r in routes:
        t = r.get("transit_time_days") if isinstance(r.get("transit_time_days"), dict) else {}
        payload.append({
            "id": str(r.get("id", "")),
            "title": r.get("title", "") or f"Route {r.get('id','')}",
            "path": r.get("path", "") or "",
            "route_status": (r.get("route_status") or "open").lower(),
            "status_label": r.get("status_label", "") or "",
            "route_type": r.get("route_type", "") or "",
            "mode_label": r.get("mode_label", "") or "",
            "modes": r.get("modes", []) or [],
            "is_recent": bool(r.get("is_recent", False)),
            "is_best": bool(r.get("is_best", False)),
            "transit_min": t.get("min") if isinstance(t, dict) else r.get("transit_min"),
            "transit_max": t.get("max") if isinstance(t, dict) else r.get("transit_max"),
        })

    return jsonify({
        "ok": True,
        "routes": payload,
        "best_route_id": str(best_route_id) if best_route_id is not None else None,
        "route_error_msg": ""
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

 # read Pick Up / Delivery Point Details (up to 4 each)
# Route matching now uses:
# origin city/country -> POL -> POD(optional alternative) -> destination city/country
# depending on route_type from routes.json
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
    shipment_mode = request.form.get("shipment_mode", "").strip()
    company_name = request.form.get("company_name", "").strip()
    salesperson_name = request.form.get("salesperson_name", "").strip()
    container_ownership = request.form.get("container_ownership", "").strip()

    soc_clearance_charges_20ft_raw = request.form.get("soc_clearance_charges_20ft", "").strip()
    soc_clearance_charges_20ft_saved = soc_clearance_charges_20ft_raw

    soc_clearance_charges_40ft_raw = request.form.get("soc_clearance_charges_40ft", "").strip()
    soc_clearance_charges_40ft_saved = soc_clearance_charges_40ft_raw

    soc_selling_price_20ft_raw = request.form.get("soc_selling_price_20ft", "").strip()
    soc_selling_price_20ft_saved = soc_selling_price_20ft_raw

    soc_selling_price_40ft_raw = request.form.get("soc_selling_price_40ft", "").strip()
    soc_selling_price_40ft_saved = soc_selling_price_40ft_raw

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

    # RAW values for ROUTE MATCHING (do not overwrite these)
    route_match_origin_city = pols[0]["city"].strip() if len(pols) > 0 else ""
    route_match_origin_country = pols[0]["country"].strip() if len(pols) > 0 else ""
    route_match_destination_city = pods[0]["city"].strip() if len(pods) > 0 else ""
    route_match_destination_country = pods[0]["country"].strip() if len(pods) > 0 else ""

    # WORKING values for PRICE / QUOTE LOGIC
    # These may use incoterm fallback, but route matching must not.
    shipping_from_1_city = route_match_origin_city
    destination_1_city = route_match_destination_city

    if (not origin_open) and (not shipping_from_1_city):
        shipping_from_1_city = port_of_loading

    if (not delivery_required) and (not delivery_optional) and (not destination_1_city):
        destination_1_city = port_of_destination


    shipment_type = request.form.get("shipment_type", "").strip()

    lifting_labor_required = request.form.get("lifting_labor_required", "").strip()
    offloading_responsible = request.form.get("offloading_responsible", "").strip()
    final_customs_responsible = request.form.get("final_customs_responsible", "").strip()

        # If either Incoterm field is FOB or FOR, responsibility fields are hidden in frontend.
    # Force them blank in backend too, so old cached/posted values cannot affect pricing.
    inc_origin_code = canon(incoterm_origin).split(" ")[0].upper() if incoterm_origin else ""
    inc_dest_code = canon(incoterm_destination).split(" ")[0].upper() if incoterm_destination else ""

    if inc_origin_code in {"FOB", "FOR"} or inc_dest_code in {"FOB", "FOR"}:
        offloading_responsible = ""
        final_customs_responsible = ""

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

    def to_20ft_count(x: str) -> int:
        """
        20ft is allowed only as 0 or 1.
        Anything invalid/non-positive becomes 0.
        Anything above 1 is kept as-is temporarily so we can show validation error.
        """
        try:
            v = int(x)
            if v <= 0:
                return 0
            return v
        except Exception:
            return 0

    size_20ft_count = to_20ft_count(size_20ft_count_raw)
    size_40ft_count = to_int_or_zero(size_40ft_count_raw)
    size_2x20ft_count = to_int_or_zero(size_2x20ft_count_raw)

    # 20ft container can only be 0 or 1
    if size_20ft_count > 1:
        route_error_msg = "20ft container quantity can only be 1. Please select either 0 or 1 for 20ft."

    # Physical container count:
    # - single 20ft counts as 1
    # - 40ft counts as 1 each
    # - 2x20ft pair counts as 2 physical containers per entered pair
    total_containers = (
        int(size_20ft_count or 0)
        + int(size_40ft_count or 0)
        + (int(size_2x20ft_count or 0) * 2)
    )

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

    incurrence_charges_raw = request.form.get("incurrence_charges", "").strip()
    incurrence_charges_saved = incurrence_charges_raw



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
        "shipment_mode": shipment_mode,
        "company_name": company_name,
        "salesperson_name": salesperson_name,
        "container_ownership": container_ownership,
        "soc_clearance_charges_20ft": soc_clearance_charges_20ft_saved,
        "soc_clearance_charges_40ft": soc_clearance_charges_40ft_saved,
        "soc_selling_price_20ft": soc_selling_price_20ft_saved,
        "soc_selling_price_40ft": soc_selling_price_40ft_saved,
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
        "incurrence_charges": incurrence_charges_saved,

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
    origin_city=route_match_origin_city,
    origin_country=route_match_origin_country,
    destination_city=route_match_destination_city,
    destination_country=route_match_destination_country,
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
    confirm_closed = (request.form.get("confirm_closed_route", "") or request.form.get("confirm_closed", "")).strip().lower()

    selected_route_text = None
    selected_route_status = ""
    selected_route_transit_days = ""
    selected_route_type = ""
    selected_route_mode_label = ""

    # Route selection is required only when routes actually exist.
    if all_routes:
        if not selected_route_id:
            route_error_msg = "Please select one route."
        else:
            chosen = next((r for r in all_routes if str(r.get("id")) == selected_route_id), None)
            if not chosen:
                route_error_msg = "Selected route not found. Please choose again."
            else:
                selected_route_text = str(chosen.get("path", "")).strip()
                selected_route_status = (chosen.get("route_status") or "open").strip().lower()
                selected_route_type = normalize_route_type(chosen.get("route_type"))
                selected_route_mode_label = chosen.get("mode_label", "") or route_mode_label(chosen)

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

    data: Dict[str, Any] = {
        "quote_id": f"QUOTE-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        "shipment_mode": shipment_mode,
        "company_name": company_name,
        "salesperson_name": salesperson_name,
        "container_ownership": container_ownership,
        "soc_clearance_charges_20ft": soc_clearance_charges_20ft_saved,
        "soc_clearance_charges_40ft": soc_clearance_charges_40ft_saved,
        "soc_selling_price_20ft": soc_selling_price_20ft_saved,
        "soc_selling_price_40ft": soc_selling_price_40ft_saved,
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
        "selected_route_type": selected_route_type,
        "selected_route_mode_label": selected_route_mode_label,

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
        "incurrence_charges": incurrence_charges_saved,

        "special_cost_option": special_cost_option,
        "special_cost_total": float(special_cost_total),
        "shipment_type": shipment_type,
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }

    for i in range(1, 11):
        data[f"special_reason_{i}"] = request.form.get(f"special_reason_{i}", "").strip()
        data[f"special_cost_{i}"] = request.form.get(f"special_cost_{i}", "").strip()

    save_warning_msg: Optional[str] = None

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

    rates, best_text, error_msg = get_strict_quotes(
        pol_port=port_of_loading,
        pod_port=port_of_destination,

        incoterm_origin=incoterm_origin,
        incoterm_destination=incoterm_destination,
        shipment_mode=shipment_mode,
        selected_route_type=selected_route_type,
        selected_route_mode_label=selected_route_mode_label,

        origin_address=pols[0]["address"],
        origin_city=shipping_from_1_city,
        origin_country=pols[0]["country"],

        dest_address=pods[0]["address"],
        dest_city=destination_1_city,
        dest_country=pods[0]["country"],

        container_size_label=(container_size_summary if container_size_summary else ""),

        selected_route_id=selected_route_id,
        selected_route_text=selected_route_text if selected_route_text else "",

        size_20ft_count=size_20ft_count,
        size_40ft_count=size_40ft_count,
        size_2x20ft_count=size_2x20ft_count,

        container_ownership=container_ownership,
        soc_clearance_cost_20ft_value=soc_clearance_charges_20ft_saved,
        soc_clearance_cost_40ft_value=soc_clearance_charges_40ft_saved,
        soc_selling_price_20ft_value=soc_selling_price_20ft_saved,
        soc_selling_price_40ft_value=soc_selling_price_40ft_saved,
        lifting_labor_required=lifting_labor_required,
        offloading_responsible=offloading_responsible,

        special_cost_lines=special_cost_items,

        insurance_amount_num=insurance_amount_num,
        incurrence_charges_value=incurrence_charges_saved,
        misc_cost_value=misc_cost_saved,

        limit=1
    )

    # Add all generated quote prices + grand totals into the same row
    # before saving to queries.xlsx.
    data = add_generated_quote_prices_to_record(data, rates)

    save_ok, save_msg = save_to_excel(data)
    if not save_ok:
        save_warning_msg = save_msg

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
        form_data=form_data,
        submitted=True,
        submitted_items=submitted_items,

        routes=all_routes,
        best_route_id=best_route_id_all,
        selected_route_id=selected_route_id if selected_route_id else None,
        route_error_msg=route_error_msg,

        rates=rates,
        best_text=best_text,
        error_msg=(f"{error_msg} {save_warning_msg}".strip() if error_msg and save_warning_msg else (error_msg or save_warning_msg))
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