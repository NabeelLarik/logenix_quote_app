from flask import Flask, request, render_template
import pandas as pd
import os
from datetime import datetime

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_FILE = os.path.join(BASE_DIR, "queries.xlsx")
PRICES_FILE = os.path.join(BASE_DIR, "prices_updated.xlsx")

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
    "Mersin Port",
    "Abu-Dhabi",
    "Jabel Ali Port",
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
    "bandar Abbas Port",
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


def get_commodities():
    """
    Build a commodity list from BASE_COMMODITIES + unique commodities
    stored in the Excel file (queries.xlsx).
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
            # If any error reading Excel, just fall back to base commodities
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


def get_rates_for_origin(origin_value):
    """
    Fetch up to 4 matching rows from prices_updated.xlsx
    where POL matches origin_value (case-insensitive).
    Assumes headers are in 1st and 2nd row, and we use the 2nd row as header.
    """
    if not os.path.exists(PRICES_FILE):
        return []

    if not origin_value:
        return []

    try:
        # Use the second row as header (header row index = 1)
        df = pd.read_excel(PRICES_FILE, header=1)
    except Exception:
        return []

    if "POL" not in df.columns:
        # If the expected column is not found, just return empty
        return []

    # Case-insensitive match on POL
    user_pol = str(origin_value).strip().lower()
    pol_series = df["POL"].astype(str).str.strip().str.lower()
    matched = df[pol_series == user_pol].head(4)

    if matched.empty:
        return []

    # Only keep the fields we care about; if any are missing, value becomes None
    desired_fields = [
        "Date",
        "POL",
        "POD",
        "Ocean Freight/40ft",
        "Type of Container",
        "Free days",
        "Rates Validity",
        "Commodity"
    ]

    results = []
    for _, row in matched.iterrows():
        record = {}
        for field in desired_fields:
            record[field] = row[field] if field in matched.columns else None
        results.append(record)

    return results


@app.route("/", methods=["GET"])
def index():
    return render_template(
        "form.html",
        countries=COUNTRIES,
        commodities=get_commodities(),
        submitted=False,
        data=None,
        rates=[]
    )


@app.route("/submit", methods=["POST"])
def submit():
    data = {
        "quote_id": f"QUOTE-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
        "company_name": request.form["company_name"],
        "shipping_from": request.form["shipping_from"],
        "destination": request.form["destination"],
        "commodity": request.form["commodity"],
        "cargo_type": request.form["cargo_type"],
        "weight_tons": request.form["weight_tons"],
        "container_type": request.form["container_type"],
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }

    # Save new query
    save_to_excel(data)

    # Fetch matching rates from prices_updated.xlsx based on POL/origin
    rates = get_rates_for_origin(data["shipping_from"])

    return render_template(
        "form.html",
        countries=COUNTRIES,
        commodities=get_commodities(),
        submitted=True,
        data=data,
        rates=rates
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
   # app.run(debug=True)
