import os
import sys
import io
import datetime as dt
import xml.etree.ElementTree as ET
from typing import List, Dict

import pandas as pd
import requests

# --- Stałe / Ścieżki ---
ECB_90D_XML_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist-90d.xml"

DATA_DIR = "data"
HIST_RAW = os.path.join(DATA_DIR, "history_eur_base.csv")   # 1 EUR = X [waluta]
HIST_PLN = os.path.join(DATA_DIR, "history_pln.csv")

TARGETS: List[str] = ["EUR", "USD", "GBP", "CHF"]

# --- Utils ---
def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)

def load_history(path: str) -> pd.DataFrame:
    if os.path.exists(path):
        df = pd.read_csv(path, parse_dates=["date"])
        df.columns = [c.upper() if c != "date" else "date" for c in df.columns]
        return df.sort_values("date")
    return pd.DataFrame(columns=["date"]).astype({"date": "datetime64[ns]"})

def save_history(path: str, df: pd.DataFrame) -> None:
    df.sort_values("date").to_csv(path, index=False)

def compute_pln_rates(df: pd.DataFrame, targets: List[str]) -> pd.DataFrame:
    if "PLN" not in df.columns:
        raise ValueError("Brak kolumny 'PLN' w danych ECB – nie policzymy kursów do PLN.")
    out = pd.DataFrame({"date": df["date"]})
    if "EUR" in targets:
        out["EUR_PLN"] = df["PLN"]
    for ccy in targets:
        if ccy in ("EUR",):
            continue
        if ccy not in df.columns:
            continue
        out[f"{ccy}_PLN"] = df["PLN"] / df[ccy]
    return out.dropna(how="all", subset=[c for c in out.columns if c != "date"]).sort_values("date")

# --- Parsowanie XML 90 dni ---
def fetch_ecb_90d_xml(url: str = ECB_90D_XML_URL, timeout: int = 30) -> pd.DataFrame:
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    tree = ET.parse(io.BytesIO(r.content))
    root = tree.getroot()
    ns_strip = lambda tag: tag.split("}")[-1]

    records = []
    for cube_time in root.iter():
        if ns_strip(cube_time.tag) == "Cube" and "time" in cube_time.attrib:
            date_str = cube_time.attrib["time"]
            rates: Dict[str, float] = {}
            for n in cube_time:
                if ns_strip(n.tag) == "Cube" and "currency" in n.attrib and "rate" in n.attrib:
                    ccy = n.attrib["currency"].upper()
                    try:
                        rates[ccy] = float(n.attrib["rate"])
                    except ValueError:
                        rates[ccy] = float("nan")
            rec = {"date": pd.to_datetime(date_str, format="%Y-%m-%d")}
            rec.update(rates)
            records.append(rec)

    df = pd.DataFrame(records)
    # kolumny na upper
    df.columns = [c.upper() if c != "date" else "date" for c in df.columns]
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    return df

# --- Merge z historią ---
def upsert_90d_into_history(hist: pd.DataFrame, last90: pd.DataFrame) -> pd.DataFrame:
    if hist.empty:
        return last90.copy()
    hist_idx = hist.set_index("date")
    last90_idx = last90.set_index("date")
    hist_idx.update(last90_idx)
    missing_idx = last90_idx.index.difference(hist_idx.index)
    if len(missing_idx) > 0:
        hist_idx = pd.concat([hist_idx, last90_idx.loc[missing_idx]], axis=0)
    hist_idx = hist_idx.sort_index()
    return hist_idx.reset_index()

def main():
    try:
        ensure_dirs()
        print("Ładuję istniejącą historię…")
        hist = load_history(HIST_RAW)
        before_rows = len(hist)

        print("Pobieram i parsuję 90 dni XML z ECB…")
        last90 = fetch_ecb_90d_xml()
        print(f"Pobrano {len(last90)} dni.")

        print("Łączenie z historią…")
        merged = upsert_90d_into_history(hist, last90)
        after_rows = len(merged)
        print(f"Historia po scaleniu: {after_rows} wierszy (wcześniej: {before_rows}).")

        save_history(HIST_RAW, merged)
        print(f"Zapisano: {HIST_RAW}")

        df_pln = compute_pln_rates(merged, TARGETS)
        df_pln.to_csv(HIST_PLN, index=False)
        print(f"Zapisano: {HIST_PLN}")

        print("Backfill 90 dni zakończony.")
    except Exception as e:
        print("Błąd:", e, file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
