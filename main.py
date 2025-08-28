import os
import io
import sys
import datetime as dt
from typing import List, Dict
import xml.etree.ElementTree as ET

import pandas as pd
import requests
import plotly.graph_objs as go
from plotly.offline import plot

# --- Stałe / Ścieżki ---
ECB_DAILY_XML_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"

DATA_DIR = "data"
HIST_RAW = os.path.join(DATA_DIR, "history_eur_base.csv")   # historia stawek ECB: 1 EUR = X [waluta]
HIST_PLN = os.path.join(DATA_DIR, "history_pln.csv")        # historia przeliczona do PLN
DASHBOARD_HTML = "dashboard.html"

# Waluty, które pokażemy w PLN (EUR to po prostu kolumna PLN)
TARGETS: List[str] = ["EUR", "USD", "GBP", "CHF"]

# Ile dni pokazywać na wykresie (None = całość)
CHART_LAST_N_DAYS: int | None = 365


# --- Pobieranie dziennego XML z ECB ---
def fetch_ecb_daily_xml(url: str = ECB_DAILY_XML_URL, timeout: int = 30) -> pd.DataFrame:
    """
    Pobiera eurofxref-daily.xml (1 EUR = X [waluta]) i zwraca 1-wierszowy DataFrame:
    kolumna 'date' + kolumny z walutami (USD, PLN, GBP, ...).
    """
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    tree = ET.parse(io.BytesIO(r.content))
    root = tree.getroot()

    # XML namespace bywa zmienne — weźmy wszystko po nazwie końcowej
    # Struktura: {…}Envelope/{…}Cube/{…}Cube[@time]/({…}Cube[@currency][@rate])*
    # Szukamy elementu z atrybutem 'time'
    ns_strip = lambda tag: tag.split("}")[-1]
    date_str = None
    rates: Dict[str, float] = {}

    for child in root.iter():
        if ns_strip(child.tag) == "Cube" and "time" in child.attrib:
            date_str = child.attrib["time"]  # YYYY-MM-DD
            # wewnętrzne "Cube" z currency/rate
            for n in child:
                if ns_strip(n.tag) == "Cube" and "currency" in n.attrib and "rate" in n.attrib:
                    ccy = n.attrib["currency"].upper()
                    try:
                        rates[ccy] = float(n.attrib["rate"])
                    except ValueError:
                        rates[ccy] = float("nan")
            break  # mamy dzień, wychodzimy

    if not date_str:
        raise ValueError("Nie znaleziono atrybutu 'time' w eurofxref-daily.xml (brak daty).")

    df = pd.DataFrame([rates])
    df.insert(0, "date", pd.to_datetime(date_str, format="%Y-%m-%d"))
    # Upewnij się, że wszystkie kolumny (poza date) są numeryczne
    for c in df.columns:
        if c != "date":
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


# --- Historia: wczytaj/zapisz, upsert ---
def ensure_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)


def load_history(path: str) -> pd.DataFrame:
    if os.path.exists(path):
        df = pd.read_csv(path, parse_dates=["date"])
        # normalizacja nazw walut (upper)
        df.columns = [c.upper() if c != "date" else "date" for c in df.columns]
        return df.sort_values("date")
    else:
        return pd.DataFrame(columns=["date"]).astype({"date": "datetime64[ns]"})


def upsert_daily_row(history: pd.DataFrame, today_row: pd.DataFrame) -> pd.DataFrame:
    """
    Jeśli w historii nie ma dzisiejszej daty -> dopisz wiersz z dzisiejszymi stawkami.
    Jeśli jest -> zwróć historię bez zmian.
    """
    if today_row.empty:
        return history

    d = today_row.iloc[0]["date"]
    if history["date"].astype("datetime64[ns]").eq(d).any():
        # nic nie robimy – dzisiejsza data już jest
        return history

    # dopasuj kolumny: dodaj brakujące waluty
    for col in today_row.columns:
        if col != "date" and col not in history.columns:
            history[col] = pd.Series(dtype="float")

    # dopisz
    history = pd.concat([history, today_row[history.columns]], ignore_index=True)
    return history.sort_values("date")


# --- Przeliczanie na PLN ---
def compute_pln_rates(df: pd.DataFrame, targets: List[str]) -> pd.DataFrame:
    """
    Z danych ECB (1 EUR = X [ccy]) tworzy tabelę kursów w PLN dla wybranych walut.
    EUR->PLN = kolumna PLN (bo 1 EUR = X PLN).
    USD->PLN = PLN / USD, itd.
    """
    if "PLN" not in df.columns:
        raise ValueError("W danych ECB brak kolumny 'PLN' — nie policzymy przeliczeń do PLN.")

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


# --- Dashboard ---
def make_dashboard(df_pln: pd.DataFrame, targets: List[str], out_html: str = DASHBOARD_HTML,
                   last_n_days: int | None = CHART_LAST_N_DAYS) -> None:
    if df_pln.empty:
        # utwórz pusty raport z wiadomością
        html = "<h2>Brak danych do wyświetlenia</h2>"
        with open(out_html, "w", encoding="utf-8") as f:
            f.write(html)
        return

    df_plot = df_pln.copy()
    if last_n_days is not None:
        cutoff = df_plot["date"].max() - pd.Timedelta(days=last_n_days)
        df_plot = df_plot[df_plot["date"] >= cutoff]

    traces = []
    for ccy in targets:
        col = "EUR_PLN" if ccy == "EUR" else f"{ccy}_PLN"
        if col in df_plot.columns:
            traces.append(go.Scatter(x=df_plot["date"], y=df_plot[col], mode="lines", name=col.replace("_", " ")))

    layout = go.Layout(
        title="FX → PLN (źródło: ECB)",
        xaxis=dict(title="Data"),
        yaxis=dict(title="PLN za 1 jednostkę waluty"),
        hovermode="x unified",
    )
    fig = go.Figure(data=traces, layout=layout)
    plot(fig, filename=out_html, auto_open=False)


# --- Główny bieg ---
def main():
    try:
        ensure_dirs()

        # 1) Wczytaj lokalną historię (EUR base)
        hist = load_history(HIST_RAW)

        # 2) Pobierz dzisiejszy XML i zbuduj 1-wierszowy DF
        print("Pobieram eurofxref-daily.xml…")
        today_df = fetch_ecb_daily_xml()

        # 3) Upsert do historii (dopisz, jeśli nie istnieje)
        before_rows = len(hist)
        hist = upsert_daily_row(hist, today_df)

        # 4) Zapisz historię
        hist.to_csv(HIST_RAW, index=False)
        print(f"Historia zapisna: {HIST_RAW} (wiersze: {len(hist)}, zmiana: {len(hist)-before_rows})")

        # 5) Policz kursy do PLN i zapisz
        df_pln = compute_pln_rates(hist, TARGETS)
        df_pln.to_csv(HIST_PLN, index=False)
        print(f"Zapisano przeliczone kursy: {HIST_PLN}")

        # 6) Dashboard
        make_dashboard(df_pln, TARGETS, DASHBOARD_HTML, CHART_LAST_N_DAYS)
        last_date = df_pln["date"].max().date() if not df_pln.empty else None
        print(f"Dashboard gotowy: {DASHBOARD_HTML} | Ostatnia data: {last_date}")

    except Exception as e:
        print("Błąd:", e, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
