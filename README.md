# etlens-quick-fx-etl

Demo ETL walut (ECB) → historia kursów → dashboard HTML z kursami do PLN (EUR, USD, GBP, CHF).

## Jak uruchomić lokalnie
```bash
python -m venv .venv
# aktywuj venv (Windows: .venv\Scripts\activate, macOS/Linux: source .venv/bin/activate)
pip install -r requirements.txt
python backfill_90d.py   # jednorazowo: budowa historii 90 dni
python main.py           # codzienny run: dopisanie dzisiejszego dnia + dashboard
