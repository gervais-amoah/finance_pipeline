import pandas as pd
from pathlib import Path

input_csv = Path("data/raw/daily_forex_rates.csv")
output_dir = Path("data/processed")
output_dir.mkdir(parents=True, exist_ok=True)

try:
    months = int(input("Combien de mois en arrière voulez-vous garder ? (ex: 10): "))
except ValueError:
    print("❌ Veuillez entrer un nombre entier.")
    exit()

df = pd.read_csv(input_csv)
df["date"] = pd.to_datetime(df["date"], errors="coerce")


today = pd.Timestamp.today()
start_date = today - pd.DateOffset(months=months)

df_recent = df[(df["date"] >= start_date) & (df["date"] <= today)]

output_csv = output_dir / f"forex_rates_{months}m.csv"

df_recent.to_csv(output_csv, index=False)

print(
    f"✅ Données de {start_date.date()} à {today.date()} sauvegardées dans : {output_csv}"
)
print(f"{len(df_recent)} lignes extraites")
