import sqlite3

conn = sqlite3.connect("database/forex_data.db")
cursor = conn.cursor()

# Vérifier les tables existantes
print("📌 Tables dans la base de données:")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(tables)

table_to_drop = (
    input("⚠️ Entrez le nom de la table à supprimer (par défaut 'forex_rates_api'): ")
    or "forex_rates_api"
)

cursor.execute(f"DROP TABLE IF EXISTS {table_to_drop}")
print(f"✅ Table '{table_to_drop}' supprimée.")

conn.commit()
conn.close()
