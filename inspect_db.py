import sqlite3
from tabulate import tabulate

table_name = "forex_rates_history"

# Connexion à la base
conn = sqlite3.connect("database/forex_data.db")
cursor = conn.cursor()

# Vérifier les tables existantes
print("📌 Tables dans la base de données:")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(tables)

table_name = (
    input(
        "🔍 Entrez le nom de la table à inspecter (par défaut 'forex_rates_history'): "
    )
    or table_name
)

# Lire les premières lignes de la table forex_data
print("\n📊 Aperçu des données dans 'forex_data':")
cursor.execute(f"SELECT * FROM {table_name} LIMIT 10;")
rows = cursor.fetchall()

# Lire les noms des colonnes
column_names = [description[0] for description in cursor.description]

# Affichage propre
print(tabulate(rows, headers=column_names, tablefmt="fancy_grid"))

# Fermer la connexion
conn.close()
