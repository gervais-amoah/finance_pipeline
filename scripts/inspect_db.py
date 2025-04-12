import sqlite3
from tabulate import tabulate

default_table_name = "forex_rates_history"

# Connexion à la base
conn = sqlite3.connect("database/forex_data.db")
cursor = conn.cursor()

# Vérifier les tables existantes
print("📌 Tables dans la base de données:")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(tables)

# Choix de la table à inspecter
table_name = (
    input(
        f"🔍 Entrez le nom de la table à inspecter (par défaut '{default_table_name}'): "
    )
    or default_table_name
)

# Vérifier les colonnes de la table
cursor.execute(f"PRAGMA table_info({table_name})")
columns_info = cursor.fetchall()
column_names = [col[1] for col in columns_info]

# Construire dynamiquement la clause ORDER BY
order_clause = "ORDER BY date DESC"
if "currency" in column_names:
    order_clause += ", currency ASC"

# Lire les premières lignes
print(f"\n📊 Aperçu des données dans '{table_name}' (triées):")
query = f"""
    SELECT * FROM {table_name}
    {order_clause}
    LIMIT 10;
"""
cursor.execute(query)
rows = cursor.fetchall()

# Lire les noms des colonnes pour affichage
column_names_result = [description[0] for description in cursor.description]

# Affichage propre
print(tabulate(rows, headers=column_names_result, tablefmt="fancy_grid"))

# Fermer la connexion
conn.close()
