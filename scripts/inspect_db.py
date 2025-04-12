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

# Lire les premières lignes triées par date DESC et currency ASC
print(f"\n📊 Aperçu des données dans '{table_name}' (triées):")
query = f"""
    SELECT * FROM {table_name}
    ORDER BY date DESC, currency ASC
    LIMIT 10;
"""
cursor.execute(query)
rows = cursor.fetchall()

# Lire les noms des colonnes
column_names = [description[0] for description in cursor.description]

# Affichage propre
print(tabulate(rows, headers=column_names, tablefmt="fancy_grid"))

# Fermer la connexion
conn.close()
