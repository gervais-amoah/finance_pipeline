import sqlite3
from tabulate import tabulate

# Connexion à la base
conn = sqlite3.connect("database/forex_data.db")
cursor = conn.cursor()

# Vérifier les tables existantes
print("📌 Tables dans la base de données:")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(tables)

# Lire les premières lignes de la table forex_data
print("\n📊 Aperçu des données dans 'forex_data':")
cursor.execute("SELECT * FROM forex_rates_history LIMIT 10;")
rows = cursor.fetchall()

# Lire les noms des colonnes
column_names = [description[0] for description in cursor.description]

# Affichage propre
print(tabulate(rows, headers=column_names, tablefmt="fancy_grid"))

# Fermer la connexion
conn.close()
