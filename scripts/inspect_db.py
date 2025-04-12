import sqlite3
from tabulate import tabulate

DB_PATH = "database/forex_data.db"


def list_tables(cursor):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [row[0] for row in cursor.fetchall()]


def display_table_data(cursor, table_name):
    # Lire les noms de colonnes
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns_info = cursor.fetchall()
    column_names = [col[1] for col in columns_info]

    # Construire ORDER BY selon les colonnes existantes
    order_clause = "ORDER BY date DESC"
    if "currency" in column_names:
        order_clause += ", currency ASC"

    # Exécuter la requête
    try:
        query = f"SELECT * FROM {table_name} {order_clause} LIMIT 10;"
        cursor.execute(query)
        rows = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        print(f"\n📊 Table: {table_name}")
        print(tabulate(rows, headers=column_names, tablefmt="fancy_grid"))
    except Exception as e:
        print(f"❌ Erreur lors de l'affichage de la table '{table_name}': {e}")


def run():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("📌 Tables dans la base de données:")
    tables = list_tables(cursor)
    print(tables)

    choice = (
        input(
            "\nSouhaitez-vous (1) afficher une seule table ou (2) toutes les tables ? (1/2, défaut 1): "
        ).strip()
        or "1"
    )

    if choice == "2":
        for table in tables:
            display_table_data(cursor, table)
    else:
        default_table = "forex_rates_history"
        selected = (
            input(
                f"🔍 Entrez le nom de la table à inspecter (défaut: '{default_table}'): "
            ).strip()
            or default_table
        )
        if selected in tables:
            display_table_data(cursor, selected)
        else:
            print(f"❌ Table '{selected}' introuvable.")

    conn.close()


if __name__ == "__main__":
    run()
