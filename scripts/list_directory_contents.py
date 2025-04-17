import os


def list_directory_contents(directory_path):
    """Parcourir le répertoire et afficher les dossiers et fichiers."""
    try:
        # Liste le contenu du répertoire
        for root, dirs, files in os.walk(directory_path):
            print(f"📁 Dossier: {root}")

            # Afficher les sous-dossiers
            if dirs:
                print("  🔹 Sous-dossiers:")
                for dir_name in dirs:
                    print(f"    - {dir_name}")

            # Afficher les fichiers
            if files:
                print("  📄 Fichiers:")
                for file_name in files:
                    print(f"    - {file_name}")

            print("-" * 50)  # Séparateur entre chaque dossier
    except Exception as e:
        print(f"❌ Erreur lors de la lecture du répertoire: {e}")


if __name__ == "__main__":
    # Répertoire à explorer (modifie-le si nécessaire)
    directory_path = input("🔍 Entrez le chemin du répertoire à explorer: ").strip()
    list_directory_contents(directory_path)
