# 📊 Forex Data Pipeline: Multi-source ETL

Un pipeline de données complet pour collecter, transformer et stocker les taux de change à partir de trois sources différentes :  
- 💾 Un fichier CSV historique (Kaggle)  
- 🌐 L'API [Frankfurter.app](https://www.frankfurter.app)  
- 🕷️ Web scraping en direct depuis [x-rates.com](https://www.x-rates.com)  

Toutes les données sont traitées et stockées dans une base de données **SQLite**, ainsi que dans des fichiers CSV organisés.

---

## 📁 Structure du projet

```
forex-data-pipeline/
│
├── main.py                    # Script principal qui lance tous les ETL
├── etl/
│   ├── csv_loader.py          # Extraction + traitement du fichier CSV local (2 mois)
│   ├── api_fetcher.py         # Extraction des données depuis l'API Frankfurter
│   ├── web_scraper.py         # Scraping en direct depuis x-rates.com
│   ├── supabase_uploader.py   # Envoi des nouvelles données vers Supabase
│
├── services/
│   ├── supabase.py   # Envoi des nouvelles données vers Supabase
│
├── data/
│   ├── raw/                   # Contient les données CSV brutes (ex: de Kaggle)
│   └── processed/             # Contient les CSV traités par chaque source (généré automatiquemant)
│
├── database/
│   └── forex_data.db          # Base de données SQLite contenant toutes les données
│
├── .github/workflows/         # Configuration GitHub Actions pour exécutions automatiques
├── .env                       # Contient les variables sensibles (email, Supabase, etc.)
├── .gitignore                 # Fichiers à ignorer par Git
└── README.md                  # Ce fichier
```

---

## ⚙️ Fonctionnalités principales

- ✅ **Orchestration** complète via main.py
- ✅ **Sauvegarde doubl**e (CSV + SQLite)
- ✅ **Nettoyage et transformation** des données
- ✅ **Traitement intelligent du CSV Kaggle** : extraction uniquement des 2 derniers mois par defaut
- ✅ **Web scraping dynamique** avec alerte email si structure HTML change
- ✅ **Synchronisation vers Supabase** des données récentes avec alerte email en cas d'échec
- ✅ **Logging clair** dans tous les scripts
- ✅ **Automatisation du pipeline** via GitHub Action

---

## 🔁 Automatisation avec GitHub Actions

Le pipeline s’exécute automatiquement chaque jour grâce à GitHub Actions :

- ⏱️ Planification quotidienne via cron
- 🔄 Lancement automatique des trois ETL (CSV, API, scraping)
- 📤 Upload des nouvelles données vers Supabase
- 🧪 À terme : ajout de tests automatiques pour garantir la qualité des données
- ⚙️ Le tout est géré dans le fichier `.github/workflows/etl.yml`

![Automatic Pipeline Sucessfuly excecuted daily](https://github.com/user-attachments/assets/2f303689-457b-492e-ab85-d88dbd268c3d)

---

## 🔔 Monitoring et alertes

Si le scraping échoue (ex: structure HTML modifiée), une fonction `alert_admin()` envoie automatiquement un **email d’alerte**.

> 🛡️ Les identifiants sont stockés en toute sécurité dans `.env`.

![screenshot - Email received when there was error in excecution](https://github.com/user-attachments/assets/094a571e-abb2-4a10-be9c-8eddd6f96911)

---

## 🚀 Lancer le projet

### 1. Cloner le dépôt
```bash
git clone https://github.com/ton-username/forex-data-pipeline.git
cd forex-data-pipeline
```

### 2. Créer un environnement virtuel
```bash
python -m venv venv
source venv/bin/activate   # ou .\venv\Scripts\activate sur Windows
pip install -r requirements.txt
```

### 3. Configurer `.env`

Créer un fichier `.env` à la racine :

```
SMTP_SERVER = "smtp.gmail.com"  # Pour Gmail (ou utilises ton provider SMTP)
SMTP_PORT = 587 # Le port
EMAIL_ADDRESS = "ton.nom@email.com"
EMAIL_PASSWORD = "tonMotDePasse" # Pour Gmail, tu peux créer un "mot de passe d'application". Visites https://myaccount.google.com/apppasswords
RECIPIENT_EMAIL = "admin.nom@email.com"

# Supabase credentials
SUPABASE_URL = "https://your-project.supabase.co"
SUPABASE_KEY = "your-secret-key"
```

### 4. Lancer le pipeline
```bash
python main.py
```

---

## 📦 Dépendances principales

- `requests`
- `pandas`
- `beautifulsoup4`
- `python-dotenv`
- `sqlite3`
- `pytz`
- `python-dotenv`
- `smtplib` (standard lib)
- `supabase-py`

---

## 🧠 Prochaines évolutions

- [ ] Dashboard simple avec GitHub Pages
- [ ] Tests unitaires avec `pytest`
- [ ] Téléchargement automatique du CSV Kaggle via API

---

## 🧑‍💻 Auteur

**Gervais Yao Amoah** – *Projet personnel Data Engineering*  
[GitHub](https://github.com/gervais-amoah) | [LinkedIn](https://linkedin.com/in/gervais-amoah)

---

## 📝 Licence

MIT – libre à utiliser, améliorer et partager.
