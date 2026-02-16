# 🏇 Turf Scraper → PostgreSQL

Outil de scraping automatique de d'un site de turf vers ta base `turf_stats`.

---

## ⚙️ Installation (une seule fois)

### 1. Python 3.9+
```bash
pip install playwright psycopg2-binary python-dotenv
playwright install chromium
```

### 2. Fichier `.env`
Crée un fichier `.env` dans le même dossier que le script :
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=turf_stats
DB_USER=postgres
DB_PASSWORD=ton_mot_de_passe
```

### 3. Contraintes SQL (une seule fois dans PostgreSQL)
Le script utilise des `ON CONFLICT` qui nécessitent des index UNIQUE.
Lance cette commande pour les afficher :
```bash
python geny_scraper.py --contraintes
```
Puis copie-colle le SQL dans psql :
```sql
ALTER TABLE hippodromes ADD CONSTRAINT hippodromes_nom_key UNIQUE (nom);
ALTER TABLE chevaux ADD CONSTRAINT chevaux_nom_key UNIQUE (nom);
ALTER TABLE acteurs ADD CONSTRAINT acteurs_nom_role_key UNIQUE (nom, role);
ALTER TABLE courses ADD CONSTRAINT courses_nom_date_key UNIQUE (nom_prix, date_course);
ALTER TABLE partants ADD CONSTRAINT partants_course_numero_key UNIQUE (course_id, numero_pmu);
```

---

## 🚀 Utilisation

### Scraper une seule journée
```bash
python geny_scraper.py --date 2026-02-15
```

### Scraper les 365 derniers jours (historique complet)
```bash
python geny_scraper.py --historique 365
```
> ⚠️ Prévois ~6-8 heures pour 365 jours (~10 courses/jour × 5 secondes chacune + pauses)

### Scraper une plage de dates
```bash
python geny_scraper.py --debut 2025-06-01 --fin 2025-12-31
```

---

## 📊 Ce qui est rempli dans la BDD

| Table        | Données extraites                                               |
|--------------|-----------------------------------------------------------------|
| hippodromes  | Nom (depuis l'URL), ville                                       |
| courses      | Nom prix, date, heure, hippodrome, discipline, distance, terrain, dotation, nb partants |
| chevaux      | Nom, sexe, âge (infère à partir des partants)                   |
| acteurs      | Jockeys/Drivers + entraîneurs (avec leur rôle)                  |
| partants     | Numéro PMU, cotes matin/direct, musique, place arrivée, gain    |

---

## ⏱️ Automatisation quotidienne

### Sur Linux/Mac (cron)
```bash
# Ouvre le crontab
crontab -e

# Ajouter cette ligne : lance le script chaque soir à 23h00
0 23 * * * cd /chemin/vers/script && python geny_scraper.py --date $(date +\%Y-\%m-\%d) >> /var/log/geny_scraper.log 2>&1
```

### Sur Windows (Planificateur de tâches)
Crée une tâche planifiée qui exécute :
```
python C:\chemin\vers\geny_scraper.py --date %date:~6,4%-%date:~3,2%-%date:~0,2%
```

---

## 🐛 Dépannage

**"Timeout" sur certaines pages** → Normal pour les courses futures (pas encore chargées).

**"ON CONFLICT do nothing" mais pas d'insertions** → Les contraintes UNIQUE ne sont pas créées. Relance `--contraintes`.

**Données vides dans partants** → Le site Geny charge ses tableaux en JavaScript. Si le Wi-Fi est lent, augmente les `wait_for_timeout` dans le script (ligne ~130).

**Trop de requêtes / ban IP** → Augmente les `asyncio.sleep` (délais entre requêtes). 2-5 secondes est un bon réglage.
