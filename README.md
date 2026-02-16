# 🏇 Turf Scraper → PostgreSQL

Outil de scraping automatique de geny.com vers ta base `turf_stats`.

---

## 📁 Fichiers

| Fichier | Rôle |
|---|---|
| `geny_scraper.py` | Scrape les partants, cotes, chronos de chaque course |
| `geny_resultats.py` | Récupère l'ordre d'arrivée et met à jour `courses.ordre_arrivee` |
| `reset_db.sql` | Remet la base à zéro (structure + contraintes) |
| `add_ordre_arrivee.sql` | Ajoute la colonne `ordre_arrivee` à la table `courses` |

---

## ⚙️ Installation (une seule fois)

### 1. Python 3.9+
```bash
pip install playwright psycopg2-binary python-dotenv
playwright install chromium
```

### 2. Fichier `.env`
Crée un fichier `.env` dans le même dossier que les scripts :
```env
DB_HOST=localhost
DB_PORT=5432
DB_NAME=turf_stats
DB_USER=turf
DB_PASSWORD=ton_mot_de_passe
```

### 3. Préparer la base PostgreSQL (une seule fois)

**Option A — Repartir d'une base propre (recommandé) :**
```bash
psql -U turf -d turf_stats -h 127.0.0.1 -f reset_db.sql
```

**Option B — Ajouter uniquement les contraintes manquantes sur une base existante :**
```sql
ALTER TABLE hippodromes ADD CONSTRAINT hippodromes_nom_key     UNIQUE (nom);
ALTER TABLE chevaux      ADD CONSTRAINT chevaux_nom_key         UNIQUE (nom);
ALTER TABLE acteurs      ADD CONSTRAINT acteurs_nom_role_key    UNIQUE (nom, role);
ALTER TABLE courses      ADD CONSTRAINT courses_nom_date_key    UNIQUE (nom_prix, date_course);
ALTER TABLE partants     ADD CONSTRAINT partants_course_num_key UNIQUE (course_id, numero_pmu);
```

### 4. Ajouter la colonne ordre_arrivee
```bash
psql -U turf -d turf_stats -h 127.0.0.1 -f add_ordre_arrivee.sql
```

---

## 🚀 Utilisation

### Scraper les partants d'une journée
```bash
python geny_scraper.py --date 2026-02-15
```

### Scraper les partants sur 365 jours (historique complet)
```bash
python geny_scraper.py --historique 365
```
> ⚠️ Prévois ~6-8 heures pour 365 jours

### Scraper une plage de dates
```bash
python geny_scraper.py --debut 2025-06-01 --fin 2025-12-31
```

---

## 🏆 Récupérer les résultats (ordre d'arrivée)

Les résultats sont stockés dans `courses.ordre_arrivee` sous forme `"3-2-1-5-4"` (numéros PMU dans l'ordre d'arrivée).

### Mettre à jour les résultats d'une journée
```bash
python geny_resultats.py --date 2026-02-14
```

Sortie attendue :
```
Prix Agitato                             → 3-2-1
Prix De La Source                        → 2-5-4-3-6
Prix Beugnot                             → 13-9-1-16-11
✓ 8 courses mises à jour
```

### Mettre à jour les résultats sur 365 jours
```bash
python geny_resultats.py --historique 365
```

### Sur une plage de dates
```bash
python geny_resultats.py --debut 2025-06-01 --fin 2025-12-31
```

> 💡 **Workflow recommandé :** lancer `geny_scraper.py` le matin (partants du jour)
> puis `geny_resultats.py` le soir (résultats une fois les courses terminées).

---

## 📊 Ce qui est rempli dans la BDD

| Table | Colonne | Source Geny |
|---|---|---|
| `hippodromes` | nom, ville | URL de la course |
| `courses` | nom_prix, date, heure, hippodrome, discipline, distance, terrain, dotation, nb_partants | Page partants |
| `courses` | **ordre_arrivee** | Page des réunions (ex: `"3-2-1-16-11"`) |
| `chevaux` | nom, sexe, âge | Tableau partants col. SA |
| `acteurs` | nom, role (`driver` / `jockey` / `entraineur`) | Tableau partants |
| `partants` | numero_pmu, poids, cotes matin/direct, musique, place_arrivee, chrono, gain | Tableau partants + résultats |

---

## ⏱️ Automatisation quotidienne

### Sur Linux/Mac (cron)
```bash
crontab -e
```
Ajouter ces deux lignes :
```bash
# Scraper les partants chaque matin à 9h
0 9 * * * cd /chemin/vers/scripts && python geny_scraper.py --date $(date +\%Y-\%m-\%d) >> /var/log/geny_scraper.log 2>&1

# Récupérer les résultats chaque soir à 23h
0 23 * * * cd /chemin/vers/scripts && python geny_resultats.py --date $(date +\%Y-\%m-\%d) >> /var/log/geny_resultats.log 2>&1
```

### Sur Windows (Planificateur de tâches)
Créer deux tâches planifiées :
```
# Matin (partants)
python C:\chemin\geny_scraper.py --date %date:~6,4%-%date:~3,2%-%date:~0,2%

# Soir (résultats)
python C:\chemin\geny_resultats.py --date %date:~6,4%-%date:~3,2%-%date:~0,2%
```

---

## 🔍 Exemples de requêtes SQL utiles

```sql
-- Taux de victoire par driver (minimum 10 courses)
SELECT a.nom, COUNT(*) AS courses,
       SUM(CASE WHEN p.place_arrivee = 1 THEN 1 ELSE 0 END) AS victoires,
       ROUND(100.0 * SUM(CASE WHEN p.place_arrivee = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) AS taux_pct
FROM partants p
JOIN acteurs a ON a.id_acteur = p.jockey_id
GROUP BY a.nom
HAVING COUNT(*) >= 10
ORDER BY taux_pct DESC;

-- Performance d'un duo cheval + driver
SELECT ch.nom AS cheval, a.nom AS driver,
       COUNT(*) AS courses, AVG(p.place_arrivee) AS place_moyenne
FROM partants p
JOIN chevaux ch ON ch.id_cheval = p.cheval_id
JOIN acteurs a  ON a.id_acteur  = p.jockey_id
GROUP BY ch.nom, a.nom
HAVING COUNT(*) >= 3
ORDER BY place_moyenne;

-- Courses où le favori a gagné (premier arrivé = numéro avec cote la plus basse)
SELECT c.nom_prix, c.date_course, c.ordre_arrivee,
       p.numero_pmu, p.cote_direct
FROM courses c
JOIN partants p ON p.course_id = c.id_course
WHERE c.ordre_arrivee LIKE CONCAT(p.numero_pmu::text, '-%')
  AND p.cote_direct = (
      SELECT MIN(p2.cote_direct)
      FROM partants p2
      WHERE p2.course_id = c.id_course
        AND p2.cote_direct IS NOT NULL
  )
ORDER BY c.date_course DESC;
```

---

## 🐛 Dépannage

**"no unique or exclusion constraint"** → Les contraintes UNIQUE ne sont pas en place. Utilise `reset_db.sql` pour repartir propre.

**"We respect your privacy!" dans nom_prix** → Le JS ne s'est pas chargé à temps. Le script corrigé extrait le nom depuis l'URL, ce problème ne devrait plus apparaître.

**"Tableau partants non trouvé"** → Le script bascule automatiquement sur le tableau #1 (fallback). Si les données sont quand même vides, augmente le `wait_for_timeout` dans `geny_scraper.py`.

**Timeout sur certaines pages** → Normal pour les courses futures. Le script ignore et continue.

**Ban IP / trop de requêtes** → Augmente les `asyncio.sleep` dans les scripts (2-5 secondes recommandé entre chaque course, 5 secondes entre chaque journée).
