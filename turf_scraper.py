"""
╔══════════════════════════════════════════════════════════════════╗
║         GENY.COM → PostgreSQL  -  Scraper de courses PMU         ║
║  Récupère les données des courses et les insère dans turf_stats  ║
╚══════════════════════════════════════════════════════════════════╝

INSTALLATION (une seule fois) :
    pip install playwright psycopg2-binary python-dotenv
    playwright install chromium

CONFIGURATION :
    Créer un fichier .env dans le même dossier avec :
        DB_HOST=localhost
        DB_PORT=5432
        DB_NAME=turf_stats
        DB_USER=postgres
        DB_PASSWORD=ton_mot_de_passe

UTILISATION :
    python geny_scraper.py --date 2026-02-15        # une seule date
    python geny_scraper.py --historique 365          # 365 derniers jours
    python geny_scraper.py --debut 2025-01-01 --fin 2026-02-15
    python geny_scraper.py --contraintes             # SQL à exécuter en BDD
"""

import asyncio
import re
import argparse
import logging
import os
from datetime import datetime, timedelta
from typing import Optional

import psycopg2
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config BDD ────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "turf_stats"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

BASE_URL = "https://www.geny.com"


# ═════════════════════════════════════════════════════════════════════════════
# HELPERS – parsing des valeurs
# ═════════════════════════════════════════════════════════════════════════════

def safe_float(val) -> Optional[float]:
    try:
        return float(str(val).replace(",", ".").replace("\xa0", "").replace(" ", "").strip())
    except Exception:
        return None


def safe_int(val) -> Optional[int]:
    try:
        cleaned = re.sub(r"[^\d]", "", str(val))
        return int(cleaned) if cleaned else None
    except Exception:
        return None


def parse_sa(sa: str):
    """
    Parse le champ SA (Sexe + Age) de Geny.
    Exemples : 'F8' -> ('F', 8) | 'H7' -> ('H', 7) | 'M9' -> ('M', 9)
    """
    m = re.match(r"([A-Za-z]+)(\d+)", sa.strip())
    if m:
        return m.group(1).upper(), safe_int(m.group(2))
    return None, None


def parse_gains(val: str) -> Optional[float]:
    """'151 180' ou '151 180' (espace insecable) -> 151180.0"""
    try:
        return float(re.sub(r"[\s\xa0\u202f]", "", str(val)))
    except Exception:
        return None


def parse_chrono(val: str) -> Optional[str]:
    """Garde uniquement les chronos au format 1'13''8"""
    val = str(val).strip()
    if re.search(r"\d+'\d+''", val):
        return val
    return None


def nettoyer_nom(val: str) -> str:
    """Supprime les icones PUA (police Geny) et espaces superflus."""
    return re.sub(r"[\ue900-\uf8ff]", "", val).strip()


def parse_discipline(url: str, texte: str) -> str:
    t = (url + texte).lower()
    if "trot" in t:
        return "Trot"
    if "plat" in t:
        return "Plat"
    if "obstacle" in t or "haie" in t or "steeple" in t:
        return "Obstacle"
    return "Trot"


# ═════════════════════════════════════════════════════════════════════════════
# BASE DE DONNÉES – upsert helpers
# ═════════════════════════════════════════════════════════════════════════════

def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def upsert_hippodrome(cur, nom: str) -> None:
    cur.execute(
        "INSERT INTO hippodromes (nom, ville) VALUES (%s, %s) ON CONFLICT (nom) DO NOTHING",
        (nom, nom),
    )


def upsert_cheval(cur, nom: str, sexe=None, race=None, pere=None, mere=None) -> int:
    cur.execute(
        """
        INSERT INTO chevaux (nom, sexe, race, pere, mere)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (nom) DO UPDATE
            SET sexe = COALESCE(EXCLUDED.sexe, chevaux.sexe),
                race = COALESCE(EXCLUDED.race, chevaux.race),
                pere = COALESCE(EXCLUDED.pere, chevaux.pere),
                mere = COALESCE(EXCLUDED.mere, chevaux.mere)
        RETURNING id_cheval
        """,
        (nom, sexe, race, pere, mere),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("SELECT id_cheval FROM chevaux WHERE nom = %s", (nom,))
    return cur.fetchone()[0]


def upsert_acteur(cur, nom: str, role: str) -> int:
    cur.execute(
        """
        INSERT INTO acteurs (nom, role) VALUES (%s, %s)
        ON CONFLICT (nom, role) DO NOTHING
        RETURNING id_acteur
        """,
        (nom, role),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("SELECT id_acteur FROM acteurs WHERE nom=%s AND role=%s", (nom, role))
    return cur.fetchone()[0]


def insert_course(cur, d: dict) -> int:
    cur.execute(
        """
        INSERT INTO courses
            (nom_prix, date_course, hippodrome, discipline, distance,
             prix_total, etat_terrain, meteo, nb_partants)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (nom_prix, date_course) DO NOTHING
        RETURNING id_course
        """,
        (d["nom_prix"], d["date_course"], d["hippodrome"], d["discipline"],
         d["distance"], d["prix_total"], d["etat_terrain"], d["meteo"], d["nb_partants"]),
    )
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "SELECT id_course FROM courses WHERE nom_prix=%s AND date_course=%s",
        (d["nom_prix"], d["date_course"]),
    )
    return cur.fetchone()[0]


def insert_partant(cur, d: dict) -> None:
    cur.execute(
        """
        INSERT INTO partants
            (course_id, cheval_id, jockey_id, entraineur_id, numero_pmu,
             age_cheval, poids_porte, recul_distance, place_dans_corde,
             ferrure, oeilleres, musique_precedente,
             cote_matin, cote_direct, est_favori_presse,
             place_arrivee, chrono_individuel, gain_course)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (course_id, numero_pmu) DO UPDATE
            SET place_arrivee     = COALESCE(EXCLUDED.place_arrivee,     partants.place_arrivee),
                chrono_individuel = COALESCE(EXCLUDED.chrono_individuel, partants.chrono_individuel),
                cote_direct       = COALESCE(EXCLUDED.cote_direct,       partants.cote_direct),
                gain_course       = COALESCE(EXCLUDED.gain_course,       partants.gain_course)
        """,
        (
            d["course_id"],    d["cheval_id"],      d.get("jockey_id"),
            d.get("entraineur_id"), d.get("numero_pmu"),
            d.get("age"),      d.get("poids"),      d.get("recul", 0),
            d.get("place_corde"), d.get("ferrure"), d.get("oeilleres"),
            d.get("musique"),
            d.get("cote_matin"), d.get("cote_direct"), d.get("est_favori", False),
            d.get("place_arrivee"), d.get("chrono"), d.get("gain"),
        ),
    )


# ═════════════════════════════════════════════════════════════════════════════
# SCRAPING – liste des courses d'une journée
# ═════════════════════════════════════════════════════════════════════════════

async def get_course_urls(page, date_str: str) -> list:
    url = f"{BASE_URL}/reunions-courses-pmu?date={date_str}"
    log.info(f"  Programme du {date_str}...")
    await page.goto(url, wait_until="networkidle", timeout=30000)
    await page.wait_for_timeout(2000)

    links = await page.query_selector_all("a[href*='/partants-pmu/']")
    courses, seen = [], set()

    for link in links:
        href = await link.get_attribute("href")
        if href and href not in seen:
            seen.add(href)
            slug = href.split("/")[-1]
            raw = slug.replace(f"{date_str}-", "")
            if "-pmu-" in raw:
                hippodrome = raw.split("-pmu-")[0].replace("-", " ").title()
            else:
                hippodrome = raw.split("-")[0].title()
            courses.append({"url": BASE_URL + href, "hippodrome": hippodrome})

    log.info(f"  -> {len(courses)} courses")
    return courses


# ═════════════════════════════════════════════════════════════════════════════
# SCRAPING – page de course
#
# Structure des tableaux Geny (confirmee par analyse du HTML reel) :
#
#  Tableau partants (identifie par les th "N°", "Cheval", "Driver") :
#   Col 0 : N°          -> numero_pmu
#   Col 1 : Cheval      -> nom_cheval
#   Col 2 : SA          -> Sexe (F/H/M) + Age (ex: F8 = Femelle 8 ans)
#   Col 3 : Dist.       -> distance de course pour ce cheval (en metres)
#   Col 4 : Driver      -> nom du driver/jockey
#   Col 5 : Entraineur  -> nom de l'entraineur
#   Col 6 : Musique     -> historique des perfs (ex: 8a6a(25)7a)
#   Col 7 : Gains       -> gains totaux du cheval (en euros)
#   Col 8+ : cotes PMU / Genybet
#
#  Tableau resultats (identifie par les th "Rg.", "Chronos") :
#   Col 0 : Rg.         -> place_arrivee
#   Col 1 : N°          -> numero_pmu
#   Col 2 : Chevaux     -> nom_cheval
#   Col 3 : SA          -> Sexe + Age
#   Col 4 : Dist.       -> distance
#   Col 5 : Drivers     -> driver
#   Col 6 : Entraineurs -> entraineur
#   Col 7 : Chronos     -> chrono_individuel
#   Col 8 : Cotes       -> cote_direct (cote finale)
# ═════════════════════════════════════════════════════════════════════════════

async def scrape_course(page, course_url: str, date_str: str) -> Optional[dict]:
    log.info(f"    -> {course_url}")

    try:
        await page.goto(course_url, wait_until="networkidle", timeout=30000)
        # Attendre que le tableau des partants soit chargé (pas juste le DOM)
        # On attend que "Loading..." disparaisse ou qu'une cellule avec un numéro apparaisse
        try:
            await page.wait_for_function(
                """() => {
                    const cells = document.querySelectorAll('table td');
                    for (const cell of cells) {
                        if (/^\\d+$/.test(cell.innerText.trim())) return true;
                    }
                    return false;
                }""",
                timeout=10000
            )
        except Exception:
            await page.wait_for_timeout(4000)  # fallback si wait_for_function échoue
    except PlaywrightTimeout:
        log.warning(f"    Timeout sur {course_url}")
        return None

    # ── Infos generales ───────────────────────────────────────────────────

    # Nom du prix : priorité à l'URL (fiable), fallback sur h1
    # Format URL : /partants-pmu/2026-02-15-vincennes-pmu-prix-de-grenade_c1633331
    nom_prix = ""
    slug = course_url.split("/")[-1]
    if "_c" in slug:
        slug_sans_id = slug.rsplit("_c", 1)[0]          # "2026-02-15-vincennes-pmu-prix-de-grenade"
        after_date = slug_sans_id.replace(f"{date_str}-", "")  # "vincennes-pmu-prix-de-grenade"
        if "-pmu-" in after_date:
            nom_slug = after_date.split("-pmu-", 1)[1]   # "prix-de-grenade"
            nom_prix = nom_slug.replace("-", " ").title() # "Prix De Grenade"

    # Fallback sur h1 si l'URL n'a pas donné de résultat probant
    if not nom_prix or "privacy" in nom_prix.lower():
        h1 = await page.query_selector("h1")
        if h1:
            h1_text = (await h1.inner_text()).strip()
            if "privacy" not in h1_text.lower() and h1_text:
                nom_prix = h1_text

    log.info(f"      Nom : {nom_prix}")

    page_text = await page.inner_text("body")

    # Distance (en metres)
    distance = None
    m = re.search(r"(\d[\d\s\xa0]*)\s*m(?:etres?)?(?:\s|$)", page_text, re.IGNORECASE)
    if m:
        distance = safe_int(m.group(1))

    # Dotation
    prix_total = None
    m = re.search(r"(\d[\d\s\xa0\u202f]*)\s*euro", page_text, re.IGNORECASE)
    if not m:
        m = re.search(r"(\d[\d\s\xa0\u202f]*)\s*€", page_text)
    if m:
        prix_total = safe_int(m.group(1))

    # Terrain
    etat_terrain = None
    m = re.search(r"[Tt]errain\s*[:\-]?\s*([A-Za-zÀ-ÿ]+)", page_text)
    if m:
        etat_terrain = m.group(1).strip()

    # Hippodrome depuis l'URL (slug déjà calculé plus haut)
    raw = slug.replace(f"{date_str}-", "")
    if "-pmu-" in raw:
        hippodrome = raw.split("-pmu-")[0].replace("-", " ").title()
    else:
        hippodrome = raw.split("-")[0].title()

    discipline = parse_discipline(course_url, nom_prix)

    date_course = datetime.strptime(date_str, "%Y-%m-%d")
    m = re.search(r"(\d{1,2})[hH](\d{2})", page_text[:2000])
    if m:
        date_course = date_course.replace(hour=int(m.group(1)), minute=int(m.group(2)))

    # ── Identifier les tableaux utiles ────────────────────────────────────
    # Stratégie : lire TOUTES les cellules (th + td) des 2 premières lignes
    # car Geny peut mettre ses en-têtes dans des <td> ou sur plusieurs <tr>

    all_tables = await page.query_selector_all("table")
    partants_table = None
    resultats_table = None

    for table in all_tables:
        # Lire th ET les td des 2 premières lignes pour attraper tous les cas
        rows = await table.query_selector_all("tr")
        header_cells = []
        for row in rows[:2]:
            cells = await row.query_selector_all("th, td")
            for cell in cells:
                t = (await cell.inner_text()).strip().lower()
                # Nettoyer les caractères spéciaux
                t = re.sub(r"[\s\xa0\n]+", " ", t)
                header_cells.append(t)

        header_text = " ".join(header_cells)

        # Tableau partants : contient ("driver" OU "jockey") ET "cheval"
        # - Trot attelé  : N° | Cheval | SA | Dist. | Driver     | Entraîneur | Musique | Gains
        # - Galop/Obstacle: N° | Cheval | SA | Dist. | Poids | Jockey | Entraîneur | Musique | Gains
        is_partants = (
            ("driver" in header_text or "jockey" in header_text) and
            ("cheval" in header_text or "n°" in header_text or "n " in header_text)
        )

        # Tableau résultats : contient "chrono" ET ("rg" OU "rang")
        is_resultats = (
            "chrono" in header_text and
            ("rg" in header_text or "rang" in header_text)
        )

        if is_partants and partants_table is None:
            partants_table = table
        elif is_resultats and resultats_table is None:
            resultats_table = table

    # Fallback : si toujours pas trouvé, prendre le 2e tableau (index 1)
    # car le 1er est toujours le calendrier (class="yui-calendar")
    if partants_table is None and len(all_tables) > 1:
        log.warning("      Fallback : utilisation du tableau #1 comme tableau partants")
        partants_table = all_tables[1]

    # ── Parser tableau des partants ───────────────────────────────────────

    partants_dict = {}  # numero_pmu (int) -> dict

    if partants_table:
        rows = await partants_table.query_selector_all("tr")
        log.info(f"      Partants : {len(rows)} lignes")

        # Détecter la structure du tableau en lisant les en-têtes
        # Trot    : N° | Cheval | SA | Dist. | Driver     | Entraîneur | Musique | Gains | cotes...
        # Galop   : N° | Cheval | SA | Dist. | Poids | Jockey | Entraîneur | Musique | Gains | cotes...
        # On détecte la présence de "poids" pour savoir si on est en galop
        header_row = rows[0] if rows else None
        col_poids = False
        if header_row:
            header_cells_txt = []
            for cell in await header_row.query_selector_all("th, td"):
                header_cells_txt.append((await cell.inner_text()).strip().lower())
            col_poids = "poids" in " ".join(header_cells_txt)

        # Indices de colonnes selon la discipline
        # Trot   : idx_driver=4, idx_entraineur=5, idx_musique=6, idx_gains=7, idx_cotes_debut=8
        # Galop  : idx_driver=5, idx_entraineur=6, idx_musique=7, idx_gains=8, idx_cotes_debut=9
        idx_driver     = 5 if col_poids else 4
        idx_entraineur = 6 if col_poids else 5
        idx_musique    = 7 if col_poids else 6
        idx_gains      = 8 if col_poids else 7
        idx_cotes      = 9 if col_poids else 8

        log.info(f"      Structure : {'Galop/Obstacle (Poids+Jockey)' if col_poids else 'Trot (Driver)'}")

        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) < 6:
                continue

            texts = [nettoyer_nom((await c.inner_text()).replace("\n", " ").strip()) for c in cells]

            # La ligne doit commencer par un numero
            if not re.match(r"^\d+$", texts[0]):
                continue

            numero = safe_int(texts[0])
            if numero is None:
                continue

            # Col 1 : Nom du cheval
            nom_cheval = re.split(r"\s{2,}", texts[1])[0].strip()

            # Col 2 : SA -> Sexe + Age
            sexe, age = parse_sa(texts[2]) if len(texts) > 2 else (None, None)

            # Col 3 : Dist. -> distance totale pour ce cheval (en metres)
            dist_cheval = safe_int(texts[3]) if len(texts) > 3 else None

            # Col 4 (galop seulement) : Poids
            poids = safe_float(texts[4]) if col_poids and len(texts) > 4 else None

            # Col idx_driver : Driver ou Jockey selon discipline
            driver = texts[idx_driver] if len(texts) > idx_driver else ""
            est_non_partant = "non-part" in driver.lower() or "non-part" in nom_cheval.lower()

            # Col idx_entraineur : Entraineur
            entraineur = texts[idx_entraineur] if len(texts) > idx_entraineur else ""

            # Col idx_musique : Musique
            musique = texts[idx_musique] if len(texts) > idx_musique else ""
            if not re.search(r"\d", musique):
                musique = None

            # Col idx_gains : Gains totaux
            gains = parse_gains(texts[idx_gains]) if len(texts) > idx_gains else None

            # Col idx_cotes+ : cotes PMU / Genybet
            cotes_valides = []
            for txt in texts[idx_cotes:]:
                v = safe_float(txt)
                if v and v > 1.0:
                    cotes_valides.append(v)

            cote_matin  = cotes_valides[0]  if len(cotes_valides) >= 2 else None
            cote_direct = cotes_valides[-1] if len(cotes_valides) >= 1 else None

            partants_dict[numero] = {
                "numero_pmu":      numero,
                "nom_cheval":      nom_cheval,
                "sexe":            sexe,
                "age":             age,
                "dist_cheval":     dist_cheval,
                "recul":           0,
                "poids":           poids,
                "driver":          driver if not est_non_partant else None,
                "role_driver":     "jockey" if col_poids else "driver",
                "entraineur":      entraineur,
                "musique":         musique,
                "gain":            gains,
                "cote_matin":      cote_matin,
                "cote_direct":     cote_direct,
                "est_non_partant": est_non_partant,
                "place_arrivee":   None,
                "chrono":          None,
            }

        # Calculer le recul reel : dist_cheval - distance_de_base
        # (la distance de base = la plus courte, 0m de recul)
        distances = [p["dist_cheval"] for p in partants_dict.values() if p["dist_cheval"]]
        if distances:
            dist_base = min(distances)
            if not distance:
                distance = dist_base
            for p in partants_dict.values():
                if p["dist_cheval"]:
                    p["recul"] = p["dist_cheval"] - dist_base

        log.info(f"      -> {len(partants_dict)} partants")
    else:
        log.warning("      Tableau partants non trouve")

    # ── Parser tableau des resultats (si course deja courue) ──────────────
    # Rg. | N° | Chevaux | SA | Dist. | Drivers | Entraineurs | Chronos | Cotes

    if resultats_table:
        rows = await resultats_table.query_selector_all("tr")
        log.info(f"      Resultats : {len(rows)} lignes")

        for row in rows:
            cells = await row.query_selector_all("td")
            if len(cells) < 7:
                continue

            texts = [nettoyer_nom((await c.inner_text()).replace("\n", " ").strip()) for c in cells]

            # Col 0 : Rang (ex: "1.", "2.", "D" pour disqualifie)
            rang_raw = texts[0].replace(".", "").strip()
            place = safe_int(rang_raw)  # None si "D", "A", etc.

            # Col 1 : Numero PMU
            numero = safe_int(texts[1])
            if numero is None:
                continue

            # Col 7 : Chrono individuel
            chrono = parse_chrono(texts[7]) if len(texts) > 7 else None

            # Col 8 : Cote PMU finale
            cote = safe_float(texts[8]) if len(texts) > 8 else None

            if numero in partants_dict:
                partants_dict[numero]["place_arrivee"] = place
                partants_dict[numero]["chrono"]        = chrono
                if cote and cote > 1:
                    partants_dict[numero]["cote_direct"] = cote
            else:
                # Cheval present en resultats mais absent du tableau partants (rare)
                sexe, age = parse_sa(texts[3]) if len(texts) > 3 else (None, None)
                partants_dict[numero] = {
                    "numero_pmu":      numero,
                    "nom_cheval":      texts[2],
                    "sexe":            sexe,
                    "age":             age,
                    "dist_cheval":     safe_int(texts[4]) if len(texts) > 4 else None,
                    "recul":           0,
                    "driver":          texts[5] if len(texts) > 5 else None,
                    "entraineur":      texts[6] if len(texts) > 6 else None,
                    "musique":         None,
                    "gain":            None,
                    "cote_matin":      None,
                    "cote_direct":     cote,
                    "est_non_partant": False,
                    "place_arrivee":   place,
                    "chrono":          chrono,
                }

    # Exclure les non-partants
    partants_list = [
        p for p in partants_dict.values()
        if not p.get("est_non_partant") and p.get("nom_cheval")
    ]

    log.info(f"      OK : {len(partants_list)} partants retenus")

    return {
        "nom_prix":     nom_prix,
        "date_course":  date_course,
        "hippodrome":   hippodrome,
        "discipline":   discipline,
        "distance":     distance,
        "prix_total":   prix_total,
        "etat_terrain": etat_terrain,
        "meteo":        None,
        "nb_partants":  len(partants_list),
        "partants":     partants_list,
    }


# ═════════════════════════════════════════════════════════════════════════════
# SAUVEGARDE EN BASE
# ═════════════════════════════════════════════════════════════════════════════

def save_to_db(course_data: dict) -> None:
    if not course_data:
        return

    conn = get_conn()
    try:
        with conn:
            with conn.cursor() as cur:

                upsert_hippodrome(cur, course_data["hippodrome"])
                id_course = insert_course(cur, course_data)

                for p in course_data["partants"]:
                    nom = p.get("nom_cheval", "").strip()
                    if not nom:
                        continue

                    cheval_id = upsert_cheval(cur, nom=nom, sexe=p.get("sexe"))

                    jockey_id = None
                    driver_nom = (p.get("driver") or "").strip()
                    if driver_nom:
                        role = p.get("role_driver", "driver")
                        jockey_id = upsert_acteur(cur, driver_nom, role)

                    entraineur_id = None
                    entraineur_nom = (p.get("entraineur") or "").strip()
                    if entraineur_nom:
                        entraineur_id = upsert_acteur(cur, entraineur_nom, "entraineur")

                    insert_partant(cur, {
                        "course_id":     id_course,
                        "cheval_id":     cheval_id,
                        "jockey_id":     jockey_id,
                        "entraineur_id": entraineur_id,
                        "numero_pmu":    p.get("numero_pmu"),
                        "age":           p.get("age"),
                        "poids":         p.get("poids"),
                        "recul":         p.get("recul", 0),
                        "place_corde":   p.get("place_corde"),
                        "ferrure":       p.get("ferrure"),
                        "oeilleres":     p.get("oeilleres"),
                        "musique":       p.get("musique"),
                        "cote_matin":    p.get("cote_matin"),
                        "cote_direct":   p.get("cote_direct"),
                        "est_favori":    p.get("est_favori", False),
                        "place_arrivee": p.get("place_arrivee"),
                        "chrono":        p.get("chrono"),
                        "gain":          p.get("gain"),
                    })

        log.info(f"    OK BDD : {course_data['nom_prix']} ({len(course_data['partants'])} partants)")
    except Exception as e:
        log.error(f"    ERREUR BDD : {e}")
        conn.rollback()
    finally:
        conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# ORCHESTRATION
# ═════════════════════════════════════════════════════════════════════════════

async def scrape_date(date_str: str) -> int:
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await ctx.new_page()
        course_infos = await get_course_urls(page, date_str)
        count = 0

        for info in course_infos:
            try:
                data = await scrape_course(page, info["url"], date_str)
                if data:
                    save_to_db(data)
                    count += 1
                await asyncio.sleep(2)
            except Exception as e:
                log.error(f"    Erreur : {info['url']} -> {e}")

        await browser.close()
    return count


async def scrape_range(date_debut: datetime, date_fin: datetime) -> None:
    current = date_debut
    total = (date_fin - date_debut).days + 1
    i = 0

    while current <= date_fin:
        i += 1
        date_str = current.strftime("%Y-%m-%d")
        log.info(f"\n{'='*60}\nJour {i}/{total} : {date_str}\n{'='*60}")
        try:
            n = await scrape_date(date_str)
            log.info(f"  OK : {n} courses sauvegardees")
        except Exception as e:
            log.error(f"  ERREUR {date_str} : {e}")

        current += timedelta(days=1)
        await asyncio.sleep(5)


# ═════════════════════════════════════════════════════════════════════════════
# CONTRAINTES SQL (a executer une seule fois dans psql)
# ═════════════════════════════════════════════════════════════════════════════

SQL_CONTRAINTES = """
-- Contraintes UNIQUE necessaires pour les ON CONFLICT
-- Executer UNE SEULE FOIS dans psql avant de lancer le scraper :

ALTER TABLE hippodromes ADD CONSTRAINT hippodromes_nom_key     UNIQUE (nom);
ALTER TABLE chevaux      ADD CONSTRAINT chevaux_nom_key         UNIQUE (nom);
ALTER TABLE acteurs      ADD CONSTRAINT acteurs_nom_role_key    UNIQUE (nom, role);
ALTER TABLE courses      ADD CONSTRAINT courses_nom_date_key    UNIQUE (nom_prix, date_course);
ALTER TABLE partants     ADD CONSTRAINT partants_course_num_key UNIQUE (course_id, numero_pmu);
"""


# ═════════════════════════════════════════════════════════════════════════════
# POINT D'ENTREE
# ═════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Scraper Geny -> PostgreSQL turf_stats")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date",        help="YYYY-MM-DD")
    group.add_argument("--historique",  type=int, metavar="JOURS")
    group.add_argument("--debut",       help="YYYY-MM-DD (avec --fin)")
    group.add_argument("--contraintes", action="store_true")
    parser.add_argument("--fin", default=None)

    args = parser.parse_args()

    if args.contraintes:
        print(SQL_CONTRAINTES)
        return

    if args.date:
        asyncio.run(scrape_date(args.date))

    elif args.historique:
        fin   = datetime.now()
        debut = fin - timedelta(days=args.historique)
        log.info(f"Historique {args.historique} jours : {debut.date()} -> {fin.date()}")
        asyncio.run(scrape_range(debut, fin))

    elif args.debut:
        debut = datetime.strptime(args.debut, "%Y-%m-%d")
        fin   = datetime.strptime(args.fin, "%Y-%m-%d") if args.fin else datetime.now()
        asyncio.run(scrape_range(debut, fin))


if __name__ == "__main__":
    main()
