# electricity_fetcher.py
# -*- coding: utf-8 -*-
"""
Robuster RTE-Scraper & Parser
- Lädt Regions-ZIPs (Eco2Mix "En-cours mensuel temps réel")
- Entpackt alle ZIP-Member in-memory
- Snifft Delimiter/Encoding, erkennt falsch gelabelte .xls==CSV
- Vereinheitlicht Kernspalten, baut Timestamp und schreibt sauberes Parquet
"""

import os, time, re, io, zipfile, shutil, csv
from typing import List, Dict, Tuple, Iterable
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

os.environ.setdefault("MPLBACKEND", "Agg")
os.environ.setdefault("BROWSER", ":")
os.environ.pop("DISPLAY", None)  # zwingt headless

# -----------------------------
# Config
# -----------------------------
# ✅ EC2 ANPASSUNG 1: Download Directory
DOWNLOAD_DIR = "/home/ec2-user/tmp_electricity_downloads"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# ✅ NEU: Lösche versteckte temporäre Dateien vor dem Start
print("[DEBUG] Bereinige temporäre Dateien...")
for f in os.listdir(DOWNLOAD_DIR):
    if f.startswith('.'):
        try:
            os.remove(os.path.join(DOWNLOAD_DIR, f))
            print(f"[DEBUG] Gelöschte temporäre Datei: {f}")
        except Exception as e:
            print(f"[DEBUG] Konnte {f} nicht löschen: {e}")

URL = "https://www.rte-france.com/en/eco2mix/download-indicators/"
SLEEP_BETWEEN = 3.0  # Erhöht für bessere Stabilität
DOWNLOAD_TIMEOUT = 300  # Erhöht auf 5 Minuten
STABLE_SECS = 5  # Erhöht für bessere Stabilität
EXPECTED_EXTS = {".zip", ".csv", ".xls", ".xlsx", ".xlsb"}

# Wenn du nur bereits vorhandene Downloads neu parsen willst, auf True setzen:
REPARSE_ONLY = False


# -----------------------------
# Helpers
# -----------------------------
def sanitize(name: str) -> str:
    """Säubere Dateinamen - robuste Version für Umlaute"""
    # Zuerst normale Umlaute ersetzen
    replacements = {
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
        'à': 'a', 'â': 'a', 'ä': 'a',
        'î': 'i', 'ï': 'i',
        'ô': 'o', 'ö': 'o',
        'ù': 'u', 'û': 'u', 'ü': 'u',
        'ç': 'c',
        'É': 'E', 'È': 'E', 'Ê': 'E', 'Ë': 'E',
        'À': 'A', 'Â': 'A', 'Ä': 'A',
        'Î': 'I', 'Ï': 'I',
        'Ô': 'O', 'Ö': 'O',
        'Ù': 'U', 'Û': 'U', 'Ü': 'U',
        'Ç': 'C'
    }
    for old, new in replacements.items():
        name = name.replace(old, new)

    # Dann Sonderzeichen ersetzen
    return re.sub(r"[^\w\-\.]+", "_", name.strip())


def _is_temp(p: str) -> bool:
    """Erkennt temporäre Download-Dateien - erweiterte Version"""
    p_lower = p.lower()
    return (p_lower.endswith((".crdownload", ".tmp", ".part")) or
            p_lower.startswith(".") or  # Versteckte Dateien wie .com.google.Chrome.XoTXEU
            "temp" in p_lower or
            "pending" in p_lower)


def wait_for_new_file_and_stable(folder: str, before_paths: set, timeout=DOWNLOAD_TIMEOUT,
                                 stable_secs=STABLE_SECS) -> str:
    """Wartet auf neue Datei und bis Größe stabil ist - ignoriert Chrome-Temp-Dateien."""
    deadline = time.time() + timeout
    candidate = None

    print(f"[DEBUG] Warte auf neuen Download in {folder}...")

    # Warte zunächst auf das Erscheinen einer neuen Datei
    while time.time() < deadline and candidate is None:
        try:
            current_files = {os.path.join(folder, f) for f in os.listdir(folder)}
            # FILTERE ALLE TEMPORÄREN DATEIEN AUS - auch die, die mit .com.google.Chrome beginnen
            new_files = [p for p in (current_files - before_paths) if not _is_temp(p)]

            if new_files:
                # Nimm die neueste Datei
                new_files.sort(key=lambda x: os.path.getctime(x), reverse=True)
                candidate = new_files[0]
                print(f"[DEBUG] Kandidat gefunden: {os.path.basename(candidate)}")

                # ✅ NEU: Prüfe ob es sich um eine echte Download-Datei handelt
                file_ext = os.path.splitext(candidate)[1].lower()
                if file_ext not in EXPECTED_EXTS:
                    print(f"[DEBUG] Ignoriere Datei mit unerwarteter Endung: {os.path.basename(candidate)}")
                    candidate = None  # Setze candidate zurück und suche weiter
                    before_paths = current_files  # Update before_paths für nächste Runde
                    continue

                break
            else:
                # Debug-Ausgabe: Zeige temporäre Dateien, die ignoriert werden
                temp_files = [p for p in (current_files - before_paths) if _is_temp(p)]
                if temp_files:
                    print(f"[DEBUG] Ignoriere temporäre Dateien: {[os.path.basename(f) for f in temp_files]}")
        except Exception as e:
            print(f"[DEBUG] Fehler bei Datei-Überwachung: {e}")

        time.sleep(1)

    if not candidate:
        raise TimeoutError("Kein neuer Download gefunden.")

    # Prüfe Stabilität
    print(f"[DEBUG] Überwache Stabilität von: {os.path.basename(candidate)}")
    stable_check_start = time.time()
    last_size = -1
    unchanged_count = 0

    while time.time() < deadline:
        try:
            if not os.path.exists(candidate):
                print(f"[DEBUG] Datei existiert nicht mehr: {candidate}")
                raise TimeoutError(f"Datei verschwand: {candidate}")

            current_size = os.path.getsize(candidate)

            if current_size == last_size:
                unchanged_count += 1
                print(f"[DEBUG] Datei unverändert ({unchanged_count}/{stable_secs}): {current_size} Bytes")
                if unchanged_count >= stable_secs:
                    print(f"[DEBUG] Datei stabil nach {unchanged_count} Prüfungen")
                    return candidate
            else:
                last_size = current_size
                unchanged_count = 0
                print(f"[DEBUG] Dateigröße geändert: {current_size} Bytes")

        except Exception as e:
            print(f"[DEBUG] Fehler bei Stabilitätsprüfung: {e}")

        time.sleep(1)

    raise TimeoutError(f"Datei wurde nicht stabil: {candidate}")


def is_probably_csv_bytes(raw: bytes) -> bool:
    """Behelf: CSV-Text? (Delimiter + Zeilenumbruch)"""
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            txt = raw.decode(enc, errors="ignore")
        except Exception:
            continue
        if any(ch in txt for ch in (";", "\t", ",", "|")) and ("\n" in txt or "\r" in txt):
            return True
    return False


def read_csv_bytes_best_effort(raw: bytes) -> pd.DataFrame:
    """Snifft Encoding + Delimiter. Vermeidet 1-Spalten-'Erfolge'."""
    # 1) Erst Versuch: Pandas selbst schnüffeln lassen (geht oft)
    try:
        df0 = pd.read_csv(io.BytesIO(raw), engine="python")
        if df0.shape[1] > 1:
            return df0
    except Exception:
        pass

    # 2) Encodings probieren + csv.Sniffer + Fallback
    encodings = ("utf-8-sig", "utf-8", "cp1252", "latin-1")
    for enc in encodings:
        try:
            text = raw.decode(enc, errors="ignore")
        except Exception:
            continue

        sample = text[:65536]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";\t,|")
            sep = dialect.delimiter
        except Exception:
            # Fallback-Heuristik
            if "\t" in sample:
                sep = "\t"
            elif ";" in sample:
                sep = ";"
            elif "|" in sample:
                sep = "|"
            else:
                sep = ","

        try:
            df = pd.read_csv(io.StringIO(text), sep=sep, engine="python")
        except Exception:
            continue

        # Wenn nur eine Spalte rauskam, alternative Sep testen
        if df.shape[1] == 1 and any(ch in sample for ch in ("\t", ";", "|", ",")):
            for alt in ("\t", ";", "|", ","):
                if alt == sep:
                    continue
                try:
                    df2 = pd.read_csv(io.StringIO(text), sep=alt, engine="python")
                    if df2.shape[1] > df.shape[1]:
                        df = df2
                        break
                except Exception:
                    pass

        if df.shape[1] > 1:
            return df

    # 3) letzte Patrone: lasse Pandas hart laufen (wirft dann Exception)
    return pd.read_csv(io.BytesIO(raw), engine="python")


def read_excel_bytes_any(raw: bytes, filename_hint: str = "") -> pd.DataFrame:
    """Bevorzuge calamine (robust für alte .xls); Fallback openpyxl/xlsb."""
    try:
        import calamine  # noqa: F401
        return pd.read_excel(io.BytesIO(raw), engine="calamine")
    except Exception:
        name = filename_hint.lower()
        if name.endswith(".xlsx"):
            return pd.read_excel(io.BytesIO(raw), engine="openpyxl")
        if name.endswith(".xlsb"):
            return pd.read_excel(io.BytesIO(raw), engine="pyxlsb")
        return pd.read_excel(io.BytesIO(raw))  # Pandas wählt Engine


def iter_dfs_from_zip(zip_path: str) -> Iterable[Dict]:
    """Für JEDES File im ZIP einen DF liefern (CSV bevorzugt, sonst Excel)."""
    print(f"[DEBUG] Öffne ZIP: {zip_path}")
    with zipfile.ZipFile(zip_path, "r") as zf:
        members = [m for m in zf.namelist() if not m.endswith("/")]
        print(f"[DEBUG] ZIP-Mitglieder: {members}")
        if not members:
            raise ValueError(f"ZIP ist leer: {zip_path}")
        for m in members:
            print(f"[DEBUG] Verarbeite ZIP-Member: {m}")
            raw = zf.read(m)
            low = m.lower()
            ext = os.path.splitext(low)[1]
            try:
                if ext in (".csv", ".txt"):
                    df = read_csv_bytes_best_effort(raw)
                elif ext in (".xls", ".xlsx", ".xlsb"):
                    if is_probably_csv_bytes(raw):
                        df = read_csv_bytes_best_effort(raw)
                    else:
                        df = read_excel_bytes_any(raw, filename_hint=m)
                else:
                    # unbekannt: erst CSV-Heuristik, dann Excel
                    if is_probably_csv_bytes(raw):
                        df = read_csv_bytes_best_effort(raw)
                    else:
                        df = read_excel_bytes_any(raw, filename_hint=m)
            except Exception as e:
                print(f"[WARN] Konnte Member nicht parsen: {m} in {zip_path}: {e}")
                continue
            yield {"member": m, "df": df, "ext": ext, "zip_name": os.path.basename(zip_path)}


def parse_download_to_dfs(path: str, region: str) -> List[pd.DataFrame]:
    """Parst eine heruntergeladene Datei in 1..n DataFrames + Metadaten."""
    print(f"[DEBUG] Starte Parsing von: {path} für Region: {region}")
    out = []
    p_lower = path.lower()
    try:
        if p_lower.endswith(".zip"):
            for item in iter_dfs_from_zip(path):
                df = item["df"]
                if df is None or df.empty:
                    continue
                df = df.copy()
                df["region"] = region
                df["zip_member"] = item["member"]
                df["zip_name"] = item["zip_name"]
                out.append(df)
        elif p_lower.endswith((".csv", ".txt")):
            with open(path, "rb") as f:
                raw = f.read()
            df = read_csv_bytes_best_effort(raw)
            if not df.empty:
                df["region"] = region
                df["zip_member"] = os.path.basename(path)
                df["zip_name"] = os.path.basename(path)
                out.append(df)
        elif p_lower.endswith((".xls", ".xlsx", ".xlsb")):
            with open(path, "rb") as f:
                raw = f.read()
            if is_probably_csv_bytes(raw):
                df = read_csv_bytes_best_effort(raw)
            else:
                df = read_excel_bytes_any(raw, filename_hint=path)
            if not df.empty:
                df["region"] = region
                df["zip_member"] = os.path.basename(path)
                df["zip_name"] = os.path.basename(path)
                out.append(df)
        else:
            print(f"[WARN] Unbekanntes Format: {path}")
    except Exception as e:
        print(f"[WARN] Konnte {path} nicht parsen: {e}")
    print(f"[DEBUG] Parsing abgeschlossen für {region}: {len(out)} DataFrames erstellt")
    return out


def robust_click(driver, elem):
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", elem)
    driver.execute_script("arguments[0].click();", elem)


def harmonize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Bereinigt offensichtliche Header-Issues & vereinheitlicht Kernspaltennamen."""
    # 1) Spaltennamen trimmen/Leerzeichen normalisieren
    cols = (pd.Index(df.columns)
            .map(lambda x: str(x).replace("\u00a0", " "))
            .map(lambda x: re.sub(r"\s+", " ", x).strip()))
    df.columns = cols

    # 2) Manche Dateien hatten den kompletten Header in EINER Spalte (mit Tabs):
    tabbed_cols = [c for c in df.columns if "\t" in c]
    for c in tabbed_cols:
        new_cols = [x.strip() for x in c.split("\t") if x.strip()]
        expanded = df[c].astype(str).str.split("\t", expand=True)
        k = min(expanded.shape[1], len(new_cols))
        expanded = expanded.iloc[:, :k]
        new_cols = new_cols[:k]
        expanded.columns = new_cols
        df = pd.concat([df.drop(columns=[c]), expanded], axis=1)

    # 3) Synonyme vereinheitlichen (Akzentverlust etc.)
    rename_map_candidates = {
        # Perimeter / Bereich
        "Périmètre": "perimetre",
        "Périmetre": "perimetre",
        "Perimetre": "perimetre",
        "Primtre": "perimetre",  # kaputtes Encoding
        # Kernspalten Erzeugung/Verbrauch
        "Consommation": "consommation_mw",
        "Thermique": "thermique_mw",
        "Nucléaire": "nucleaire_mw",
        "Nuclaire": "nucleaire_mw",
        "Eolien": "eolien_mw",
        "Éolien": "eolien_mw",
        "Solaire": "solaire_mw",
        "Hydraulique": "hydraulique_mw",
        "Pompage": "pompage_mw",
        "Bioénergies": "bioenergies_mw",
        "Bionergies": "bioenergies_mw",
        "Stockage batterie": "stockage_batterie_mw",
        "Déstockage batterie": "destockage_batterie_mw",
        "Dstockage batterie": "destockage_batterie_mw",
        "Éolien terrestre": "eolien_terrestre_mw",
        "Eolien terrestre": "eolien_terrestre_mw",
        "Éolien offshore": "eolien_offshore_mw",
        "Eolien offshore": "eolien_offshore_mw",
        # Physische Flüsse / CO2
        "Ech. physiques": "ech_physiques_mw",
        "Taux de Co2": "co2_g_per_kwh",
        "Taux de CO2": "co2_g_per_kwh",
        # Oft vorhandene Kopfspalten
        "Date": "date",
        "Heures": "heure",
        "Nature": "nature",
        "Périmètre géographique": "perimetre",
    }
    intersect = {k: v for k, v in rename_map_candidates.items() if k in df.columns}
    if intersect:
        df = df.rename(columns=intersect)

    return df


def coerce_numeric(df: pd.DataFrame, regex_hint: str = "") -> pd.DataFrame:
    """Konvertiert offensichtliche MW/CO2 Spalten in numerisch."""
    candidates = [c for c in df.columns if any(tok in c.lower()
                                               for tok in ["mw", "consommation", "therm", "nucl", "eolien",
                                                           "solaire", "hydraul", "ech_phys", "co2", "pompage",
                                                           "bio", "batterie", "offshore", "terrestre"])]
    for c in candidates:
        # Kommas als Dezimaltrenner fixen
        df[c] = (df[c]
                 .astype(str)
                 .str.replace("\u00a0", " ")
                 .str.replace(" ", "")
                 .str.replace(",", ".", regex=False))
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def build_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """Baut ts_local aus date + heure (falls vorhanden)."""
    if {"date", "heure"}.issubset(df.columns):
        date_str = df["date"].astype(str).str.replace(".", "/", regex=False)
        heure_str = df["heure"].astype(str)
        ts = pd.to_datetime(date_str + " " + heure_str, dayfirst=True, errors="coerce")
        try:
            df["ts_local"] = ts.dt.tz_localize("Europe/Paris", nonexistent="shift_forward", ambiguous="NaT")
        except TypeError:
            df["ts_local"] = ts
    return df


# -----------------------------
# Main
# -----------------------------
def scrape_and_parse() -> pd.DataFrame:
    dfs_all: List[pd.DataFrame] = []

    if REPARSE_ONLY:
        print("[INFO] REPARSE_ONLY=True → keine Selenium-Session, parse vorhandene Downloads.")
        files = [os.path.join(DOWNLOAD_DIR, f) for f in os.listdir(DOWNLOAD_DIR)
                 if os.path.splitext(f)[1].lower() in EXPECTED_EXTS]
        for path in files:
            region_guess = os.path.basename(path).split("_encours_mensuel")[0]
            for df in parse_download_to_dfs(path, region_guess):
                df = harmonize_columns(df)
                df = build_timestamp(df)
                df = coerce_numeric(df)
                dfs_all.append(df)
        return pd.concat(dfs_all, ignore_index=True, sort=False) if dfs_all else pd.DataFrame()

    # ✅ NEU: Bereinige vor Selenium-Start temporäre Dateien
    print("[DEBUG] Bereinige temporäre Dateien vor Selenium-Start...")
    for f in os.listdir(DOWNLOAD_DIR):
        if f.startswith('.') or _is_temp(f):
            try:
                os.remove(os.path.join(DOWNLOAD_DIR, f))
                print(f"[DEBUG] Gelöschte temporäre Datei: {f}")
            except Exception as e:
                print(f"[DEBUG] Konnte {f} nicht löschen: {e}")

    # Selenium Setup
    opts = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "download.download_path": DOWNLOAD_DIR,  # Explizit setzen
        "profile.default_content_settings.popups": 0,
        "profile.content_settings.exceptions.automatic_downloads.*.setting": 1,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "plugins.always_open_pdf_externally": True,
        "download.extensions_to_open": "",
    }
    opts.add_experimental_option("prefs", prefs)

    # ✅ EC2 ANPASSUNG 2: Headless Mode für EC2
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    # ✅ NEU: Zusätzliche Stabilitäts-Optionen
    opts.add_argument("--disable-features=VizDisplayCompositor")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--dns-prefetch-disable")
    opts.add_argument("--disable-background-timer-throttling")

    # ✅ EC2 ANPASSUNG 3: Manuelles ChromeDriver
    driver = webdriver.Chrome(service=Service("/usr/local/bin/chromedriver"), options=opts)
    wait = WebDriverWait(driver, 25)

    try:
        driver.get(URL)

        # Cookies (EN/FR)
        for label in ("Accept all", "Tout accepter", "Accepter tout"):
            try:
                btn = wait.until(EC.element_to_be_clickable((By.XPATH, f"//button[contains(., '{label}')]")))
                robust_click(driver, btn)
                break
            except Exception:
                pass

        # 1) Scope: Region
        scope_select = wait.until(EC.presence_of_element_located((
            By.XPATH, "//select[option[normalize-space()='France'] and option[contains(.,'Region')]]"
        )))
        Select(scope_select).select_by_visible_text("Region")
        time.sleep(SLEEP_BETWEEN)

        # 2) Regions-Liste
        region_select = wait.until(EC.presence_of_element_located((
            By.XPATH, "//select[option[contains(.,'Auvergne') or contains(.,'Bourgogne') or contains(.,'Normandie')]]"
        )))
        regions = [o.text.strip() for o in region_select.find_elements(By.TAG_NAME, "option")]
        regions = [r for r in regions if r and r.lower() not in {"region", "sélectionner", "selectionner"}]
        print(f"[INFO] Gefundene Regionen ({len(regions)}): {regions}")

        # 3) Pro Region: Download "En-cours mensuel temps réel"
        for reg in regions:
            print(f"[DEBUG] Starte Verarbeitung für Region: {reg}")

            # Region re-select (DOM wechselt)
            region_select = wait.until(EC.presence_of_element_located((
                By.XPATH,
                "//select[option[contains(.,'Auvergne') or contains(.,'Bourgogne') or contains(.,'Normandie')]]"
            )))
            Select(region_select).select_by_visible_text(reg)
            time.sleep(SLEEP_BETWEEN)

            link = wait.until(EC.element_to_be_clickable((
                By.XPATH, "//a[contains(., 'En-cours') and contains(., 'mensuel') and contains(., 'temps')]"
            )))
            before_paths = {os.path.join(DOWNLOAD_DIR, f) for f in os.listdir(DOWNLOAD_DIR)}
            robust_click(driver, link)

            dl_path = wait_for_new_file_and_stable(DOWNLOAD_DIR, before_paths)
            ext = os.path.splitext(dl_path)[1].lower()
            if ext not in EXPECTED_EXTS:
                print(f"[WARN] Unerwartete Datei ({ext}): {dl_path} — überspringe.")
                continue

            new_name = os.path.join(DOWNLOAD_DIR, f"{sanitize(reg)}_encours_mensuel{ext}")
            try:
                if os.path.abspath(new_name) != os.path.abspath(dl_path):
                    if os.path.exists(new_name):
                        os.remove(new_name)
                    shutil.move(dl_path, new_name)
            except Exception:
                new_name = dl_path
            print(f"[OK] Downloaded: {new_name}")

            # ✅ NEU: Debug-Code für ZIP-Datei
            try:
                file_size = os.path.getsize(new_name)
                print(f"[DEBUG] Dateigröße: {file_size} Bytes")

                # Prüfe ob es sich um eine echte ZIP-Datei handelt
                if new_name.endswith('.zip'):
                    with zipfile.ZipFile(new_name, 'r') as zf:
                        file_list = zf.namelist()
                        print(f"[DEBUG] ZIP-Inhalt: {file_list}")

                        # Versuche das erste File zu lesen
                        if file_list:
                            first_file = file_list[0]
                            print(f"[DEBUG] Teste erstes File: {first_file}")
                            with zf.open(first_file) as f:
                                first_bytes = f.read(100)  # Erste 100 Bytes
                                print(f"[DEBUG] Erste Bytes: {first_bytes}")
            except Exception as e:
                print(f"[ERROR] Datei-Check fehlgeschlagen: {e}")

            # Parsen → 1..n DFs
            parsed = parse_download_to_dfs(new_name, reg)
            if not parsed:
                print(f"[WARN] Keine DFs aus {new_name}")
                continue

            for df in parsed:
                df = harmonize_columns(df)
                df = build_timestamp(df)
                df = coerce_numeric(df)
                dfs_all.append(df)

            print(f"[DEBUG] Fertig mit Region {reg}, fahre fort...")
            time.sleep(SLEEP_BETWEEN)

    finally:
        driver.quit()

    if not dfs_all:
        return pd.DataFrame()

    combined_df = pd.concat(dfs_all, ignore_index=True, sort=False)

    # Daten strukturieren
    if not combined_df.empty:
        lead_cols = [c for c in ["region", "date", "heure", "ts_local", "nature", "perimetre"] if
                     c in combined_df.columns]
        other_cols = [c for c in combined_df.columns if c not in lead_cols]
        combined_df = combined_df[lead_cols + other_cols]

    return combined_df