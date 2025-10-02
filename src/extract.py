import pandas as pd
import xml.etree.ElementTree as ET
import os
import requests
from schema import COLUMNS
from dotenv import load_dotenv

load_dotenv(override=True)

# --- Configuración GitHub ---
GITHUB_OWNER = "markodavid"
GITHUB_REPO  = "ETL-Sivigila"
BRANCH       = "main"
PATH_PREFIX  = ""
INCLUDE_SUBFOLDERS = True
FILE_EXTS    = (".csv", ".xlsx", ".xls")   # se amplía para incluir CSV
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", None)

# Carpeta destino
DEST_DIR = "..\\data\\github_files"
os.makedirs(DEST_DIR, exist_ok=True)

class DataExtractor:

    @staticmethod
    def from_csv(ruta: str, archivo: str):
        try:
            df = pd.read_csv(ruta + archivo, sep=",")
            log = f"CSV cargado correctamente: {archivo}"
            return log, df
        except Exception as e:
            log = f"[ERROR] Error al leer CSV: {e}"
            return log
    @staticmethod
    def from_json(ruta: str, archivo: str):
        try:
            df = pd.read_json(ruta + archivo)
            log = f"JSON cargado correctamente: {archivo}"
            return log, df
        except Exception as e:
            log = f"[ERROR] Error al leer JSON: {e}"
            return log
    @staticmethod
    def from_xml(ruta: str, archivo: str):
        try:
            tree = ET.parse(ruta + archivo)
            root = tree.getroot()
            df = pd.read_xml(ruta + archivo, parser="lxml")
            log = f"XML cargado correctamente con ElementTree: {archivo}"
            return log, df
        except Exception as e:
            log = f"[ERROR] Error al leer XML: {e}"
            return  log
    @staticmethod
    def from_github():

        headers = {}
        if GITHUB_TOKEN:
            headers["Authorization"] = f"token {GITHUB_TOKEN}"

        url = f"https://api.github.com/repos/{GITHUB_OWNER}/{GITHUB_REPO}/git/trees/{BRANCH}?recursive=1"
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()

        files = resp.json().get("tree", [])
        target_files = [f for f in files if f["path"].endswith(FILE_EXTS)]

        print(f"🔎 Se encontraron {len(target_files)} archivos con extensión {FILE_EXTS} en el repo.")

        local_files = []
        for f in target_files:
            file_url = f"https://raw.githubusercontent.com/{GITHUB_OWNER}/{GITHUB_REPO}/{BRANCH}/{f['path']}"
            file_name = os.path.join(DEST_DIR, os.path.basename(f["path"]))

            print(f"⬇️ Descargando: {file_url}")
            r = requests.get(file_url, headers=headers)
            r.raise_for_status()

            with open(file_name, "wb") as out:
                out.write(r.content)
            local_files.append(file_name)

        log = f"🔎 Se encontraron {len(target_files)} archivos. ✅ Descarga completada. Archivos guardados en + {DEST_DIR}"
        return log, local_files

    @staticmethod
    def load_data(files: list = "df"):
        dataframes = []
        for file_path in files:
            try:
                if file_path.endswith(".csv"):
                    df = pd.read_csv(file_path, encoding="utf-8", sep=",")
                elif file_path.endswith(".xls"):
                    df = pd.read_excel(file_path, engine="xlrd")
                elif file_path.endswith(".xlsx"):
                    df = pd.read_excel(file_path, engine="openpyxl")
                else:
                    raise ValueError("Formato no soportado")
                
                for col in COLUMNS:
                    if col not in df.columns:
                        df[col] = None
                df = df[COLUMNS]  # solo columnas definidas


                dataframes.append(df)
                
                final_df = pd.concat(dataframes, ignore_index=True)
            except Exception as e:
                print(f"❌ Error al cargar {os.path.basename(file_path)}: {e}")
            log = f"✅ DataFrame cargado con {len(final_df)} filas y {len(final_df.columns)} columnas."
        return log, final_df


