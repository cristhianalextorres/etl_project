import pandas as pd
import xml.etree.ElementTree as ET
import os
import requests
from schema import COLUMNS
import json
import logging
import random
import math
import time
import sqlite3
from requests.auth import HTTPBasicAuth
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional, Literal
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

# 2) Al hacer el push de eventos, pasa los idSchemes:
IDSCHEMES_EVENTS_CODED = {
    "orgUnitIdScheme": "CODE",   # <— clave para tus OU en código
    # por si algún día quieres usar codes para otros metadatos:
    # "dataElementIdScheme": "UID",
    # "programIdScheme": "UID",
    # "programStageIdScheme": "UID",
}


# ===================== CONFIG BÁSICA =====================
#SQLITE_PATH = os.getenv("SQLITE_PATH", "./dengue.db")
SQLITE_PATH = r"..\\data\\dbProject.db"
# Parámetros DHIS2 (ponlos en tu .env)
DHIS2_BASE_URL = os.getenv("DHIS2_BASE_URL", "https://datos.hispcolombia.org/dhis")
DHIS2_USER     = os.getenv("DHIS2_USER", "usuario_dengue")
DHIS2_PASS     = os.getenv("DHIS2_PASS", "Dengue2025!")

# Carpeta destino
DEST_DIR = "..\\data\\github_files"
os.makedirs(DEST_DIR, exist_ok=True)

class DataExtractor:
    def __init__(self):
        pass

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
    
    def _int_positive_or_none(self, v):

        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        # strings -> número
        if isinstance(v, str):
            s = v.strip().replace(",", ".")
            if s == "":
                return None
            try:
                v = float(s)
            except Exception:
                return None
        try:
            vf = float(v)
        except Exception:
            return None
        if not math.isfinite(vf):
            return None
        vi = int(round(vf))
        # acepta solo si es prácticamente entero (p.ej., 1.0 → 1)
        if abs(vf - vi) < 1e-9 and vi > 0:
            return vi
        return None

     # --- NUEVO: obtener UIDs del servidor DHIS2 y asignarlos a eventos ---
    def fetch_system_ids(self, count: int, max_per_request: int = 10000) -> list[str]:
        """
        Pide UIDs al servidor en bloques de máximo 10,000 por petición.
        Devuelve una lista de UIDs (strings).
        """
        ids = []
        while count > 0:
            req = min(count, max_per_request)
            status, data = self.dhis2_get("/api/system/id", params={"limit": req})
            batch_ids = (data.get("codes") or data.get("uids") or data.get("ids")
                        or ([data.get("code")] if data.get("code") else None))
            if status != 200 or not batch_ids:
                raise RuntimeError(f"No pude obtener UIDs del servidor: {status=} {data=}")
            if len(batch_ids) < req:
                raise RuntimeError(f"El servidor devolvió {len(batch_ids)} UIDs pero se pidieron {req}.")
            ids.extend(batch_ids)
            count -= req
        return ids
    
    def ensure_event_uids(sefl, events: list[dict], prefetch: bool = True) -> list[dict]:
        """
        Si algún evento no trae 'event', solicita UIDs a DHIS2 y los asigna.
        Modifica y retorna la lista de eventos.
        """
        missing_idx = [i for i, ev in enumerate(events) if not ev.get("event")]
        if prefetch and missing_idx:
            new_ids = sefl.fetch_system_ids(len(missing_idx))
            for k, i in enumerate(missing_idx):
                events[i]["event"] = new_ids[k]
        return events

    @staticmethod
    def connect_sqlite(db_path: str) -> sqlite3.Connection:
        p = Path(db_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(p)
        conn.row_factory = sqlite3.Row  # para dict-like rows
        print(f"✅ Conectado a SQLite: {p}")
        return conn
    
    @staticmethod
    def fetch_df(conn: sqlite3.Connection, sql: str, params: Optional[tuple] = None) -> pd.DataFrame:
        df = pd.read_sql(sql, conn, params=params)
        print(f"📥 Consulta trajo {len(df)} filas × {len(df.columns)} columnas")
        return df
    
    def chunk_list(self, items: List[Any], size: int) -> List[List[Any]]:
        return [items[i:i+size] for i in range(0, len(items), size)]
    
    def dhis2_post(self, path: str, payload: dict, params: dict | None = None, timeout: int = 60) -> dict:
        url = DHIS2_BASE_URL.rstrip("/") + "/" + path.lstrip("/")
        resp = requests.post(url, json=payload,  headers = {'Accept': '*/*', "Content-Type": "application/json"},params=params, auth=HTTPBasicAuth(DHIS2_USER, DHIS2_PASS), timeout=timeout)
        try:
            data = resp.json()
        except Exception:
            data = {"text": resp.text, "status_code": resp.status_code}
        if not resp.ok:
            raise RuntimeError(f"DHIS2 POST {path} failed {resp.status_code}: {data}")
        return data

    def dhis2_get(self, path: str, params: Optional[dict] = None, timeout: int = 60):
        url = DHIS2_BASE_URL.rstrip("/") + "/" + path.lstrip("/")
        resp = requests.get(url, params=params, auth=(DHIS2_USER, DHIS2_PASS), timeout=timeout)
        try:
            data = resp.json()
        except Exception:
            data = {"text": resp.text}
        return resp.status_code, data
    
    def _pair_summaries_with_events(self, batch: list[dict], summaries: list[dict]) -> list[tuple[dict, dict]]:
        """
        Empareja cada importSummary con su evento del batch.
        1) Por 'reference' (UID del evento creado).
        2) Por 'index' (si viene).
        3) Fallback: por orden (evita quedarse sin par).
        """
        ref_to_idx = {ev.get("event"): i for i, ev in enumerate(batch) if ev.get("event")}
        used = set()
        pairs = []
        cursor = 0
        for s in summaries:
            idx = None
            ref = s.get("reference")
            if ref and ref in ref_to_idx and ref_to_idx[ref] not in used:
                idx = ref_to_idx[ref]
            elif isinstance(s.get("index"), int) and 0 <= s["index"] < len(batch) and s["index"] not in used:
                idx = s["index"]
            else:
                # fallback secuencial
                while cursor < len(batch) and cursor in used:
                    cursor += 1
                if cursor < len(batch):
                    idx = cursor
                    cursor += 1
            if idx is None:
                # último recurso: apunta al 0 (no debería pasar)
                idx = 0
            used.add(idx)
            pairs.append((s, batch[idx]))
        return pairs
    
    def append_jsonl(self, path: Path, record: dict):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

# ==================== ENVÍO CON LOGGING =====================
    def push_events_with_logging(
        self,
        events: list[dict],
        batch_size: int = 100,
        sleep_secs: float = 3.0,
        dry_run: bool = False,
        verify: bool = True,
        verify_sample: int = 10,
        log_dir: str = "..\\logs",
        log_name_prefix: str = "push_events",
        error_dir: str = "..\\logs",
        error_name_prefix: str = "events_failed",
        ensure_uid: bool = True,
        idscheme_params: dict | None = None,   # p.ej. {"orgUnitIdScheme":"CODE"}
        ):
        """
        Envía eventos y:
        - Loggea batches en <log_dir>/<prefix>_<ts>.jsonl
        - Escribe resumen en <log_dir>/<prefix>_<ts>_summary.json
        - **Guarda SOLO los eventos con error** en <error_dir>/<error_prefix>_<ts>.jsonl (1 por línea)
        """
        ts = time.strftime("%Y%m%d_%H%M%S")
        log_path = Path(log_dir) / f"{log_name_prefix}_{ts}.jsonl"
        summary_path = Path(log_dir) / f"{log_name_prefix}_{ts}_summary.json"
        errors_path = Path(error_dir) / f"{error_name_prefix}_{ts}.jsonl"
        logger = self._setup_logger(log_path)
        

        # Asegura UIDs cliente si faltan
        if ensure_uid:
            events = self.ensure_event_uids(events, prefetch=True)

        total = len(events)
        if total == 0:
            self.append_jsonl(log_path, {"ts": self._now_iso(), "level": "WARN", "msg": "No hay eventos para enviar."})
            with open(summary_path, "w", encoding="utf-8") as f:
                json.dump({"total": 0, "imported": 0, "updated": 0, "ignored": 0, "deleted": 0,
                        "errors": 0, "error_file": str(errors_path)}, f, ensure_ascii=False, indent=2)
            return {"total": 0, "imported": 0, "updated": 0, "ignored": 0, "deleted": 0,
                    "errors": 0, "error_file": str(errors_path), "log": str(log_path), "summary": str(summary_path)}

        batches = [events[i:i+batch_size] for i in range(0, total, batch_size)]
        created_uids: list[str] = []
        totals = {"imported": 0, "updated": 0, "deleted": 0, "ignored": 0}
        error_count = 0

        self.append_jsonl(log_path, {"ts": self._now_iso(), "level": "INFO", "msg": "Inicio push eventos",
                                "total_events": total, "batches": len(batches), "dry_run": dry_run})

        for idx, batch in enumerate(batches, start=1):
            payload = {"events": batch}
            if dry_run:
                self.append_jsonl(log_path, {"ts": self._now_iso(), "level": "DRYRUN", "batch": idx, "batch_size": len(batch)})
                continue

            try:
                status, data = self.dhis2_post("/api/events", payload, params=idscheme_params)
                parsed = self._parse_events_import_response(data)
                totals["imported"] += parsed["imported"]
                totals["updated"]  += parsed["updated"]
                totals["deleted"]  += parsed["deleted"]
                totals["ignored"]  += parsed["ignored"]
                print(parsed)

                # Emparejar summaries con eventos para aislar errores
                pairs = self._pair_summaries_with_events(batch, parsed["summaries"])
                new_refs = []
                for s, ev in pairs:
                    ok_status = str(s.get("status", "")).upper() in {"SUCCESS", "OK"}
                    has_conflicts = bool(s.get("conflicts"))
                    ignored_local = int((s.get("importCount") or {}).get("ignored", 0))
                    if ok_status and not has_conflicts and ignored_local == 0:
                        if s.get("reference"): new_refs.append(s["reference"])
                    else:
                        # Registrar evento con error en archivo aparte
                        error_payload = {
                            "ts": self._now_iso(),
                            "batch": idx,
                            "event_uid": ev.get("event"),
                            "http_status": status,
                            "summary": s,
                            "event": ev
                        }
                        self.append_jsonl(errors_path, error_payload)
                        error_count += 1

                created_uids.extend([r for r in new_refs if r])

                # Log por batch
                conflicts = []
                for s in parsed["summaries"]:
                    if s.get("conflicts"): conflicts.extend(s["conflicts"])
                self.append_jsonl(log_path, {
                    "ts": self._now_iso(), "level": "INFO", "kind": "batch_result",
                    "batch": idx, "batches_total": len(batches),
                    "http_status": status,
                    "counts": parsed | {"summaries_len": len(parsed["summaries"])},
                    "created_refs": new_refs,
                    "conflicts": conflicts[:50]
                })

            except Exception as e:
                # Si falló el batch completo (HTTP u otra excepción), guardamos todos los eventos del batch como error
                self.append_jsonl(log_path, {"ts": self._now_iso(), "level": "ERROR", "batch": idx, "error": str(e)})
                for ev in batch:
                    self.append_jsonl(errors_path, {
                        "ts": self._now_iso(), "batch": idx, "event_uid": ev.get("event"),
                        "http_status": None, "summary": {"status": "BATCH_ERROR", "description": str(e)},
                        "event": ev
                    })
                    error_count += 1

            if sleep_secs:
                time.sleep(sleep_secs)

        # Verificación (muestral)
        verification = {"sampled": [], "ok": 0, "fail": 0}
        if verify and created_uids:
            sample = random.sample(created_uids, k=min(verify_sample, len(created_uids)))
            for ev_uid in sample:
                s2, d2 = self.dhis2_get(f"/api/events/{ev_uid}")
                ok = (s2 == 200) and (d2.get("event") == ev_uid or d2.get("httpStatusCode") == 200)
                verification["sampled"].append({"event": ev_uid, "status": s2, "ok": ok})
                verification["ok"] += 1 if ok else 0
                verification["fail"] += 0 if ok else 1
                self.append_jsonl(log_path, {"ts": self._now_iso(), "level": "VERIFY", "event": ev_uid, "http_status": s2, "ok": ok})
        else:
            self.append_jsonl(log_path, {"ts": self._now_iso(), "level": "INFO", "msg": "Verificación omitida o no hay UIDs creados."})

        # Resumen final
        summary = {
            "total": total, **totals,
            "errors": error_count,
            "created_uids_count": len(created_uids),
            "created_uids": created_uids,
            "verification": verification,
            "log_file": str(log_path),
            "error_file": str(errors_path)
        }
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        self.append_jsonl(log_path, {"ts": self._now_iso(), "level": "INFO", "msg": "Fin push eventos",
                                "totals": totals, "created_uids": len(created_uids),
                                "errors": error_count, "summary_file": str(summary_path)})
        return summary


    def _setup_logger(self, log_path: Path) -> logging.Logger:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        logger = logging.getLogger("dhis2_push")
        # Evita duplicar handlers si se llama varias veces
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            fh = logging.FileHandler(log_path, encoding="utf-8")
            fh.setLevel(logging.INFO)
            fmt = logging.Formatter('%(message)s')
            fh.setFormatter(fmt)
            logger.addHandler(fh)
            # también consola
            ch = logging.StreamHandler()
            ch.setLevel(logging.INFO)
            ch.setFormatter(logging.Formatter('%(message)s'))
            logger.addHandler(ch)
        return logger

    def _now_iso(self):
        return time.strftime("%Y-%m-%dT%H:%M:%S")
    

    def _parse_events_import_response(self, data: dict) -> dict:
        """
        Normaliza respuesta de /api/events (versiones DHIS2 varían).
        Devuelve: imported, updated, deleted, ignored, summaries[list].
        """
        base = {"imported": 0, "updated": 0, "deleted": 0, "ignored": 0, "summaries": []}
        container = data.get("response", data)
        base["imported"] = container.get("imported", 0)
        base["updated"]  = container.get("updated", 0)
        base["deleted"]  = container.get("deleted", 0)
        base["ignored"]  = container.get("ignored", 0)
        summaries = container.get("importSummaries") or data.get("importSummaries") or []
        for s in summaries:
            base["summaries"].append({
                "reference": s.get("reference"),
                "status": s.get("status"),
                "description": s.get("description"),
                "conflicts": s.get("conflicts", []),
                "importCount": s.get("importCount", {}),
                "index": s.get("index")  # algunas versiones lo incluyen
            })
        return base
    # ==================== TRANSFORMACIONES ======================

    def to_dhis2_events(self, df: pd.DataFrame, mapping: Dict[str, Any]) -> List[dict]:
        """
        Transforma un DataFrame a payloads /api/events.
        Un evento por fila (o por agrupación si lo prefieres).
        """
        assert mapping.get("mode") == "events", "El mapeo no es 'events'."
        required = ["program", "programStage", "dataElements"]
        for r in required:
            if r not in mapping:
                raise ValueError(f"Falta '{r}' en el mapeo events.")

        program = mapping["program"]
        stage = mapping["programStage"]
        de_map: Dict[str, str] = mapping["dataElements"]
        types: dict = mapping.get("types", {})

        org_col = mapping.get("orgUnit_col")
        date_col = mapping.get("eventDate_col")
        org_const = mapping.get("orgUnit_const")
        date_const = mapping.get("eventDate_const")

        if not org_const and (not org_col or org_col not in df.columns):
            raise ValueError("Debes indicar 'orgUnit_const' o 'orgUnit_col' existente en el DataFrame.")
        if not date_const and (not date_col or date_col not in df.columns):
            raise ValueError("Debes indicar 'eventDate_const' o 'eventDate_col' existente en el DataFrame.")

        events = []
        for _, row in df.iterrows():
            org = org_const if org_const else row[org_col]
            ev_date = date_const if date_const else row[date_col]
            data_values = []
            for src_col, de_uid in de_map.items():
                if src_col not in df.columns:
                    continue
                val = row[src_col]
                t = types.get(src_col)
                if t == "integer_positive":
                    val = self._int_positive_or_none(val)
                    if val is None:
                        continue
                if pd.isna(val):
                    continue
                data_values.append({
                    "dataElement": de_uid,
                    "value": val
                })

            events.append({
                "program": program,
                "programStage": stage,
                "orgUnit": org,
                "eventDate": str(ev_date),
                "status": "COMPLETED",
                "completedDate": str(ev_date),
                "dataValues": data_values
            })

        return events


