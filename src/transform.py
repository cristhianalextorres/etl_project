import pandas as pd
import io
from schema import EXPECTED_COLS, DATE_COLS, BOOLEAN_COLS, ALIASES, TXT
class Transformer:
    def __init__(self):
        pass

    def to_iso_date(self, v):
        if pd.isna(v): return None
        s = str(v).strip()
        if not s: return None
        try:
            return pd.to_datetime(s, errors="coerce").date().isoformat()
        except Exception:
            return None

    def to_bool_int(self, v):
        if pd.isna(v): return None
        if isinstance(v, bool): return 1 if v else 0
        s = str(v).strip().lower()
        if s in {"true","t","1","yes","y","si","sí"}: return 1
        if s in {"false","f","0","no","n"}: return 0
        try:
            return 1 if int(float(s)) != 0 else 0
        except Exception:
            return None

    def normalize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        # Renombrar por alias
        rename_map = {c: ALIASES[c] for c in df.columns if c in ALIASES}
        df = df.rename(columns=rename_map)

        # Solo columnas esperadas y agregar faltantes
        keep = [c for c in EXPECTED_COLS if c in df.columns]
        df = df[keep].copy()
        for c in EXPECTED_COLS:
            if c not in df.columns:
                df[c] = None
        df = df[EXPECTED_COLS]

        # Normalizar fechas y booleanos
        for c in DATE_COLS:
            df[c] = df[c].map(self.to_iso_date)
        for c in BOOLEAN_COLS:
            df[c] = df[c].map(self.to_bool_int)

        for c in TXT:
            if c in df.columns:
                df[c] = df[c].astype("string").str.strip()

        return df

