import pandas as pd
import dateparser
from schema import EXPECTED_COLS, DATE_COLS, BOOLEAN_COLS, ALIASES, TXT


def to_iso_date(v):
    if pd.isna(v): return None
    s = str(v).strip()
    if not s: return None
    try:
        return pd.to_datetime(s, errors="coerce").date().isoformat()
    except Exception:
        return None

file = 'C:\ProjectBI\Proyecto_ETL\data\github_files\Datos_2010_210.xlsx'

df = pd.read_excel(file)

rename_map = {c: ALIASES[c] for c in df.columns if c in ALIASES}
df = df.rename(columns=rename_map)

# Solo columnas esperadas y agregar faltantes
keep = [c for c in EXPECTED_COLS if c in df.columns]
df = df[keep].copy()
for c in EXPECTED_COLS:
    if c not in df.columns:
        df[c] = None
df = df[EXPECTED_COLS]

for c in DATE_COLS:
    df[c] = df[c].map(to_iso_date)

print(df.head())