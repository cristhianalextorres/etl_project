CREATE TABLE IF NOT EXISTS etl_monitor (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fecha TEXT,
                registros_leidos INTEGER,
                registros_validos INTEGER,
                registros_descartados INTEGER,
                duracion_segundos REAL,
                error TEXT
);