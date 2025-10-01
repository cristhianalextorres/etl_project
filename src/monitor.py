
import sqlite3
import time
from datetime import datetime

class MonitorETL:

    def __init__(self):
        self.db_file = "..\\data\\lab2.db"
        self.start_time = None
        self.registros_leidos = 0
        self.registros_validos = 0
        self.registros_descartados = 0

    def start(self):
        self.start_time = time.time()

    def set_metrics(self, leidos: int, validos: int, descartados: int):
        self.registros_leidos = leidos
        self.registros_validos = validos
        self.registros_descartados = descartados

    def end(self, error: str = None):
        duracion = round(time.time() - self.start_time, 2) if self.start_time else 0.0
        self.error = error

        conn = sqlite3.connect(self.db_file)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO etl_monitor (fecha, registros_leidos, registros_validos, registros_descartados, duracion_segundos, error)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            self.registros_leidos,
            self.registros_validos,
            self.registros_descartados,
            duracion,
            self.error

        ))
        conn.commit()
        conn.close()

        print("===== Resumen corrida ETL =====")
        print(f"Registros leídos:      {self.registros_leidos}")
        print(f"Registros válidos:     {self.registros_validos}")
        print(f"Registros descartados: {self.registros_descartados}")
        print(f"Duración (segundos):   {duracion}")
