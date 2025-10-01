
import sqlite3
import pandas as pd
import os


class DataLoader:

    @staticmethod
    def insert_data(conn, table_name: str, df: pd.DataFrame):
        try:
            total_rows = len(df)
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            log = f"{total_rows} registros insertados en tabla '{table_name}'"

            return log

        except Exception as e:
            log = f"[ERROR] Error insertando en tabla '{table_name}': {e}"
            return log

    @staticmethod
    def create_table(conn, create_table_sql: str, table_name: str):
        try:
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys = ON;")
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.close()

            log = f"Tabla '{table_name}' creada correctamente"
            return log

        except Exception as e:
            log = f"[ERROR] Error creando tabla '{table_name}': {e}"
            return log
    
    @staticmethod
    def init_db(path_db: str):
        try:
            if os.path.exists(path_db):
                log= f"La BD ya existe: {path_db}"
                conn = sqlite3.connect(path_db)
                return conn, log
            else:
                conn = sqlite3.connect(path_db)
                log = f"BD creada: {path_db}"
                return conn, log
        except Exception as e:
            log = f"[ERROR] Error creando la bd '{path_db}': {e}"
            return log        

    @staticmethod
    def create_table(conn, create_table_sql: str, table_name: str):
        try:
            cursor = conn.cursor()

            # Verificar si la tabla ya existe
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name=?;
            """, (table_name,))
            table_exists = cursor.fetchone()

            if table_exists:
                log = f"La tabla '{table_name}' ya existe."
                return log
            
            cursor.execute("PRAGMA foreign_keys = ON;")
            cursor.execute(create_table_sql)
            conn.commit()
            cursor.close()

            log = f"Tabla '{table_name}' creada correctamente."
            return log

        except Exception as e:
            log = f"[ERROR] Error creando tabla '{table_name}': {e}"
            return log
