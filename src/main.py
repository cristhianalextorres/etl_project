from load import DataLoader
from transform import Transformer
from extract import DataExtractor
from logger import LoggerETL
from monitor import  MonitorETL
#from schema import EXPECTED_COLS, DATE_COLS, BOOLEAN_COLS, ALIASES, TXT, COLUMNS

file_path = "..\\data\\"
path_db = "..\\data\\dbProject.db"
registros_validos = 0
registros_validos = 0
registros_descartados = 0

monitor = MonitorETL()
logger = LoggerETL(monitor=monitor)

try:
    logger.info("Inicio del proceso ETL")
    monitor.start()

    conn, log = DataLoader.init_db(path_db = path_db)
    logger.info(log)

    with open('..\\consultas\\datos_dengue.sql', 'r', encoding="ISO-8859-1") as archivo:
        datos_dengue = archivo.read()
    with open('..\\consultas\\etl_monitor.sql', 'r', encoding="ISO-8859-1") as archivo:
        etl_monitor = archivo.read()

    tables = [
        ("etl_monitor", etl_monitor),
        ("datos_dengue", datos_dengue)
    ]

    # Crear todas las tablas en ciclo
    for name, create_sql in tables:
        log = DataLoader.create_table(conn= conn, create_table_sql = create_sql, table_name=name)
        logger.info(log)

    log, files = DataExtractor.from_github()
    logger.info(log)

    log, df = DataExtractor.load_data(files)
    logger.info(log)

    transformer = Transformer()

    df_normalizado = transformer.normalize_df(df)

    registros_leidos = int(len(df_normalizado))

    df_normalizado.drop_duplicates(inplace=True)

    registros_validos = int(len(df_normalizado))

    registros_descartados = registros_leidos - registros_validos

    print(df_normalizado.info())

    log = DataLoader.insert_data(conn, "datos_dengue", df_normalizado)
    logger.info(log)
    monitor.set_metrics(leidos=registros_leidos, validos=registros_validos, descartados=registros_descartados)
    monitor.end()
    logger.info("Fin del proceso ETL Satisfactoriamente")

except Exception as e:
    logger.info(f"[ERROR] Error en la corrida ETL: {e}")