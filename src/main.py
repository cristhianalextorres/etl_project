from load import DataLoader
from transform import Transformer
from extract import DataExtractor
from logger import LoggerETL
from monitor import  MonitorETL
from typing import Dict, List, Any, Optional, Literal
import json
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
    
    conn = DataExtractor.connect_sqlite(path_db)

    # 👉 Cambia la consulta según tu esquema/tabla:
    SQL = """
        SELECT *
        FROM datos_dengue
        WHERE fec_not BETWEEN '2022-01-01' AND '2022-01-31'
    """
    df = DataExtractor.fetch_df(conn, SQL)
    conn.close()

    if df.empty:
            print("⚠️ DataFrame vacío. Nada que enviar a DHIS2.")
            raise SystemExit(0)
    
    mapping_events = {
        "mode": "events",
        "program": "GHWnp9BxnEG",
        "programStage": "bhPmyvCyvSa",
        "orgUnit_col": "cod_mun_n",
        "eventDate_col": "fec_not",
        "dataElements": {
            "consecutive":"BNqQUv1De64",
            "cod_eve":"Tfcg4sronvs",
            "fec_not":"RXS1fys0q3E",
            "semana":"Ii2ywM5DdTU",
            "ano":"xnsxcTj1Jxi",
            "cod_pre":"RPkWTvUTojz",
            "cod_sub":"EItvwlgb26a",
            "edad":"ddss8QrmiMR",
            "uni_med":"vRQ94HGTxsj",
            "nacionalidad":"i93V8fvLtQW",
            "nombre_nacionalidad":"xJ5Uje4m2M1",
            "sexo":"NXBCl46WLd3",
            "cod_pais_o":"um91Wj3IB4z",
            "cod_dpto_o":"mfJBxCF4zvH",
            "cod_mun_o":"jNNfidWy6yk",
            "area":"iYrfkZY5KaG",
            "ocupacion":"Jr3qCoiwTbj",
            "tip_ss":"MzmDjKIdHG8",
            "cod_ase":"HlPjhEQ92aw",
            "per_etn":"wckLQjDbAcR",
            "gru_pob":"T5VnexDo929",
            "nom_grupo":"VNVwAdn1TYh",
            "estrato":"qbR2itzqrFC",
            "gp_discapa":"rdUQn2bx4hP",
            "gp_desplaz":"d40XuSY7DQh",
            "gp_migrant":"OYzIi0dBT4i",
            "gp_carcela":"LMLzR4ISsXE",
            "gp_gestan":"V77njQs0Jua",
            "sem_ges":"HPRwXPHfhjX",
            "gp_indigen":"gUtoI8ZLTiU",
            "gp_pobicfb":"nQu5qnxzIL0",
            "gp_mad_com":"X66c58eMCLC",
            "gp_desmovi":"ARkNuwOHTV9",
            "gp_psiquia":"AyEfB4QVJgU",
            "gp_vic_vio":"jWpEEq40E3G",
            "gp_otros":"vsRaAPw2rKe",
            "fuente":"kEaq4tmteQa",
            "cod_pais_r":"PUBLw7ypjrm",
            "cod_dpto_r":"g7HMcxqXAiM",
            "cod_mun_r":"orExDU5REx3",
            "cod_dpto_n":"oNENXZcp0JY",
            "cod_mun_n":"HyGIqbGq9gw",
            "fec_con":"Qs3BgnyYGG2",
            "ini_sin":"PUmD8xUEkYJ",
            "tip_cas":"pG1qqU91ibJ",
            "pac_hos":"SEp0OqjsG3g",
            "fec_hos":"nBF4J3DDNqa",
            "con_fin":"PUpcXLjgdBa",
            "fec_def":"QQ2BLLBMNly",
            "ajuste":"TCJU7MFyXry",
            "fecha_nto":"ymKfjFIGF8u",
            "cer_def":"EEIY1dokFHS",
            "cbmte":"MshkhS3ZovJ",
            "fec_arc_xl":"TM0WK4j5JEY",
            "fec_aju":"uBGxhVF0yov",
            "fm_fuerza":"EjFU6xs993L",
            "fm_unidad":"zdaELERaSpI",
            "fm_grado":"aisz0ySbGB9",
            "confirmados":"J482hfJ3oJQ",
            "consecutive_origen":"LybAgBKNnc4",
            "va_sispro":"gcJs9u9NOC8",
            "estado_final_de_caso":"NFBNx6cAQ5v",
            "nom_est_f_caso":"TPl9AGgSeSm",
            "nom_upgd":"RnxI7xkbTVG",
            "pais_ocurrencia":"hWn5OWB1Jyv",
            "nombre_evento":"WSmXi1jAYtf",
            "departamento_ocurrencia":"xtz6tECyh9L",
            "municipio_ocurrencia":"fg9cSZJzU6s",
            "pais_residencia":"g1DmfxlYAl9",
            "departamento_residencia":"fDNbxhi2gE7",
            "municipio_residencia":"VRDCvTTBYN7",
            "departamento_notificacion":"YVunC0p09HT",
            "municipio_notificacion":"LcMKlqotd2e"
        },
        "types": {
        "estrato": "integer_positive"
    }
        # o usa orgUnit_const / eventDate_const si aplican
    }

    USE_MODE: Literal["aggregated", "events"] = "events"

    if USE_MODE == "aggregated":
        #payloads = to_dhis2_dataValueSets(df, mapping_aggregated)
        if not payloads:
            print("⚠️ No se generaron payloads aggregated.")
        else:
            # Revisa un ejemplo antes de enviar
            print(json.dumps(payloads[0], ensure_ascii=False, indent=2)[:1000])
            # push_aggregated(payloads, batch_size=1, sleep_secs=0.0)  # normalmente 1 payload por (period, OU)
    else:  # events
        extractor = DataExtractor()
        events = extractor.to_dhis2_events(df, mapping_events)
        if not events:
            print("⚠️ No se generaron eventos.")
        else:
            # Vista previa de 2 eventos antes de enviar:
            print(json.dumps({"events": events[:2]}, ensure_ascii=False, indent=2)[:1000])
            extractor = DataExtractor()
            resumen = extractor.push_events_with_logging(
                events,
                batch_size=100,
                sleep_secs=3.0,
                dry_run=False,        # pon True para ensayo sin enviar
                verify=True,          # consulta algunos IDs creados
                verify_sample=1,
                log_dir="./logs",
                log_name_prefix="push_events",
                idscheme_params={"orgUnitIdScheme": "CODE"}
            )

            print("Resumen:", json.dumps(resumen, ensure_ascii=False, indent=2))
            #push_events(events, batch_size=10, sleep_secs=10.0)
    monitor.end()
    logger.info("Fin del proceso ETL Satisfactoriamente")

except Exception as e:
    logger.info(f"[ERROR] Error en la corrida ETL: {e}")


