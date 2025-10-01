import pandas as pd
import xml.etree.ElementTree as ET

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

