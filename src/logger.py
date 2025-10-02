import logging


class LoggerETL:
    def __init__(self, monitor=None, log_file: str = "..\\logs\\etl.log"):
        logging.basicConfig(
            filename=log_file,
            filemode="a",
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s"
        )
        self.logger = logging.getLogger("ETL")
        self.monitor = monitor

    def info(self, mensaje: str):
        self.logger.info(mensaje)
        print(f"{mensaje}")
        if mensaje.startswith("[ERROR]") and self.monitor:
            self.monitor.set_metrics(0, 0, 0)
            self.monitor.end(error=mensaje)