import logging
import logging.handlers
import sys
from pathlib import Path

def get_root_dir():
    if getattr(sys, 'frozen', False):
        return Path(sys.executable).parent.resolve()
    else:
        return Path(__file__).parent.parent.parent.resolve()

def configurar_logger():
    ROOT_DIR = get_root_dir()
    LOG_DIR = ROOT_DIR / "logs"
    LOG_DIR.mkdir(exist_ok=True)

    log_file_path = LOG_DIR / "integrador.log"
    
    log_formatter = logging.Formatter(
        fmt='%(asctime)s - [%(levelname)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    handler_rotativo = logging.handlers.TimedRotatingFileHandler(
        filename=log_file_path,
        when='midnight',      # Rotaciona todo dia à meia-noite
        interval=1,           # A cada 1 (dia, baseado no 'when')
        backupCount=7,        # Mantém os últimos 7 arquivos de log
        encoding='utf-8'
    )
    handler_rotativo.setFormatter(log_formatter)
    handler_rotativo.setLevel(logging.INFO)
    logger.addHandler(handler_rotativo)
    logging.getLogger("httpx").setLevel(logging.WARNING)  # Evita logs verbosos do httpx

    if sys.stdout:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(log_formatter)
        stream_handler.setLevel(logging.INFO)
        logger.addHandler(stream_handler)