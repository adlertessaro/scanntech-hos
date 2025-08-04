from datetime import date
from pathlib import Path
import sys

def configurar_saida_terminal():
    log_dir = Path(__file__).parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"monitor_{date.today().strftime('%Y-%m-%d')}.log"

    class LoggerWriter:
        def __init__(self, stream):
            self.stream = stream
            self.log_file = open(log_file, "a", encoding="utf-8")

        def write(self, message):
            self.stream.write(message)
            self.log_file.write(message)
            self.log_file.flush()

        def flush(self):
            self.stream.flush()
            self.log_file.flush()

    sys.stdout = LoggerWriter(sys.stdout)
    sys.stderr = LoggerWriter(sys.stderr)
