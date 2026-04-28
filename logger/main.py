from logger_printer import LoggerPrinter
import time
import os



config = {
    'bootstrap.servers': os.getenv("KAFKA_URI"),
    'group.id': 'gatekeeper',
    'auto.offset.reset': 'earliest'#,
    # "auto.create.topics.enable": True
        }


if __name__ == "__main__":
    time.sleep(15)
    cons = LoggerPrinter(config)
    cons.consum_and_print_logs()