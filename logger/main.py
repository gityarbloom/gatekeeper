from logger_printer import LoggerPrinter
import os



config = {
    'bootstrap.servers': os.getenv("KAFKA_URI"),
    'auto.offset.reset': 'earliest'
        }


if __name__ == "__main__":

    cons = LoggerPrinter(config)
    cons.consum_and_print_logs()