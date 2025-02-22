import logging

logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def log_event(message):
    logging.info(message)

log_event("Data preprocessing completed successfully.")
