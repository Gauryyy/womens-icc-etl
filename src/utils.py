import yaml
import logging
import os

# ✅ KEEP THIS (DO NOT REMOVE)
def load_config(path="config/config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

# ✅ UPDATED LOGGER (console + file)
def setup_logger(log_file, level="INFO"):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level))

    # Prevent duplicate logs
    if logger.hasHandlers():
        logger.handlers.clear()

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger