import yaml
import logging
import os

def load_config(path="config/config.yaml"):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def setup_logger(log_file, level="INFO"):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    logging.basicConfig(
        filename=log_file,
        level=getattr(logging, level),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

    return logging.getLogger()