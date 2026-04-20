import os
from utils import load_config, setup_logger
from extract import extract_zip
from transform import transform_data
from load import save_to_csv

def run_pipeline():
    # Change to project root directory
    os.chdir(os.path.dirname(os.path.dirname(__file__)))
    
    config = load_config()

    logger = setup_logger(
        config["logging"]["log_file"],
        config["logging"]["level"]
    )

    logger.info("ETL pipeline started")

    extract_zip(
        config["paths"]["zip_path"],
        config["paths"]["extract_path"],
        logger
    )

    matches, deliveries = transform_data(
        config["paths"]["extract_path"],
        logger
    )

    save_to_csv(
        matches,
        deliveries,
        config["paths"]["processed_path"],
        config["output"]["matches_file"],
        config["output"]["deliveries_file"],
        logger
    )

    logger.info("ETL pipeline finished")

if __name__ == "__main__":
    run_pipeline()