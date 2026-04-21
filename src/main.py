import os
from src.utils import load_config, setup_logger
from src.extract import extract_zip
from src.transform import transform_data
from src.load import save_to_csv

from src.auth.models import create_tables, seed_data
from src.auth.auth_service import create_user
from src.auth.rbac import has_permission


def run_pipeline(user_id=1):
    # Change to project root directory
    os.chdir(os.path.dirname(os.path.dirname(__file__)))

    # Initialize RBAC
    create_tables()
    seed_data()

    # Create test user (safe due to check)
    create_user("admin_user", "1234", "admin")

    # Permission check
    if not has_permission(user_id, "run_pipeline"):
        raise Exception("Access Denied")

    config = load_config()

    logger = setup_logger(
        config["logging"]["log_file"],
        config["logging"]["level"]
    )

    logger.info(f"User {user_id} triggered ETL pipeline")

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