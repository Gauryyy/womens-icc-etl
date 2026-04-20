import zipfile
import os

def extract_zip(zip_path, extract_path, logger):
    os.makedirs(extract_path, exist_ok=True)

    logger.info(f"Extracting {zip_path}...")

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_path)

    logger.info("Extraction complete")