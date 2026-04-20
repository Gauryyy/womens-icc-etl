import pandas as pd
import os

def save_to_csv(matches, deliveries, output_path, matches_file, deliveries_file, logger):
    os.makedirs(output_path, exist_ok=True)

    matches_df = pd.DataFrame(matches)
    deliveries_df = pd.DataFrame(deliveries)

    matches_df.to_csv(os.path.join(output_path, matches_file), index=False)
    deliveries_df.to_csv(os.path.join(output_path, deliveries_file), index=False)

    logger.info("Data saved successfully")