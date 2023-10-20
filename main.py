import sys
import logging

# Append 'src' and 'utils' directories to PYTHONPATH
sys.path.append("./src")
sys.path.append("./utils")

from src.raw_processed_db import run_raw_processing
from src.knn_data_v1 import main_logic

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="project.log",
)  # Log to a file; remove this to log to console


def main():
    logging.info("Main function started.")
    logging.info("Starting preprocessing")
    run_raw_processing()
    logging.info("Preprocessing COMPLETED")
    logging.info("Starting KNN data preparacion")
    main_logic()
    logging.info("KNN data is ready for training")


if __name__ == "__main__":
    main()
