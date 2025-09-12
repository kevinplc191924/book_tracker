from elt.extract import extract
from elt.exceptions import LoadError
from elt.logger import get_logger
import os
import pandas as pd

# Set logger
logger = get_logger(__name__)

import os
import pandas as pd

# Avoid duplicates in records
def log_record_if_new(directory: str, today: pd.Timestamp, current_count: int):
    """
    Append a new record to `records.csv` if the last entry does not match the current count.

    Parameters
    ----------
    directory : str
        Path to the directory containing `records.csv`.
    today : pd.Timestamp
        Timestamp representing the current extraction time.
    current_count : int
        Number of records extracted in the current run.

    Raises
    ------
    FileNotFoundError
        If `records.csv` does not exist in the specified directory.
    """

    file_path = os.path.join(directory, "records.csv")
    new_record = pd.DataFrame({"date": [today], "records_current": [current_count]})
    
    # Check if the last entry is equal to the current
    existing = pd.read_csv(file_path)
    if existing.empty or int(existing["records_current"].iloc[-1]) != current_count:
        new_record.to_csv(file_path, mode="a", header=False, index=False)


def load(directory: str, save_df: bool = False):
    """
    Extract data from the origin and optionally save it to disk, logging record counts.

    Parameters
    ----------
    directory : str
        Path to the directory where extracted data and logs will be saved.
    save_df : bool, optional
        Whether to save the extracted DataFrames (`books_current.csv` and `consolidate.csv`),
        by default False.

    Raises
    ------
    ValueError
        If `directory` is not a string or `save_df` is not a boolean.
    LoadError
        If saving data or logging records fails.
    """

    # Validate parameter types
    if not isinstance(directory, str):
        logger.exception("Invalid parameter format: directory")
        raise ValueError("A valid directory is a string.") 
    if not isinstance(save_df, bool):
        logger.exception("Invalid parameter format: save_df")
        raise ValueError("The save data frame parameter must be a bool.")

    # Validate directory existence
    if not os.path.exists(directory):
        os.makedirs(directory)
        logger.info(f"Created directory: {directory}")
    
    # Create an empty records.csv in the provided directory (only in the first call)
    file_path = os.path.join(directory, "records.csv")
    if not os.path.exists(file_path):
        empty_csv = pd.DataFrame({"date": [], "records_current": []})
        empty_csv.to_csv(file_path, index=False)

    # Extraction
    books_current, consolidate = extract()
    today = pd.Timestamp.now().isoformat() # To add in the record tracking
    current_count = books_current.shape[0] # To compare entries

    # Save dataframes if needed
    if save_df:
        try:
            books_current.to_csv(os.path.join(directory, "books_current.csv"), index=False)
            consolidate.to_csv(os.path.join(directory, "consolidate.csv"), index=False)

        except Exception as e:
            logger.exception("Data loading process failed.")
            raise LoadError(f"{e}")
    
    # Get only new entries and dates
    try:
        log_record_if_new(directory, today, current_count)
        logger.info("Successfull loading process.")
    
    except Exception as e:
        logger.exception("Data loading process failed.")
        raise LoadError(f"{e}")
