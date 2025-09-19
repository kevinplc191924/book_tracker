import os

import pandas as pd

from elt.logger import get_logger
from elt.exceptions import LoadError

# Set logger
logger = get_logger(__name__)

# Avoid duplicates in records
def log_record_if_new(
    directory: str,
    today: pd.Timestamp,
    current_count: int
):
    """
    Append a new record to `raw_records.csv` if the last entry does not match the current count.

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
        If `raw_records.csv` does not exist in the specified directory.
    """

    file_path = os.path.join(directory, "raw_records.csv")
    new_record = pd.DataFrame({"date": [today], "records_current": [current_count]})
    
    # Check if the last entry is equal to the current
    existing = pd.read_csv(file_path)
    if existing.empty or int(existing["records_current"].iloc[-1]) != current_count:
        new_record.to_csv(file_path, mode="a", header=False, index=False)


def load(
    directory: str,
    raw_books_current: pd.DataFrame,
    raw_consolidate: pd.DataFrame,
    save_df: bool = False
):

    """
    Load extracted data from the origin and optionally save it to disk, logging record counts
    as `raw_records.csv`.

    Parameters
    ----------
    directory : str
        Path to the directory where extracted data and logs will be saved.
    raw_books_current : pd.DataFrame
        Book database with the current format (from extract).
    raw_consolidate : pd.DataFrame
        Book database with the previous format (from extract).
    save_df : bool, optional
        Whether to save the extracted DataFrames (`raw_books_current.csv` and `raw_consolidate.csv`),
        by default False.

    Raises
    ------
    ValueError
        If `directory` is not a string
        If any DataFrame is empty 
        If `save_df` is not a boolean
    
    LoadError
        If saving data or logging records fails.
    """

    # Validate parameter types
    if not isinstance(directory, str):
        logger.exception("Invalid parameter format: directory")
        raise ValueError("A valid directory is a string.") 
    if not isinstance(save_df, bool):
        logger.exception("Invalid parameter format: save_df")
        raise ValueError("The parameter to save the DataFrame must be a bool.")

    # Validate directory existence
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # Validate dataframes are not empty
    if raw_books_current.empty:
        logger.exception("Empty DataFrame.")
        raise ValueError("Empty DataFrame: books_current. Can't proceed.")
    if raw_consolidate.empty:
        logger.exception("Empty DataFrame.")
        raise ValueError("Empty DataFrame: consolidate. Can't proceed.")
    
    # Create an empty records.csv in the provided directory (only in the first call)
    file_path = os.path.join(directory, "raw_records.csv")
    if not os.path.exists(file_path):
        empty_csv = pd.DataFrame({"date": [], "records_current": []})
        empty_csv.to_csv(file_path, index=False)

    # Tracking
    today = pd.Timestamp.now().isoformat() # To add in the record tracking
    current_count = raw_books_current.shape[0] # To compare entries

    # Save dataframes if needed
    if save_df:
        try:
            raw_books_current.to_csv(os.path.join(directory, "raw_books_current.csv"), index=False)
            raw_consolidate.to_csv(os.path.join(directory, "raw_consolidate.csv"), index=False)

        except Exception as e:
            logger.exception("Loading process failed: raw book DataFrames.")
            raise LoadError(f"{e}")
    
    # Get only new entries and dates
    try:
        log_record_if_new(directory, today, current_count)

    except Exception as e:
        logger.exception("Loading process failed: records tracking.")
        raise LoadError(f"{e}")
