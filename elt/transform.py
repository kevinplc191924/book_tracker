import os

import pandas as pd

from elt.extract import extract
from elt.logger import get_logger
from elt.exceptions import TransformationError

# Set logger
logger = get_logger(__name__)

def transform(directory: str, save_df: bool = False) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Transforms raw book tracking data into enriched DataFrames for metric analysis.

    This function loads 'books' and 'consolidate' data either from saved CSV files or
    directly from the source via `extract()`. It parses relevant date columns, computes
    reading duration and reading rate metrics, and returns the processed datasets, along
    with the records tracking.

    Parameters
    ----------
    directory : str
        Path to the directory containing the CSV files or where data will be saved.
    save_df : bool, optional
        Set True if the same parameter in the load function is also True. By default is False
        to handle data directly with the extract function and avoid non-existing file issues.

    Returns
    -------
    books : pandas.DataFrame
        DataFrame containing enriched book records with time-based metrics.
    consolidate : pandas.DataFrame
        DataFrame containing historical book data in a previous format.
    records : pandas.DataFrame
        DataFrame tracking the number of records of books over time.

    Raises
    ------
    TransformationError
        If any file loading, extraction, or transformation step fails.
    """

    if save_df:
        try:
            # Get books (parse dates) and consolidate datasets
            books = pd.read_csv(os.path.join(directory, "books.csv"), parse_dates=["start_date", "end_date"])
            consolidate = pd.read_csv(os.path.join(directory, "consolidate.csv"))

            # Get records track
            records = pd.read_csv(os.path.join(directory, "records.csv"), parse_dates=["date"])

        except Exception as e:
            logger.exception("Data transformation failed.")
            raise TransformationError(f"{e}")
    
    else:
        try:
            # Load books and consolidate using extract()
            books, consolidate = extract()
            
            # Parse dates
            for col in ["start_date", "end_date"]:
                books[col] = pd.to_datetime(books[col])
            
            # Get records track
            records = pd.read_csv(os.path.join(directory, "records.csv"), parse_dates=["date"])
        
        except Exception as e:
            logger.exception("Data transformation failed.")
            raise TransformationError(f"{e}")

    # Time-related columns
    books["days"] = (books["end_date"] - books["start_date"]).dt.days
    books["pages_per_day"] = (books["total_pages"] / books["days"]).round(2)
    logger.info("Data sucessfully transformed.")

    return books, consolidate, records
