import os

import numpy as np
import pandas as pd

from elt.logger import get_logger
from elt.exceptions import TransformationError

# Set logger
logger = get_logger(__name__)

def transform(
    directory: str,
    raw_books_current: pd.DataFrame,
    raw_consolidate: pd.DataFrame,
    save_df: bool = False
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    """
    Transforms raw book tracking data into enriched DataFrames for metric analysis.

    This function gets 'raw_books_current' and 'raw_consolidate' data either from saved CSV files or
    directly from the source via `extract()` output. It parses relevant date columns, computes
    reading duration and reading rate metrics, and returns the processed datasets, along
    with the records tracking with parsed dates in `transformed_records.csv`.

    Parameters
    ----------
    directory : str
        Path to the directory containing the CSV files.
    raw_books_current : pd.DataFrame
        Book database with the current format (from extract).
    raw_consolidate : pd.DataFrame
        Book database with the previous format (from extract).
    save_df : bool, optional
        Set True if the same parameter in `load()` is also True. By default is False
        to handle data directly with the `extract()` output and avoid non-existing file issues.

    Returns
    -------
    transformed_books_current : pandas.DataFrame
        DataFrame containing enriched book records with time-based metrics.
    transformed_consolidate : pandas.DataFrame
        DataFrame containing historical book data in a previous format.
    transformed_records : pandas.DataFrame
        DataFrame tracking the number of records of books over time.

    Raises
    ------
    TransformationError
        If any transformation step fails.
    """

    if save_df:
        try:
            # Transform: raw_books_current

            # Parse dates
            transformed_books_current = pd.read_csv(
                os.path.join(directory, "raw_books_current.csv"),
                parse_dates=["start_date", "end_date"]
            )

            # Time-related columns
            transformed_books_current["days"] = \
            (transformed_books_current["end_date"] - transformed_books_current["start_date"]).dt.days
            transformed_books_current["pages_per_day"] = \
            (transformed_books_current["total_pages"] / transformed_books_current["days"]).round(2)
            
            # Transform: raw_consolidate

            # Get the data from CSV
            transformed_consolidate = pd.read_csv(
                os.path.join(directory, "raw_consolidate.csv")
            )

            # Transform: raw_records

            # Get the data from CSV and parse dates
            transformed_records = pd.read_csv(
                os.path.join(directory, "raw_records.csv"),
                parse_dates=["date"]
            )

            return transformed_books_current, transformed_consolidate, transformed_records

        except Exception as e:
            logger.exception("Data transformation failed.")
            raise TransformationError(f"{e}")
    
    else:
        try:
            # Transform: raw_books_current
            
            # Parse dates
            transformed_books_current = raw_books_current.copy()
            # Replace empties with nulls
            transformed_books_current["end_date"] = transformed_books_current["end_date"].replace("", np.nan)
            for col in ["start_date", "end_date"]:
                transformed_books_current[col] = pd.to_datetime(transformed_books_current[col])

            # Force column dtype: score must be float
            transformed_books_current["score"] = pd.to_numeric(transformed_books_current["score"], errors="coerce")
            
            # Time-related columns
            transformed_books_current["days"] = \
            (transformed_books_current["end_date"] - transformed_books_current["start_date"]).dt.days
            transformed_books_current["pages_per_day"] = \
            (transformed_books_current["total_pages"] / transformed_books_current["days"]).round(2)
            
            # Transform: raw_consolidate (skipped: no transformation needed)
            
            # Transform: raw_records

            # Get data from CSV and parse dates
            transformed_records = pd.read_csv(
                os.path.join(directory, "raw_records.csv"),
                parse_dates=["date"]
            )

            return transformed_books_current, raw_consolidate, transformed_records
        
        except Exception as e:
            logger.exception("Data transformation failed.")
            raise TransformationError(f"{e}")
