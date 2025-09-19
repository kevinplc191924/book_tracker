"""
Book Tracker - General Schema v2
Author: Kevin Loachamín Caiza
Date: 2025-09-19
Description:
    Consolidated script containing all core components of the book tracking pipeline,
    including credential loading, ETL stages, metric computation, and CLI reporting.

Modules included:
    - load_credentials
    - elt.logger
    - elt.extract
    - elt.load
    - elt.transform
    - manipulation.summary
    - manipulation.generate_report
    - main

Usage:
    python general_schema_v2.py
"""


# --- load_credentials.py ---
import base64
import os

# Get the base64 string from the environment
encoded = os.getenv("BOOK_TRACKER_CREDS_B64")
if not encoded:
    raise ValueError("Missing BOOK_TRACKER_CREDS_B64 environment variable")

# Decode and write to file
with open("book_tracker_creds.json", "wb") as f:
    f.write(base64.b64decode(encoded))



# --- elt/logger.py ---
import logging

def get_logger(name: str) -> logging.Logger:
    """Set a logger instance."""
    
    # Retrieve a logger instance with the given name
    logger = logging.getLogger(name)
    
    # Prevent multiple handlers from being added to the same logger
    # Handler: decides where the logs go (console, file, etc.)
    if not logger.hasHandlers():
        # Create a handler to output logs to the console
        handler = logging.StreamHandler()
        # How the message will look like
        # Complete format: "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        formatter = logging.Formatter(
            '%(message)s'
        )
        # Set the format to the handler
        handler.setFormatter(formatter)
        # Add the handler to the logger
        logger.addHandler(handler)
        # Set the minimum severity level for messages to be logged
        logger.setLevel(logging.INFO)
    
    return logger

# --- elt/extract.py ---
import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

from elt.exceptions import ExtractionError
from elt.logger import get_logger

# Set the logger
logger = get_logger(__name__)

def extract() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Extracts data from a Google Sheets document and returns two DataFrames.

    This function connects to a Google Sheets file using service account credentials,
    retrieves data from two specific worksheets ("books" and "consolidate"), and returns
    them as pandas DataFrames. It handles authentication, sheet access, and logs the
    extraction process.

    Returns
    -------
    books_current : pd.DataFrame
        DataFrame containing records from the "books" worksheet (current format).
    consolidate : pd.DataFrame
        DataFrame containing records from the "consolidate" worksheet (previous format).

    Raises
    ------
    ExtractionError
        If authentication fails or if the specified worksheets cannot be accessed.
    """

    # File extraction from Google Sheets
    try:
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        creds = ServiceAccountCredentials.from_json_keyfile_name("book_tracker_creds.json", scope)
        client = gspread.authorize(creds)
        # Sheet retrieval
        sheets = client.open_by_key("1mRx4CClu1io5Ievu9b5PTJ6nIEDOFfl-oFgIv55Q37g")

    except Exception as e:
        logger.exception("Data extraction failed.")
        raise ExtractionError(f"{e}")

    # Getting books and consolidate sheets
    try:
        books_ws = sheets.worksheet("books")
        consolidate_ws = sheets.worksheet("consolidate")

        books_current = pd.DataFrame(books_ws.get_all_records()) # Current format
        consolidate = pd.DataFrame(consolidate_ws.get_all_records()) # Previous format

        return books_current, consolidate

    except Exception as e:
        logger.exception("Sheet access failed.")
        raise ExtractionError(f"{e}")


# --- elt/load.py ---
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


# --- elt/transform.py ---
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


# --- manipulation/summary.py ---
import pandas as pd

from elt.logger import get_logger

# Set logger
logger = get_logger(__name__)

def get_measures(
  transformed_books_current: pd.DataFrame,
  transformed_consolidate: pd.DataFrame,
  transformed_records: pd.DataFrame,
  year: int
) -> dict:

    """
    Computes summary metrics and extracts highlights from book tracking data.

    This function loads transformed datasets, validates the input year, and calculates
    various reading-related metrics including totals, averages, top-ranked books, and
    recent activity. It also identifies new entries based on record tracking.

    Parameters
    ----------
    transformed_books_current : pd.DataFrame
        Transformed book DataFrame.
    transformed_consolidate : pd.DataFrame
        Transformed consolidate DataFrame.
    transformed_records : pd.DataFrame
        Transformed records DataFrame.
    year : int
        The year to filter metrics by. If the year is outside the available range,
        it defaults to the most recent year in the dataset.

    Returns
    -------
    results : dict
        A dictionary containing the following keys:
        
        - 'overall_total' : int  
            Total number of completed books across all years.
        - 'total_current' : int  
            Number of completed books in the specified year.
        - 'ongoing' : int  
            Number of books currently being read in the specified year.
        - 'dropped' : int  
            Number of books marked as dropped in the specified year.
        - 'mean_pages_per_day' : float  
            Average reading rate across all completed books.
        - 'mean_time_reading' : float  
            Average reading duration across all completed books.
        - 'mean_pages_per_day_current' : float  
            Average reading rate for books completed in the specified year.
        - 'mean_time_reading_current' : float  
            Average reading duration for books completed in the specified year.
        - 'best' : pd.DataFrame  
            Top 3 highest-scoring books in the specified year.
        - 'last' : pd.DataFrame  
            Most recently completed book in the specified year.
        - 'days_since_last' : int  
            Number of days since the last completed book.
        - 'feedback_new' : str  
            Message indicating the number of new entries since the last run.
        - 'new_entries' : pd.DataFrame  
            DataFrame of newly added books, if any.

    Raises
    ------
    ValueError
        If the input year is not an integer.
    TransformationError
        If data loading or transformation fails.
    """

    # Parameter validation
    min_year_available = int(transformed_books_current["year"].min())
    max_year_available = int(transformed_books_current["year"].max())

    if not isinstance(year, int):
        logger.exception("Invalid parameter format: year")
        raise ValueError("The year should be a positive integer.")
    
    if year < min_year_available or year > max_year_available:
        logger.info("The year selected is not available in the dataset.")
        logger.info("The year should span from 2024 to the current year ideally.")
        logger.info(f"The year to filter will be set to the maximum value available: {max_year_available}")
        print()
        year = max_year_available
    
    ## Summary measures ##

    # Counts
    overall_total = (
        transformed_consolidate.shape[0] + transformed_books_current[transformed_books_current["status"] == "Completed"].shape[0]
    )
    total_current = transformed_books_current.query(f'status == "Completed" & year == {year}').shape[0]
    ongoing = transformed_books_current.query('status == "Ongoing"').shape[0]
    dropped = transformed_books_current.query('status == "Dropped"').shape[0]

    # Averages
    mean_pages_per_day = transformed_books_current["pages_per_day"].dropna().mean().round(2)
    mean_time_reading = transformed_books_current["days"].dropna().mean().round(2)
    # Current year subset for the following measures
    sub = transformed_books_current.query(f"year == {year}")
    mean_pages_per_day_current = sub["pages_per_day"].dropna().mean().round(2)
    mean_time_reading_current = sub["days"].dropna().mean().round(2)

    # Top-3 best ranked books in the current year
    best = sub.nlargest(3, "score")[["book_name", "author", "score"]].reset_index(drop=True)  # Include ties

    # Last complete reading
    last = sub.loc[
      sub["status"] == "Completed",
      ["book_name", "author", "score", "end_date"]
    ].sort_values(by="end_date").tail(1).reset_index(drop=True)
    
    days_since_last = (pd.to_datetime("today") - last["end_date"].iloc[0]).days

    # New entries
    if len(transformed_records) >= 2:
        diff = transformed_records["records_current"].iloc[-1] - transformed_records["records_current"].iloc[-2]
        format_last_date = \
        last["end_date"].iloc[-1].strftime("%Y-%m-%d")
        feedback_new = f"New entries since {format_last_date}: {diff}."
        new_entries = transformed_books_current.tail(diff)[["book_name", "author"]].reset_index(drop=True)
    else:
        feedback_new = "No new entries to show." # Only for the first execution or no new entries case
        new_entries = pd.DataFrame(columns=["book_name", "author"])  

    # Dictionary to store results
    results = {
        "overall_total": overall_total,
        "total_current": total_current,
        "ongoing": ongoing,
        "dropped": dropped,
        "mean_pages_per_day": mean_pages_per_day,
        "mean_time_reading": mean_time_reading,
        "mean_pages_per_day_current": mean_pages_per_day_current,
        "mean_time_reading_current": mean_time_reading_current,
        "best": best,
        "last": last,
        "days_since_last": days_since_last,
        "feedback_new": feedback_new,
        "new_entries": new_entries
    }

    return results


# --- manipulation/generate_report.py ---
from rich.console import Console
from rich.table import Table

def report(results: dict):
    """
    Displays a formatted reading report using CLI tables.

    This function prints a summary of reading metrics, top-ranked books,
    the most recently completed book, and newly added entries using `rich.Table`.

    Parameters
    ----------
    results : dict
        Dictionary containing reading metrics and DataFrames, including:
        
        - 'overall_total', 'total_current', 'ongoing', 'dropped'
        - 'mean_pages_per_day', 'mean_time_reading'
        - 'mean_pages_per_day_current', 'mean_time_reading_current'
        - 'days_since_last'
        - 'best', 'last', 'new_entries', `feedback_new`

    Returns
    -------
    None
        Output is printed directly to the console.
    """
    
    console = Console()

    # Main summary table
    summary_table = Table(title="Reading Report")
    summary_table.add_column("Metric", style="cyan", no_wrap=True)
    summary_table.add_column("Value", style="magenta")

    metrics = [
        ("Total books read", results["overall_total"]),
        ("Books completed this year", results["total_current"]),
        ("Currently reading", results["ongoing"]),
        ("Books dropped", results["dropped"]),
        ("Avg pages/day (overall)", results["mean_pages_per_day"]),
        ("Avg days/book (overall)", results["mean_time_reading"]),
        ("Avg pages/day (this year)", results["mean_pages_per_day_current"]),
        ("Avg days/book (this year)", results["mean_time_reading_current"]),
        ("Days since last book finished", results["days_since_last"])
    ]
    for label, value in metrics:
        summary_table.add_row(label, str(value))

    console.print(summary_table)

    # Top-3 Best Ranked Books
    console.print()
    title_best = "Top-3 Best Ranked Books This Year"
    df_best = results["best"]
    best_table = Table(show_header=True, header_style="bold green", title=title_best)
    for col in df_best.columns:
        best_table.add_column(col.title().replace("_", " "), style="white")
    for row in df_best.itertuples(index=False):
        # Make sure values are strings
        best_table.add_row(*[str(cell) for cell in row])
    console.print(best_table)

    # Last Book Read
    console.print()
    title_last = "Last Book Read"
    df_last = results["last"].copy()
    df_last["end_date"] = df_last["end_date"].dt.strftime("%Y-%m-%d") # Date format to display
    last_table = Table(show_header=True, header_style="bold blue", title=title_last)
    for col in df_last.columns:
        last_table.add_column(col.title().replace("_", " "), style="white")
    for row in df_last.itertuples(index=False):
        last_table.add_row(*[str(cell) for cell in row])
    console.print(last_table)

    # New additions
    console.print()
    title_new = "New Book Additions"
    df_new = results["new_entries"]

    if df_new.empty:
        new_table = Table(title=title_new)
        new_table.add_row(f"[italic]{results['feedback_new']}[/italic]")
        console.print(new_table)
        console.print()
    else:
        new_table = Table(show_header=True, header_style="bold red", title=title_new)
        for col in df_new.columns:
            new_table.add_column(col.title().replace("_", " "), style="white")
        for row in df_new.itertuples(index=False):
            new_table.add_row(*[str(cell) for cell in row])
        console.print(new_table)
        console.print(results["feedback_new"])
        console.print()


# --- main.py ---
from elt.extract import extract
from elt.exceptions import ExtractionError, LoadError, TransformationError
from elt.logger import get_logger
from manipulation.summary import get_measures
from elt.load import load
from manipulation.generate_report import report
from elt.transform import transform

# Set logger
logger = get_logger(__name__)

### Parameter Definition ###

# Load
directory = ".datasets/"
save_df_load = False # Same as transform

# Transform
save_df_transform = False # Same as load

# Get measures
year = 2025


def main():
    try:
        # Extraction
        logger.info("Starting extraction...")
        raw_books_current, raw_consolidate = extract()

        # Loading
        logger.info("Loading data...")
        load(
            directory=directory,
            raw_books_current= raw_books_current,
            raw_consolidate= raw_consolidate,
            save_df=save_df_load
        )

        # Transformation
        logger.info("Transforming data...")
        transformed_books_current, transformed_consolidate, transformed_records = transform(
            directory=directory,
            raw_books_current = raw_books_current,
            raw_consolidate = raw_consolidate,
            save_df=save_df_transform
        )

        # Summary and report
        logger.info("Getting summary and creating report...\n")
        results = get_measures(
            transformed_books_current=transformed_books_current,
            transformed_consolidate=transformed_consolidate,
            transformed_records=transformed_records,
            year=year
        )

        report(results)
    
    except ExtractionError as e:
        logger.error(f"Pipeline stopped due to extraction error: {e}")
    except ValueError as e:
        logger.error(f"Pipeline stopped due to parameter issue in load: {e}")
    except LoadError as e:
        logger.error(f"Pipeline stopped due to loading error: {e}")
    except TransformationError as e:
        logger.error(f"Pipeline stopped due to transformation error: {e}")
    except Exception as e:
        logger.error(f"Error: {e}")


if __name__ == "__main__":
    main()


