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
        - 'new_entries' : pandas.DataFrame  
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
        logger.info("The year to filter will be set to the maximum value available: {max_year_available}")
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
    ].tail(1).reset_index(drop=True)
    
    days_since_last = (pd.to_datetime("today") - last["end_date"].iloc[0]).days

    # New entries
    if len(transformed_records) >= 2:
        diff = transformed_records["records_current"].iloc[-1] - transformed_records["records_current"].iloc[-2]
        feedback_new = f"New entries since {transformed_records.index.iloc[-2]}: {diff}."
        new_entries = transformed_books_current.tail(diff)[["book_name", "author"]].reset_index(drop=True)
    else:
        feedback_new = "No new entries to show." # Only for the first execution  

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
