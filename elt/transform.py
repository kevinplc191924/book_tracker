import pandas as pd


def transform():
    """Prepare the data for further analysis."""

    # Load updated data, parse dates
    books = pd.read_csv("./database/books.csv", parse_dates=["start_date", "end_date"])

    # Load consolidate data
    consolidate = pd.read_csv("./database/consolidate.csv")

    # Time-related columns
    books["days"] = (books["end_date"] - books["start_date"]).dt.days
    books["pages_per_day"] = (books["total_pages"] / books["days"]).round(2)

    return books, consolidate
