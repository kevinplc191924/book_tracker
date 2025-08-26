import pandas as pd
from elt.transform import transform


def get_measures(year=2025):
    # Get the datasets needed
    books, consolidate = transform()

    ## Summary measures ##

    # Counts
    overall_total = (
        consolidate.shape[0] + books[books["status"] == "Completed"].shape[0]
    )
    total_current = books.query(f'status == "Completed" & year == {year}').shape[0]
    ongoing = books.query('status == "Ongoing"').shape[0]
    dropped = books.query('status == "Dropped"').shape[0]

    # Averages
    mean_pages_per_day = books["pages_per_day"].dropna().mean().round(2)
    mean_time_reading = books["days"].dropna().mean().round(2)
    sub = books.query(
        f"year == {year}"
    )  # Current year subset for the following measures
    mean_pages_per_day_current = sub["pages_per_day"].dropna().mean().round(2)
    mean_time_reading_current = sub["days"].dropna().mean().round(2)

    # Top-3 best ranked books in the current year
    best = sub.nlargest(3, "score")[["book_name", "author", "score"]].reset_index(
        drop=True
    )  # Include ties

    # Last complete reading
    last = (
        sub.loc[
            sub["status"] == "Completed", ["book_name", "author", "score", "end_date"]
        ]
        .tail(1)
        .reset_index(drop=True)
    )
    days_since_last = (pd.to_datetime("today") - last["end_date"].iloc[0]).days

    return (
        overall_total,
        total_current,
        ongoing,
        dropped,
        mean_pages_per_day,
        mean_time_reading,
        mean_pages_per_day_current,
        mean_time_reading_current,
        best,
        last,
        days_since_last,
    )
