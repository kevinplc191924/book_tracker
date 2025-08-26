import pandas as pd
from pathlib import Path

"""
# Structure
- book_tracker/
  - database/
    - books.csv
    - categorical_vars.csv
    - consolidate.csv
    - textbooks.csv
  - elt/
    - extract.py
    - load.py
    - transform.py
  - manipulation/
    - generate_report.py
    - summary.py
  main.py
"""

# extract.py
def extract():
    """Data extraction from the origin"""

    # Windows/OneDrive path
    path = Path(r"C:\Users\DESKTOP_A\OneDrive\books_database_complete.xlsx")
    excel_file = pd.ExcelFile(path)

    # Create df for comparison: current
    books_current = pd.read_excel(excel_file, sheet_name="books")

    # Stored df: previous books file that serves as a baseline
    books_previous = pd.read_csv("./database/books.csv")

    # No new additions case
    diff = len(books_current) - len(books_previous)
    if diff == 0:
        return None

    # Identify new entries
    new_entries = books_current.tail(diff)

    return new_entries


# load.py
def load():
    """Add new entries to books.csv"""

    new_entries = extract()

    if new_entries is None:
        print("The database has not been updated recently.")
    else:
        new_entries.to_csv("./database/books.csv", mode="a", index=False, header=False)

        print("Added:")
        for name, author in zip(new_entries["book_name"].tolist(), new_entries["author"].tolist()):
            print(f" ➺  {name} by {author}")
        print("\n")


# transform.py
def transform():
    """Prepare the data for further analysis."""

    books = pd.read_csv("./database/books.csv", parse_dates=["start_date", "end_date"])
    consolidate = pd.read_csv("./database/consolidate.csv")

    books["days"] = (books["end_date"] - books["start_date"]).dt.days
    books["pages_per_day"] = (books["total_pages"] / books["days"]).round(2)

    return books, consolidate


# generate_report.py
def report(*args):
    text = f"""
- So far, you have read {args[0]} books.
- This year you have completed {args[1]} books. You are currently reading {args[2]} books,
  and {args[3]} books were dropped.
- Per day, you generally read an average of {args[4]} pages, and it takes you {args[5]} days to finish a book.
- However, this year you have read {args[6]} pages per day, and complete a book in {args[7]} days.
- There have been {args[10]} days since your last book completed.
    """
    print(text)

    print("\n")
    print("Top-3 best ranked books this year:")
    print(args[8])

    print("\n")
    print("Last book read:")
    print(args[9])


# summary.py
def get_measures(year=2025):
    books, consolidate = transform()

    # Counts
    overall_total = consolidate.shape[0] + books[books["status"] == "Completed"].shape[0]
    total_current = books.query(f'status == "Completed" & year == {year}').shape[0]
    ongoing = books.query('status == "Ongoing"').shape[0]
    dropped = books.query('status == "Dropped"').shape[0]

    # Averages
    mean_pages_per_day = books["pages_per_day"].dropna().mean().round(2)
    mean_time_reading = books["days"].dropna().mean().round(2)
    sub = books.query(f"year == {year}")
    mean_pages_per_day_current = sub["pages_per_day"].dropna().mean().round(2)
    mean_time_reading_current = sub["days"].dropna().mean().round(2)

    # Top-3 best ranked books in the current year
    best = sub.nlargest(3, "score")["book_name", "author", "score"]

    # Last complete reading
    last = sub.loc[sub["status"] == "Completed", ["book_name", "author", "score", "end_date"]].tail(1)
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


# main.py
def main():
    extract()
    load()
    transform()
    report(*get_measures())


if __name__ == "__main__":
    main()
