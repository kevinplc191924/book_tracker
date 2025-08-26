import pandas as pd
from pathlib import Path

def extract():
    """Data extraction from the origin"""

    ## Data extraction from online Excel file: up-to-date file ##

    # Windows/OneDrive path
    # Make sure the path is properly read with Path
    path = Path(r"C:\Users\DESKTOP_A\OneDrive\books_database_complete.xlsx")
    excel_file = pd.ExcelFile(path)  # Multiple sheets

    ## Preparing datasets for comparison ##

    # Create df for comparison: current
    books_current = pd.read_excel(excel_file, sheet_name="books")

    # Stored df: previous books file that serves as a baseline
    books_previous = pd.read_csv("./database/books.csv")

    # No new additions case
    diff = len(books_current) - len(books_previous)
    if diff == 0:
        return None

    ## Identify new entries ##
    new_entries = books_current.tail(diff)

    return new_entries
