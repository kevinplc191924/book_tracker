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

        logger.info("Data successfully extracted.")
        return books_current, consolidate

    except Exception as e:
        logger.exception("Sheet access failed.")
        raise ExtractionError(f"{e}")
