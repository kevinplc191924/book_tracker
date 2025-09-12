import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from elt.exceptions import ExtractionError
from elt.logger import get_logger

# Set the logger
logger = get_logger(__name__)

def extract() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Data extraction from the origin.
    """
    # File extraction from Google Sheets
    try:
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        creds = ServiceAccountCredentials.from_json_keyfile_name("book_tracker_creds.json", scope)
        client = gspread.authorize(creds)

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
