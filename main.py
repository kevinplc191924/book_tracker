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
save_df_load = False

# Transform
save_df_transform = False

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
