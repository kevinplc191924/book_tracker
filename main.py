from elt.extract import extract
from elt.logger import get_logger
from manipulation.summary import get_measures
from elt.load import load
from manipulation.generate_report import report
from elt.transform import transform

# Set logger
logger = get_logger(__name__)

# Define parameters

# load()
directory = ".datasets/"
save_df = False

# get_measures()
year = 2025

def main():
    try:
        # Extraction
        logger.info("Starting extraction...")
        books_current, consolidate = extract()

        # Loading
        logger.info("Loading data...")
        load(
            directory=directory,
            books_current= books_current,
            consolidate= consolidate,
            save_df=save_df
        )

        # Transformation
        logger.info("Transforming data...")
        books, consolidate, records = transform(directory=directory, save_df=save_df)

        # Summary and report
        logger.info("Getting summary and creating report...")
        results = get_measures(
            books=books,
            consolidate=consolidate,
            records=records,
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
