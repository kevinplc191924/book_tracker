from elt.extract import extract
from elt.load import load
from elt.transform import transform
from manipulation.generate_report import report
from manipulation.summary import get_measures


def main():
    extract()
    load()
    transform()
    report(*get_measures())


if __name__ == "__main__":
    main()
