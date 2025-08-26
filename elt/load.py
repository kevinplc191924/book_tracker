from elt.extract import extract


def load():
    """Add new entries to books.csv"""

    # Get the new entries
    new_entries = extract()

    # Do the addition only when there are new entries
    if new_entries is None:
        print("The database has not been updated recently.")
    else:
        # Write them on the database books file: append to the previous file
        new_entries.to_csv("./database/books.csv", mode="a", index=False, header=False)

        # Show new entries
        print("Added:")
        for name, author in zip(
            new_entries["book_name"].tolist(), new_entries["author"].tolist()
        ):
            print(f" ➺  {name} by {author}")
        print("\n")
