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

    print("Top-3 best ranked books this year:")
    print(args[8])

    print("\n")
    print("Last book read:")
    print(args[9])
