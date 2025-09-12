import base64
import os

# Get the base64 string from the environment
encoded = os.getenv("BOOK_TRACKER_CREDS_B64")
if not encoded:
    raise ValueError("Missing BOOK_TRACKER_CREDS_B64 environment variable")

# Decode and write to file
with open("book_tracker_creds.json", "wb") as f:
    f.write(base64.b64decode(encoded))

