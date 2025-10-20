# 🔐 Google Sheets Credential Setup

This project accesses a Google Sheets file using credentials stored securely via GitHub environment variables. Below is the full setup process:

1. **Obtain Google Sheets Credentials**

- Go to Google Cloud Console.
- Create a new project or select an existing one.
- Enable the Google Sheets API and Google Drive API.
- Navigate to Credentials → Create Credentials → Service Account.

After creating the service account:

- Grant it access to your target Google Sheet (share the sheet with the service account email).
- Download the JSON key file.

2. **Encode the Credentials**

To store the credentials securely in GitHub, encode the JSON file using base64:

```bash
base64 -w 0 book_tracker_creds.json
```

```powershell
[Convert]::ToBase64String([IO.File]::ReadAllBytes("book_tracker_creds.json"))
```

3. **Store in GitHub Environment Variable**

- Go to the GitHub repository → Settings → Secrets and variables → Actions.

Add a new Environment Variable:

- Name: `BOOK_TRACKER_CREDS_B64`
- Value: (*paste the base64 string*)

4. **Load Credentials in Code**

The script `load_credentials.py` decodes the base64 string and writes it to `book_tracker_creds.json`:

```python
import base64, os

encoded = os.getenv("BOOK_TRACKER_CREDS_B64")
if not encoded:
    raise ValueError("Missing BOOK_TRACKER_CREDS_B64 environment variable")

with open("book_tracker_creds.json", "wb") as f:
    f.write(base64.b64decode(encoded))
```

*This script is safe to run multiple times—it simply overwrites the file with the same content.*