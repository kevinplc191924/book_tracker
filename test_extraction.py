import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = ServiceAccountCredentials.from_json_keyfile_name("book_tracker_creds.json", scope)
client = gspread.authorize(creds)

sheet = client.open_by_key("1mRx4CClu1io5Ievu9b5PTJ6nIEDOFfl-oFgIv55Q37g").sheet1
data = sheet.get_all_records()
df = pd.DataFrame(data)
#print(df.info())
print(df["score"].sample(5))
df_copy = df.copy()
df_copy["score"] = pd.to_numeric(df_copy["score"], errors="coerce")
print(df_copy.info())
