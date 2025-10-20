from pathlib import Path

files_to_merge = [
    "load_credentials.py",
    "elt/logger.py",
    "elt/extract.py",
    "elt/load.py",
    "elt/transform.py",
    "manipulation/summary.py",
    "manipulation/generate_report.py",
    "main.py"
]

with open("general_schema_v2.py", "w") as outfile:
    for fname in files_to_merge:
        content = Path(fname).read_text()
        outfile.write(f"# --- {fname} ---\n")
        outfile.write(content + "\n\n")
