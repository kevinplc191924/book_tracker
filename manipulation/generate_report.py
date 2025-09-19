from rich.console import Console
from rich.table import Table

def report(results: dict):
    """
    Displays a formatted reading report using CLI tables.

    This function prints a summary of reading metrics, top-ranked books,
    the most recently completed book, and newly added entries using `rich.Table`.

    Parameters
    ----------
    results : dict
        Dictionary containing reading metrics and DataFrames, including:
        
        - 'overall_total', 'total_current', 'ongoing', 'dropped'
        - 'mean_pages_per_day', 'mean_time_reading'
        - 'mean_pages_per_day_current', 'mean_time_reading_current'
        - 'days_since_last'
        - 'best', 'last', 'new_entries', `feedback_new`

    Returns
    -------
    None
        Output is printed directly to the console.
    """
    
    console = Console()

    # Main summary table
    summary_table = Table(title="Reading Report")
    summary_table.add_column("Metric", style="cyan", no_wrap=True)
    summary_table.add_column("Value", style="magenta")

    metrics = [
        ("Total books read", results["overall_total"]),
        ("Books completed this year", results["total_current"]),
        ("Currently reading", results["ongoing"]),
        ("Books dropped", results["dropped"]),
        ("Avg pages/day (overall)", results["mean_pages_per_day"]),
        ("Avg days/book (overall)", results["mean_time_reading"]),
        ("Avg pages/day (this year)", results["mean_pages_per_day_current"]),
        ("Avg days/book (this year)", results["mean_time_reading_current"]),
        ("Days since last book finished", results["days_since_last"])
    ]
    for label, value in metrics:
        summary_table.add_row(label, str(value))

    console.print(summary_table)

    # Top-3 Best Ranked Books
    console.print()
    title_best = "Top-3 Best Ranked Books This Year"
    df_best = results["best"]
    best_table = Table(show_header=True, header_style="bold green", title=title_best)
    for col in df_best.columns:
        best_table.add_column(col.title().replace("_", " "), style="white")
    for row in df_best.itertuples(index=False):
        # Make sure values are strings
        best_table.add_row(*[str(cell) for cell in row])
    console.print(best_table)

    # Last Book Read
    console.print()
    title_last = "Last Book Read"
    df_last = results["last"].copy()
    df_last["end_date"] = df_last["end_date"].dt.strftime("%Y-%m-%d") # Date format to display
    last_table = Table(show_header=True, header_style="bold blue", title=title_last)
    for col in df_last.columns:
        last_table.add_column(col.title().replace("_", " "), style="white")
    for row in df_last.itertuples(index=False):
        last_table.add_row(*[str(cell) for cell in row])
    console.print(last_table)

    # New additions
    console.print()
    title_new = "New Book Additions"
    df_new = results["new_entries"]

    if df_new.empty:
        new_table = Table(title=title_new)
        new_table.add_row(f"[italic]{results['feedback_new']}[/italic]")
        console.print(new_table)
        console.print()
    else:
        new_table = Table(show_header=True, header_style="bold red", title=title_new)
        for col in df_new.columns:
            new_table.add_column(col.title().replace("_", " "), style="white")
        for row in df_new.itertuples(index=False):
            new_table.add_row(*[str(cell) for cell in row])
        console.print(new_table)
        console.print(results["feedback_new"])
        console.print()
