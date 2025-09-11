from rich.table import Table
from rich.console import Console

def report(*args):
    console = Console()

    # Main summary table
    summary_table = Table(title="📚 Reading Report")
    summary_table.add_column("Metric", style="cyan", no_wrap=True)
    summary_table.add_column("Value", style="magenta")

    metrics = [
        ("Total books read", args[0]),
        ("Books completed this year", args[1]),
        ("Currently reading", args[2]),
        ("Books dropped", args[3]),
        ("Avg pages/day (overall)", args[4]),
        ("Avg days/book (overall)", args[5]),
        ("Avg pages/day (this year)", args[6]),
        ("Avg days/book (this year)", args[7]),
        ("Days since last book finished", args[10])
    ]
    for label, value in metrics:
        summary_table.add_row(label, str(value))

    console.print(summary_table)

    # Top-3 Best Ranked Books
    console.print("\n✨ [bold]Top-3 Best Ranked Books This Year:[/bold]")
    df_best = args[8]
    best_table = Table(show_header=True, header_style="bold green")
    for col in df_best.columns:
        best_table.add_column(col.title().replace("_", " "), style="white")
    for _, row in df_best.iterrows():
        best_table.add_row(*[str(cell) for cell in row])
    console.print(best_table)

    # Last Book Read
    console.print("\n📖 [bold]Last Book Read:[/bold]")
    df_last = args[9]
    last_table = Table(show_header=True, header_style="bold blue")
    for col in df_last.columns:
        last_table.add_column(col.title().replace("_", " "), style="white")
    for _, row in df_last.iterrows():
        last_table.add_row(*[str(cell) for cell in row])
    console.print(last_table)
