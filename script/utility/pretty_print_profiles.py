from rich.table import Table
from rich.console import Console
from rich import box
import pandas as pd

console = Console()

def print_pretty_profile(profiles_dict: dict):
    """
    Prints a dictionary of profiles in a visually appealing way.

    Parameters:
        profiles_dict (dict): Dictionary of table names to DataFrames with profile data

    Each table is printed with the following columns:
        - Column: Name of the column
        - Type: Data type of the column
        - Null %: Percentage of null values in the column
        - Distinct %: Percentage of distinct values in the column
        - Top Value → Count: Top value in the column and its count
    """
    for table_name, profile_df in profiles_dict.items():
        if profile_df.empty:
            console.print(f"[bold red]Table '{table_name}' → no data[/]")
            continue

        total_rows = int(profile_df.iloc[0]["row_count"])
        console.print()
        console.rule(f"[bold cyan]Table: {table_name}[/]  |  [green]{total_rows:,} rows[/]", style="cyan")

        table = Table(box=box.ROUNDED, show_header=True, header_style="bold magenta")
        table.add_column("Column", style="dim", width=20)
        table.add_column("Type", width=12)
        table.add_column("Null %", justify="right", width=10)
        table.add_column("Distinct %", justify="right", width=12)
        table.add_column("Top Value → Count", justify="left")

        for _, row in profile_df.iterrows():
            null_p = f"{row['null_%']:.4f}%".rstrip('0').rstrip('.') + "%"
            dist_p = f"{row['distinct_%']:.4f}%".rstrip('0').rstrip('.') + "%"

            top_val = row.get("top_value")
            top_freq = row.get("top_value_freq")
            if pd.notna(top_val) and pd.notna(top_freq):
                top_str = f"[yellow]{top_val}[/] → [bold]{int(top_freq):,}[/]"
            else:
                top_str = "—"

            table.add_row(
                row["column_name"],
                str(row["data_type"]),
                null_p,
                dist_p,
                top_str
            )

        console.print(table)
        console.print()  # blank line between tables