# pretty_data.py
from rich.table import Table
from rich.console import Console
from rich import box
import pandas as pd

console = Console()

def print_pretty_data(data_dict: dict):
    """
    Beautifully prints every DataFrame in the dictionary using rich.
    """
    for table_name, df in data_dict.items():
        if df.empty:
            console.print(f"[bold red]Table '{table_name}' → Empty[/]")
            continue

        # Header with table name and shape
        rows, cols = df.shape
        console.print()
        console.rule(f"[bold green]Table: {table_name.upper()}[/] | [yellow]{rows:,} rows × {cols} columns[/]", style="green")

        # Create rich table
        table = Table(box=box.DOUBLE_EDGE, show_lines=False, expand=True)
        
        # Add columns (with smart truncation for long names)
        for col in df.columns:
            table.add_column(str(col), overflow="fold", max_width=25)

        # Add rows (limit to first 30 rows for readability)
        preview_rows = df.head(30).itertuples(index=False, name=None)
        for row in preview_rows:
            # Convert NaN → "—", format dates nicely if possible
            formatted_row = []
            for val in row:
                if pd.isna(val):
                    formatted_row.append("[dim]—[/]")
                elif isinstance(val, (pd.Timestamp, pd.DatetimeIndex)):
                    formatted_row.append(f"[cyan]{val}[/]")
                else:
                    formatted_row.append(str(val))
            table.add_row(*formatted_row)

        # Show if more rows exist
        if len(df) > 30:
            footer = f"[dim]... and {len(df) - 30:,} more rows[/]"
            console.print(footer)

        console.print(table)
        console.print()  # spacing