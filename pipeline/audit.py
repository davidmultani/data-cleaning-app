from datetime import datetime
import pandas as pd

# We have created the AuditLog class because it need to maintain -
# a running list of entries across multiple separate function calls.
# A plain function cannot remember previous calls.
# A class can store data as instance variables (self.entries).


class AuditLog:
    def __init__(self):
        self.entries = []

#   log function adds one transformation record to the log.
#   Parameters -
#       action - short name like "Drop Duplicates"
#       detail - human-readable description of what changed
#       rows_before - row count before the transformation
#       rows_after - row count after the transformation
#       cols_before - column count before the transformation
#       cols)after - column count after the transformation
#       All row/col parameters are option (= None by default)
    def log(self):
        action: str
        detail: str
        rows_before: int = None
        rows_after: int = None
        cols_before: int = None
        cols_after: int = None

        entry = {
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "action": action,
            "rows_before": rows_before,
            "rows_after": rows_after,
            "cols_before": cols_before,
            "cols_after": cols_after,
            "rows_removed": (rows_before - rows_after
                             if rows_before is not None and rows_after is not None
                             else None),
            "cols_removed": (cols_before - cols_after
                             if cols_before is not None and cols_after is not None
                             else None),
        }

        # .append() adds the new dictionary to the end of the list.
        self.entries.append(entry)

#   to_dateframe(self) converts all the log entries into a pandas dataframe.
    def to_dateframe(self) -> pd.DataFrame:
        # returns an empty DataFrame, if the list is empty.
        if not self.entries:
            return pd.DataFrame(columns=[
                "timestamp", "action", "detail",
                "rows_removed", "cols_removed"
            ])
        # pd.DataFrame(list_of_dicts) automatically uses keys as column names
        return pd.DataFrame(self.entries)

    # Converts the log to a plain text string for download
    def to_text_report(self) -> str:
        lines = [
            "DATA TRANSFORMATION AUDIT LOG",
            "=" * 50,
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H-%M-%S')}",
            ""
        ]

        for i, entry in enumerate(self.entries, start=1):
            # enumerate(list, start=1) gives both the index and the value
            # i starts at 1 instead of 0 so the numbering looks natural
            if entry["rows_removed"] is not None:
                lines.append(
                    f" -> Rows: {entry['rows_before']} -> "
                    f"{entry['rows_after']}"
                    f"(removed {entry['rows_removed']})"
                )
            if entry['cols_removed'] is not None:
                lines.append(
                    f" -> Cols: {entry['cols_before']} -> "
                    f"{entry['cols_after']}"
                    f"(removed {entry['cols_removed']})"
                )
            lines.append("")
            # Empty string creates a blank line between entries
        return "\n".join(lines)
        # "\n".join(list) joins all strings with a newline between them

    def clear(self):
        """ Resets the log. Called when the user uploads a file. """
        self.entries = []
