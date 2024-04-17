import logging


class TruncateSQLFilter(logging.Filter):
    def filter(self, record):
        if hasattr(record, "sql"):
            # Limit to the first 50 characters, for example
            # record.sql = record.sql[:100] + "..."
            record.args = tuple(
                arg[:100] + "..." if isinstance(arg, str) else arg for arg in record.args
            )
            record.args = tuple(
                arg[:5] + ("...",) if isinstance(arg, tuple) and len(arg) > 5 else arg
                for arg in record.args
            )
        return True
