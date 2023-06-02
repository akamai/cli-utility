from __future__ import annotations

import logging
import os


# Create a custom formatter that includes the folder name
class CLIFormatter(logging.Formatter):
    def format(self, record):
        record.filename = os.path.join(os.path.basename(os.path.dirname(record.pathname)), os.path.basename(record.filename))
        return super().format(record)
