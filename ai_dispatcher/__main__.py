"""Enable ``python -m ai_dispatcher``."""

from __future__ import annotations

import sys

from ai_dispatcher.cli import main

if __name__ == "__main__":
    sys.exit(main())
