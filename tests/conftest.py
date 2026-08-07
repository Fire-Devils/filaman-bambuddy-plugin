"""Test path setup: import pure modules without the full FilaMan app."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bambuddy"))
