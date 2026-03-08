import sys
from pathlib import Path

dags_path = str(Path(__file__).parent / "dags")
if dags_path not in sys.path:
    sys.path.insert(0, dags_path)