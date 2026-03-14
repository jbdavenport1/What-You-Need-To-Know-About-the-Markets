import os
import json
from typing import Dict, Any, List, Tuple

import matplotlib.pyplot as plt


OUTPUT_DIR = "output"
ROOT_WEEKLY_DATA_FILENAME = "weekly_market_data.json"


def ensure_output_dir() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)


def load_weekly_data() -> Dict[str, Any]:
    if not os.path.exists(ROOT_WEEKLY_DATA_FILENAME):
        raise File

