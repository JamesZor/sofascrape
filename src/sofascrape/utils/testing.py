import json
from pathlib import Path


def load_match_ids(file_name: str = "pl_24_25_match_ids.json") -> list[int]:
    """Load match IDs from JSON file"""
    try:
        base_path: Path = Path(__file__).parent.parent.parent.parent
        dir: Path = base_path / "example_json" / "test"
        file_path = dir / file_name

        with open(file_path, "r") as f:
            data = json.load(f)
            return data["match_ids"]
    except FileNotFoundError:
        # Fallback to a few IDs if file not found
        print(f"Warning: {file_path} not found, using fallback IDs")
        return [11352303, 11352251, 11352250, 11352253, 11352252]
