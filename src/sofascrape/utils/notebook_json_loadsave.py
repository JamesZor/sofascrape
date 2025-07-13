import json
from pathlib import Path


class NotebookSaveLoad:
    def __init__(self, type: str = "general") -> None:

        self.base_path: Path = Path(__file__).parent.parent.parent.parent
        self.dir: Path = self.base_path / "example_json" / type

    def load(self, file_name: str):
        file_path: Path = self.dir / f"{file_name}.json"
        with open(file_path, "r") as f:
            data = json.load(f)
        return data

    def save(self, file_name: str, data, **kwags):
        file_path: Path = self.dir / f"{file_name}.json"
        with open(file_path, "w") as f:
            json.dump(obj=data, fp=f, **kwags)
        print(f"File saved to {file_path}")

    def __repr__(self):
        return f"{self.base_path =}, {self.dir =}"
