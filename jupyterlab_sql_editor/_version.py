import json
from pathlib import Path

__all__ = ["__version__"]

def _fetch_version():
    current_path = Path(__file__).parent.resolve()

    for settings in current_path.rglob("package.json"):
        try:
            with settings.open() as file:
                return json.load(file)["version"]
        except FileNotFoundError:
            pass

    raise FileNotFoundError(f"Could not find package.json under dir {current_path!s}")

__version__ = _fetch_version()
