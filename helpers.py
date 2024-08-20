import hashlib
import math
from pathlib import Path
from typing import Optional

import pandas as pd

from config_loader import config


def translate(key: str, **kwargs) -> str:
    """Translate a message key to the current language."""
    return config['translations'][config['language']][key].format(**kwargs)


def select_directory() -> Path:
    """Prompt the user to enter a directory path."""
    selected_directory = input(translate("enter_directory"))
    path = Path(selected_directory).expanduser().resolve()
    if not path.is_dir():
        print(translate("invalid_directory"))
        exit(1)
    return path


def calculate_file_hash(file_path: Path) -> Optional[str]:
    """Calculate the MD5 hash of the given file, return None if an error occurs."""
    hash_md5 = hashlib.md5()
    try:
        with file_path.open("rb") as f:
            for chunk in iter(lambda: f.read(config['chunk_size']), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except PermissionError:
        print(translate("permission_denied", file_path=file_path))
        return None
    except Exception as e:
        print(translate("error_hashing_file", file_path=file_path, error=e))
        return None


def convert_size(size_bytes: int) -> str:
    """Convert bytes to a human-readable format in Mo, Go, etc."""
    if size_bytes == 0:
        return "0B"

    size_name = ("B", "Ko", "Mo", "Go", "To", "Po", "Eo", "Zo", "Yo")
    i = int(math.log(size_bytes, 1024))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 3)
    return f"{s} {size_name[i]}"


def save_csv(df: pd.DataFrame, dir: Path, filename: str) -> None:
    """Save the DataFrame to a CSV file in the given directory."""
    dir.mkdir(parents=True, exist_ok=True)
    output_path = dir / filename
    df.to_csv(output_path, index=False)
    print(translate("results_written", output_file=output_path))
