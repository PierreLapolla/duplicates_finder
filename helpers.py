import hashlib
import math
from pathlib import Path
from typing import Optional

import pandas as pd

from config_loader import config


def select_directory() -> Path:
    """Prompt the user to enter a directory path."""
    selected_directory = input("Enter the directory to scan for duplicates: ")
    path = Path(selected_directory).expanduser().resolve()
    if not path.is_dir():
        raise NotADirectoryError(f"{path} is not a valid directory.")
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
        print(f"Permission Denied: Cannot access file {file_path}")
        return None
    except Exception as e:
        print(f"Error hashing file {file_path}: {e}")
        return None


def convert_size(size_bytes: int) -> str:
    """Convert bytes to a human-readable format in Mo, Go, etc."""
    if size_bytes == 0:
        return "0B"

    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")

    i = int(math.log(size_bytes, 1024))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 3)
    return f"{s} {size_name[i]}"


def save_results(df: pd.DataFrame, dir: Path, filename: str) -> None:
    """Save the DataFrame to a CSV file in the given directory."""
    dir.mkdir(parents=True, exist_ok=True)
    output_path = dir / filename
    df.to_csv(output_path.with_suffix(".csv"), index=False)
    df.to_excel(output_path.with_suffix(".xlsx"), index=False)
    print(f"Results written to {output_path}")
