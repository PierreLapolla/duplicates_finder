import hashlib
import math
import os
from pathlib import Path
from typing import List, Optional

import pandas as pd

from config_loader import config
from tqdm import tqdm


def select_directory() -> Path:
    """Prompt the user to enter a directory path."""
    selected_directory = input("Enter a working directory: ")
    path = Path(selected_directory).expanduser().resolve()
    if not path.is_dir():
        raise NotADirectoryError(f"{path} is not a valid directory.")
    return path


def file_scan(directory: Path) -> List[Path]:
    """Custom recursive file collector that handles permission errors gracefully."""
    file_list = []
    with tqdm(desc="Scanning files") as pbar:
        for root, _, files in os.walk(directory):
            root_path = Path(root)
            for file_name in files:
                file_path = root_path / file_name
                try:
                    file_path = file_path.resolve(strict=True)
                    file_list.append(file_path)
                except (PermissionError, FileNotFoundError) as e:
                    if config['print_exceptions']:
                        print(f"Skipped {file_path}: {e}")
                except Exception as e:
                    print(f"An unexpected error occurred with {file_path}: {e}")
                pbar.update(1)
    return file_list


def file_filter(files: List[Path]) -> List[Path]:
    """Filter the list of files based on the allowed extensions."""
    return [file for file in tqdm(files, desc="Filtering files", total=len(files)) if file.suffix in config['allowed_extensions']]

def file_hash(file_path: Path) -> Optional[str]:
    """Calculate the MD5 hash of the given file, return None if an error occurs."""
    hash_md5 = hashlib.md5()
    try:
        with file_path.open("rb") as f:
            for chunk in iter(lambda: f.read(config['chunk_size']), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except (PermissionError, FileNotFoundError) as e:
        if config['print_exceptions']:
            print(f"Skipped {file_path}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred with {file_path}: {e}")
    return None


def convert_size(size_bytes: int) -> str:
    """Convert bytes to a human-readable format (KB, MB, GB, etc.)."""
    if size_bytes == 0:
        return "0B"

    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = int(math.log(size_bytes, 1024))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 3)
    return f"{s} {size_name[i]}"


def save_results(df: pd.DataFrame, dir: Path, filename: str) -> None:
    """Save the DataFrame to an Excel file in the given directory."""
    dir.mkdir(parents=True, exist_ok=True)
    output_path = dir / filename
    df.to_excel(output_path.with_suffix(".xlsx"), index=False)
    print(f"Results saved to {output_path.with_suffix('.xlsx')}")
