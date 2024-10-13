import os
from pathlib import Path
from typing import List

from tqdm import tqdm


def scan_files(search_directories: List, allowed_extensions: List) -> List[Path]:
    """Scan directories for files and filter by allowed extensions, handling errors."""
    file_list = []
    for directory in search_directories:
        directory_path = Path(directory).expanduser().resolve()
        if directory_path.is_dir():
            with tqdm(desc=f"Scanning {directory}", mininterval=1) as pbar:
                for root, _, files in os.walk(directory_path):
                    for file in files:
                        file_path = Path(root) / file
                        if file_path.suffix.lower() in allowed_extensions and file_path.is_file():
                            file_list.append(file_path)
                    pbar.update(len(files))
        else:
            print(f"{directory} is not a valid directory.")
    return file_list
