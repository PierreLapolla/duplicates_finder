from pathlib import Path
import os
from tqdm import tqdm
from typing import List

def file_scan(search_directories: List) -> List[Path]:
    """Scan directories for files, handling errors."""
    file_list = []
    for directory in search_directories:
        directory_path = Path(directory).expanduser().resolve()
        if directory_path.is_dir():
            with tqdm(desc=f"Scanning {directory}") as pbar:
                for root, _, files in os.walk(directory_path):
                    file_list.extend([Path(root) / file for file in files])
                    pbar.update(len(files))
        else:
            print(f"{directory} is not a valid directory.")
    return file_list

def file_filter(file_list: List[Path], allowed_extensions: List) -> List[Path]:
    """Filter files by their allowed extensions."""
    return [
        file for file in tqdm(file_list, desc="Filtering files", total=len(file_list))
        if file.suffix.lower() in allowed_extensions and file.is_file()
    ]
