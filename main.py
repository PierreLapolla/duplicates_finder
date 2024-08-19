import hashlib
import math
import multiprocessing
from concurrent.futures import as_completed, ProcessPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

from tqdm import tqdm
import pandas as pd

ALLOWED_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.CR3', '.txt', '.pdf', '.csv'}
CHUNK_SIZE = 16384
MAX_WORKERS = 8


def calculate_file_hash(file_path: Path) -> Optional[str]:
    """Calculate the MD5 hash of the given file, return None if an error occurs."""
    hash_md5 = hashlib.md5()
    try:
        with file_path.open("rb") as f:
            for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except PermissionError:
        print(f"Permission Denied: Cannot access file {file_path}")
        return None
    except Exception as e:
        print(f"Error hashing file {file_path}: {e}")
        return None


def find_duplicate_files(directory: Union[str, Path]) -> Optional[Dict[str, List[Path]]]:
    """Find and return a dictionary of duplicate files in the given directory."""
    if isinstance(directory, str):
        directory = Path(directory)

    file_hashes: Dict[str, List[Path]] = {}
    try:
        files = list(tqdm(directory.rglob("*"), desc="Scanning files"))
    except Exception as e:
        print(f"Error accessing files in directory {directory}: {e}")
        return None

    files = [file for file in tqdm(files, total=len(files), desc="Filtering files") if
             file.is_file() and file.suffix in ALLOWED_EXTENSIONS]
    num_workers = min(MAX_WORKERS, multiprocessing.cpu_count())

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        future_to_file = {executor.submit(calculate_file_hash, file): file for file in files}

        for future in tqdm(as_completed(future_to_file), total=len(future_to_file),
                           desc=f"Processing files using {num_workers} workers"):
            file = future_to_file[future]
            try:
                file_hash = future.result()
                if file_hash:
                    if file_hash in file_hashes:
                        file_hashes[file_hash].append(file)
                    else:
                        file_hashes[file_hash] = [file]
            except Exception as e:
                print(f"Error processing file {file}: {e}")

    duplicates = {hash: paths for hash, paths in file_hashes.items() if len(paths) > 1}
    return duplicates


def convert_size(size_bytes: int) -> str:
    """Convert bytes to a human-readable format in Mo, Go, etc."""
    if size_bytes == 0:
        return "0B"

    size_name = ("B", "Ko", "Mo", "Go", "To", "Po", "Eo", "Zo", "Yo")
    i = int(math.log(size_bytes, 1024))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 3)
    return f"{s} {size_name[i]}"


def show_duplicates(duplicates: Dict[str, List[Path]], output_file: Path) -> None:
    """Write the list of duplicates to a CSV file and calculate potential space saved."""
    if not duplicates:
        print("No duplicate files found.")
        return

    data = []
    total_space_saved = 0

    for file_hash, paths in duplicates.items():
        group_size = sum(file.stat().st_size for file in paths)
        size_to_keep = paths[0].stat().st_size
        space_saved = group_size - size_to_keep
        total_space_saved += space_saved

        for file_path in paths:
            data.append({
                "hash": file_hash,
                "path": str(file_path),
                "size": file_path.stat().st_size
            })

    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)

    print(f"\nResults written to {output_file}")
    print(f"Total space that can be saved: {convert_size(total_space_saved)}")


def select_directory() -> Optional[Path]:
    """Prompt the user to enter a directory path."""
    selected_directory = input("Enter the directory to scan for duplicates: ")
    path = Path(selected_directory).expanduser().resolve()
    if not path.is_dir():
        print("Invalid directory selected")
        return None
    return path


def main() -> None:
    directory = select_directory()
    duplicates = find_duplicate_files(directory)

    output_dir = Path(f"out")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"duplicates_report_{datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}.csv"

    show_duplicates(duplicates, output_file)


if __name__ == "__main__":
    main()
