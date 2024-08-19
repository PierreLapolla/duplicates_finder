import hashlib
import math
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Union, Optional

from tqdm import tqdm

ALLOWED_EXTENSIONS = {'.png', '.jpg', '.jpeg', '.CR3'}
CHUNK_SIZE = 16384
MAX_WORKERS = 8


def calculate_file_hash(file_path: Path) -> Union[str, None]:
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
        files = list(directory.rglob("*"))
    except Exception as e:
        print(f"Error accessing files in directory {directory}: {e}")
        return None

    files = [file for file in files if file.is_file() and file.suffix in ALLOWED_EXTENSIONS]
    num_workers = min(MAX_WORKERS, multiprocessing.cpu_count())
    print(f"Processing files using {num_workers} workers")

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        future_to_file = {executor.submit(calculate_file_hash, file): file for file in files}

        for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc="Processing files"):
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


def show_duplicates(duplicates: Dict[str, List[Path]]) -> None:
    """Display the list of duplicates and allow the user to delete selected files."""
    if not duplicates:
        print("No duplicate files found.")
        return

    total_space_saved = 0
    for hash, paths in duplicates.items():
        print(f"\nDuplicate Group: {hash}")
        for file_path in paths:
            print(f"  {file_path}")

        group_size = sum(file.stat().st_size for file in paths)
        size_to_keep = paths[0].stat().st_size
        space_saved = group_size - size_to_keep
        total_space_saved += space_saved

    space_saved_readable = convert_size(total_space_saved)
    print(f"\nTotal space that can be saved by removing duplicates: {space_saved_readable}")


def select_directory() -> Path:
    """Prompt the user to enter a directory path."""
    selected_directory = input("Enter the directory to scan for duplicates: ")
    return Path(selected_directory).expanduser().resolve()


def main() -> None:
    directory = select_directory()
    if not directory.is_dir():
        print("Invalid directory selected, exiting.")
        return

    duplicates = find_duplicate_files(directory)
    show_duplicates(duplicates)


if __name__ == "__main__":
    main()
