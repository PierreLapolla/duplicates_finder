from concurrent.futures import as_completed, ProcessPoolExecutor
from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path
from typing import Dict, List

import pandas as pd
import blake3
from tqdm import tqdm


def calculate_hash(file_path: Path, chunk_size: int = 4 * 1024 * 1024) -> str:
    """Compute the hash of a file in chunks using blake3."""
    try:
        hasher = blake3.blake3(max_threads=1)
        with file_path.open('rb') as file:
            for chunk in iter(lambda: file.read(chunk_size), b''):
                hasher.update(chunk)
        return hasher.hexdigest()
    except FileNotFoundError:
        return f"Error: File not found."
    except PermissionError:
        return f"Error: Permission denied."
    except Exception as e:
        return f"Error: {str(e)}"


def find_duplicates(file_list: List[Path], chunk_size: int, num_workers: int) -> Dict:
    """Find files with duplicate content based on their hash, using parallel processing."""
    hash_map = {}
    max_workers = cpu_count() or 1
    num_workers = min(num_workers, max_workers)

    if num_workers <= 0:
        print(f"Invalid number of workers: {num_workers}. Using 1 worker.")

    if chunk_size < 4 * 1024 * 1024:
        print(f"Chunk size is too small: {chunk_size}. Using 4MB.")
        chunk_size = 4 * 1024 * 1024

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        future_to_file = {
            executor.submit(calculate_hash, file, chunk_size):
                file for file in tqdm(file_list, desc="Preparing parallel hashing", total=len(file_list), mininterval=1)
        }

        for future in tqdm(as_completed(future_to_file), total=len(file_list), mininterval=1,
                           desc=f"Hashing files using {num_workers}/{max_workers} workers"):
            file = future_to_file[future]
            file_hash = future.result()
            hash_map.setdefault(file_hash, []).append(file)

    return {hash_value: files for hash_value, files in hash_map.items() if len(files) > 1}


def save_results(duplicates: dict):
    """Save the duplicate files to an Excel file with file size and optimized batch writing."""
    if not duplicates:
        print("No duplicates found.")
        return

    output_dir = Path('out')
    output_dir.mkdir(exist_ok=True)

    filename = f'duplicates_{datetime.now().strftime("%Y_%m_%d__%H_%M_%S")}.xlsx'
    output_file = output_dir / filename

    all_data = []

    for hash_value, files in tqdm(duplicates.items(), desc="Preparing excel data", total=len(duplicates),
                                  mininterval=1):
        for file in files:
            all_data.append({
                "hash": hash_value,
                "path": str(file),
                "size (MB)": file.stat().st_size / (1024 * 1024)
            })

    df = pd.DataFrame(all_data)

    with pd.ExcelWriter(output_file) as writer:
        for _ in tqdm(range(1), desc=f"Writing to {output_file}", mininterval=1):
            df.to_excel(writer, index=False, sheet_name="duplicates")
