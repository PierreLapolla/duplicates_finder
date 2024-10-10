from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
import xxhash
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count


def calculate_hash(file_path: Path, chunk_size: int = 16384) -> str:
    """Compute the hash of a file in chunks using xxHash (extremely fast)."""
    try:
        hasher = xxhash.xxh64()
        with file_path.open('rb') as file:
            for chunk in iter(lambda: file.read(chunk_size), b''):
                hasher.update(chunk)
        return hasher.hexdigest()
    except Exception:
        return 'error'


def find_duplicates(file_list: List[Path], chunk_size: int) -> Dict:
    """Find files with duplicate content based on their hash, using parallel processing."""
    hash_map = {}

    with ProcessPoolExecutor(max_workers=cpu_count() or 1) as executor:

        future_to_file = {
            executor.submit(calculate_hash, file, chunk_size):
                file for file in tqdm(file_list, desc="Preparing parallel hashing", total=len(file_list))
        }

        for future in tqdm(as_completed(future_to_file), total=len(file_list), desc="Hashing files"):
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

    for hash_value, files in tqdm(duplicates.items(), desc="Preparing excel data", total=len(duplicates)):
        for file in files:
            all_data.append({
                "hash": hash_value,
                "path": str(file),
                "size": file.stat().st_size
            })

    df = pd.DataFrame(all_data)

    with pd.ExcelWriter(output_file) as writer:
        df.to_excel(writer, index=False, sheet_name="duplicates")

    print(f"Results saved to {output_file}")
