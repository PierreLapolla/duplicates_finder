from concurrent.futures import as_completed, ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
from tqdm import tqdm

from config_loader import config
from helpers import calculate_file_hash, convert_size, save_csv, select_directory, translate


def find_duplicate_files(directory: Union[str, Path]) -> Dict[str, List[Path]]:
    """Find and return a dictionary of duplicate files in the given directory."""
    if isinstance(directory, str):
        directory = Path(directory)

    file_hashes: Dict[str, List[Path]] = {}
    try:
        files = list(tqdm(directory.rglob("*"), desc=translate("scanning_files")))
    except Exception as e:
        print(translate("error_accessing_files", directory=directory, error=e))
        exit(1)

    files = [file for file in tqdm(files, total=len(files), desc=translate("filtering_files")) if
             file.is_file() and file.suffix in config['allowed_extensions']]

    with ThreadPoolExecutor(max_workers=config['max_workers']) as executor:
        future_to_file = {executor.submit(calculate_file_hash, file): file for file in
                          tqdm(files, total=len(files), desc=translate("preparing_parallel"))}

        for future in tqdm(as_completed(future_to_file), total=len(future_to_file),
                           desc=translate("processing_files", num_workers=config['max_workers'])):
            file = future_to_file[future]
            try:
                file_hash = future.result()
                if file_hash:
                    if file_hash in file_hashes:
                        file_hashes[file_hash].append(file)
                    else:
                        file_hashes[file_hash] = [file]
            except Exception as e:
                print(translate("error_processing_file", file=file, error=e))

    duplicates = {hash: paths for hash, paths in file_hashes.items() if len(paths) > 1}

    if not duplicates:
        print(translate("no_duplicates_found"))
        exit(0)

    return duplicates


def show_duplicates(duplicates: Dict[str, List[Path]]) -> Optional[pd.DataFrame]:
    """Return a DataFrame of duplicate files and print a summary."""
    data = []
    total_space_saved = 0
    num_duplicates = sum(len(paths) for paths in duplicates.values())
    num_file_to_delete = num_duplicates - len(duplicates)

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

    print(translate("total_files_to_delete", num_file_to_delete=num_file_to_delete))
    print(translate("total_space_saved", space_saved=convert_size(total_space_saved)))

    return pd.DataFrame(data)


def main() -> None:
    directory = select_directory()
    duplicates = find_duplicate_files(directory)
    df = show_duplicates(duplicates)
    save_csv(df, Path("out"), f"duplicates_report_{datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}.csv")


if __name__ == "__main__":
    main()
