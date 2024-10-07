from datetime import datetime
from pathlib import Path
from typing import Dict, List

import pandas as pd
from tqdm import tqdm

from helpers import convert_size, file_filter, file_hash, file_scan, save_results, select_directory


def find_duplicates(files: List[Path]) -> Dict[str, List[Path]]:
    """Return a dictionary of duplicate files based on their hash."""
    duplicates = {}

    for file in tqdm(files, desc="Calculating hashes", total=len(files)):
        file_hash_value = file_hash(file)
        if file_hash_value is None:
            continue

        if file_hash_value in duplicates:
            duplicates[file_hash_value].append(file)
        else:
            duplicates[file_hash_value] = [file]

    duplicates = {hash_value: paths for hash_value, paths in duplicates.items() if len(paths) > 1}

    return duplicates


def get_duplicates_df(duplicates: Dict[str, List[Path]]) -> pd.DataFrame:
    """Return a DataFrame of duplicate files and print a summary."""
    data = []
    total_space_saved = 0

    for hash_value, paths in duplicates.items():
        group_size = sum(file.stat().st_size for file in paths)
        size_to_keep = paths[0].stat().st_size
        space_saved = group_size - size_to_keep
        total_space_saved += space_saved

        for path in paths:
            data.append({
                "hash": hash_value,
                "path": str(path),
                "size": path.stat().st_size
            })

    print(f"Total space that can be saved: {convert_size(total_space_saved)}")

    return pd.DataFrame(data)


def remove_duplicates(df: pd.DataFrame) -> None:
    """Prompt the user to delete duplicate files, keeping one of each unique file based on the hash."""
    duplicates = df[df.duplicated(subset='hash', keep='first')]

    if duplicates.empty:
        print("No duplicate files to delete.")
        return

    response = input("Do you want to delete the duplicate files? (keep one copy of each) (y/n): ")
    if response.lower() in ["yes", "y"]:
        for _, row in duplicates.iterrows():
            try:
                Path(row['path']).unlink()
            except Exception as e:
                print(f"An unexpected error occurred with {row['path']}: {e}")


def main() -> None:
    directory = select_directory()
    file_list = file_scan(directory)
    file_list = file_filter(file_list)
    duplicates = find_duplicates(file_list)
    df = get_duplicates_df(duplicates)
    save_results(df, Path("out"), f"duplicates_report_{datetime.now().strftime('%Y_%m_%d__%H_%M_%S')}")
    # remove_duplicates(df)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        input("Press any key to quit")
