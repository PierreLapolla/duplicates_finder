from pathlib import Path

from config_loader import load_config
from duplicates_finder import find_duplicates, save_results
from file_manager import scan_files


def main():
    try:
        config = load_config(Path('config.yaml'))

        files = scan_files(config['search_directories'], config['allowed_extensions'])

        duplicates = find_duplicates(files, config['chunk_size'], config['max_workers'])

        save_results(duplicates)
    except KeyboardInterrupt:
        print("Execution interrupted by the user.")


if __name__ == "__main__":
    main()
