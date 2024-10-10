from pathlib import Path

from config_loader import load_config
from file_manager import file_scan, file_filter
from duplicates_finder import find_duplicates, save_results


def main():
    config = load_config(Path('config.yaml'))

    files = file_scan(config['search_directories'])

    files = file_filter(files, config['allowed_extensions'])

    duplicates = find_duplicates(files, config['chunk_size'])

    save_results(duplicates)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Execution interrupted by the user.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        input("Press Enter to exit...")
