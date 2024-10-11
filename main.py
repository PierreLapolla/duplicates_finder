from pathlib import Path

from config_loader import load_config
from duplicates_finder import find_duplicates, save_results
from file_manager import file_filter, file_scan


def main():
    try:
        config = load_config(Path('config.yaml'))

        files = file_scan(config['search_directories'])

        files = file_filter(files, config['allowed_extensions'])

        duplicates = find_duplicates(files, config['chunk_size'])

        save_results(duplicates)
    except KeyboardInterrupt:
        print("Execution interrupted by the user.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        input("Press Enter to exit...")


if __name__ == "__main__":
    main()
