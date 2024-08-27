from pathlib import Path
import random
import string
import os
import shutil
from datetime import datetime, timedelta
import codecs
import stat


def remove_readonly(func, path, excinfo):
    """Clear the readonly bit and reattempt the removal."""
    os.chmod(path, stat.S_IWRITE)
    func(path)


def random_string(length: int) -> str:
    """Generate a random string of fixed length."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def create_test_files(base_path: Path) -> None:
    base_path.mkdir(parents=True, exist_ok=True)

    # Determine the maximum allowable filename length for this path
    max_filename_length = 255 - len(str(base_path.resolve()))

    # Ensure filename length does not exceed maximum path length
    long_filename = base_path / ('a' * min(max_filename_length, 255) + '.txt')
    long_filename.write_text("Long filename test.", encoding='utf-8')

    # Edge case: filenames with special characters
    special_filenames = [
        "sp@c!@l_#file$.txt",
        "中文文件名.txt",
        "file with spaces.txt",
        "12345.txt",
        " file_with_leading_space.txt",
        "file_with_trailing_space .txt",
        "hyphen-name-file.txt",
        "underscore_name_file.txt",
        "file_with_ümlauts.txt"
    ]
    for filename in special_filenames:
        (base_path / filename).write_text(f"Testing special filename: {filename}", encoding='utf-8')

    # Edge case: empty file
    (base_path / 'empty_file.txt').touch()

    # Edge case: very large file
    large_file = base_path / 'large_file.bin'
    large_file.write_bytes(os.urandom(500 * 1024 * 1024))  # 10 MB random binary file

    # Edge case: large number of small files
    small_files_dir = base_path / 'small_files'
    small_files_dir.mkdir(exist_ok=True)
    for i in range(1000):
        small_file = small_files_dir / f'small_file_{i}.txt'
        small_file.write_text(f"This is small file number {i}", encoding='utf-8')

    # Edge case: nested directories with path length check
    nested_dir = base_path / 'nested_dir'
    max_path_length = 260  # Maximum path length for Windows

    for i in range(10):
        # Calculate the path for the current level
        next_level_dir = nested_dir / f"level_{i}"

        # Check if the resulting path exceeds the maximum allowed path length
        if len(str(next_level_dir.resolve())) >= max_path_length:
            print(f"Stopping directory creation at level {i} due to path length limitations.")
            break

        # Create the directory and a file within it
        next_level_dir.mkdir(parents=True, exist_ok=True)
        (next_level_dir / f'nested_file_{i}.txt').write_text(f"This is a file in nested directory level {i}",
                                                             encoding='utf-8')

    # Edge case: different file types
    file_types = {
        'example.txt': "Text file content.",
        'example.json': '{"key": "value"}',
        'example.xml': '<root><element>Value</element></root>',
        'example.csv': 'col1,col2\nval1,val2\n',
        'example.bin': os.urandom(1024),  # 1 KB random binary file
    }
    for filename, content in file_types.items():
        path = base_path / filename
        if isinstance(content, bytes):
            path.write_bytes(content)
        else:
            path.write_text(content, encoding='utf-8')

    # Edge case: unusual file permissions (read-only)
    read_only_file = base_path / 'read_only_file.txt'
    read_only_file.write_text("This is a read-only file.", encoding='utf-8')
    read_only_file.chmod(0o444)  # Read-only

    # Edge case: files with future or old modification dates
    future_file = base_path / 'future_date_file.txt'
    old_file = base_path / 'old_date_file.txt'
    future_file.write_text("This file has a future date.", encoding='utf-8')
    old_file.write_text("This file has an old date.", encoding='utf-8')

    future_time = datetime.now() + timedelta(days=365)
    old_time = datetime.now() - timedelta(days=365 * 50)
    os.utime(future_file, (future_time.timestamp(), future_time.timestamp()))
    os.utime(old_file, (old_time.timestamp(), old_time.timestamp()))

    # Edge case: non-UTF-8 encoded file
    non_utf8_file = base_path / 'non_utf8_file.txt'
    with codecs.open(non_utf8_file, 'w', encoding='latin-1') as f:
        f.write("This file is encoded in Latin-1.")

    # Adding duplicate files
    duplicate_content = "This is a duplicate file."

    # Duplicate files in the same directory
    (base_path / 'duplicate_1.txt').write_text(duplicate_content, encoding='utf-8')
    (base_path / 'duplicate_2.txt').write_text(duplicate_content, encoding='utf-8')

    # Duplicate files in different directories
    nested_duplicate_dir = base_path / 'nested_duplicates'
    nested_duplicate_dir.mkdir(exist_ok=True)
    (nested_duplicate_dir / 'duplicate_1.txt').write_text(duplicate_content, encoding='utf-8')
    (nested_duplicate_dir / 'duplicate_2.txt').write_text(duplicate_content, encoding='utf-8')

    print(f"Test folder created at {base_path} with duplicates.")


if __name__ == "__main__":
    test_folder = Path("test_folder")
    # Clean up the directory if it already exists
    if test_folder.exists():
        shutil.rmtree(test_folder, onerror=remove_readonly)

    create_test_files(test_folder)
