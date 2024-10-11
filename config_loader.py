from pathlib import Path
from typing import Dict

import yaml

default_config_dict = {
    'search_directories': ['/home'],
    'allowed_extensions': ['.png', '.jpg', '.jpeg'],
    'chunk_size': 16384,
}


def load_config(config_file: Path) -> Dict:
    """Load the configuration from a YAML file or create a new one with default values."""
    if not config_file.exists():
        print(f"Config file {config_file} not found. Creating default config.")
        save_default_config(config_file)
    return _load_config_from_file(config_file)


def _load_config_from_file(config_file: Path) -> Dict:
    with config_file.open("r") as file:
        return yaml.safe_load(file)


def save_default_config(config_file: Path) -> None:
    """Create a default configuration file."""
    with config_file.open("w") as file:
        yaml.safe_dump(default_config_dict, file)
    print(f"Default config saved at {config_file}")
