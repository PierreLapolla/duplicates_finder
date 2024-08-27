import multiprocessing
from pathlib import Path

import yaml

default_config_dict = {
    'allowed_extensions': ['.png', '.jpg', '.jpeg'],
    'chunk_size': 16384,
    'max_workers': 16
}


def load_config(config_file: Path) -> dict:
    """Load the configuration from a YAML file."""
    config_path = Path(config_file)
    if not config_file.exists():
        print(f"Configuration file {config_file} not found, creating one...")

        with config_path.open("w") as file:
            yaml.safe_dump(default_config_dict, file)

    with config_path.open("r") as file:
        config = yaml.safe_load(file)

    config['max_workers'] = min(config['max_workers'], multiprocessing.cpu_count())

    return config


try:
    config = load_config(Path("config.yaml"))
except Exception as e:
    print(f"Error loading configuration: {e}, using default configuration.")
    config = default_config_dict
