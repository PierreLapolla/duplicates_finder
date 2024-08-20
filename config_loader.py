import multiprocessing
from pathlib import Path
from typing import Union

import yaml


def load_config(config_file: Union[str, Path] = "config.yaml") -> dict:
    """Load the configuration from a YAML file."""
    config_path = Path(config_file)
    if not config_path.is_file():
        raise FileNotFoundError(f"Configuration file {config_file} not found.")

    with config_path.open("r") as file:
        config = yaml.safe_load(file)

    config['max_workers'] = min(config['max_workers'], multiprocessing.cpu_count())

    return config


config = load_config()
