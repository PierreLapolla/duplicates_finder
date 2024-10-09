from pathlib import Path

import yaml

default_config_dict = {
    'search_directories': ['/home/pierre'],
    'allowed_extensions': ['.png', '.jpg', '.jpeg'],
    'chunk_size': 16384,
    'print_exceptions': False,
}


def load_config(config_file: Path) -> dict:
    """Load the configuration from a YAML file, or create a new one with default values if not found."""
    config_path = Path(config_file)
    if not config_path.exists():
        print(f"Configuration file {config_file} not found, creating a new one...")
        with config_path.open("w") as file:
            yaml.safe_dump(default_config_dict, file)

    with config_path.open("r") as file:
        config = yaml.safe_load(file)

    return config


try:
    config = load_config(Path("config.yaml"))
except Exception as e:
    print(f"Error loading configuration: {e}. Using default configuration.")
    config = default_config_dict
