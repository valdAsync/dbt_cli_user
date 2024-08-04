import json
from pathlib import Path
from typing import Dict

from xdg_base_dirs import xdg_config_home, xdg_data_home


def dbt_cli_directory(root: Path) -> Path:
    directory = root / "dbt_cli_user"
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def data_directory() -> Path:
    return dbt_cli_directory(xdg_data_home())


def config_directory() -> Path:
    return dbt_cli_directory(xdg_config_home())


def config_file() -> Path:
    return config_directory() / "config.json"


def load_config() -> Dict:
    config_path = config_file()
    if config_path.exists():
        with open(config_path, "r") as f:
            return json.load(f)
    else:
        default_config = {"projects": {}}
        save_config(default_config)
        return default_config


def save_config(config: Dict):
    with open(config_file(), "w") as f:
        json.dump(config, f, indent=2)
