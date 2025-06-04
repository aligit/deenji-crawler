import json
import logging
from pathlib import Path
from urllib.parse import parse_qs, urlparse

# Directory to store bbox configurations
BBOX_CONFIGS_DIR = "bbox_configs"
Path(BBOX_CONFIGS_DIR).mkdir(parents=True, exist_ok=True)


def load_bbox_config(filename):
    """
    Load bbox configuration from a file in the bbox_configs directory

    Args:
        filename: Name of the config file (without directory path)

    Returns:
        dict: Bbox configuration or None if invalid/not found
    """
    file_path = Path(BBOX_CONFIGS_DIR) / filename

    if not file_path.exists():
        logging.error(f"Bbox config file not found: {file_path}")
        return None

    try:
        with open(file_path, "r") as f:
            config = json.load(f)

        # Validate the config
        required_keys = [
            "min_latitude",
            "min_longitude",
            "max_latitude",
            "max_longitude",
        ]
        if not all(key in config for key in required_keys):
            logging.error(
                f"Invalid bbox config in {filename}. Must contain: {required_keys}"
            )
            return None

        logging.info(f"Successfully loaded bbox config from {filename}: {config}")
        return config
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON in bbox config file: {file_path}")
        return None
    except Exception as e:
        logging.error(f"Error reading bbox config: {e}")
        return None


def create_bbox_config_from_url(url, filename):
    """
    Create a bbox config file from a Divar URL

    Args:
        url: Divar URL containing bbox parameter
        filename: Name to save the config as (without directory path)

    Returns:
        bool: True if successful, False otherwise
    """
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)

    # Try to get bbox first, then map_bbox if bbox is not available
    bbox_str = None
    if "bbox" in query_params:
        bbox_str = query_params["bbox"][0]
    elif "map_bbox" in query_params:
        bbox_str = query_params["map_bbox"][0]

    if not bbox_str:
        print("Error: URL does not contain bbox or map_bbox parameter")
        return False

    parts = bbox_str.split(",")

    if len(parts) != 4:
        print("Error: Invalid bbox format in URL")
        return False

    min_longitude, min_latitude, max_longitude, max_latitude = parts

    config = {
        "min_latitude": float(min_latitude),
        "min_longitude": float(min_longitude),
        "max_latitude": float(max_latitude),
        "max_longitude": float(max_longitude),
    }

    file_path = Path(BBOX_CONFIGS_DIR) / filename
    with open(file_path, "w") as f:
        json.dump(config, f, indent=2)

    print(f"Created bbox config file: {file_path}")
    return True
