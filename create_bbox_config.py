#!/usr/bin/env python3
"""
Utility to create bbox configuration files from Divar URLs

Usage:
    python create_bbox_config.py --url "https://divar.ir/s/iran/buy-residential?bbox=..." --name area_name.json
"""

import argparse
import os
import sys
from pathlib import Path

# Make sure this can be imported
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from viewport_api import create_bbox_config_from_url


def main():
    parser = argparse.ArgumentParser(
        description="Create a bbox config file from a Divar URL"
    )
    parser.add_argument(
        "--url", required=True, help="Divar URL containing bbox parameter"
    )
    parser.add_argument(
        "--name", required=True, help="Name for the config file (e.g., 'vanak.json')"
    )

    args = parser.parse_args()

    # Create bbox config from URL
    success = create_bbox_config_from_url(args.url, args.name)

    if success:
        print(
            f"Success! You can now use this config with: python main.py --bbox {args.name}"
        )
    else:
        print("Failed to create bbox config file.")


if __name__ == "__main__":
    main()
