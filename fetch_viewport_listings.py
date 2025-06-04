import json
import logging
import random
import aiohttp
from typing import List, Dict, Optional

# New API endpoint
DIVAR_VIEWPORT_API = "https://api.divar.ir/v8/mapview/viewport"

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
]


async def fetch_viewport_listings(
    session: aiohttp.ClientSession,
    bbox_config: Dict,
    city_ids: List[str] = ["1", "1708"],  # Default to Tehran
) -> Dict:
    """
    Fetches property listings using the mapview/viewport API

    Args:
        session: aiohttp ClientSession
        bbox_config: Dictionary containing bbox coordinates and zoom
        city_ids: List of city IDs to search in

    Returns:
        Dict containing the API response with property tokens and count
    """
    # Extract bbox parameters
    min_latitude = bbox_config["min_latitude"]
    min_longitude = bbox_config["min_longitude"]
    max_latitude = bbox_config["max_latitude"]
    max_longitude = bbox_config["max_longitude"]
    zoom = bbox_config.get("zoom", 14.568499456622654)  # Use default if not provided

    # Construct the payload
    payload = {
        "city_ids": city_ids,
        "search_data": {
            "form_data": {
                "data": {
                    "bbox": {
                        "repeated_float": {
                            "value": [
                                {"value": min_longitude},
                                {"value": min_latitude},
                                {"value": max_longitude},
                                {"value": max_latitude},
                            ]
                        }
                    },
                    "category": {"str": {"value": "residential-sell"}},
                }
            }
        },
        "camera_info": {
            "bbox": {
                "min_latitude": min_latitude,
                "min_longitude": min_longitude,
                "max_latitude": max_latitude,
                "max_longitude": max_longitude,
            },
            "place_hash": f"{','.join(city_ids)}||residential-sell",
            "zoom": zoom,
        },
    }

    # Random user agent
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://divar.ir/",
        "Origin": "https://divar.ir",
        "Content-Type": "application/json",
    }

    logging.info(
        f"Fetching viewport listings with bbox: {min_longitude},{min_latitude},{max_longitude},{max_latitude}"
    )

    try:
        async with session.post(
            DIVAR_VIEWPORT_API, json=payload, headers=headers, timeout=20
        ) as response:
            response_text = await response.text()
            logging.debug(f"API Response Status: {response.status}")
            response.raise_for_status()
            data = json.loads(response_text)

            # Extract tokens from the response
            tokens = []
            try:
                tokens = (
                    data.get("map_idle_action_log", {})
                    .get("server_side_info", {})
                    .get("info", {})
                    .get("post_tokens", [])
                )
                count = data.get("count", 0)
                count_text = data.get("count_text", "")

                logging.info(
                    f"Fetched {count} properties ({len(tokens)} tokens) from viewport API: {count_text}"
                )
            except (KeyError, TypeError) as e:
                logging.error(f"Error extracting tokens from response: {e}")

            return data
    except aiohttp.ClientResponseError as e:
        logging.error(f"API request failed: Status {e.status}, Message: {e.message}")
        return {"error": f"API request failed: {e.status} {e.message}"}
    except Exception as e:
        logging.error(
            f"Unexpected error fetching viewport listings: {e}", exc_info=True
        )
        return {"error": f"Unexpected error: {str(e)}"}


def extract_tokens_from_viewport_response(response_data: Dict) -> List[str]:
    """
    Extract property tokens from viewport API response

    Args:
        response_data: The API response data

    Returns:
        List of property tokens
    """
    try:
        tokens = (
            response_data.get("map_idle_action_log", {})
            .get("server_side_info", {})
            .get("info", {})
            .get("post_tokens", [])
        )
        return tokens
    except (KeyError, TypeError) as e:
        logging.error(f"Error extracting tokens from response: {e}")
        return []
