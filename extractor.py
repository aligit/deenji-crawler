# extractor.py

import asyncio
import json
import logging
import random
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiohttp
from bs4 import BeautifulSoup

from text_utils import classify_property_type

JSON_OUTPUT_DIR = "output_json"
Path(JSON_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

persian_num_map = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")
arabic_num_map = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


# Rate limiting for API calls
class APIRateLimiter:
    def __init__(self, min_delay=1.0, max_delay=3.0):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.last_call_time = 0

    async def wait(self):
        """Wait before making the next API call"""
        current_time = datetime.now().timestamp()
        time_since_last_call = current_time - self.last_call_time

        # Calculate delay based on randomization
        delay = random.uniform(self.min_delay, self.max_delay)

        if time_since_last_call < delay:
            wait_time = delay - time_since_last_call
            logging.debug(f"Rate limiting: waiting {wait_time:.2f} seconds")
            await asyncio.sleep(wait_time)

        self.last_call_time = datetime.now().timestamp()


# Global rate limiter instance
api_rate_limiter = APIRateLimiter(min_delay=2.0, max_delay=5.0)

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
]


def parse_persian_number(s):
    if not s or not isinstance(s, str):
        return None
    try:
        # Remove common unit words in Persian
        s = (
            s.replace("متر", "")
            .replace("تومان", "")
            .replace("مترمربع", "")
            .replace("٬", "")
            .replace(",", "")
        )

        # Translate Persian/Arabic digits to Latin
        cleaned_s = s.translate(persian_num_map).translate(arabic_num_map)

        # Remove commas and other non-numeric characters
        cleaned_s = re.sub(r"[^\d.-]+", "", cleaned_s.strip())

        if not cleaned_s or cleaned_s == "-":
            return None
        num = float(cleaned_s)
        return int(num) if num == int(num) else num
    except (ValueError, TypeError) as e:
        logging.debug(f"Could not parse number string '{s}' to number: {e}")
        return None


async def fetch_divar_api_data(token: str) -> dict:
    """Fetch property details from Divar API with rate limiting and user agent rotation"""
    url = f"https://api.divar.ir/v8/posts-v2/web/{token}"

    # Wait for rate limiter
    await api_rate_limiter.wait()

    # Random user agent
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "fa-IR,fa;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://divar.ir/",
        "Origin": "https://divar.ir",
    }

    try:
        async with aiohttp.ClientSession() as session:
            logging.info(f"[{token}] Fetching from API...")
            async with session.get(url, headers=headers, timeout=10) as response:
                if response.status == 200:
                    data = await response.json()
                    logging.info(f"[{token}] API data fetched successfully")
                    return data
                elif response.status == 429:
                    logging.warning(
                        f"[{token}] Rate limited by API (429). Waiting longer..."
                    )
                    await asyncio.sleep(10)  # Wait longer if rate limited
                    return {}
                else:
                    logging.error(
                        f"[{token}] API call failed with status {response.status}"
                    )
                    return {}
    except asyncio.TimeoutError:
        logging.error(f"[{token}] API request timed out")
        return {}
    except Exception as e:
        logging.error(f"[{token}] Error fetching API data: {e}")
        return {}


def extract_attributes_from_api(api_data: dict) -> dict:
    """Extract attributes from API response"""
    attributes = []

    # Initialize all fields with defaults
    area = bedrooms = price = price_per_meter = year_built = None
    land_area = property_type = None
    has_parking = has_storage = has_balcony = False
    title_deed_type = building_direction = renovation_status = None
    floor_material = bathroom_type = cooling_system = None
    heating_system = hot_water_system = None
    floor_info = None

    # Process each section
    for section in api_data.get("sections", []):
        for widget in section.get("widgets", []):
            widget_type = widget.get("widget_type")
            data = widget.get("data", {})

            if widget_type == "GROUP_INFO_ROW":
                # Process basic attributes like area, year, bedrooms
                for item in data.get("items", []):
                    title = item.get("title", "")
                    value = item.get("value", "")

                    # Store in attributes list
                    attributes.append({"title": title, "value": value})

                    # Map to specific fields
                    if title == "متراژ":
                        area = parse_persian_number(value)
                    elif title == "متراژ زمین":
                        land_area = parse_persian_number(value)
                    elif title == "ساخت":
                        year_built = parse_persian_number(value)
                    elif title == "اتاق":
                        bedrooms = parse_persian_number(value)
                    elif title == "نوع ملک":
                        property_type = value

            elif widget_type == "UNEXPANDABLE_ROW":
                # Process single row attributes like price, floor
                title = data.get("title", "")
                value = data.get("value", "")

                # Store in attributes list
                attributes.append({"title": title, "value": value})

                # Map to specific fields
                if title == "قیمت کل":
                    price = parse_persian_number(value)
                elif title == "قیمت هر متر":
                    price_per_meter = parse_persian_number(value)
                elif title == "طبقه":
                    floor_info = value

            elif widget_type == "GROUP_FEATURE_ROW":
                # Process features like parking, storage, balcony
                items = data.get("items", [])
                for item in items:
                    title = item.get("title", "").strip()
                    available = item.get("available", False)
                    icon = item.get("icon", {})
                    icon_name = icon.get("icon_name", "")

                    # Store in attributes list
                    attributes.append(
                        {"title": title, "available": available, "key": icon_name}
                    )

                    # Map to boolean fields
                    if "پارکینگ" in title:
                        has_parking = available
                    elif "انباری" in title:
                        has_storage = available
                    elif "بالکن" in title:
                        has_balcony = available

                    # Map feature fields with key
                    if "جنس کف" in title and available:
                        floor_material = title.replace("جنس کف", "").strip()
                    elif "سرویس بهداشتی" in title and available and icon_name == "WC":
                        bathroom_type = title.replace("سرویس بهداشتی", "").strip()
                    elif "سرمایش" in title and available and icon_name == "SNOWFLAKE":
                        cooling_system = title.replace("سرمایش", "").strip()
                    elif "گرمایش" in title and available and icon_name == "SUNNY":
                        heating_system = title.replace("گرمایش", "").strip()
                    elif (
                        "تأمین‌کننده آب گرم" in title
                        and available
                        and icon_name == "THERMOMETER"
                    ):
                        hot_water_system = title.replace(
                            "تأمین‌کننده آب گرم", ""
                        ).strip()

                # Process modal page data (advanced attributes)
                action = data.get("action", {})
                if action.get("type") == "LOAD_MODAL_PAGE":
                    modal_data = action.get("payload", {}).get("modal_page", {})
                    for widget in modal_data.get("widget_list", []):
                        if widget.get("widget_type") == "UNEXPANDABLE_ROW":
                            modal_item = widget.get("data", {})
                            title = modal_item.get("title", "")
                            value = modal_item.get("value", "")

                            # Add to attributes
                            attributes.append({"title": title, "value": value})

                            # Map advanced fields
                            if title == "سند":
                                title_deed_type = value
                            elif title == "جهت ساختمان":
                                building_direction = value
                            elif title == "وضعیت واحد":
                                renovation_status = value

                        elif widget.get("widget_type") == "FEATURE_ROW":
                            feature_data = widget.get("data", {})
                            title = feature_data.get("title", "").strip()
                            icon_data = feature_data.get("icon", {})
                            icon_name = icon_data.get("icon_name", "")

                            # Add to attributes
                            attributes.append(
                                {
                                    "title": title,
                                    "available": True,  # Features in modal are available
                                    "key": icon_name,
                                }
                            )

                            # Map feature fields
                            if "جنس کف" in title:
                                floor_material = title.replace("جنس کف", "").strip()
                            elif "سرویس بهداشتی" in title and icon_name == "WC":
                                bathroom_type = title.replace(
                                    "سرویس بهداشتی", ""
                                ).strip()
                            elif "سرمایش" in title and icon_name == "SNOWFLAKE":
                                cooling_system = title.replace("سرمایش", "").strip()
                            elif "گرمایش" in title and icon_name == "SUNNY":
                                heating_system = title.replace("گرمایش", "").strip()
                            elif (
                                "تأمین‌کننده آب گرم" in title
                                and icon_name == "THERMOMETER"
                            ):
                                hot_water_system = title.replace(
                                    "تأمین‌کننده آب گرم", ""
                                ).strip()

    # Second pass - if values are still None, try to extract from the compiled attributes list
    if not bedrooms:
        bedrooms = extract_value_from_attributes(attributes, "اتاق", is_numeric=True)

    if not year_built:
        year_built = extract_value_from_attributes(attributes, "ساخت", is_numeric=True)

    if not bathroom_type:
        bathroom_type = extract_feature_from_attributes(
            attributes, "سرویس بهداشتی", key="WC"
        )

    if not heating_system:
        heating_system = extract_feature_from_attributes(
            attributes, "گرمایش", key="SUNNY"
        )

    if not cooling_system:
        cooling_system = extract_feature_from_attributes(
            attributes, "سرمایش", key="SNOWFLAKE"
        )

    if not hot_water_system:
        hot_water_system = extract_feature_from_attributes(
            attributes, "تأمین‌کننده آب گرم", key="THERMOMETER"
        )

    if not floor_material:
        floor_material = extract_feature_from_attributes(
            attributes, "جنس کف", key="TEXTURE"
        )

    return {
        "attributes": attributes,
        "area": area,
        "land_area": land_area,
        "property_type": property_type,
        "bedrooms": bedrooms,
        "price": price,
        "price_per_meter": price_per_meter,
        "year_built": year_built,
        "has_parking": has_parking,
        "has_storage": has_storage,
        "has_balcony": has_balcony,
        "title_deed_type": title_deed_type,
        "building_direction": building_direction,
        "renovation_status": renovation_status,
        "floor_material": floor_material,
        "bathroom_type": bathroom_type,
        "cooling_system": cooling_system,
        "heating_system": heating_system,
        "hot_water_system": hot_water_system,
        "floor_info": floor_info,
    }


async def extract_property_details(
    html_content: str, token: str, extract_api_only: bool = False
) -> dict | None:
    """Extract property details using API + HTML for best results

    Args:
        html_content: The HTML content from the page
        token: The Divar ad token
        extract_api_only: If True, only extracts basic info from API (for testing)
    """

    if not html_content and not extract_api_only:
        logging.warning(f"[{token}] HTML content is empty.")
        return None

    details = {"external_id": token}

    try:
        # If extract_api_only is True, skip HTML processing
        if not extract_api_only:
            soup = BeautifulSoup(html_content, "lxml")

            # Extract title (the good stuff from HTML)
            title = None
            title_selectors = [
                "h1.kt-page-title__title.kt-page-title__title--responsive-sized",
                "h1[class*='kt-page-title__title']",
                "div.kt-page-title h1",
                "h1",
            ]

            for selector in title_selectors:
                title_tag = soup.select_one(selector)
                if title_tag:
                    title = title_tag.get_text(strip=True)
                    logging.debug(f"[{token}] Found title using selector: {selector}")
                    break

            details["title"] = title or "N/A"

            # Extract description
            description = None
            description_selectors = [
                "div[class*='kt-description-row'] > div > p[class*='kt-description-row__text']",
                "p.kt-description-row__text--primary",
                "div.kt-base-row.kt-base-row--large.kt-description-row p",
            ]

            for selector in description_selectors:
                desc_elements = soup.select(selector)
                if desc_elements:
                    description = "\n".join(
                        [
                            elem.get_text(strip=True)
                            for elem in desc_elements
                            if elem.get_text(strip=True)
                        ]
                    )
                    if description:
                        logging.debug(
                            f"[{token}] Found description using selector: {selector}"
                        )
                        break

            details["description"] = description or ""

            # Extract images
            image_urls = []
            picture_tags = soup.select(
                'div[class*=kt-carousel] picture img[src*="divarcdn"]'
            )
            for img in picture_tags:
                src = img.get("src")
                srcset = img.get("srcset")
                if srcset:
                    sources = [s.strip().split(" ")[0] for s in srcset.split(",")]
                    src = sources[-1] if sources else src
                if src and src not in image_urls:
                    image_urls.append(src)
            details["image_urls"] = image_urls
            logging.debug(f"[{token}] Found {len(image_urls)} images.")

            # Extract location from script tags
            location = None
            script_tags_ld = soup.find_all("script", type="application/ld+json")
            for script in script_tags_ld:
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict) and data.get("@type") == "Product":
                        location_info = data.get("offers", {}).get("price", "")
                        if location_info:
                            location = location_info
                            break
                except:
                    pass
            details["location"] = location
        else:
            # For API-only extraction, set minimal values
            details.update(
                {
                    "title": "API Test Mode",
                    "description": "",
                    "image_urls": [],
                    "location": None,
                }
            )

        # Get ALL attributes from the API
        api_data = await fetch_divar_api_data(token)
        if api_data:
            api_attributes = extract_attributes_from_api(api_data)
            details.update(api_attributes)
            logging.info(f"[{token}] Successfully extracted attributes from API")

            # Extract location data if available in API response
            try:
                # Extract latitude and longitude from API response
                latitude = None
                longitude = None

                # Look for coordinates in seo.post_seo_schema.geo
                seo_data = api_data.get("seo", {})
                post_seo_schema = seo_data.get("post_seo_schema", {})
                geo_data = post_seo_schema.get("geo", {})

                if geo_data:
                    latitude = geo_data.get("latitude")
                    longitude = geo_data.get("longitude")

                    if latitude and longitude:
                        logging.info(
                            f"[{token}] Found location coordinates: lat={latitude}, lon={longitude}"
                        )

                # Add location coordinates to details if found
                if latitude and longitude:
                    if "location_coords" not in details:
                        details["location_coords"] = {}
                    details["location_coords"]["latitude"] = latitude
                    details["location_coords"]["longitude"] = longitude

            except Exception as e:
                logging.debug(f"[{token}] Could not extract location coordinates: {e}")

        else:
            # Default values if API fails
            logging.warning(f"[{token}] API failed, using default values")
            details.update(
                {
                    "attributes": [],
                    "area": None,
                    "land_area": None,
                    "property_type": None,
                    "bedrooms": None,
                    "price": None,
                    "price_per_meter": None,
                    "year_built": None,
                    "has_parking": False,
                    "has_storage": False,
                    "has_balcony": False,
                    "title_deed_type": None,
                    "building_direction": None,
                    "renovation_status": None,
                    "floor_material": None,
                    "bathroom_type": None,
                    "cooling_system": None,
                    "heating_system": None,
                    "hot_water_system": None,
                    "floor_info": None,
                }
            )

        return details

    except Exception as e:
        logging.error(f"[{token}] Error during extraction: {e}", exc_info=True)
        return None


def transform_for_db(extracted_data: dict) -> dict | None:
    """
    Transform extracted data into database-ready format with robust fallback extraction for special attributes
    """
    if not extracted_data:
        return None
    if (
        not extracted_data.get("external_id")
        or not extracted_data.get("title")
        or extracted_data.get("title") == "N/A"
    ):
        logging.error(
            f"Transform: Missing critical data (ID or Title) for token {extracted_data.get('external_id')}. Skipping DB insert."
        )
        return None

    # Start with the basic structure
    db_data = {
        "p_external_id": extracted_data.get("external_id"),
        "p_title": extracted_data.get("title"),
        "p_description": extracted_data.get("description"),
        "p_location": extracted_data.get("location"),
        "p_attributes": extracted_data.get("attributes", []),
        "p_image_urls": extracted_data.get("image_urls", []),
        "p_investment_score": None,
        "p_market_trend": None,
        "p_neighborhood_fit_score": None,
        "p_rent_to_price_ratio": None,
        "p_highlight_flags": [],
        "p_similar_properties": [],
    }

    # Handle numeric fields with proper conversion and extra validation
    numeric_fields = [
        "price",
        "price_per_meter",
        "area",
        "land_area",
        "year_built",
        "bedrooms",
    ]
    for field in numeric_fields:
        value = extracted_data.get(field)
        if value is not None:
            try:
                # Convert to string first to handle edge cases
                str_value = str(value).strip()
                if str_value and str_value != "None" and str_value != "null":
                    # Try to convert to float first, then int
                    float_value = float(str_value)
                    # For integers, convert to int
                    if field in ["year_built", "bedrooms"]:
                        db_data[f"p_{field}"] = int(float_value)
                    # For potentially decimal values, keep as int if whole number
                    else:
                        db_data[f"p_{field}"] = (
                            int(float_value)
                            if float_value == int(float_value)
                            else float_value
                        )
                else:
                    db_data[f"p_{field}"] = None
            except (ValueError, TypeError, OverflowError) as e:
                logging.debug(
                    f"[{db_data['p_external_id']}] Could not convert {field}={value} to number: {e}"
                )
                db_data[f"p_{field}"] = None
        else:
            db_data[f"p_{field}"] = None

    # Handle boolean fields properly - ensure they're actual booleans, not strings
    boolean_fields = ["has_parking", "has_storage", "has_balcony"]
    for field in boolean_fields:
        # Convert to boolean explicitly - make sure it's True/False, not 'true'/'false'
        value = extracted_data.get(field, False)
        if isinstance(value, str):
            # If it's a string, convert properly
            db_data[f"p_{field}"] = value.lower() in ("true", "1", "yes", "بله")
        else:
            # If it's already a boolean, keep it as is
            db_data[f"p_{field}"] = bool(value)

        # Debug logging
        logging.debug(
            f"[{db_data['p_external_id']}] {field}: {db_data[f'p_{field}']} (type: {type(db_data[f'p_{field}'])})"
        )

    # Handle text fields - ensure they're not empty strings where we want None
    text_fields = [
        "property_type",
        "title_deed_type",
        "building_direction",
        "renovation_status",
        "floor_material",
        "bathroom_type",
        "cooling_system",
        "heating_system",
        "hot_water_system",
        "floor_info",
    ]
    for field in text_fields:
        value = extracted_data.get(field)
        if value is not None and str(value).strip():
            db_data[f"p_{field}"] = str(value).strip()
        else:
            db_data[f"p_{field}"] = None

    # Handle location coordinates - map to exact PostgreSQL column names
    if "location_coords" in extracted_data:
        location_coords = extracted_data["location_coords"]
        if "latitude" in location_coords:
            try:
                db_data["p_latitude"] = float(location_coords["latitude"])
            except (ValueError, TypeError) as e:
                logging.debug(
                    f"[{db_data['p_external_id']}] Could not convert latitude to float: {e}"
                )
                db_data["p_latitude"] = None
        else:
            db_data["p_latitude"] = None

        if "longitude" in location_coords:
            try:
                db_data["p_longitude"] = float(location_coords["longitude"])
            except (ValueError, TypeError) as e:
                logging.debug(
                    f"[{db_data['p_external_id']}] Could not convert longitude to float: {e}"
                )
                db_data["p_longitude"] = None
        else:
            db_data["p_longitude"] = None
    else:
        db_data["p_latitude"] = None
        db_data["p_longitude"] = None

    # FALLBACK EXTRACTION: If specific fields are still None, try to extract them from attributes
    attributes = db_data.get("p_attributes", [])

    # Try to extract bedrooms from attributes if still None
    if db_data.get("p_bedrooms") is None:
        for attr in attributes:
            if attr.get("title") == "اتاق":
                value = attr.get("value")
                if value:
                    try:
                        db_data["p_bedrooms"] = parse_persian_number(value)
                        logging.info(
                            f"[{db_data['p_external_id']}] Extracted bedrooms from attributes: {db_data['p_bedrooms']}"
                        )
                        break
                    except Exception as e:
                        logging.debug(
                            f"[{db_data['p_external_id']}] Error parsing bedroom value: {e}"
                        )

    # Try to extract bathroom_type from attributes if still None
    if db_data.get("p_bathroom_type") is None:
        for attr in attributes:
            title = attr.get("title", "")
            key = attr.get("key")
            if (
                "سرویس بهداشتی" in title
                and key == "WC"
                and attr.get("available", False)
            ):
                db_data["p_bathroom_type"] = title.replace("سرویس بهداشتی", "").strip()
                logging.info(
                    f"[{db_data['p_external_id']}] Extracted bathroom_type from attributes: {db_data['p_bathroom_type']}"
                )
                break

    # Try to extract heating_system from attributes if still None
    if db_data.get("p_heating_system") is None:
        for attr in attributes:
            title = attr.get("title", "")
            key = attr.get("key")
            if "گرمایش" in title and key == "SUNNY" and attr.get("available", False):
                db_data["p_heating_system"] = title.replace("گرمایش", "").strip()
                logging.info(
                    f"[{db_data['p_external_id']}] Extracted heating_system from attributes: {db_data['p_heating_system']}"
                )
                break

    # Try to extract cooling_system from attributes if still None
    if db_data.get("p_cooling_system") is None:
        for attr in attributes:
            title = attr.get("title", "")
            key = attr.get("key")
            if (
                "سرمایش" in title
                and key == "SNOWFLAKE"
                and attr.get("available", False)
            ):
                db_data["p_cooling_system"] = title.replace("سرمایش", "").strip()
                logging.info(
                    f"[{db_data['p_external_id']}] Extracted cooling_system from attributes: {db_data['p_cooling_system']}"
                )
                break

    # Try to extract hot_water_system from attributes if still None
    if db_data.get("p_hot_water_system") is None:
        for attr in attributes:
            title = attr.get("title", "")
            key = attr.get("key")
            if (
                "تأمین‌کننده آب گرم" in title
                and key == "THERMOMETER"
                and attr.get("available", False)
            ):
                db_data["p_hot_water_system"] = title.replace(
                    "تأمین‌کننده آب گرم", ""
                ).strip()
                logging.info(
                    f"[{db_data['p_external_id']}] Extracted hot_water_system from attributes: {db_data['p_hot_water_system']}"
                )
                break

    # Try to extract floor_material from attributes if still None
    if db_data.get("p_floor_material") is None:
        for attr in attributes:
            title = attr.get("title", "")
            key = attr.get("key")
            if "جنس کف" in title and key == "TEXTURE" and attr.get("available", False):
                db_data["p_floor_material"] = title.replace("جنس کف", "").strip()
                logging.info(
                    f"[{db_data['p_external_id']}] Extracted floor_material from attributes: {db_data['p_floor_material']}"
                )
                break

    # Add core attributes if they're not already there
    core_attrs = {
        "متراژ": extracted_data.get("area"),
        "متراژ زمین": extracted_data.get("land_area"),
        "نوع ملک": extracted_data.get("property_type"),
        "ساخت": extracted_data.get("year_built"),
        "اتاق": extracted_data.get("bedrooms"),
        "قیمت کل": extracted_data.get("price"),
        "قیمت هر متر": extracted_data.get("price_per_meter"),
        "طبقه": extracted_data.get("floor_info"),
    }

    # Only add non-None attributes that aren't already in the list
    existing_attr_titles = {a["title"] for a in db_data["p_attributes"]}
    for title, value in core_attrs.items():
        if value is not None and title not in existing_attr_titles:
            db_data["p_attributes"].append({"title": title, "value": str(value)})

    # If property_type is still None, try to extract it from attributes
    if db_data.get("p_property_type") is None:
        for attr in db_data.get("p_attributes", []):
            if attr.get("title") == "نوع ملک":
                db_data["p_property_type"] = attr.get("value")
                break

    # Automatic property type classification
    if db_data.get("p_property_type") is None:
        title = db_data.get("p_title", "")
        description = db_data.get("p_description", "")
        property_type = classify_property_type(title, description)
        if property_type:
            db_data["p_property_type"] = property_type
            # Also add to attributes if not already there
            attr_titles = {
                attr.get("title") for attr in db_data.get("p_attributes", [])
            }
            if "نوع ملک" not in attr_titles:
                db_data["p_attributes"].append(
                    {"title": "نوع ملک", "value": property_type}
                )
            logging.info(
                f"[{db_data['p_external_id']}] Classified property type: {property_type}"
            )

    # Final validation for numeric fields that will be sent to the database
    for field in ["investment_score", "neighborhood_fit_score", "rent_to_price_ratio"]:
        if field in db_data and db_data[field] == "":
            db_data[field] = None

    logging.debug(f"[{db_data['p_external_id']}] Transformed data for DB.")
    return db_data


def extract_value_from_attributes(attributes, title_key, is_numeric=False):
    """
    Extract a value from attributes array by title key

    Args:
        attributes: List of attribute dictionaries
        title_key: The title key to search for (exact match)
        is_numeric: Whether to parse the value as a number using parse_persian_number

    Returns:
        The extracted value, or None if not found
    """
    if not attributes or not isinstance(attributes, list):
        return None

    for attr in attributes:
        if attr.get("title") == title_key:
            value = attr.get("value")
            if is_numeric and value:
                return parse_persian_number(value)
            return value
    return None


def extract_feature_from_attributes(attributes, title_prefix, key=None):
    """
    Extract a feature value from attributes array by title prefix

    Args:
        attributes: List of attribute dictionaries
        title_prefix: The prefix to search for in the title
        key: Optional key to match (like 'WC', 'SUNNY', etc.)

    Returns:
        The extracted value (part after prefix), or None if not found
    """
    if not attributes or not isinstance(attributes, list):
        return None

    for attr in attributes:
        title = attr.get("title", "")
        attr_key = attr.get("key")
        # Check if title contains prefix and either key matches or key check is disabled
        if (
            title_prefix in title
            and (key is None or attr_key == key)
            and attr.get("available", False)
        ):
            # Extract part after the prefix
            return title.replace(title_prefix, "").strip()
    return None
