import json
import re
import logging
from bs4 import BeautifulSoup

# Helper to convert Persian/Arabic numbers to Latin digits
persian_num_map = str.maketrans('۰۱۲۳۴۵۶۷۸۹', '0123456789')
arabic_num_map = str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')

def parse_persian_number(s):
    """Converts a string containing Persian/Arabic numbers and commas to an integer or float."""
    if not s or not isinstance(s, str):
        return None
    try:
        # Remove non-numeric chars except for potential decimal points (.)
        # Translate Persian/Arabic digits first
        cleaned_s = s.translate(persian_num_map).translate(arabic_num_map)
        # Remove thousand separators (commas) and extra whitespace
        cleaned_s = cleaned_s.replace(',', '').strip()
        # Remove any remaining non-digit characters except '.' and '-'
        cleaned_s = re.sub(r'[^\d.-]+', '', cleaned_s)

        if not cleaned_s or cleaned_s == '-': # Handle empty or just hyphen
             return None

        # Try float conversion, then int if it's a whole number
        num = float(cleaned_s)
        return int(num) if num == int(num) else num # Return int if whole number
    except (ValueError, TypeError) as e:
        logging.debug(f"Could not parse number string '{s}' to number: {e}")
        return None

def extract_property_details(html_content: str, token: str) -> dict | None:
    """
    Parses the HTML content of a Divar property detail page.
    Args:
        html_content: The HTML string.
        token: The Divar property token (external_id).
    Returns:
        A dictionary containing the extracted property details, or None if parsing fails.
    """
    if not html_content:
        logging.warning(f"[{token}] HTML content is empty, cannot extract details.")
        return None

    soup = BeautifulSoup(html_content, 'lxml')
    details = {"external_id": token}
    logging.debug(f"[{token}] Starting HTML parsing.")

    try:
        # --- Extract Core Information ---
        # Title: Usually the main H1
        title_tag = soup.find('h1', class_=lambda c: c and 'kt-page-title__title' in c)
        details['title'] = title_tag.get_text(strip=True) if title_tag else 'N/A'
        if details['title'] == 'N/A':
             logging.warning(f"[{token}] Title not found using primary selector. Trying fallback.")
             # Fallback: Maybe a simple h1?
             title_tag = soup.find('h1')
             details['title'] = title_tag.get_text(strip=True) if title_tag else 'N/A'


        # Description: Specific div class
        desc_div = soup.find('div', class_=lambda c: c and 'kt-description-row__text--primary' in c)
        details['description'] = desc_div.get_text(separator='\n', strip=True) if desc_div else ''
        if not details['description']:
             logging.warning(f"[{token}] Description not found using primary selector.")
             # Add fallback if necessary


        # --- Extract Images ---
        image_urls = []
        # Carousel images often use <picture><img ...></picture>
        picture_tags = soup.select('div[class*=kt-carousel] picture img[src*="divarcdn"]')
        for img in picture_tags:
            src = img.get('src')
            if src and src not in image_urls:
                image_urls.append(src)
        details['image_urls'] = image_urls
        logging.debug(f"[{token}] Found {len(image_urls)} images.")


        # --- Extract Location ---
        location = None
        # Prioritize ld+json script tags
        script_tags_ld = soup.find_all('script', type='application/ld+json')
        for script in script_tags_ld:
            try:
                data = json.loads(script.string)
                # Check common paths (might be nested in a list)
                items_to_check = []
                if isinstance(data, list):
                    items_to_check.extend(data)
                elif isinstance(data, dict):
                    items_to_check.append(data)

                for item in items_to_check:
                     if isinstance(item, dict) and item.get('@type') == 'Apartment' and 'geo' in item:
                         geo_data = item['geo']
                         if isinstance(geo_data, dict) and 'latitude' in geo_data and 'longitude' in geo_data:
                             try:
                                 lat = float(geo_data['latitude'])
                                 lon = float(geo_data['longitude'])
                                 location = {"latitude": lat, "longitude": lon}
                                 logging.debug(f"[{token}] Extracted location from ld+json: {location}")
                                 break # Found location
                             except (ValueError, TypeError):
                                 logging.warning(f"[{token}] Found geo keys in ld+json, but values were invalid: {geo_data}")
                if location: break
            except (json.JSONDecodeError, TypeError, KeyError) as e:
                logging.debug(f"[{token}] Could not parse ld+json script for location: {e}")
                continue

        # Fallback: check window.__PRELOADED_STATE__ if ld+json fails
        if not location:
            logging.debug(f"[{token}] Location not found in ld+json. Checking window.__PRELOADED_STATE__...")
            script_tags_preload = soup.find_all('script')
            for script in script_tags_preload:
                if script.string and 'window.__PRELOADED_STATE__' in script.string:
                    try:
                         match = re.search(r'window\.__PRELOADED_STATE__\s*=\s*({.*?});', script.string, re.DOTALL)
                         if match:
                              preloaded_data = json.loads(match.group(1))
                              # Navigate the potentially complex structure (this path is a guess)
                              post_data = preloaded_data.get('currentPost', {}).get('post', {})
                              widget_list = post_data.get('widgets', [])
                              for widget in widget_list:
                                   if widget.get('widget_type') == 'MAP_INFO_ROW':
                                        map_data = widget.get('data', {})
                                        lat = map_data.get('latitude')
                                        lon = map_data.get('longitude')
                                        if lat is not None and lon is not None:
                                             location = {"latitude": float(lat), "longitude": float(lon)}
                                             logging.debug(f"[{token}] Extracted location from PRELOADED_STATE: {location}")
                                             break
                              if location: break
                    except (json.JSONDecodeError, ValueError, TypeError, KeyError) as e:
                         logging.debug(f"[{token}] Could not parse PRELOADED_STATE for location: {e}")
                    break # Assume only one preloaded state script

        details['location'] = location
        if not location:
             logging.warning(f"[{token}] Location could not be extracted.")


        # --- Extract Attributes ---
        attributes = []
        processed_titles = set()
        # Target rows more specifically
        attr_rows = soup.select("div[class^='kt-unexpandable-row_item'], div[class^='kt-base-row_item']") # Combine selectors
        # Fallback selectors if the above fail
        if not attr_rows:
             attr_rows = soup.select("div > p[class*='__title'] + p[class*='__value']") # Find value following title
             # This is less structured, needs careful pairing if used

        logging.debug(f"[{token}] Found {len(attr_rows)} potential attribute rows.")

        for row in attr_rows:
             # Try finding title and value within the row context
            title_tag = row.find(['p', 'span', 'dt'], class_=lambda c: c and ('title' in c.lower() or 'label' in c.lower()))
            value_tag = row.find(['p', 'span', 'dd'], class_=lambda c: c and ('value' in c.lower() or 'data' in c.lower()))

            # More robust extraction if tags are siblings
            if not title_tag and value_tag: title_tag = value_tag.find_previous_sibling(['p','span','dt'])
            if not value_tag and title_tag: value_tag = title_tag.find_next_sibling(['p','span','dd'])


            title = title_tag.get_text(strip=True) if title_tag else None
            # Clean common suffixes like " :"
            if title: title = title.strip().rstrip(':').strip()

            value = value_tag.get_text(strip=True) if value_tag else None

            if title and title not in processed_titles:
                key = None
                # Check for icons or other markers for 'key'
                icon_tag = row.find('i', class_=lambda c: c and 'kt-icon-' in c) # More specific icon search
                if icon_tag and icon_tag.get('class'):
                    for css_class in icon_tag['class']:
                        if 'kt-icon-' in css_class: # Divar specific pattern
                            key_candidate = css_class.split('kt-icon-')[-1].upper()
                            # Simple validation: Ensure it's not just a generic icon class
                            if len(key_candidate) > 2 and not any(char.isdigit() for char in key_candidate):
                                 key = key_candidate
                                 break # Found a plausible key

                attr_dict = {"title": title}
                if value:
                    attr_dict["value"] = value
                if key:
                    attr_dict["key"] = key
                    # Refined boolean check
                    if value is None and any(neg in title for neg in ["ندارد", "نیست"]):
                         attr_dict["available"] = False
                    elif value is None: # No value and no negative word => likely available
                         attr_dict["available"] = True

                attributes.append(attr_dict)
                processed_titles.add(title)
            elif title:
                 logging.debug(f"[{token}] Skipping duplicate attribute title: {title}")

        details['attributes'] = attributes
        logging.debug(f"[{token}] Extracted {len(attributes)} attributes.")


        # --- Extract Price ---
        price = None
        price_per_meter = None
        # Look for rows specifically containing price information
        price_rows = soup.select("div[class*='kt-unexpandable-row']:has(p:contains('قیمت'))")
        for prow in price_rows:
             title_tag = prow.find('p', class_=lambda c: c and 'title' in c)
             value_tag = prow.find('p', class_=lambda c: c and 'value' in c)
             if title_tag and value_tag:
                  title_text = title_tag.get_text(strip=True)
                  value_text = value_tag.get_text(strip=True).replace(' تومان', '')
                  parsed_value = parse_persian_number(value_text)
                  if 'قیمت کل' in title_text and price is None:
                       price = parsed_value
                  elif 'قیمت هر متر' in title_text and price_per_meter is None:
                       price_per_meter = parsed_value

        details['price'] = price
        details['price_per_meter'] = price_per_meter # Add this field
        logging.debug(f"[{token}] Extracted price: {price}, price/meter: {price_per_meter}")


        logging.info(f"[{token}] Successfully parsed details for: {details.get('title', 'N/A')}")
        return details

    except Exception as e:
        logging.error(f"[{token}] Error during BeautifulSoup parsing: {e}", exc_info=True)
        return None


def transform_for_db(extracted_data: dict) -> dict:
    """
    Transforms the extracted data dictionary into the format expected
    by the import_mongodb_property PostgreSQL function.
    """
    if not extracted_data:
        return None

    # Prepare data for the SQL function call
    db_data = {
        "p_external_id": extracted_data.get("external_id"),
        "p_title": extracted_data.get("title"),
        "p_description": extracted_data.get("description"),
        "p_price": extracted_data.get("price"),
        "p_location": extracted_data.get("location"),
        "p_attributes": extracted_data.get("attributes"),
        "p_image_urls": extracted_data.get("image_urls"),
        # --- Analytics/Placeholder Data ---
        "p_investment_score": extracted_data.get("Property Investment Score"),
        "p_market_trend": extracted_data.get("Market Trend Prediction"),
        "p_neighborhood_fit_score": extracted_data.get("Neighborhood Fit Score"),
        "p_rent_to_price_ratio": extracted_data.get("r/p"),
        "p_highlight_flags": extracted_data.get("Property Highlight Flags"),
        "p_similar_properties": extracted_data.get("Similar Properties Comparison")
    }

    # Clean up None values for JSONB fields before passing to DB function
    # Ensure arrays are empty lists if None, location remains None if missing
    for key in ["p_attributes", "p_image_urls", "p_highlight_flags", "p_similar_properties"]:
         if db_data[key] is None:
             db_data[key] = []

    # Price conversion (already done in extractor, but double-check type)
    if db_data["p_price"] is not None:
        try:
            # Ensure it's int or bigint compatible
            db_data["p_price"] = int(float(db_data["p_price"]))
        except (ValueError, TypeError):
            logging.warning(f"Could not convert price {db_data['p_price']} to int for {db_data['p_external_id']}")
            db_data["p_price"] = None

    # Add price_per_meter if extracted
    price_per_meter = extracted_data.get('price_per_meter')
    if price_per_meter is not None:
         try:
              # Assuming the migration expects bigint for price_per_meter too
              # You might need to adjust SQL if it expects NUMERIC
              # db_data["p_price_per_meter"] = int(float(price_per_meter)) # Add if needed by function
              # Currently, the SQL function doesn't take price_per_meter,
              # it gets set via the JSONB attributes or trigger potentially.
              # Let's add it to the attributes JSONB for now.
              if db_data["p_attributes"]:
                   db_data["p_attributes"].append({"title": "قیمت هر متر", "value": str(price_per_meter)}) # Store as string
         except (ValueError, TypeError):
              logging.warning(f"Could not convert price_per_meter {price_per_meter} for {db_data['p_external_id']}")


    # Basic validation before returning
    if not db_data["p_external_id"] or not db_data["p_title"]:
        logging.error(f"Missing critical data (ID or Title) for token {extracted_data.get('external_id')}. Skipping DB insert.")
        return None

    return db_data
