import json
import re
import logging
from bs4 import BeautifulSoup

# Helper to convert Persian/Arabic numbers to Latin digits
persian_num_map = str.maketrans('۰۱۲۳۴۵۶۷۸۹', '0123456789')
arabic_num_map = str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')

def parse_persian_number(s):
    """Converts a string containing Persian/Arabic numbers and commas to an integer."""
    if not s:
        return None
    try:
        cleaned_s = s.translate(persian_num_map).translate(arabic_num_map).replace(',', '').strip()
        # Handle potential non-numeric characters remaining after translation
        cleaned_s = re.sub(r'[^\d.-]+', '', cleaned_s)
        if not cleaned_s: return None
        # Try converting to float first to handle decimals, then to int if possible
        num = float(cleaned_s)
        return int(num) if num.is_integer() else num
    except (ValueError, TypeError) as e:
        logging.warning(f"Could not parse number string '{s}': {e}")
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

    try:
        # --- Extract Core Information ---
        title_tag = soup.find('h1') # Adjust selector if needed
        details['title'] = title_tag.get_text(strip=True) if title_tag else 'N/A'

        # Description often in a specific div, might need adjustment
        desc_div = soup.find('div', class_=lambda c: c and 'kt-description-row__text' in c) # Example class
        details['description'] = desc_div.get_text(separator='\n', strip=True) if desc_div else ''

        # --- Extract Images ---
        image_urls = []
        # Look for image tags, often within specific containers or carousels
        img_tags = soup.select('picture img[src*="divarcdn"]') # Example selector
        for img in img_tags:
            if img.get('src'):
                image_urls.append(img['src'])
        details['image_urls'] = list(set(image_urls)) # Use set to remove duplicates

        # --- Extract Location ---
        # Location data is often embedded in a script tag as JSON
        location = None
        script_tags = soup.find_all('script', type='application/ld+json')
        if not script_tags:
             script_tags = soup.find_all('script') # Broader search if ld+json fails

        for script in script_tags:
            try:
                script_content = script.string
                if script_content and '"geo"' in script_content.lower():
                     # Attempt to find JSON within the script
                    json_match = re.search(r'{.*"latitude"\s*:\s*([\d.]+).*"longitude"\s*:\s*([\d.]+).*?}', script_content, re.DOTALL | re.IGNORECASE)
                    if json_match:
                        lat = float(json_match.group(1))
                        lon = float(json_match.group(2))
                        location = {"latitude": lat, "longitude": lon}
                        logging.debug(f"Extracted location from script: {location}")
                        break # Found location
                    else: # Try parsing as full JSON if regex fails
                        data = json.loads(script_content)
                        # Check common paths for geo data (adjust as needed)
                        if isinstance(data, dict) and 'geo' in data and isinstance(data['geo'], dict):
                            geo_data = data['geo']
                            if 'latitude' in geo_data and 'longitude' in geo_data:
                                location = {
                                    "latitude": float(geo_data['latitude']),
                                    "longitude": float(geo_data['longitude'])
                                }
                                break
                        elif isinstance(data, list): # Sometimes it's in a list
                            for item in data:
                                if isinstance(item, dict) and 'geo' in item and isinstance(item['geo'], dict):
                                     geo_data = item['geo']
                                     if 'latitude' in geo_data and 'longitude' in geo_data:
                                        location = {
                                            "latitude": float(geo_data['latitude']),
                                            "longitude": float(geo_data['longitude'])
                                        }
                                        break
                            if location: break

            except (json.JSONDecodeError, TypeError, KeyError, ValueError) as e:
                # logging.debug(f"Could not parse script tag for location: {e}")
                continue # Try next script tag
        details['location'] = location

        # --- Extract Attributes ---
        # Attributes are often in definition lists (dl/dt/dd) or specific divs
        attributes = []
        # Example using divs with specific classes (adjust based on actual HTML)
        attr_rows = soup.find_all('div', class_=lambda c: c and 'kt-unexpandable-row__item' in c)
        if not attr_rows:
             # Fallback: Look for definition lists maybe?
             attr_rows = soup.select('dl > div') # Another common pattern

        processed_titles = set() # Avoid duplicate attributes if structure is weird

        for row in attr_rows:
            title_tag = row.find(['p', 'span', 'dt'], class_=lambda c: c and ('kt-unexpandable-row__title' in c or 'attribute-title' in c))
            value_tag = row.find(['p', 'span', 'dd'], class_=lambda c: c and ('kt-unexpandable-row__value' in c or 'attribute-value' in c))

            title = title_tag.get_text(strip=True) if title_tag else None
            value = value_tag.get_text(strip=True) if value_tag else None

            if title and title not in processed_titles:
                # Try to extract a key if available (e.g., from an icon class or data attribute)
                key = None
                icon_tag = row.find('div', class_=lambda c: c and 'kt-unexpandable-row__icon' in c)
                if icon_tag and icon_tag.get('class'):
                    # Example: Extract key from icon class like 'Icon--balcony' -> 'BALCONY'
                    for css_class in icon_tag['class']:
                        if 'Icon--' in css_class:
                            key = css_class.split('Icon--')[-1].upper()
                            break

                attr_dict = {"title": title}
                if value:
                    attr_dict["value"] = value
                if key:
                    attr_dict["key"] = key
                    # Check if this represents a boolean feature based on title
                    if value is None and any(neg in title for neg in ["ندارد", "نیست"]):
                         attr_dict["available"] = False
                    elif value is None:
                         attr_dict["available"] = True # Assume available if no value and no negation

                attributes.append(attr_dict)
                processed_titles.add(title)

        details['attributes'] = attributes

        # --- Extract Price (often needs specific selectors) ---
        price = None
        price_tag = soup.find('p', class_=lambda c: c and 'kt-unexpandable-row__value' in c and ' تومان' in c.get_text())
        if price_tag:
            price_text = price_tag.get_text(strip=True).replace(' تومان', '')
            price = parse_persian_number(price_text)
        details['price'] = price

        logging.info(f"[{token}] Successfully extracted details for: {details.get('title', 'N/A')}")
        return details

    except Exception as e:
        logging.error(f"[{token}] Error parsing details page: {e}")
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
        "p_price": extracted_data.get("price"), # Already parsed
        "p_location": extracted_data.get("location"), # Should be {"latitude": float, "longitude": float}
        "p_attributes": extracted_data.get("attributes"), # Should be list of dicts
        "p_image_urls": extracted_data.get("image_urls"), # Should be list of strings
        # --- Analytics/Placeholder Data ---
        # These fields were in the initial MongoDB example but might not be
        # directly extractable from Divar. We'll set defaults or None.
        "p_investment_score": extracted_data.get("Property Investment Score"), # Assuming it might be in attributes sometimes
        "p_market_trend": extracted_data.get("Market Trend Prediction"),
        "p_neighborhood_fit_score": extracted_data.get("Neighborhood Fit Score"),
        "p_rent_to_price_ratio": extracted_data.get("r/p"),
        "p_highlight_flags": extracted_data.get("Property Highlight Flags"),
        "p_similar_properties": extracted_data.get("Similar Properties Comparison")
    }

    # Clean up None values for JSONB fields before passing to DB function
    for key in ["p_location", "p_attributes", "p_image_urls", "p_highlight_flags", "p_similar_properties"]:
         if db_data[key] is None:
             db_data[key] = [] if key in ["p_attributes", "p_image_urls", "p_highlight_flags", "p_similar_properties"] else None


    # Ensure price is integer if not None
    if db_data["p_price"] is not None:
        try:
            db_data["p_price"] = int(db_data["p_price"])
        except (ValueError, TypeError):
            logging.warning(f"Could not convert price {db_data['p_price']} to int for {db_data['p_external_id']}")
            db_data["p_price"] = None # Set to null if conversion fails

    return db_data
