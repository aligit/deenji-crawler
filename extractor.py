import json
import re
import logging
from bs4 import BeautifulSoup

# ... (keep parse_persian_number and number maps from previous version) ...
persian_num_map = str.maketrans('۰۱۲۳۴۵۶۷۸۹', '0123456789')
arabic_num_map = str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')

def parse_persian_number(s):
    """Converts a string containing Persian/Arabic numbers and commas to an integer or float."""
    if not s or not isinstance(s, str): return None
    try:
        cleaned_s = s.translate(persian_num_map).translate(arabic_num_map)
        cleaned_s = cleaned_s.replace(',', '').strip()
        cleaned_s = re.sub(r'[^\d.-]+', '', cleaned_s)
        if not cleaned_s or cleaned_s == '-': return None
        num = float(cleaned_s)
        return int(num) if num == int(num) else num
    except (ValueError, TypeError) as e:
        logging.debug(f"Could not parse number string '{s}' to number: {e}")
        return None


def extract_property_details(html_content: str, token: str) -> dict | None:
    """ Parses the HTML content of a Divar property detail page. """
    if not html_content:
        logging.warning(f"[{token}] HTML content is empty.")
        return None

    soup = BeautifulSoup(html_content, 'lxml')
    details = {"external_id": token}
    logging.debug(f"[{token}] Starting HTML parsing.")

    try:
        # --- Extract Title (More Robust Selectors) ---
        title = None
        # Try specific class first
        title_tag = soup.select_one("h1[class*='kt-page-title__title']")
        if title_tag:
            title = title_tag.get_text(strip=True)
        else:
            # Fallback 1: Any H1 within common main content areas
            main_content_areas = ['main', 'article', "div[class*='kt-col']", "div[class*='post-page']"]
            for area_selector in main_content_areas:
                 area = soup.select_one(area_selector)
                 if area:
                      h1 = area.find('h1')
                      if h1:
                           title = h1.get_text(strip=True)
                           logging.debug(f"[{token}] Found title using fallback selector: {area_selector} > h1")
                           break
            # Fallback 2: Use the <title> tag content if H1 fails
            if not title:
                 title_tag_html = soup.find('title')
                 if title_tag_html:
                      title = title_tag_html.get_text(strip=True)
                      # Often includes extra site info, try to clean it
                      title = title.split('|')[0].split('-')[0].strip()
                      logging.debug(f"[{token}] Found title using fallback <title> tag.")

        details['title'] = title if title else 'N/A'
        if details['title'] == 'N/A':
            logging.warning(f"[{token}] Title extraction failed with all selectors.")


        # --- Extract Description (More Robust Selectors) ---
        description = None
        # Try specific class first
        desc_tag = soup.select_one("p[class*='kt-description-row__text--primary']")
        if desc_tag:
            description = desc_tag.get_text(separator='\n', strip=True)
        else:
            # Fallback 1: Look for a div often containing the description text
            desc_container = soup.select_one("div[class*='kt-description-row']") # Find the container
            if desc_container:
                desc_p = desc_container.find('p') # Find any <p> inside it
                if desc_p:
                    description = desc_p.get_text(separator='\n', strip=True)
                    logging.debug(f"[{token}] Found description using fallback container selector.")
            # Fallback 2: Look for a div that might *directly* contain description text
            if not description:
                 desc_div = soup.select_one("div[class*='description__text']") # Another common pattern
                 if desc_div:
                     description = desc_div.get_text(separator='\n', strip=True)
                     logging.debug(f"[{token}] Found description using fallback direct div selector.")

        details['description'] = description if description else ''
        if not details['description']:
            logging.warning(f"[{token}] Description extraction failed.")


        # --- Extract Images (Keep previous logic, seems okay) ---
        image_urls = []
        picture_tags = soup.select('div[class*=kt-carousel] picture img[src*="divarcdn"]')
        for img in picture_tags:
            src = img.get('src')
            # Prioritize higher resolution sources if available in srcset
            srcset = img.get('srcset')
            if srcset:
                 sources = [s.strip().split(' ')[0] for s in srcset.split(',')]
                 if sources: src = sources[-1] # Assume last one is highest res
            if src and src not in image_urls:
                image_urls.append(src)
        details['image_urls'] = image_urls
        logging.debug(f"[{token}] Found {len(image_urls)} images.")


        # --- Extract Location (Keep previous logic, seems okay) ---
        location = None
        # ... (keep the ld+json and PRELOADED_STATE logic from previous version) ...
        script_tags_ld = soup.find_all('script', type='application/ld+json')
        for script in script_tags_ld:
            try:
                data = json.loads(script.string)
                items_to_check = []
                if isinstance(data, list): items_to_check.extend(data)
                elif isinstance(data, dict): items_to_check.append(data)
                for item in items_to_check:
                    if isinstance(item, dict) and item.get('@type') in ['Apartment', 'Place', 'GeoCoordinates'] and 'geo' in item:
                         geo_data = item['geo']
                         if isinstance(geo_data, dict) and 'latitude' in geo_data and 'longitude' in geo_data:
                             try:
                                 lat = float(geo_data['latitude'])
                                 lon = float(geo_data['longitude'])
                                 location = {"latitude": lat, "longitude": lon}; break
                             except (ValueError, TypeError): pass
                    # Direct GeoCoordinates check
                    elif isinstance(item, dict) and item.get('@type') == 'GeoCoordinates' and 'latitude' in item and 'longitude' in item:
                         try:
                            lat = float(item['latitude']); lon = float(item['longitude'])
                            location = {"latitude": lat, "longitude": lon}; break
                         except (ValueError, TypeError): pass
                if location: break
            except (json.JSONDecodeError, TypeError, KeyError): continue

        if not location:
            script_tags_preload = soup.find_all('script')
            for script in script_tags_preload:
                if script.string and 'window.__PRELOADED_STATE__' in script.string:
                    try:
                        match = re.search(r'window\.__PRELOADED_STATE__\s*=\s*({.*?});', script.string, re.DOTALL)
                        if match:
                            preloaded_data = json.loads(match.group(1))
                            # Updated path based on inspection/common patterns
                            map_info_widget = next((w for section in preloaded_data.get('currentPost', {}).get('post', {}).get('sections', {}).values()
                                                    for w in section if isinstance(w, dict) and w.get('widget_type') == 'MAP_INFO_ROW'), None)
                            if map_info_widget:
                                map_data = map_info_widget.get('data', {})
                                lat = map_data.get('latitude')
                                lon = map_data.get('longitude')
                                if lat is not None and lon is not None:
                                    location = {"latitude": float(lat), "longitude": float(lon)}
                                    break
                    except Exception as e: logging.debug(f"[{token}] Error parsing PRELOADED_STATE: {e}")
                    if location: break # Exit outer loop if found

        details['location'] = location
        if not location: logging.warning(f"[{token}] Location extraction failed.")


        # --- Extract Attributes and Prices (More Robust) ---
        attributes = []
        processed_titles = set()
        price, price_per_meter, area, year_built, bedrooms = None, None, None, None, None

        # General selectors for rows containing label/value pairs
        # Using broader selectors and checking content within the loop
        possible_rows = soup.select("div[class*='unexpandable-row'], div[class*='group-row-item'], li[class*='attribute'], dl > div")
        logging.debug(f"[{token}] Found {len(possible_rows)} potential attribute/info rows.")

        for row in possible_rows:
            # Extract potential title/label and value from the row
            # Look for common patterns: a title element followed by a value element
            title_el = row.find(['p', 'span', 'dt', 'div'], class_=lambda c: c and ('title' in c.lower() or 'label' in c.lower()))
            value_el = row.find(['p', 'span', 'dd', 'div'], class_=lambda c: c and ('value' in c.lower() or 'data' in c.lower()))

            # Alternative: Check for sibling relationship if not direct children
            if not value_el and title_el: value_el = title_el.find_next_sibling(['p', 'span', 'dd', 'div'])
            if not title_el and value_el: title_el = value_el.find_previous_sibling(['p', 'span', 'dt', 'div'])

            # Check direct text if it's a simple row like group-row-item
            if not title_el and not value_el and 'group-row-item' in row.get('class', []):
                 title_el = row.find('span', class_='kt-group-row-item__title')
                 value_el = row # Value is the cell's main text

            title = title_el.get_text(strip=True).strip().rstrip(':').strip() if title_el else None
            value_text_raw = value_el.get_text(strip=True) if value_el else None

            # Clean value text (remove title if it's part of the value like in group rows)
            if value_text_raw and title and is_group_item:
                 value = value_text_raw.replace(title, '').strip()
            else:
                 value = value_text_raw

            if title and title not in processed_titles:
                logging.debug(f"[{token}] Processing row: Title='{title}', Value='{value}'")
                parsed_num = parse_persian_number(value.replace(' تومان', '')) if value else None

                # Assign to specific fields if match found
                if title == 'متراژ': area = parsed_num
                elif title == 'ساخت': year_built = parsed_num
                elif title == 'اتاق': bedrooms = parsed_num
                elif 'قیمت کل' in title: price = parsed_num
                elif 'قیمت هر متر' in title: price_per_meter = parsed_num

                # Extract key for boolean/enum attributes (e.g., from icon)
                key = None
                icon_tag = row.find('i', class_=lambda c: c and 'kt-icon-' in c)
                if icon_tag and icon_tag.get('class'):
                    for css_class in icon_tag['class']:
                        if 'kt-icon-' in css_class:
                            key_candidate = css_class.split('kt-icon-')[-1].upper().replace('-', '_') # Convert to snake_case like PARKING
                            if len(key_candidate) > 2 and not any(char.isdigit() for char in key_candidate):
                                key = key_candidate; break

                # Add to generic attributes list
                attr_dict = {"title": title}
                if value: attr_dict["value"] = value
                if key:
                    attr_dict["key"] = key
                    if value is None and any(neg in title for neg in ["ندارد", "نیست", "فاقد"]): attr_dict["available"] = False
                    elif value is None: attr_dict["available"] = True

                attributes.append(attr_dict)
                processed_titles.add(title)


        details['price'] = price
        details['price_per_meter'] = price_per_meter
        details['area'] = area
        details['year_built'] = year_built
        details['bedrooms'] = bedrooms
        details['attributes'] = attributes

        logging.info(f"[{token}] Successfully parsed details. Title: '{details.get('title', 'N/A')}', Price: {price}")
        return details

    except Exception as e:
        logging.error(f"[{token}] Error during BeautifulSoup parsing: {e}", exc_info=True)
        # Save raw HTML for debugging failed parses
        debug_html_path = Path(JSON_OUTPUT_DIR) / f"{token}_error.html"
        try:
             debug_html_path.write_text(html_content or "", encoding='utf-8')
             logging.info(f"[{token}] Saved raw HTML to {debug_html_path} for debugging.")
        except Exception as write_e:
             logging.error(f"[{token}] Failed to save debug HTML: {write_e}")
        return None

# --- Transform Function (Keep as is, but added logging) ---
def transform_for_db(extracted_data: dict) -> dict | None:
    """ Transforms the extracted data for the DB function. """
    if not extracted_data: return None

    # Basic validation moved here
    if not extracted_data.get("external_id") or not extracted_data.get("title") or extracted_data.get("title") == 'N/A':
        logging.error(f"Transform: Missing critical data (ID or Title) for token {extracted_data.get('external_id')}. Skipping.")
        return None

    db_data = {
        "p_external_id": extracted_data.get("external_id"),
        "p_title": extracted_data.get("title"),
        "p_description": extracted_data.get("description"),
        "p_price": extracted_data.get("price"),
        "p_location": extracted_data.get("location"),
        "p_attributes": extracted_data.get("attributes", []),
        "p_image_urls": extracted_data.get("image_urls", []),
        "p_investment_score": None, "p_market_trend": None, "p_neighborhood_fit_score": None,
        "p_rent_to_price_ratio": None, "p_highlight_flags": [], "p_similar_properties": []
    }

    # Add core fields to attributes if not already present by title match
    core_attrs = {
        'متراژ': extracted_data.get('area'),
        'ساخت': extracted_data.get('year_built'),
        'اتاق': extracted_data.get('bedrooms'),
        'قیمت هر متر': extracted_data.get('price_per_meter')
    }
    existing_attr_titles = {a['title'] for a in db_data['p_attributes']}
    for title, value in core_attrs.items():
        if value is not None and title not in existing_attr_titles:
            db_data['p_attributes'].append({"title": title, "value": str(value)}) # Ensure value is string for JSON

    # Clean up price type
    if db_data["p_price"] is not None:
        try: db_data["p_price"] = int(float(db_data["p_price"]))
        except (ValueError, TypeError): db_data["p_price"] = None

    logging.debug(f"[{db_data['p_external_id']}] Transformed data for DB: { {k:v for k,v in db_data.items() if k != 'p_attributes'} }") # Log without huge attributes list
    return db_data
