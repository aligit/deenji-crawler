import json
import re
import logging
from bs4 import BeautifulSoup
from pathlib import Path # Import Path

# Define JSON_OUTPUT_DIR here as well if saving debug files
JSON_OUTPUT_DIR = "output_json"
Path(JSON_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


# --- Number Parsing ---
persian_num_map = str.maketrans('۰۱۲۳۴۵۶۷۸۹', '0123456789')
arabic_num_map = str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')

def parse_persian_number(s):
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

# --- Main Extraction Logic ---
def extract_property_details(html_content: str, token: str) -> dict | None:
    if not html_content:
        logging.warning(f"[{token}] HTML content is empty.")
        return None

    soup = BeautifulSoup(html_content, 'lxml')
    details = {"external_id": token}
    logging.debug(f"[{token}] Starting HTML parsing. Content length: {len(html_content)}")

    try:
        # --- Extract Title ---
        title = None
        title_tag = soup.select_one("h1.kt-page-title__title.kt-page-title__title--responsive-sized")
        if not title_tag: title_tag = soup.select_one("h1[class*='kt-page-title__title']") # Fallback 1
        if not title_tag: # Fallback 2
            main_content_areas = ['main', 'article', "div[class*='kt-col-5']"]
            for area_selector in main_content_areas:
                area = soup.select_one(area_selector); h1 = area.find('h1') if area else None
                if h1: title_tag = h1; break
        if not title_tag: title_tag = soup.find('title') # Fallback 3

        if title_tag:
            title = title_tag.get_text(strip=True)
            if title_tag.name == 'title': title = title.split('|')[0].split('-')[0].strip() # Clean <title>
            details['title'] = title if title else 'N/A'
        else:
            details['title'] = 'N/A'

        if details['title'] == 'N/A': logging.warning(f"[{token}] Title extraction FAILED.")
        else: logging.info(f"[{token}] Extracted Title: '{details['title'][:50]}...'")

        # --- Extract Description ---
        description = None
        desc_tag = soup.select_one("p.kt-description-row__text--primary") # Primary selector
        if not desc_tag: # Fallback 1
            desc_title_row = soup.find(lambda tag: tag.name in ['h2', 'div', 'span'] and 'توضیحات' in tag.get_text(strip=True) and 'title' in tag.get('class', []))
            if desc_title_row:
                 desc_row = desc_title_row.find_next_sibling('div', class_=lambda c: c and 'kt-description-row' in c)
                 if desc_row: desc_p = desc_row.find('p'); description = desc_p.get_text(separator='\n', strip=True) if desc_p else None
        if not description: # Fallback 2
             main_content_areas = ['main', 'article', "div[class*='kt-col-5']"]
             for area_selector in main_content_areas:
                 area = soup.select_one(area_selector)
                 if area:
                      possible_desc_p = area.find('p', class_=lambda c: c and 'text' in c and len(c) <= 2)
                      if possible_desc_p and len(possible_desc_p.get_text(strip=True)) > 20: # Min length for desc
                           description = possible_desc_p.get_text(separator='\n', strip=True); break

        details['description'] = description if description else ''
        if not details['description']: logging.warning(f"[{token}] Description extraction FAILED.")
        else: logging.info(f"[{token}] Extracted Description: '{details['description'][:50]}...'")

        # --- Extract Images ---
        image_urls = []
        picture_tags = soup.select('div[class*=kt-carousel] picture img[src*="divarcdn"]')
        for img in picture_tags:
            src = img.get('src'); srcset = img.get('srcset')
            if srcset: sources = [s.strip().split(' ')[0] for s in srcset.split(',')]; src = sources[-1] if sources else src
            if src and src not in image_urls: image_urls.append(src)
        details['image_urls'] = image_urls; logging.debug(f"[{token}] Found {len(image_urls)} images.")

        # --- Extract Location ---
        location = None
        script_tags_ld = soup.find_all('script', type='application/ld+json')
        for script in script_tags_ld: # Corrected comment
            try:
                data = json.loads(script.string); items_to_check = []
                if isinstance(data, list): items_to_check.extend(data)
                elif isinstance(data, dict): items_to_check.append(data)
                for item in items_to_check:
                    geo_data = item.get('geo') if isinstance(item, dict) and item.get('@type') == 'Apartment' else None
                    if not geo_data and isinstance(item, dict) and item.get('@type') == 'GeoCoordinates': geo_data = item
                    if isinstance(geo_data, dict) and 'latitude' in geo_data and 'longitude' in geo_data:
                        try: lat = float(geo_data['latitude']); lon = float(geo_data['longitude']); location = {"latitude": lat, "longitude": lon}; break
                        except (ValueError, TypeError): pass
                if location: break
            except (json.JSONDecodeError, TypeError, KeyError): continue
        if not location: # Check PRELOADED_STATE
             script_tags_preload = soup.find_all('script')
             for script in script_tags_preload:
                 if script.string and 'window.__PRELOADED_STATE__' in script.string:
                     try:
                         match = re.search(r'window\.__PRELOADED_STATE__\s*=\s*({.*?});', script.string, re.DOTALL)
                         if match:
                              preloaded_data = json.loads(match.group(1))
                              map_info_widget = next((w for section in preloaded_data.get('currentPost', {}).get('post', {}).get('sections', {}).values() for w in section if isinstance(w, dict) and w.get('widget_type') == 'MAP_INFO_ROW'), None)
                              if map_info_widget:
                                   map_data = map_info_widget.get('data', {}); lat = map_data.get('latitude'); lon = map_data.get('longitude')
                                   if lat is not None and lon is not None: location = {"latitude": float(lat), "longitude": float(lon)}; break
                     except Exception as e: logging.debug(f"[{token}] Error parsing PRELOADED_STATE: {e}")
                     if location: break
        details['location'] = location
        if not location: logging.warning(f"[{token}] Location extraction failed.")

        # --- Extract Attributes and Prices ---
        attributes = []; processed_titles = set()
        price, price_per_meter, area, year_built, bedrooms = None, None, None, None, None
        possible_rows = soup.select("div[class*='unexpandable-row'], div[class*='group-row-item'], li[class*='attribute'], dl > div")
        for row in possible_rows: # Corrected Comment
            is_group_item = 'kt-group-row-item' in row.get('class', [])
            title_el = row.find(['p', 'span', 'dt', 'div'], class_=lambda c: c and ('title' in c.lower() or 'label' in c.lower()))
            value_el = row.find(['p', 'span', 'dd', 'div'], class_=lambda c: c and ('value' in c.lower() or 'data' in c.lower()))
            if not value_el and title_el: value_el = title_el.find_next_sibling(['p', 'span', 'dd', 'div'])
            if not title_el and value_el: title_el = value_el.find_previous_sibling(['p', 'span', 'dt', 'div'])
            if not title_el and not value_el and is_group_item: title_el = row.find('span', class_='kt-group-row-item__title'); value_el = row

            title = title_el.get_text(strip=True).strip().rstrip(':').strip() if title_el else None
            value_text_raw = value_el.get_text(strip=True) if value_el else None
            value = value_text_raw.replace(title, '').strip() if value_text_raw and title and is_group_item else value_text_raw

            if title and title not in processed_titles:
                parsed_num = parse_persian_number(value.replace(' تومان', '')) if value else None
                if title == 'متراژ': area = parsed_num
                elif title == 'ساخت': year_built = parsed_num
                elif title == 'اتاق': bedrooms = parsed_num
                elif 'قیمت کل' in title: price = parsed_num
                elif 'قیمت هر متر' in title: price_per_meter = parsed_num

                key = None; icon_tag = row.find('i', class_=lambda c: c and 'kt-icon-' in c)
                if icon_tag and icon_tag.get('class'):
                    for css_class in icon_tag['class']:
                        if 'kt-icon-' in css_class:
                            key_candidate = css_class.split('kt-icon-')[-1].upper().replace('-', '_')
                            if len(key_candidate) > 2 and not any(c.isdigit() for c in key_candidate): key = key_candidate; break

                attr_dict = {"title": title};
                if value: attr_dict["value"] = value
                if key:
                    attr_dict["key"] = key
                    if value is None and any(neg in title for neg in ["ندارد", "نیست", "فاقد"]): attr_dict["available"] = False
                    elif value is None: attr_dict["available"] = True
                attributes.append(attr_dict); processed_titles.add(title)

        details['price'] = price; details['price_per_meter'] = price_per_meter
        details['area'] = area; details['year_built'] = year_built; details['bedrooms'] = bedrooms
        details['attributes'] = attributes
        logging.debug(f"[{token}] Parsed area: {area}, year: {year_built}, beds: {bedrooms}, price: {price}")

        # Final check and return
        if details['title'] == 'N/A':
            logging.error(f"[{token}] Critical data (Title) could not be extracted. Returning partial data.")

        logging.info(f"[{token}] Successfully parsed details for: '{details.get('title', 'N/A')[:50]}...'")
        return details

    except Exception as e:
        logging.error(f"[{token}] Error during BeautifulSoup parsing: {e}", exc_info=True)
        debug_html_path = Path(JSON_OUTPUT_DIR) / f"{token}_error.html"
        try: debug_html_path.write_text(html_content or "", encoding='utf-8'); logging.info(f"[{token}] Saved raw HTML to {debug_html_path} for debugging.")
        except Exception as write_e: logging.error(f"[{token}] Failed to save debug HTML: {write_e}")
        return None

# --- Transform Function ---
def transform_for_db(extracted_data: dict) -> dict | None:
    """ Transforms the extracted data for the DB function. """
    if not extracted_data: return None
    if not extracted_data.get("external_id") or not extracted_data.get("title") or extracted_data.get("title") == 'N/A':
        logging.error(f"Transform: Missing critical data (ID or Title) for token {extracted_data.get('external_id')}. Skipping DB insert.")
        return None
    db_data = {
        "p_external_id": extracted_data.get("external_id"), "p_title": extracted_data.get("title"),
        "p_description": extracted_data.get("description"), "p_price": extracted_data.get("price"),
        "p_location": extracted_data.get("location"), "p_attributes": extracted_data.get("attributes", []),
        "p_image_urls": extracted_data.get("image_urls", []), "p_investment_score": None, "p_market_trend": None,
        "p_neighborhood_fit_score": None, "p_rent_to_price_ratio": None, "p_highlight_flags": [], "p_similar_properties": []
    }
    core_attrs = {'متراژ': extracted_data.get('area'), 'ساخت': extracted_data.get('year_built'), 'اتاق': extracted_data.get('bedrooms'), 'قیمت هر متر': extracted_data.get('price_per_meter')}
    existing_attr_titles = {a['title'] for a in db_data['p_attributes']}
    for title, value in core_attrs.items():
        if value is not None and title not in existing_attr_titles: db_data['p_attributes'].append({"title": title, "value": str(value)})
    if db_data["p_price"] is not None:
        try: db_data["p_price"] = int(float(db_data["p_price"]))
        except (ValueError, TypeError): db_data["p_price"] = None
    logging.debug(f"[{db_data['p_external_id']}] Transformed data for DB.")
    return db_data
