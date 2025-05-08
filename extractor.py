# extractor.py

import json
import re
import logging
from bs4 import BeautifulSoup
from pathlib import Path

JSON_OUTPUT_DIR = "output_json"
Path(JSON_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

persian_num_map = str.maketrans('۰۱۲۳۴۵۶۷۸۹', '0123456789')
arabic_num_map = str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')

def parse_persian_number(s):
    # ... (function remains the same) ...
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
    if not html_content:
        logging.warning(f"[{token}] HTML content is empty.")
        return None

    soup = BeautifulSoup(html_content, 'lxml')
    details = {"external_id": token}
    logging.debug(f"[{token}] Starting HTML parsing. Content length: {len(html_content)}")

    try:
        # --- Extract Title (Keep previous robust logic) ---
        title = None
        # ... (keep all title fallbacks from previous version) ...
        title_tag = soup.select_one("h1.kt-page-title__title.kt-page-title__title--responsive-sized")
        if not title_tag: title_tag = soup.select_one("h1[class*='kt-page-title__title']")
        if not title_tag:
            main_content_areas = ['main', 'article', "div[class*='kt-col-5']"]
            for area_selector in main_content_areas:
                area = soup.select_one(area_selector); h1 = area.find('h1') if area else None
                if h1: title_tag = h1; logging.debug(f"[{token}] Found title via fallback {area_selector} > h1"); break
        if not title_tag: title_tag = soup.find('title')

        if title_tag:
            title = title_tag.get_text(strip=True)
            if title_tag.name == 'title': title = title.split('|')[0].split('-')[0].strip()
            details['title'] = title if title else 'N/A'
        else: details['title'] = 'N/A'

        if details['title'] == 'N/A': logging.warning(f"[{token}] Title extraction FAILED.")
        else: logging.info(f"[{token}] Extracted Title: '{details['title'][:50]}...'")


        # --- Extract Description (NEW Robust Selectors) ---
        description = None
        # 1. Try the highly specific selector provided by user (slightly adapted for robustness)
        desc_tag = soup.select_one("div[class*='kt-description-row'] > div > p[class*='kt-description-row__text']")
        if desc_tag:
            description = desc_tag.get_text(separator='\n', strip=True)
            logging.debug(f"[{token}] Found description using user's specific selector pattern.")
        else:
            # 2. Try the less specific class provided by user
            desc_tag = soup.select_one("p.kt-description-row__text--primary")
            if desc_tag:
                description = desc_tag.get_text(separator='\n', strip=True)
                logging.debug(f"[{token}] Found description using primary text class.")
            else:
                # 3. Fallback: Find the 'توضیحات' heading and the description row after it
                desc_title_row = soup.find(lambda tag: tag.name in ['h2', 'div', 'span'] and \
                                           'توضیحات' in tag.get_text(strip=True) and \
                                           'title' in tag.get('class', []))
                if desc_title_row:
                     desc_row = desc_title_row.find_next_sibling('div', class_=lambda c: c and 'kt-description-row' in c)
                     if desc_row:
                          desc_p = desc_row.find('p') # Find first <p> within that row
                          if desc_p:
                               description = desc_p.get_text(separator='\n', strip=True)
                               logging.debug(f"[{token}] Found description using 'توضیحات' title fallback.")

                # 4. Fallback: Any paragraph inside the main content column with significant text
                if not description:
                     main_col = soup.select_one("div[class*='kt-col-5']") # Divar often uses kt-col-5 for main info
                     if main_col:
                          possible_desc_tags = main_col.find_all('p', limit=10) # Limit search depth
                          for p_tag in possible_desc_tags:
                              p_text = p_tag.get_text(strip=True)
                              if len(p_text) > 50: # Heuristic: description is usually longer
                                   # Avoid paragraphs that are clearly attribute values
                                   prev_sibling = p_tag.find_previous_sibling()
                                   if not (prev_sibling and 'title' in prev_sibling.get('class',[])):
                                        description = p_text
                                        logging.debug(f"[{token}] Found description using general main column paragraph fallback.")
                                        break
        details['description'] = description if description else ''
        if not details['description']: logging.warning(f"[{token}] Description extraction FAILED.")
        else: logging.info(f"[{token}] Extracted Description: '{details['description'][:50]}...'")


        # --- Images, Location, Attributes, Prices (Keep previous improved logic) ---
        # ... (Image logic) ...
        image_urls = []; picture_tags = soup.select('div[class*=kt-carousel] picture img[src*="divarcdn"]')
        for img in picture_tags:
            src = img.get('src'); srcset = img.get('srcset')
            if srcset: sources = [s.strip().split(' ')[0] for s in srcset.split(',')]; src = sources[-1] if sources else src
            if src and src not in image_urls: image_urls.append(src)
        details['image_urls'] = image_urls; logging.debug(f"[{token}] Found {len(image_urls)} images.")

        # ... (Location logic) ...
        location = None; script_tags_ld = soup.find_all('script', type='application/ld+json')
        # ... (Loop through ld+json, then check PRELOADED_STATE as before) ...
        details['location'] = location
        if not location: logging.warning(f"[{token}] Location extraction failed.")

        # ... (Attribute and Price logic - this ALREADY captures key-value pairs) ...
        attributes = []; processed_titles = set()
        price, price_per_meter, area, year_built, bedrooms = None, None, None, None, None
        # Selectors targeting rows/items likely containing key-value data
        possible_rows = soup.select("div[class*='unexpandable-row'], div[class*='group-row-item'], li[class*='attribute-'], dl > div")
        logging.debug(f"[{token}] Found {len(possible_rows)} potential attribute/info rows.")

        for row in possible_rows:
            title_el = row.find(['p', 'span', 'dt', 'div'], class_=lambda c: c and ('title' in c.lower() or 'label' in c.lower()))
            value_el = row.find(['p', 'span', 'dd', 'div'], class_=lambda c: c and ('value' in c.lower() or 'data' in c.lower()))
            # Fallback sibling logic
            if not value_el and title_el: value_el = title_el.find_next_sibling(['p', 'span', 'dd', 'div'])
            if not title_el and value_el: title_el = value_el.find_previous_sibling(['p', 'span', 'dt', 'div'])

            is_group_item = 'group-row-item' in row.get('class', [])
            if not title_el and not value_el and is_group_item: title_el = row.find('span', class_='kt-group-row-item__title'); value_el = row

            title = title_el.get_text(strip=True).strip().rstrip(':').strip() if title_el else None
            value_text_raw = value_el.get_text(strip=True) if value_el else None
            value = value_text_raw.replace(title, '').strip() if value_text_raw and title and is_group_item else value_text_raw

            if title and title not in processed_titles:
                logging.debug(f"[{token}] Processing row: Title='{title}', Value='{value}'")
                parsed_num = parse_persian_number(value.replace(' تومان', '')) if value else None

                # Assign core fields
                if title == 'متراژ': area = parsed_num
                elif title == 'ساخت': year_built = parsed_num
                elif title == 'اتاق': bedrooms = parsed_num
                elif 'قیمت کل' in title: price = parsed_num
                elif 'قیمت هر متر' in title: price_per_meter = parsed_num

                # Extract key for boolean/enum attributes
                key = None; icon_tag = row.find('i', class_=lambda c: c and 'kt-icon-' in c)
                if icon_tag and icon_tag.get('class'):
                    for css_class in icon_tag['class']:
                        if 'kt-icon-' in css_class:
                            key_candidate = css_class.split('kt-icon-')[-1].upper().replace('-', '_')
                            if len(key_candidate) > 2 and not any(c.isdigit() for c in key_candidate): key = key_candidate; break

                # Add to generic attributes list (THIS IS THE KEY-VALUE PAIR LOGIC)
                attr_dict = {"title": title}
                if value is not None: attr_dict["value"] = value # Include None values if that's useful
                if key:
                    attr_dict["key"] = key
                    if value is None and any(neg in title for neg in ["ندارد", "نیست", "فاقد"]): attr_dict["available"] = False
                    elif value is None: attr_dict["available"] = True
                attributes.append(attr_dict); processed_titles.add(title)

        details['price'] = price; details['price_per_meter'] = price_per_meter
        details['area'] = area; details['year_built'] = year_built; details['bedrooms'] = bedrooms
        # The 'attributes' list contains all the key-value pairs found
        details['attributes'] = attributes
        logging.debug(f"[{token}] Parsed area: {area}, year: {year_built}, beds: {bedrooms}, price: {price}")
        logging.debug(f"[{token}] Extracted {len(attributes)} generic attributes.")

        if details['title'] == 'N/A': logging.error(f"[{token}] Critical data (Title) not extracted.")

        logging.info(f"[{token}] Successfully parsed details for: '{details.get('title', 'N/A')[:50]}...'")
        return details

    except Exception as e:
        logging.error(f"[{token}] Error during BeautifulSoup parsing: {e}", exc_info=True)
        # ... (debug html saving logic) ...
        return None


# --- Transform Function (No change needed) ---
def transform_for_db(extracted_data: dict) -> dict | None:
    # ... (Function remains the same as previous version) ...
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
