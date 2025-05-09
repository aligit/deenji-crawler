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
        # --- IMPROVED Title Extraction Logic ---
        title = None
        # Try multiple selectors with more variations
        title_selectors = [
            "h1.kt-page-title__title.kt-page-title__title--responsive-sized",
            "h1[class*='kt-page-title__title']",
            "div.kt-page-title h1",
            "div.kt-page-title__texts h1",
            "div[class*='kt-page-title'] h1",
            "h1"  # Last resort: any h1
        ]
        
        for selector in title_selectors:
            title_tag = soup.select_one(selector)
            if title_tag:
                title = title_tag.get_text(strip=True)
                logging.debug(f"[{token}] Found title using selector: {selector}")
                break
                
        # If still no title, try additional fallbacks
        if not title:
            # Try main content areas
            main_content_areas = ['main', 'article', "div[class*='kt-col-5']"]
            for area_selector in main_content_areas:
                area = soup.select_one(area_selector)
                h1 = area.find('h1') if area else None
                if h1:
                    title_tag = h1
                    title = title_tag.get_text(strip=True)
                    logging.debug(f"[{token}] Found title via fallback {area_selector} > h1")
                    break
                    
            # Try page title as last resort
            if not title:
                title_tag = soup.find('title')
                if title_tag:
                    title = title_tag.get_text(strip=True)
                    title = title.split('|')[0].split('-')[0].strip()
                    logging.debug(f"[{token}] Found title from page <title> tag")
        
        # Clean up title if found
        if title:
            # Remove any extra whitespace or line breaks
            title = ' '.join(title.split())
            details['title'] = title
        else:
            details['title'] = 'N/A'
            
        if details['title'] == 'N/A':
            logging.warning(f"[{token}] Title extraction FAILED.")
        else:
            logging.info(f"[{token}] Extracted Title: '{details['title'][:50]}...'")

        # --- IMPROVED Description Extraction Logic ---
        description = None
        
        # 1. Try a broader range of description selectors
        description_selectors = [
            # Specific selectors
            "div[class*='kt-description-row'] > div > p[class*='kt-description-row__text']",
            "p.kt-description-row__text--primary",
            "div.kt-base-row.kt-base-row--large.kt-description-row div.kt-base-row__start p",
            "div.kt-base-row.kt-base-row--large.kt-description-row p",
            # More general selectors
            "div[class*='description'] p",
            "div[class*='kt-description'] p",
            "div[class*='description-row'] p"
        ]
        
        for selector in description_selectors:
            desc_elements = soup.select(selector)
            if desc_elements:
                # Combine all matching elements
                description = '\n'.join([elem.get_text(strip=True) for elem in desc_elements if elem.get_text(strip=True)])
                if description:
                    logging.debug(f"[{token}] Found description using selector: {selector}")
                    break
        
        # 2. If still no description, try finding by heading text
        if not description:
            # Look for heading with "توضیحات" (description in Persian)
            description_headings = soup.find_all(['h2', 'h3', 'div', 'span'], 
                                               string=lambda s: s and 'توضیحات' in s)
            
            for heading in description_headings:
                # Try to find description in siblings or parent's children
                desc_container = None
                
                # Check next siblings
                desc_container = heading.find_next_sibling(['div', 'p'])
                
                # If not found, check parent's children after this element
                if not desc_container and heading.parent:
                    siblings = list(heading.parent.children)
                    try:
                        idx = siblings.index(heading)
                        if idx < len(siblings) - 1:
                            desc_container = siblings[idx + 1]
                    except ValueError:
                        pass
                
                # If found a container, extract text
                if desc_container:
                    desc_text = desc_container.get_text(strip=True)
                    if len(desc_text) > 30:  # Minimum length for description
                        description = desc_text
                        logging.debug(f"[{token}] Found description after heading: {heading.get_text(strip=True)}")
                        break
        
        # 3. Last resort: Find any substantial paragraph in the main content
        if not description:
            # First, identify the main content column
            main_columns = soup.select("div[class*='kt-col-5'], div[class*='kt-col-6'], article, main")
            
            for main_col in main_columns:
                # Find all paragraphs with substantial text
                paragraphs = main_col.find_all('p')
                substantial_paragraphs = [p for p in paragraphs 
                                         if len(p.get_text(strip=True)) > 80 
                                         and not p.find_parent(['header', 'footer', 'nav'])]
                
                if substantial_paragraphs:
                    # Use the longest paragraph as the description
                    substantial_paragraphs.sort(key=lambda p: len(p.get_text(strip=True)), reverse=True)
                    description = substantial_paragraphs[0].get_text(strip=True)
                    logging.debug(f"[{token}] Found description using longest paragraph in main content")
                    break
        
        # Clean up and store description
        if description:
            # Normalize whitespace
            description = ' '.join(description.split())
            details['description'] = description
        else:
            details['description'] = ''
            
        if not details['description']:
            logging.warning(f"[{token}] Description extraction FAILED.")
        else:
            logging.info(f"[{token}] Extracted Description: '{details['description'][:50]}...'")

        # --- Images, Location, Attributes, Prices ---
        image_urls = []
        picture_tags = soup.select('div[class*=kt-carousel] picture img[src*="divarcdn"]')
        for img in picture_tags:
            src = img.get('src')
            srcset = img.get('srcset')
            if srcset:
                sources = [s.strip().split(' ')[0] for s in srcset.split(',')]
                src = sources[-1] if sources else src
            if src and src not in image_urls:
                image_urls.append(src)
        details['image_urls'] = image_urls
        logging.debug(f"[{token}] Found {len(image_urls)} images.")

        # --- Location logic ---
        location = None
        script_tags_ld = soup.find_all('script', type='application/ld+json')
        # Process location from script tags
        # [Keep your existing code here]
        details['location'] = location
        if not location:
            logging.warning(f"[{token}] Location extraction failed.")

        # --- Enhanced Attribute and Price logic ---
        attributes = []
        processed_titles = set()
        price, price_per_meter, area, year_built, bedrooms = None, None, None, None, None
        land_area, property_type = None, None  # New fields
        has_parking, has_storage, has_balcony = False, False, False  # New boolean flags
        title_deed_type, building_direction, renovation_status = None, None, None
        floor_material, bathroom_type, cooling_system = None, None, None
        heating_system, hot_water_system = None, None
        
        # Enhanced mapping of attribute titles to field names
        field_mapping = {
            'متراژ': 'area',
            'متراژ زمین': 'land_area',
            'نوع ملک': 'property_type',
            'ساخت': 'year_built',
            'اتاق': 'bedrooms',
            'قیمت کل': 'price',
            'قیمت هر متر': 'price_per_meter',
            'پارکینگ': 'has_parking',
            'انباری': 'has_storage',
            'بالکن': 'has_balcony'
        }
        
        # Additional details mapping for the modal dialog
        advanced_field_mapping = {
            'سند': 'title_deed_type',
            'جهت ساختمان': 'building_direction',
            'وضعیت واحد': 'renovation_status',
            'جنس کف': 'floor_material',
            'سرویس بهداشتی': 'bathroom_type',
            'سرمایش': 'cooling_system',
            'گرمایش': 'heating_system',
            'تأمین‌کننده آب گرم': 'hot_water_system'
        }
        
        # Selectors targeting rows/items likely containing key-value data
        possible_rows = soup.select("div[class*='unexpandable-row'], div[class*='group-row-item'], li[class*='attribute-'], dl > div")
        logging.debug(f"[{token}] Found {len(possible_rows)} potential attribute/info rows.")

        for row in possible_rows:
            title_el = row.find(['p', 'span', 'dt', 'div'], class_=lambda c: c and ('title' in c.lower() or 'label' in c.lower()))
            value_el = row.find(['p', 'span', 'dd', 'div'], class_=lambda c: c and ('value' in c.lower() or 'data' in c.lower()))
            
            # Fallback sibling logic
            if not value_el and title_el:
                value_el = title_el.find_next_sibling(['p', 'span', 'dd', 'div'])
            if not title_el and value_el:
                title_el = value_el.find_previous_sibling(['p', 'span', 'dt', 'div'])

            is_group_item = 'group-row-item' in row.get('class', [])
            if not title_el and not value_el and is_group_item:
                title_el = row.find('span', class_='kt-group-row-item__title')
                value_el = row

            title = title_el.get_text(strip=True).strip().rstrip(':').strip() if title_el else None
            value_text_raw = value_el.get_text(strip=True) if value_el else None
            value = value_text_raw.replace(title, '').strip() if value_text_raw and title and is_group_item else value_text_raw

            if title and title not in processed_titles:
                logging.debug(f"[{token}] Processing row: Title='{title}', Value='{value}'")
                parsed_num = parse_persian_number(value.replace(' تومان', '')) if value else None

                # Map known fields based on our mappings
                if title in field_mapping:
                    field_name = field_mapping[title]
                    
                    # Handle numeric fields
                    if field_name in ['area', 'land_area', 'year_built', 'bedrooms', 'price', 'price_per_meter']:
                        locals()[field_name] = parsed_num
                    # Handle boolean fields
                    elif field_name in ['has_parking', 'has_storage', 'has_balcony']:
                        # Set to True if exists in any form (could enhance logic based on values)
                        locals()[field_name] = True if value is None or value == 'دارد' else False
                
                # Handle advanced fields if in the main attributes
                if title in advanced_field_mapping:
                    field_name = advanced_field_mapping[title]
                    locals()[field_name] = value

                # Extract key for boolean/enum attributes
                key = None
                icon_tag = row.find('i', class_=lambda c: c and 'kt-icon-' in c)
                if icon_tag and icon_tag.get('class'):
                    for css_class in icon_tag['class']:
                        if 'kt-icon-' in css_class:
                            key_candidate = css_class.split('kt-icon-')[-1].upper().replace('-', '_')
                            if len(key_candidate) > 2 and not any(c.isdigit() for c in key_candidate):
                                key = key_candidate
                                break

                # Add to generic attributes list
                attr_dict = {"title": title}
                if value is not None:
                    attr_dict["value"] = value
                if key:
                    attr_dict["key"] = key
                    if value is None and any(neg in title for neg in ["ندارد", "نیست", "فاقد"]):
                        attr_dict["available"] = False
                    elif value is None:
                        attr_dict["available"] = True
                
                attributes.append(attr_dict)
                processed_titles.add(title)

        # Look for the modal dialog content
        modal_dialog = soup.select_one("div.modal-dialog__content, div[class*='kt-modal-dialog'] div[class*='content']")
        if modal_dialog:
            logging.debug(f"[{token}] Found modal dialog content, extracting details...")
            
            # Look for features section
            feature_sections = modal_dialog.select("div[class*='dialog-section']")
            for section in feature_sections:
                section_title = section.select_one("div[class*='section-title'], h3, h4")
                if section_title and "ویژگی‌ها" in section_title.get_text(strip=True):
                    feature_items = section.select("div[class*='row-item'], div[class*='unexpandable-row']")
                    
                    for item in feature_items:
                        item_title = item.select_one("div[class*='title'], span[class*='title']")
                        item_value = item.select_one("div[class*='value'], span[class*='value']")
                        
                        if item_title:
                            feature_title = item_title.get_text(strip=True).strip().rstrip(':').strip()
                            feature_value = item_value.get_text(strip=True) if item_value else None
                            
                            # Map to our advanced fields
                            if feature_title in advanced_field_mapping:
                                field_name = advanced_field_mapping[feature_title]
                                locals()[field_name] = feature_value
                                
                                # Add to attributes if not already processed
                                if feature_title not in processed_titles:
                                    attr_dict = {"title": feature_title}
                                    if feature_value:
                                        attr_dict["value"] = feature_value
                                    attributes.append(attr_dict)
                                    processed_titles.add(feature_title)
                
                # Amenities section
                elif section_title and "امکانات" in section_title.get_text(strip=True):
                    amenity_items = section.select("div.row-item, span.row-item")
                    
                    for amenity in amenity_items:
                        amenity_text = amenity.get_text(strip=True)
                        
                        # Check if the amenity contains keywords we're interested in
                        if "پارکینگ" in amenity_text:
                            has_parking = True
                        elif "انباری" in amenity_text:
                            has_storage = True
                        elif "بالکن" in amenity_text:
                            has_balcony = True
                            
                        # Add to attributes if not already processed
                        if amenity_text and amenity_text not in processed_titles:
                            attributes.append({"title": amenity_text, "available": True})
                            processed_titles.add(amenity_text)

        # Assign all extracted values to the details dictionary
        details['price'] = price
        details['price_per_meter'] = price_per_meter
        details['area'] = area
        details['land_area'] = land_area
        details['property_type'] = property_type
        details['year_built'] = year_built
        details['bedrooms'] = bedrooms
        details['has_parking'] = has_parking
        details['has_storage'] = has_storage
        details['has_balcony'] = has_balcony
        details['title_deed_type'] = title_deed_type
        details['building_direction'] = building_direction
        details['renovation_status'] = renovation_status
        details['floor_material'] = floor_material
        details['bathroom_type'] = bathroom_type
        details['cooling_system'] = cooling_system
        details['heating_system'] = heating_system
        details['hot_water_system'] = hot_water_system
        details['attributes'] = attributes
        
        logging.debug(f"[{token}] Parsed area: {area}, year: {year_built}, beds: {bedrooms}, price: {price}")
        logging.debug(f"[{token}] Extracted {len(attributes)} generic attributes.")

        if details['title'] == 'N/A':
            logging.error(f"[{token}] Critical data (Title) not extracted.")

        logging.info(f"[{token}] Successfully parsed details for: '{details.get('title', 'N/A')[:50]}...'")
        return details

    except Exception as e:
        logging.error(f"[{token}] Error during BeautifulSoup parsing: {e}", exc_info=True)
        # Save debug HTML if needed
        if logging.getLogger().level <= logging.DEBUG:
            debug_filename = Path(JSON_OUTPUT_DIR) / f"{token}_error.html"
            try:
                with open(debug_filename, "w", encoding="utf-8") as f:
                    f.write(html_content)
                logging.debug(f"[{token}] Saved debug HTML to {debug_filename}")
            except Exception as debug_e:
                logging.debug(f"[{token}] Could not save debug HTML: {debug_e}")
        return None


# --- Transform Function (No change needed) ---
def transform_for_db(extracted_data: dict) -> dict | None:
    if not extracted_data: return None
    if not extracted_data.get("external_id") or not extracted_data.get("title") or extracted_data.get("title") == 'N/A':
        logging.error(f"Transform: Missing critical data (ID or Title) for token {extracted_data.get('external_id')}. Skipping DB insert.")
        return None
        
    db_data = {
        "p_external_id": extracted_data.get("external_id"), 
        "p_title": extracted_data.get("title"),
        "p_description": extracted_data.get("description"), 
        "p_price": extracted_data.get("price"),
        "p_location": extracted_data.get("location"), 
        "p_attributes": extracted_data.get("attributes", []),
        "p_image_urls": extracted_data.get("image_urls", []), 
        "p_investment_score": None, 
        "p_market_trend": None,
        "p_neighborhood_fit_score": None, 
        "p_rent_to_price_ratio": None, 
        "p_highlight_flags": [], 
        "p_similar_properties": [],
        
        # New fields from the enhancement
        "p_area": extracted_data.get("area"),
        "p_price_per_meter": extracted_data.get("price_per_meter"),
        "p_year_built": extracted_data.get("year_built"),
        "p_bedrooms": extracted_data.get("bedrooms"),
        "p_land_area": extracted_data.get("land_area"),
        "p_property_type": extracted_data.get("property_type"),
        "p_has_parking": extracted_data.get("has_parking", False),
        "p_has_storage": extracted_data.get("has_storage", False),
        "p_has_balcony": extracted_data.get("has_balcony", False),
        "p_title_deed_type": extracted_data.get("title_deed_type"),
        "p_building_direction": extracted_data.get("building_direction"),
        "p_renovation_status": extracted_data.get("renovation_status"),
        "p_floor_material": extracted_data.get("floor_material"),
        "p_bathroom_type": extracted_data.get("bathroom_type"),
        "p_cooling_system": extracted_data.get("cooling_system"),
        "p_heating_system": extracted_data.get("heating_system"),
        "p_hot_water_system": extracted_data.get("hot_water_system")
    }
    
    # Add core attributes to the attributes array if they're not already there
    core_attrs = {
        'متراژ': extracted_data.get('area'), 
        'متراژ زمین': extracted_data.get('land_area'),
        'نوع ملک': extracted_data.get('property_type'),
        'ساخت': extracted_data.get('year_built'), 
        'اتاق': extracted_data.get('bedrooms'), 
        'قیمت کل': extracted_data.get('price'),
        'قیمت هر متر': extracted_data.get('price_per_meter'),
        'پارکینگ': 'دارد' if extracted_data.get('has_parking') else 'ندارد',
        'انباری': 'دارد' if extracted_data.get('has_storage') else 'ندارد',
        'بالکن': 'دارد' if extracted_data.get('has_balcony') else 'ندارد',
        'سند': extracted_data.get('title_deed_type'),
        'جهت ساختمان': extracted_data.get('building_direction'),
        'وضعیت واحد': extracted_data.get('renovation_status'),
        'جنس کف': extracted_data.get('floor_material'),
        'سرویس بهداشتی': extracted_data.get('bathroom_type'),
        'سرمایش': extracted_data.get('cooling_system'),
        'گرمایش': extracted_data.get('heating_system'),
        'تأمین‌کننده آب گرم': extracted_data.get('hot_water_system')
    }
    
    existing_attr_titles = {a['title'] for a in db_data['p_attributes']}
    
    for title, value in core_attrs.items():
        if value is not None and title not in existing_attr_titles:
            db_data['p_attributes'].append({"title": title, "value": str(value)})
    
    # Convert price to integer
    if db_data["p_price"] is not None:
        try: 
            db_data["p_price"] = int(float(db_data["p_price"]))
        except (ValueError, TypeError): 
            db_data["p_price"] = None
    
    # Convert numeric fields to appropriate types
    for field in ['p_area', 'p_land_area', 'p_price_per_meter', 'p_year_built', 'p_bedrooms']:
        if db_data[field] is not None:
            try:
                db_data[field] = int(float(db_data[field]))
            except (ValueError, TypeError):
                db_data[field] = None
    
    # Ensure boolean fields are actual booleans
    for field in ['p_has_parking', 'p_has_storage', 'p_has_balcony']:
        db_data[field] = bool(db_data[field])
    
    logging.debug(f"[{db_data['p_external_id']}] Transformed data for DB with {len(db_data['p_attributes'])} attributes.")
    return db_data
