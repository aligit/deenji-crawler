# extractor.py

import json
import re
import logging
from bs4 import BeautifulSoup
from pathlib import Path

JSON_OUTPUT_DIR = "output_json"
Path(JSON_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# Persian/Arabic number translation maps
persian_num_map = str.maketrans('۰۱۲۳۴۵۶۷۸۹', '0123456789')
arabic_num_map = str.maketrans('٠١٢٣٤٥٦٧٨٩', '0123456789')

def parse_persian_number(s):
    """Parse Persian/Arabic numbers to standard digits and convert to numeric types."""
    if not s or not isinstance(s, str): 
        return None
    try:
        # First, remove common unit words in Persian
        s = s.replace('متر', '').replace('تومان', '').replace('مترمربع', '')
        
        # Then translate Persian/Arabic digits to Latin
        cleaned_s = s.translate(persian_num_map).translate(arabic_num_map)
        
        # Remove commas and other non-numeric characters
        cleaned_s = cleaned_s.replace(',', '').strip()
        cleaned_s = re.sub(r'[^\d.-]+', '', cleaned_s)
        
        if not cleaned_s or cleaned_s == '-': 
            return None
        num = float(cleaned_s)
        return int(num) if num == int(num) else num
    except (ValueError, TypeError) as e:
        logging.debug(f"Could not parse number string '{s}' to number: {e}")
        return None

def extract_property_details(html_content: str, token: str) -> dict | None:
    """
    Extract detailed information from a Divar property listing.
    
    Args:
        html_content: The HTML content of the property page
        token: The property identifier
        
    Returns:
        Dictionary containing property details or None if extraction failed
    """
    if not html_content:
        logging.warning(f"[{token}] HTML content is empty.")
        return None

    soup = BeautifulSoup(html_content, 'lxml')
    details = {"external_id": token}
    logging.debug(f"[{token}] Starting HTML parsing. Content length: {len(html_content)}")

    try:
        # --- IMPROVED Title Extraction Logic ---
        title = None
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
        
        # Try a broader range of description selectors
        description_selectors = [
            "div[class*='kt-description-row'] > div > p[class*='kt-description-row__text']",
            "p.kt-description-row__text--primary",
            "div.kt-base-row.kt-base-row--large.kt-description-row div.kt-base-row__start p",
            "div.kt-base-row.kt-base-row--large.kt-description-row p",
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
        
        # If still no description, try finding by heading text
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
        
        # Last resort: Find any substantial paragraph in the main content
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

        # --- IMPROVED Attribute Extraction Logic ---
        attributes = []
        processed_titles = set()
        price, price_per_meter, area, year_built, bedrooms = None, None, None, None, None
        land_area, property_type = None, None  # New fields
        has_parking, has_storage, has_balcony = False, False, False  # Initialize boolean flags
        title_deed_type, building_direction, renovation_status = None, None, None
        floor_material, bathroom_type, cooling_system = None, None, None
        heating_system, hot_water_system = None, None
        
        # Define attribute selectors that cover various Divar UI patterns
        attribute_selectors = [
            "div[class*='kt-group-row'] div[class*='kt-group-row-item']",
            "div[class*='kt-base-row'] div[class*='kt-base-row__start']",
            "div[class*='unexpandable-row']",
            "div[class*='expandable-part'] div[class*='kt-group-row-item']",
            "li[class*='kt-attr-item']",
            "div[class*='kt-accordion-item']",
            # Add any additional patterns you observe in Divar's HTML
        ]
        
        # Process each selector type to find attributes
        for selector in attribute_selectors:
            attr_items = soup.select(selector)
            logging.debug(f"[{token}] Found {len(attr_items)} items with selector: {selector}")
            
            for item in attr_items:
                # Multiple approaches to find title elements
                title_candidates = [
                    item.find(['span', 'p', 'div'], class_=lambda c: c and ('title' in c.lower() or 'label' in c.lower())),
                    item.find(['span', 'p', 'div'], class_=lambda c: c and 'kt-group-row-item__title' in c),
                    item.select_one('span.kt-group-row-item__title, span.title, p.title')
                ]
                
                title_el = next((t for t in title_candidates if t), None)
                
                # Multiple approaches to find value elements
                value_candidates = [
                    item.find(['span', 'p', 'div'], class_=lambda c: c and ('value' in c.lower() or 'data' in c.lower())),
                    title_el.find_next_sibling(['span', 'p', 'div']) if title_el else None,
                    item if 'kt-group-row-item' in item.get('class', []) else None
                ]
                
                value_el = next((v for v in value_candidates if v), None)
                
                # Extract title and value text
                if title_el:
                    title = title_el.get_text(strip=True).strip().rstrip(':').strip()
                    
                    # Extract value text - handle different cases
                    value_text = ""
                    if value_el:
                        if value_el == item and 'kt-group-row-item' in item.get('class', []):
                            # For group-row-items where the title is inside the value element
                            full_text = value_el.get_text(strip=True)
                            if title in full_text:
                                value_text = full_text.replace(title, '', 1).strip()
                            else:
                                value_text = full_text
                        else:
                            value_text = value_el.get_text(strip=True)
                    
                    # Skip if we've already processed this title or if value is empty
                    if not title or title in processed_titles or not value_text:
                        continue
                    
                    logging.debug(f"[{token}] Processing attribute: {title} = {value_text}")
                    processed_titles.add(title)
                    
                    # Parse numeric values
                    parsed_num = parse_persian_number(value_text)
                    
                    # --- DIRECT FIELD ASSIGNMENTS ---
                    # Map common field names
                    if title == 'متراژ':
                        area = parsed_num
                        logging.debug(f"[{token}] Extracted area: {area}")
                    elif title == 'متراژ زمین':
                        land_area = parsed_num
                        logging.debug(f"[{token}] Extracted land area: {land_area}")
                    elif title == 'ساخت':
                        year_built = parsed_num
                        logging.debug(f"[{token}] Extracted year built: {year_built}")
                    elif title == 'اتاق':
                        bedrooms = parsed_num
                        logging.debug(f"[{token}] Extracted bedrooms: {bedrooms}")
                    elif title == 'قیمت کل':
                        price = parsed_num
                        logging.debug(f"[{token}] Extracted price: {price}")
                    elif title == 'قیمت هر متر':
                        price_per_meter = parsed_num
                        logging.debug(f"[{token}] Extracted price per meter: {price_per_meter}")
                    elif title == 'نوع ملک':
                        property_type = value_text
                        logging.debug(f"[{token}] Extracted property type: {property_type}")
                    # --- BOOLEAN FIELDS HANDLING ---
                    elif title == 'پارکینگ':
                        has_parking = False if value_text == 'ندارد' else True
                        logging.debug(f"[{token}] Extracted parking: {has_parking}")
                    elif title == 'انباری':
                        has_storage = False if value_text == 'ندارد' else True
                        logging.debug(f"[{token}] Extracted storage: {has_storage}")
                    elif title == 'بالکن':
                        has_balcony = False if value_text == 'ندارد' else True
                        logging.debug(f"[{token}] Extracted balcony: {has_balcony}")
                    # Advanced field types
                    elif title == 'سند':
                        title_deed_type = value_text
                    elif title == 'جهت ساختمان':
                        building_direction = value_text
                    elif title == 'وضعیت واحد':
                        renovation_status = value_text
                    elif title == 'جنس کف':
                        floor_material = value_text
                    elif title == 'سرویس بهداشتی':
                        bathroom_type = value_text
                    elif title == 'سرمایش':
                        cooling_system = value_text
                    elif title == 'گرمایش':
                        heating_system = value_text
                    elif title == 'تأمین‌کننده آب گرم':
                        hot_water_system = value_text
                    
                    # Add to general attributes list
                    attr_dict = {"title": title, "value": value_text}
                    
                    # Add availability info for boolean fields
                    if 'ندارد' in value_text:
                        attr_dict["available"] = False
                    elif 'دارد' in value_text:
                        attr_dict["available"] = True
                    
                    # Extract icon-based key if available
                    icon_tag = item.find('i', class_=lambda c: c and 'kt-icon-' in c)
                    if icon_tag and icon_tag.get('class'):
                        for css_class in icon_tag['class']:
                            if 'kt-icon-' in css_class:
                                key_candidate = css_class.split('kt-icon-')[-1].upper().replace('-', '_')
                                if len(key_candidate) > 2 and not any(c.isdigit() for c in key_candidate):
                                    attr_dict["key"] = key_candidate
                                    break
                    
                    attributes.append(attr_dict)

        # --- ENHANCED Modal Dialog Extraction ---
        # Look for the modal dialog content (shown after clicking "Show all details")
        modal_dialog_selectors = [
            "div.modal-dialog__content", 
            "div[class*='kt-modal-dialog'] div[class*='content']",
            "div[class*='modal-content']",
            "div[class*='kt-dialog-content']"
        ]
        
        for modal_selector in modal_dialog_selectors:
            modal_dialog = soup.select_one(modal_selector)
            if modal_dialog:
                logging.debug(f"[{token}] Found modal dialog content using selector: {modal_selector}")
                
                # Look for features section in modal
                feature_section_selectors = [
                    "div[class*='dialog-section']",
                    "div[class*='kt-accordion']",
                    "div[class*='feature-section']",
                    "section[class*='feature']"
                ]
                
                for section_selector in feature_section_selectors:
                    feature_sections = modal_dialog.select(section_selector)
                    
                    for section in feature_sections:
                        # Find section title element
                        section_title_el = section.select_one("div[class*='section-title'], h3, h4, div[class*='accordion-title'], div[class*='title']")
                        
                        if section_title_el and ("ویژگی‌ها" in section_title_el.get_text(strip=True) or 
                                               "مشخصات" in section_title_el.get_text(strip=True) or
                                               "امکانات" in section_title_el.get_text(strip=True)):
                            
                            # Find feature items
                            feature_item_selectors = [
                                "div[class*='row-item']", 
                                "div[class*='unexpandable-row']", 
                                "div[class*='item']",
                                "div[class*='kt-base-row']",
                                "div[class*='kt-group-row-item']"
                            ]
                            
                            for item_selector in feature_item_selectors:
                                feature_items = section.select(item_selector)
                                
                                for item in feature_items:
                                    # Similar to main extraction, extract title and value
                                    title_el = item.select_one("div[class*='title'], span[class*='title']")
                                    value_el = item.select_one("div[class*='value'], span[class*='value']")
                                    
                                    if title_el:
                                        title = title_el.get_text(strip=True).strip().rstrip(':').strip()
                                        value_text = value_el.get_text(strip=True) if value_el else ""
                                        
                                        # Skip if already processed
                                        if not title or title in processed_titles:
                                            continue
                                            
                                        processed_titles.add(title)
                                        
                                        # Handle boolean fields and other advanced fields
                                        if "پارکینگ" in title:
                                            has_parking = False if "ندارد" in value_text else True
                                            logging.debug(f"[{token}] Modal: Found parking = {has_parking}")
                                        elif "انباری" in title:
                                            has_storage = False if "ندارد" in value_text else True
                                            logging.debug(f"[{token}] Modal: Found storage = {has_storage}")
                                        elif "بالکن" in title:
                                            has_balcony = False if "ندارد" in value_text else True
                                            logging.debug(f"[{token}] Modal: Found balcony = {has_balcony}")
                                        elif title == 'سند':
                                            title_deed_type = value_text
                                        elif title == 'جهت ساختمان':
                                            building_direction = value_text
                                        elif title == 'وضعیت واحد':
                                            renovation_status = value_text
                                        elif title == 'جنس کف':
                                            floor_material = value_text
                                        elif title == 'سرویس بهداشتی':
                                            bathroom_type = value_text
                                        elif title == 'سرمایش':
                                            cooling_system = value_text
                                        elif title == 'گرمایش':
                                            heating_system = value_text
                                        elif title == 'تأمین‌کننده آب گرم':
                                            hot_water_system = value_text
                                        
                                        # Add to attributes
                                        attr_dict = {"title": title}
                                        if value_text:
                                            attr_dict["value"] = value_text
                                        
                                        if "ندارد" in value_text:
                                            attr_dict["available"] = False
                                        elif "دارد" in value_text:
                                            attr_dict["available"] = True
                                            
                                        attributes.append(attr_dict)
                
                # Check for amenities sections specifically
                amenity_sections = modal_dialog.select("div[class*='amenities'], div[class*='features'], div[class*='امکانات']")
                
                for amenity_section in amenity_sections:
                    amenity_items = amenity_section.select("div.row-item, span.row-item, div[class*='item'], li")
                    
                    for amenity in amenity_items:
                        amenity_text = amenity.get_text(strip=True)
                        
                        # Check for key amenities
                        if "پارکینگ" in amenity_text:
                            has_parking = True
                            logging.debug(f"[{token}] Modal amenities: Found parking")
                        elif "انباری" in amenity_text:
                            has_storage = True
                            logging.debug(f"[{token}] Modal amenities: Found storage")
                        elif "بالکن" in amenity_text:
                            has_balcony = True
                            logging.debug(f"[{token}] Modal amenities: Found balcony")
                            
                        # Add to attributes if not already processed
                        if amenity_text and amenity_text not in processed_titles:
                            attributes.append({"title": amenity_text, "available": True})
                            processed_titles.add(amenity_text)

        # Log conversion counts for key fields
        if not area:
            logging.warning(f"[{token}] Failed to extract area field")
        if not bedrooms:
            logging.warning(f"[{token}] Failed to extract bedrooms field")
        if not price:
            logging.warning(f"[{token}] Failed to extract price field")
            
        # Check for boolean fields - provide explicit debugging
        logging.debug(f"[{token}] Boolean values - Parking: {has_parking}, Storage: {has_storage}, Balcony: {has_balcony}")

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

        # Final validation
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
        "p_similar_properties": []
    }
    
    # Handle numeric fields with proper conversion
    numeric_fields = ['price', 'price_per_meter', 'area', 'land_area', 'year_built', 'bedrooms']
    for field in numeric_fields:
        value = extracted_data.get(field)
        if value is not None:
            try:
                db_data[f"p_{field}"] = int(float(value))
            except (ValueError, TypeError):
                db_data[f"p_{field}"] = None
        else:
            db_data[f"p_{field}"] = None
    
    # Handle boolean fields
    boolean_fields = ['has_parking', 'has_storage', 'has_balcony']
    for field in boolean_fields:
        db_data[f"p_{field}"] = bool(extracted_data.get(field, False))
    
    # Handle text fields
    text_fields = [
        'property_type', 'title_deed_type', 'building_direction', 
        'renovation_status', 'floor_material', 'bathroom_type',
        'cooling_system', 'heating_system', 'hot_water_system'
    ]
    for field in text_fields:
        db_data[f"p_{field}"] = extracted_data.get(field)
    
    # Add core attributes if they're not already there
    core_attrs = {
        'متراژ': extracted_data.get('area'), 
        'متراژ زمین': extracted_data.get('land_area'),
        'نوع ملک': extracted_data.get('property_type'),
        'ساخت': extracted_data.get('year_built'), 
        'اتاق': extracted_data.get('bedrooms'), 
        'قیمت کل': extracted_data.get('price'),
        'قیمت هر متر': extracted_data.get('price_per_meter')
    }
    
    # Only add non-None attributes that aren't already in the list
    existing_attr_titles = {a['title'] for a in db_data['p_attributes']}
    for title, value in core_attrs.items():
        if value is not None and title not in existing_attr_titles:
            db_data['p_attributes'].append({"title": title, "value": str(value)})
    
    logging.debug(f"[{db_data['p_external_id']}] Transformed data for DB.")
    return db_data
