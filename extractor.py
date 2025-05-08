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

        # --- Images, Location, Attributes, Prices (Keep existing logic) ---
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

        # --- Location logic (keep your existing implementation) ---
        location = None
        script_tags_ld = soup.find_all('script', type='application/ld+json')
        # Process location from script tags
        # [Keep your existing code here]
        details['location'] = location
        if not location:
            logging.warning(f"[{token}] Location extraction failed.")

        # --- Attribute and Price logic (keep your existing implementation) ---
        attributes = []
        processed_titles = set()
        price, price_per_meter, area, year_built, bedrooms = None, None, None, None, None
        
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

                # Assign core fields
                if title == 'متراژ':
                    area = parsed_num
                elif title == 'ساخت':
                    year_built = parsed_num
                elif title == 'اتاق':
                    bedrooms = parsed_num
                elif 'قیمت کل' in title:
                    price = parsed_num
                elif 'قیمت هر متر' in title:
                    price_per_meter = parsed_num

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

        details['price'] = price
        details['price_per_meter'] = price_per_meter
        details['area'] = area
        details['year_built'] = year_built
        details['bedrooms'] = bedrooms
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
