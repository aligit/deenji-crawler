import logging

def classify_property_type(title: str, description: str) -> str:
    """
    Classify property type based on title and description.
    Returns: 'آپارتمان' (apartment), 'ویلا' (villa), 'زمین' (land), or None
    """
    title = title.lower() if title else ""
    description = description.lower() if description else ""
    
    # Check for villa
    if ('ویلا' in title or 'ویلا' in description or
        'ویلایی' in title or 'ویلایی' in description):
        return 'ویلا'  # Villa
    
    # Check for apartment
    if ('آپارتمان' in title or 'آپارتمان' in description or
        'اپارتمان' in title or 'اپارتمان' in description or
        'برج' in title or 'برج' in description or
        'مجتمع مسکونی' in title or 'مجتمع مسکونی' in description or
        (('واحد' in title or 'واحد' in description) and
         not ('ویلا' in title or 'ویلا' in description) and
         not ('زمین' in title or 'زمین' in description))):
        return 'آپارتمان'  # Apartment
    
    # Check for land
    if ('زمین' in title or 'زمین' in description or
        'قطعه زمین' in title or 'قطعه زمین' in description or
        'قطعه' in title or 'قطعه' in description or
        (('باغ' in title or 'باغ' in description) and
         not ('ویلا' in title or 'ویلا' in description) and
         not ('آپارتمان' in title or 'آپارتمان' in description) and
         not ('اپارتمان' in title or 'اپارتمان' in description)) or
        (('باغچه' in title or 'باغچه' in description) and
         not ('ویلا' in title or 'ویلا' in description) and
         not ('آپارتمان' in title or 'آپارتمان' in description) and
         not ('اپارتمان' in title or 'اپارتمان' in description))):
        return 'زمین'  # Land
    
    logging.debug(f"Could not classify property type for title: '{title[:30]}...'")
    return None  # Unknown
