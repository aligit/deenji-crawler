import logging


def classify_property_type(title: str, description: str) -> str:
    """
    Classify property type based on title and description.
    Returns: 'آپارتمان' (apartment), 'ویلا' (villa), 'زمین' (land), or None
    """
    title = title.lower() if title else ""
    description = description.lower() if description else ""

    # Check for villa
    if (
        "ویلا" in title
        or "ویلا" in description
        or "ویلایی" in title
        or "ویلایی" in description
    ):
        return "ویلا"  # Villa

    # Check for apartment
    if (
        "آپارتمان" in title
        or "آپارتمان" in description
        or "اپارتمان" in title
        or "اپارتمان" in description
        or "برج" in title
        or "برج" in description
        or "مجتمع مسکونی" in title
        or "مجتمع مسکونی" in description
        or (
            ("واحد" in title or "واحد" in description)
            and not ("ویلا" in title or "ویلا" in description)
            and not ("زمین" in title or "زمین" in description)
        )
    ):
        return "آپارتمان"  # Apartment

    # Check for land
    if (
        "زمین" in title
        or "زمین" in description
        or "قطعه زمین" in title
        or "قطعه زمین" in description
        or "قطعه" in title
        or "قطعه" in description
        or (
            ("باغ" in title or "باغ" in description)
            and not ("ویلا" in title or "ویلا" in description)
            and not ("آپارتمان" in title or "آپارتمان" in description)
            and not ("اپارتمان" in title or "اپارتمان" in description)
        )
        or (
            ("باغچه" in title or "باغچه" in description)
            and not ("ویلا" in title or "ویلا" in description)
            and not ("آپارتمان" in title or "آپارتمان" in description)
            and not ("اپارتمان" in title or "اپارتمان" in description)
        )
    ):
        return "زمین"  # Land

    logging.debug(f"Could not classify property type for title: '{title[:30]}...'")
    return None  # Unknown


def convert_to_persian_digits(text: str) -> str:
    """
    Convert Arabic/English digits to Persian digits

    Args:
        text: Text containing Arabic/English digits

    Returns:
        Text with Persian digits
    """
    persian_digits = {
        "0": "۰",
        "1": "۱",
        "2": "۲",
        "3": "۳",
        "4": "۴",
        "5": "۵",
        "6": "۶",
        "7": "۷",
        "8": "۸",
        "9": "۹",
    }

    result = ""
    for char in str(text):
        result += persian_digits.get(char, char)

    return result


def generate_bedroom_variants(number: int) -> list[str]:
    """
    Generate different variations of bedroom descriptions in Persian

    Args:
        number: Number of bedrooms

    Returns:
        List of bedroom description variations
    """
    number_str = str(number)
    persian_number = convert_to_persian_digits(number_str)

    variants = [
        persian_number,
        f"{persian_number}خوابه",
        f"{persian_number} خوابه",
    ]

    # Add word forms for common numbers
    word_forms = {1: "یک", 2: "دو", 3: "سه", 4: "چهار", 5: "پنج"}

    if number in word_forms:
        variants.append(f"{word_forms[number]} خوابه")
        variants.append(f"{word_forms[number]} خواب")

    return variants
