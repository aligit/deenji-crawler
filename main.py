import asyncio
import aiohttp
import logging
import os
import json
from urllib.parse import quote

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from db_utils import init_db_pool, close_db_pool, save_property_to_db
from extractor import extract_property_details, transform_for_db

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DIVAR_SEARCH_API = "https://api.divar.ir/v8/postlist/w/search"
DIVAR_DETAIL_URL_FORMAT = "https://divar.ir/v/{slug}/{token}" # Slug needed for URL but not essential for data
TARGET_CITY_ID = "1" # Tehran
PAGES_TO_CRAWL = 5
MAX_CONCURRENT_CRAWLS = 3 # Limit concurrency for crawl4ai

# --- Main Logic ---

async def fetch_divar_listings(session, page=1, last_post_date=None):
    """Fetches a page of listings from the Divar API."""
    payload = {
        "city_ids": [TARGET_CITY_ID],
        "source_view": "CATEGORY", # As per user prompt
        "disable_recommendation": False,
         "map_state": { # Simplified, might need adjustment based on real usage
            "camera_info": {
            "bbox": { # BBox for Tehran area approx. Adjust if needed.
                "min_latitude": 35.56, "min_longitude": 51.1,
                "max_latitude": 35.84, "max_longitude": 51.61
            },
            "place_hash": f"{TARGET_CITY_ID}||real-estate",
            "zoom": 9.8
            },
            "page_state": "HALF_STATE",
            "interaction": {"list_only_used": {}}
        },
        "search_data": {
            "form_data": {
                "data": {
                    "category": {"str": {"value": "residential-sell"}}
                }
            },
            "server_payload": {
                "@type": "type.googleapis.com/widgets.SearchData.ServerPayload",
                "additional_form_data": {
                    "data": {
                        "sort": {"str": {"value": "sort_date"}}
                        # Pagination logic placeholder:
                        # If last_post_date is provided, add it here
                        # Divar's exact pagination key needs verification
                        # Example: "last_post_date": {"str": {"value": last_post_date}}
                    }
                }
            }
        }
    }

    # Add pagination key if provided (adjust key name based on actual API)
    if last_post_date:
         # This key name is a guess, might need 'last_post_sort_date' or similar
        payload["search_data"]["server_payload"]["additional_form_data"]["data"]["last_post_date"] = {
             "str": {"value": last_post_date}
        }


    logging.info(f"Fetching listings page {page}...")
    try:
        async with session.post(DIVAR_SEARCH_API, json=payload) as response:
            response.raise_for_status() # Raise exception for bad status codes
            data = await response.json()
            logging.info(f"Fetched {len(data.get('list_widgets', []))} potential listings from API for page {page}.")
            return data
    except aiohttp.ClientError as e:
        logging.error(f"API request failed for page {page}: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode API response for page {page}: {e}")
        logging.error(f"Response text: {await response.text()[:500]}") # Log response snippet
        return None

async def crawl_and_save_property(crawler, db_conn, listing_widget):
    """Crawls a single property's detail page and saves it to the DB."""
    widget_data = listing_widget.get('data', {})
    action_payload = widget_data.get('action', {}).get('payload', {})
    token = action_payload.get('token')
    web_info = action_payload.get('web_info', {})
    slug = web_info.get('title', f'property-{token}') # Use title as slug, fallback

    if not token:
        logging.warning("Skipping widget, missing token.")
        return

    detail_url = DIVAR_DETAIL_URL_FORMAT.format(slug=quote(slug), token=token)
    logging.info(f"[{token}] Crawling detail page: {detail_url}")

    try:
        # Configure crawl4ai - using default scraping for now
        browser_config = BrowserConfig(headless=True, verbose=False) # Keep verbose off for individual crawls
        run_config = CrawlerRunConfig(
            cache_mode=CacheMode.ENABLED, # Use cache for efficiency
            page_timeout=45000 # 45 seconds timeout
            # No specific extraction strategy, rely on default + manual parsing
        )

        # Use the provided crawler instance
        result = await crawler.arun(url=detail_url, config=run_config)

        if result.success and result.cleaned_html:
            logging.info(f"[{token}] Crawl successful. Parsing HTML...")
            extracted_data = extract_property_details(result.cleaned_html, token)
            if extracted_data:
                db_data = transform_for_db(extracted_data)
                if db_data:
                    await save_property_to_db(db_conn, db_data)
                else:
                    logging.warning(f"[{token}] Failed to transform data for DB.")
            else:
                logging.warning(f"[{token}] Failed to extract details from HTML.")
        elif result.success and not result.cleaned_html:
             logging.warning(f"[{token}] Crawl successful but no cleaned HTML content found.")
        else:
            logging.error(f"[{token}] Failed to crawl detail page: {result.error_message} (Status: {result.status_code})")

    except Exception as e:
        logging.error(f"[{token}] Unexpected error during crawl/save for {detail_url}: {e}", exc_info=True)


async def main():
    """Main orchestration function."""
    db_pool = await init_db_pool()
    if not db_pool:
        logging.error("Failed to connect to database. Exiting.")
        return

    last_item_sort_date = None
    crawled_tokens = set() # Keep track of processed tokens to avoid duplicates per run
    total_properties_processed = 0

    # Initialize crawl4ai crawler once
    browser_config_main = BrowserConfig(headless=True, verbose=False) # Global config
    async with AsyncWebCrawler(config=browser_config_main) as crawler, \
               aiohttp.ClientSession() as http_session, \
               db_pool.acquire() as db_conn: # Use one connection for the batch

        for page_num in range(1, PAGES_TO_CRAWL + 1):
            listings_data = await fetch_divar_listings(http_session, page=page_num, last_post_date=last_item_sort_date)

            if not listings_data or 'list_widgets' not in listings_data:
                logging.warning(f"No listings found or error on page {page_num}. Stopping.")
                break

            property_widgets = [
                widget for widget in listings_data['list_widgets']
                if widget.get('widget_type') == 'POST_ROW'
            ]

            if not property_widgets:
                logging.info(f"No 'POST_ROW' widgets found on page {page_num}.")
                # Maybe update pagination logic or stop if truly empty
                last_item_sort_date = listings_data.get("last_post_date") # Check if API provides next page token
                if not last_item_sort_date and page_num < PAGES_TO_CRAWL:
                     logging.warning("No properties found and no pagination info. Stopping early.")
                     break
                continue

            # Process properties concurrently using asyncio.gather with a semaphore
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_CRAWLS)
            tasks = []

            async def crawl_with_semaphore(widget):
                 async with semaphore:
                    token = widget.get('data', {}).get('action', {}).get('payload', {}).get('token')
                    if token and token not in crawled_tokens:
                        crawled_tokens.add(token)
                        await crawl_and_save_property(crawler, db_conn, widget)
                    elif token:
                         logging.debug(f"Token {token} already processed in this run. Skipping.")

            for widget in property_widgets:
                 tasks.append(crawl_with_semaphore(widget))

            await asyncio.gather(*tasks)

            total_properties_processed += len(crawled_tokens) # Rough count

            # Update pagination marker (assuming last item's sort_date)
            if property_widgets:
                 last_widget_action_log = property_widgets[-1].get('action_log', {})
                 server_info = last_widget_action_log.get('server_side_info', {}).get('info', {})
                 last_item_sort_date = server_info.get('sort_date')
                 logging.info(f"Last item sort_date for next page fetch: {last_item_sort_date}")

            if not last_item_sort_date and page_num < PAGES_TO_CRAWL:
                 logging.warning(f"Could not determine next page marker after page {page_num}. Stopping.")
                 break # Stop if pagination fails

            # Optional delay between fetching list pages
            await asyncio.sleep(2) # Be polite to the API

    await close_db_pool()
    logging.info(f"Crawling finished. Processed approximately {len(crawled_tokens)} unique properties.")

if __name__ == "__main__":
    # Run the main async function
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Crawling interrupted by user.")
    finally:
        # Ensure pool is closed if interruption happens before main finishes cleanly
        asyncio.run(close_db_pool())
