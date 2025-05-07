import asyncio
import aiohttp
import logging
import os
import json
from urllib.parse import quote

# Make sure crawl4ai is imported correctly
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from db_utils import init_db_pool, close_db_pool, save_property_to_db, pool # Import pool
from extractor import extract_property_details, transform_for_db

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DIVAR_SEARCH_API = "https://api.divar.ir/v8/postlist/w/search"
# Let's try using the token directly, sometimes slugs cause issues if they change
DIVAR_DETAIL_URL_FORMAT = "https://divar.ir/v/{token}"
TARGET_CITY_ID = "1" # Tehran
PAGES_TO_CRAWL = 5
MAX_CONCURRENT_CRAWLS = 3 # Keep concurrency low initially

# --- Main Logic ---

async def fetch_divar_listings(session, page=1, last_post_date=None):
    """Fetches a page of listings from the Divar API."""
    payload = {
      "city_ids": [TARGET_CITY_ID],
      "source_view": "CATEGORY",
      "disable_recommendation": False,
      "map_state": {
          "camera_info": {
              "bbox": {
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
              "data": {"category": {"str": {"value": "residential-sell"}}}
          },
          "server_payload": {
              "@type": "type.googleapis.com/widgets.SearchData.ServerPayload",
              "additional_form_data": {
                  "data": {"sort": {"str": {"value": "sort_date"}}}
              }
          }
      }
    }

    # Add pagination key if provided (adjust key name based on actual API)
    # Divar's pagination is tricky and might involve more than just last_post_date
    # It often uses a combination of 'last_post_date' and potentially other keys
    # from the 'page' object in the API response. Let's keep it simple for now.
    if last_post_date:
        if "additional_form_data" not in payload["search_data"]["server_payload"]:
            payload["search_data"]["server_payload"]["additional_form_data"] = {"data": {}}
        elif "data" not in payload["search_data"]["server_payload"]["additional_form_data"]:
             payload["search_data"]["server_payload"]["additional_form_data"]["data"] = {}

        # Key name might be 'last_post_date', 'page[last_post_date]', etc. Needs verification.
        # Using a common pattern found in some APIs:
        payload["search_data"]["server_payload"]["additional_form_data"]["data"]["page"] = {
            "str": {"value": last_post_date } # Assuming it's a cursor string now
        }
        # Or it might still be the date directly:
        # payload["search_data"]["server_payload"]["additional_form_data"]["data"]["last_post_date"] = {
        #      "str": {"value": last_post_date}
        # }


    logging.info(f"Fetching listings page {page} (cursor: {last_post_date})...")
    try:
        async with session.post(DIVAR_SEARCH_API, json=payload, timeout=20) as response: # Added timeout
            response_text = await response.text() # Read text first for logging
            logging.debug(f"API Response Status: {response.status}")
            # logging.debug(f"API Response Body Snippet: {response_text[:1000]}") # Log more if needed
            response.raise_for_status() # Raise exception for bad status codes
            data = json.loads(response_text)
            logging.info(f"Fetched {len(data.get('list_widgets', []))} potential listings from API for page {page}.")
            return data
    except aiohttp.ClientResponseError as e:
         logging.error(f"API request failed for page {page}: Status {e.status}, Message: {e.message}, Headers: {e.headers}")
         logging.error(f"Payload sent: {json.dumps(payload, indent=2, ensure_ascii=False)}")
         return None
    except aiohttp.ClientError as e:
        logging.error(f"API request failed for page {page}: {e}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode API response for page {page}: {e}")
        logging.error(f"Response text snippet: {response_text[:500]}") # Log response snippet
        return None

# Modify this function to acquire connection from the pool
async def crawl_and_save_property(crawler, token: str, slug: str):
    """Crawls a single property's detail page and saves it to the DB."""
    global pool # Access the global pool

    if not token:
        logging.warning("Skipping widget, missing token.")
        return

    # Using simplified URL format
    detail_url = DIVAR_DETAIL_URL_FORMAT.format(token=token)
    logging.info(f"[{token}] Crawling detail page: {detail_url}")

    async with pool.acquire() as db_conn: # Acquire connection for this task
        try:
            # --- crawl4ai Configuration ---
            # Give the page more time and wait for potential dynamic content
            run_config = CrawlerRunConfig(
                cache_mode=CacheMode.ENABLED,
                page_timeout=45000, # 45 seconds timeout
                # Wait for a common element that appears after JS load (e.g., attribute rows)
                # Adjust selector if needed based on inspection
                wait_for="css:div[class*='kt-unexpandable-row__item']",
                delay_before_return_html=1.5 # Extra 1.5 seconds delay
            )

            # Use the provided crawler instance
            result = await crawler.arun(url=detail_url, config=run_config)

            if result.success and result.cleaned_html:
                logging.info(f"[{token}] Crawl successful. Parsing HTML...")
                extracted_data = extract_property_details(result.cleaned_html, token)
                if extracted_data:
                    db_data = transform_for_db(extracted_data)
                    if db_data:
                        # Pass the connection acquired for this task
                        await save_property_to_db(db_conn, db_data)
                    else:
                        logging.warning(f"[{token}] Failed to transform data for DB.")
                else:
                    logging.warning(f"[{token}] Failed to extract details from HTML for URL {detail_url}.")
            elif result.success: # Successful status but no useful HTML
                 logging.warning(f"[{token}] Crawl successful but no cleaned HTML content found for {detail_url}. Status: {result.status_code}")
            else:
                logging.error(f"[{token}] Failed to crawl detail page {detail_url}: {result.error_message} (Status: {result.status_code})")

        except Exception as e:
            logging.error(f"[{token}] Unexpected error during crawl/save for {detail_url}: {e}", exc_info=True)
        # Connection is automatically released when exiting 'async with pool.acquire()'

async def main():
    """Main orchestration function."""
    db_pool = await init_db_pool() # pool is now global
    if not db_pool:
        logging.error("Failed to connect to database. Exiting.")
        return

    # Use 'widget_list_identifier' for pagination if available, otherwise fall back to date
    next_page_cursor = None # Start with no cursor
    crawled_tokens = set() # Keep track of processed tokens to avoid duplicates per run
    total_properties_processed = 0

    # Initialize crawl4ai crawler once
    browser_config_main = BrowserConfig(headless=True, verbose=False) # Global config
    async with AsyncWebCrawler(config=browser_config_main) as crawler, \
               aiohttp.ClientSession() as http_session:

        for page_num in range(1, PAGES_TO_CRAWL + 1):
            listings_data = await fetch_divar_listings(http_session, page=page_num, last_post_date=next_page_cursor)

            if not listings_data or 'list_widgets' not in listings_data:
                logging.warning(f"No listings found or error on page {page_num}. Stopping.")
                break

            property_widgets = [
                widget for widget in listings_data.get('list_widgets', [])
                if widget.get('widget_type') == 'POST_ROW'
            ]

            if not property_widgets:
                logging.info(f"No 'POST_ROW' widgets found on page {page_num}.")
                # Update pagination marker based on API response structure
                next_page_cursor = listings_data.get("next_page") # Assuming API returns 'next_page' token/cursor
                if not next_page_cursor and page_num < PAGES_TO_CRAWL:
                     logging.warning("No properties found and no pagination cursor provided by API. Stopping early.")
                     break
                continue

            semaphore = asyncio.Semaphore(MAX_CONCURRENT_CRAWLS)
            tasks = []

            async def crawl_with_semaphore_wrapper(widget):
                async with semaphore:
                    widget_data = widget.get('data', {})
                    action_payload = widget_data.get('action', {}).get('payload', {})
                    token = action_payload.get('token')
                    web_info = action_payload.get('web_info', {})
                    # Using title as slug is prone to errors if title changes/has special chars
                    # Let's build slug carefully or potentially just use token if URL allows
                    raw_slug = web_info.get('title', f'property-{token}')
                    # Basic slugification - replace spaces, remove unsafe chars
                    safe_slug = quote(raw_slug.replace(" ", "-"))

                    if token and token not in crawled_tokens:
                        crawled_tokens.add(token)
                        # Pass crawler instance, token, and slug to the crawl function
                        await crawl_and_save_property(crawler, token, safe_slug)
                    elif token:
                         logging.debug(f"Token {token} already processed in this run. Skipping.")

            for widget in property_widgets:
                 tasks.append(crawl_with_semaphore_wrapper(widget))

            await asyncio.gather(*tasks)

            total_properties_processed = len(crawled_tokens) # More accurate count

            # Update pagination cursor (Adapt based on actual API response key)
            next_page_cursor = listings_data.get("next_page") # Example key
            logging.info(f"Next page cursor from API: {next_page_cursor}")

            if not next_page_cursor and page_num < PAGES_TO_CRAWL:
                 logging.warning(f"Could not determine next page cursor after page {page_num}. Stopping.")
                 break

            # Optional delay between fetching list pages
            await asyncio.sleep(1) # Reduced delay slightly

    await close_db_pool()
    logging.info(f"Crawling finished. Processed {len(crawled_tokens)} unique properties.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Crawling interrupted by user.")
    finally:
        # Ensure pool is closed if interruption happens before main finishes cleanly
        # Check if loop is running before trying to close pool
        loop = asyncio.get_event_loop()
        if loop.is_running():
             loop.create_task(close_db_pool())
        else:
             asyncio.run(close_db_pool())
