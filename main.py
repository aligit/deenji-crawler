import asyncio
import aiohttp
import asyncpg # Ensure this is imported
import logging
import os
import json
from urllib.parse import quote
from pathlib import Path

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from db_utils import init_db_pool, close_db_pool, save_property_to_db
from extractor import extract_property_details, transform_for_db

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DIVAR_SEARCH_API = "https://api.divar.ir/v8/postlist/w/search"
DIVAR_DETAIL_URL_FORMAT = "https://divar.ir/v/{token}"
TARGET_CITY_ID = "1" # Tehran
PAGES_TO_CRAWL = 5
MAX_CONCURRENT_CRAWLS = 3
JSON_OUTPUT_DIR = "output_json"

Path(JSON_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# --- API Fetch Function ---
async def fetch_divar_listings(session, page=1, last_sort_date_cursor=None):
    """Fetches a page of listings from the Divar API using the last sort_date."""
    payload = {
      "city_ids": [TARGET_CITY_ID], "source_view": "CATEGORY", "disable_recommendation": False,
      "map_state": {"camera_info": {"bbox": {"min_latitude": 35.56, "min_longitude": 51.1, "max_latitude": 35.84, "max_longitude": 51.61}, "place_hash": f"{TARGET_CITY_ID}||real-estate", "zoom": 9.8}, "page_state": "HALF_STATE", "interaction": {"list_only_used": {}}},
      "search_data": {"form_data": {"data": {"category": {"str": {"value": "residential-sell"}}}}, "server_payload": {"@type": "type.googleapis.com/widgets.SearchData.ServerPayload", "additional_form_data": {"data": {"sort": {"str": {"value": "sort_date"}}}}}}
    }
    if last_sort_date_cursor:
        # Ensure structure exists before adding pagination key
        server_payload = payload["search_data"]["server_payload"]
        if "additional_form_data" not in server_payload: server_payload["additional_form_data"] = {}
        if "data" not in server_payload["additional_form_data"]: server_payload["additional_form_data"]["data"] = {}
        # Add the pagination cursor key (verify 'last_post_date' is correct)
        server_payload["additional_form_data"]["data"]["last_post_date"] = {"str": {"value": last_sort_date_cursor}}
        logging.info(f"Adding pagination cursor: {last_sort_date_cursor}")

    logging.info(f"Fetching listings page {page} (cursor: {last_sort_date_cursor})...")
    try:
        async with session.post(DIVAR_SEARCH_API, json=payload, timeout=20) as response:
            response_text = await response.text(); logging.debug(f"API Response Status: {response.status}")
            response.raise_for_status(); data = json.loads(response_text)
            logging.info(f"Fetched {len(data.get('list_widgets', []))} potential listings from API for page {page}.")
            return data
    except aiohttp.ClientResponseError as e: logging.error(f"API request failed for page {page}: Status {e.status}, Message: {e.message}"); return None
    except Exception as e: logging.error(f"Unexpected error fetching listings page {page}: {e}", exc_info=True); return None

# --- Crawl and Save Function ---
async def crawl_and_save_property(crawler, db_pool: asyncpg.Pool | None, token: str, slug: str):
    """Crawls a single property, saves data to JSON, and attempts DB save."""
    if not db_pool:
        logging.error(f"[{token}] Database pool is not available. Cannot acquire connection.")
        # Decide if you want to still attempt extraction and JSON save even if DB isn't available
        # Let's proceed to crawl but skip DB saving later.

    if not token:
         logging.warning("Skipping widget, missing token.")
         return

    detail_url = DIVAR_DETAIL_URL_FORMAT.format(token=token)
    logging.info(f"[{token}] Crawling detail page: {detail_url}")

    db_data = None
    extracted_data = None

    try:
        run_config = CrawlerRunConfig(
            cache_mode=CacheMode.ENABLED,
            page_timeout=45000,
            # Remove wait_for to avoid timeout issues on inconsistent pages
            # wait_for="css:h1[class*='kt-page-title__title']",
            delay_before_return_html=3.0 # Rely on delay
        )
        result = await crawler.arun(url=detail_url, config=run_config)

        if result.success and result.html: # Use raw HTML
            logging.info(f"[{token}] Crawl successful. Parsing RAW HTML (length: {len(result.html)})...")
            extracted_data = extract_property_details(result.html, token) # Pass result.html
            if extracted_data:
                db_data = transform_for_db(extracted_data) # Try transforming even if DB is down
                if not db_data:
                    logging.warning(f"[{token}] Failed to transform data for DB (missing title/id?).")
            else:
                logging.warning(f"[{token}] Failed to extract details from HTML for URL {detail_url}.")
        elif result.success:
             logging.warning(f"[{token}] Crawl successful but no HTML content found for {detail_url}. Status: {result.status_code}")
        else:
            logging.error(f"[{token}] Failed to crawl detail page {detail_url}: {result.error_message} (Status: {result.status_code})")

    except Exception as e:
        logging.error(f"[{token}] Unexpected error during crawl/parse for {detail_url}: {e}", exc_info=True)

    # --- Save to JSON (if extraction produced anything) ---
    if extracted_data:
        json_filename = Path(JSON_OUTPUT_DIR) / f"{token}.json"
        try:
            extracted_data_json = json.dumps(extracted_data, indent=2, ensure_ascii=False)
            await asyncio.to_thread(json_filename.write_text, extracted_data_json, encoding='utf-8')
            logging.info(f"[{token}] Saved extracted data to {json_filename}")
        except Exception as json_e:
            logging.error(f"[{token}] Failed to save data to JSON file {json_filename}: {json_e}")

    # --- Attempt to Save to DB (if data valid and pool exists) ---
    if db_data and db_pool:
        try:
            async with db_pool.acquire() as db_conn:
                await save_property_to_db(db_conn, db_data)
        except Exception as db_e:
            logging.error(f"[{token}] Failed to acquire DB connection or save: {db_e}", exc_info=True)
    elif db_data and not db_pool:
        logging.error(f"[{token}] Database pool is not available. Data saved to JSON only.")
    # Logging for other cases handled within transform_for_db or the extraction process

# --- Main Orchestration ---
async def main():
    """Main orchestration function."""
    db_pool_instance = await init_db_pool() # Initialize the pool

    if not db_pool_instance:
        logging.warning("Database pool initialization failed. Proceeding with JSON saving only.") # Changed to warning

    next_page_cursor = None
    crawled_tokens = set()
    total_properties_processed = 0
    browser_config_main = BrowserConfig(headless=True, verbose=False)

    try:
        async with AsyncWebCrawler(config=browser_config_main) as crawler, \
                   aiohttp.ClientSession() as http_session:

            for page_num in range(1, PAGES_TO_CRAWL + 1):
                listings_data = await fetch_divar_listings(http_session, page=page_num, last_sort_date_cursor=next_page_cursor)

                if not listings_data: logging.warning(f"Stopping crawl: No listings data from page {page_num}."); break
                if 'list_widgets' not in listings_data: logging.warning(f"API response for page {page_num} missing 'list_widgets'."); break

                property_widgets = [w for w in listings_data.get('list_widgets', []) if w.get('widget_type') == 'POST_ROW']

                if not property_widgets:
                    logging.info(f"No 'POST_ROW' widgets on page {page_num}.")
                    last_sort_date_cursor = None # Reset cursor for next attempt if needed
                    try: # Try finding sort_date even on empty pages for pagination
                        last_widget_action_log = listings_data['list_widgets'][-1].get('action_log', {})
                        sort_date_cursor = last_widget_action_log['server_side_info']['info']['sort_date']
                        if sort_date_cursor: next_page_cursor = sort_date_cursor
                    except (KeyError, IndexError, TypeError): pass
                    if not next_page_cursor and page_num < PAGES_TO_CRAWL: logging.warning("No props & no pagination cursor. Stopping."); break
                    continue

                semaphore = asyncio.Semaphore(MAX_CONCURRENT_CRAWLS)
                tasks = []

                async def crawl_with_semaphore_wrapper(widget, current_pool):
                    async with semaphore:
                        widget_data = widget.get('data', {})
                        action_payload = widget_data.get('action', {}).get('payload', {})
                        token = action_payload.get('token')
                        web_info = action_payload.get('web_info', {})
                        raw_slug = web_info.get('title', f'property-{token}')
                        safe_slug = quote(raw_slug.replace(" ", "-"))

                        if token and token not in crawled_tokens:
                            crawled_tokens.add(token)
                            await crawl_and_save_property(crawler, current_pool, token, safe_slug) # Pass pool
                        elif token: logging.debug(f"Token {token} already processed. Skipping.")

                for widget in property_widgets:
                     tasks.append(crawl_with_semaphore_wrapper(widget, db_pool_instance)) # Pass the pool

                await asyncio.gather(*tasks)
                total_properties_processed = len(crawled_tokens)

                # Determine cursor for NEXT page from the last widget of THIS page
                last_widget = property_widgets[-1]
                try:
                    sort_date_cursor = last_widget['action_log']['server_side_info']['info']['sort_date']
                    if sort_date_cursor: next_page_cursor = sort_date_cursor
                    else: next_page_cursor = None; logging.warning(f"Could not extract sort_date from last item on page {page_num}.")
                except (KeyError, IndexError, TypeError) as e:
                    next_page_cursor = None; logging.error(f"Error extracting sort_date: {e}")

                logging.info(f"Next page cursor for fetch: {next_page_cursor}")
                if not next_page_cursor and page_num < PAGES_TO_CRAWL: logging.warning(f"No next page cursor. Stopping."); break
                await asyncio.sleep(1.5)

    except Exception as e:
         logging.error(f"An error occurred during the main crawl loop: {e}", exc_info=True)
    finally:
        if db_pool_instance: await close_db_pool()
        logging.info(f"Crawling finished or stopped. Processed {len(crawled_tokens)} unique properties.")
        logging.info(f"Check the '{JSON_OUTPUT_DIR}' directory for saved JSON files.")

# --- Script Entry Point ---
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Crawling interrupted by user.")
        # Cleanup is handled in main's finally block
