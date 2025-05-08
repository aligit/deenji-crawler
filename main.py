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

# --- API Fetch Function (Keep as is from previous fix) ---
async def fetch_divar_listings(session, page=1, last_sort_date_cursor=None):
    # ... (Function content from previous correct version) ...
    payload = {
      "city_ids": [TARGET_CITY_ID], "source_view": "CATEGORY", "disable_recommendation": False,
      "map_state": {"camera_info": {"bbox": {"min_latitude": 35.56, "min_longitude": 51.1, "max_latitude": 35.84, "max_longitude": 51.61}, "place_hash": f"{TARGET_CITY_ID}||real-estate", "zoom": 9.8}, "page_state": "HALF_STATE", "interaction": {"list_only_used": {}}},
      "search_data": {"form_data": {"data": {"category": {"str": {"value": "residential-sell"}}}}, "server_payload": {"@type": "type.googleapis.com/widgets.SearchData.ServerPayload", "additional_form_data": {"data": {"sort": {"str": {"value": "sort_date"}}}}}}
    }
    if last_sort_date_cursor:
        payload["search_data"]["server_payload"]["additional_form_data"]["data"]["last_post_date"] = {"str": {"value": last_sort_date_cursor}}
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
    # *** FIX 1: Correct variable name check ***
    if not db_pool:
        logging.error(f"[{token}] Database pool is not available. Cannot acquire connection.")

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
            # --- FIX 2: Remove unreliable wait_for ---
            # wait_for="css:h1[class*='kt-page-title__title']",
            # --- Rely on delay instead ---
            delay_before_return_html=3.0 # Increased delay to 3 seconds
        )
        result = await crawler.arun(url=detail_url, config=run_config)

        if result.success and result.cleaned_html:
            logging.info(f"[{token}] Crawl successful. Parsing HTML...")
            extracted_data = extract_property_details(result.cleaned_html, token)
            if extracted_data:
                db_data = transform_for_db(extracted_data)
                if not db_data:
                    logging.warning(f"[{token}] Failed to transform data for DB.")
            else:
                logging.warning(f"[{token}] Failed to extract details from HTML for URL {detail_url}.")
        elif result.success:
             logging.warning(f"[{token}] Crawl successful but no usable HTML content found for {detail_url}. Status: {result.status_code}")
        else:
            logging.error(f"[{token}] Failed to crawl detail page {detail_url}: {result.error_message} (Status: {result.status_code})")

    except Exception as e:
        logging.error(f"[{token}] Unexpected error during crawl/parse for {detail_url}: {e}", exc_info=True)

    # --- Save to JSON (if extraction was successful) ---
    if extracted_data:
        json_filename = Path(JSON_OUTPUT_DIR) / f"{token}.json"
        try:
            extracted_data_json = json.dumps(extracted_data, indent=2, ensure_ascii=False)
            # *** FIX 3: Correct usage of asyncio.to_thread ***
            await asyncio.to_thread(json_filename.write_text, extracted_data_json, encoding='utf-8')
            logging.info(f"[{token}] Saved extracted data to {json_filename}")
        except Exception as json_e:
            logging.error(f"[{token}] Failed to save data to JSON file {json_filename}: {json_e}")

    # --- Attempt to Save to DB ---
    if db_data and db_pool:
        try:
            async with db_pool.acquire() as db_conn:
                await save_property_to_db(db_conn, db_data)
        except Exception as db_e:
            logging.error(f"[{token}] Failed to acquire DB connection or save: {db_e}", exc_info=True)
    elif db_data and not db_pool:
        logging.error(f"[{token}] Database pool is not available. Data saved to JSON only.")
    elif not db_data and extracted_data:
         logging.debug(f"[{token}] Data extracted but not transformed/valid for DB.")
    elif not extracted_data:
        logging.debug(f"[{token}] No data extracted, nothing to save.")


# --- Main Orchestration (Fix for event loop error handling) ---
async def main():
    """Main orchestration function."""
    db_pool_instance = await init_db_pool() # Initialize the pool

    if not db_pool_instance:
        logging.error("Database pool initialization failed. Proceeding with JSON saving only.")

    next_page_cursor = None
    crawled_tokens = set()
    total_properties_processed = 0
    browser_config_main = BrowserConfig(headless=True, verbose=False)

    try:
        async with AsyncWebCrawler(config=browser_config_main) as crawler, \
                   aiohttp.ClientSession() as http_session:

            for page_num in range(1, PAGES_TO_CRAWL + 1):
                listings_data = await fetch_divar_listings(http_session, page=page_num, last_sort_date_cursor=next_page_cursor)
                # ... (rest of the listing fetch/check logic) ...
                if not listings_data: break
                if 'list_widgets' not in listings_data: break

                property_widgets = [w for w in listings_data.get('list_widgets', []) if w.get('widget_type') == 'POST_ROW']

                if not property_widgets:
                    # ... (handle empty page/pagination check) ...
                    logging.info(f"No 'POST_ROW' widgets found on page {page_num}.")
                    next_page_cursor = listings_data.get("next_page")
                    if not next_page_cursor: break
                    continue

                semaphore = asyncio.Semaphore(MAX_CONCURRENT_CRAWLS)
                tasks = []

                async def crawl_with_semaphore_wrapper(widget, current_pool):
                    async with semaphore:
                        # ... (extract token, slug) ...
                        widget_data = widget.get('data', {})
                        action_payload = widget_data.get('action', {}).get('payload', {})
                        token = action_payload.get('token')
                        web_info = action_payload.get('web_info', {})
                        raw_slug = web_info.get('title', f'property-{token}')
                        safe_slug = quote(raw_slug.replace(" ", "-"))

                        if token and token not in crawled_tokens:
                            crawled_tokens.add(token)
                            await crawl_and_save_property(crawler, current_pool, token, safe_slug) # Pass pool
                        elif token:
                             logging.debug(f"Token {token} already processed in this run. Skipping.")

                for widget in property_widgets:
                     tasks.append(crawl_with_semaphore_wrapper(widget, db_pool_instance)) # Pass the pool

                await asyncio.gather(*tasks)
                total_properties_processed = len(crawled_tokens)

                # --- Determine cursor for NEXT page ---
                last_widget = property_widgets[-1]
                try:
                    sort_date_cursor = last_widget['action_log']['server_side_info']['info']['sort_date']
                    if sort_date_cursor: next_page_cursor = sort_date_cursor
                    else: next_page_cursor = None; logging.warning(f"Could not extract sort_date.")
                except Exception as e:
                    next_page_cursor = None; logging.error(f"Error extracting sort_date: {e}")

                logging.info(f"Next page cursor for fetch: {next_page_cursor}")
                if not next_page_cursor and page_num < PAGES_TO_CRAWL: break
                await asyncio.sleep(1.5)

    except Exception as e:
         logging.error(f"An error occurred during the main crawl loop: {e}", exc_info=True)
    finally:
        # Close the pool if it was successfully initialized
        if db_pool_instance: # Check the variable holding the pool result
            await close_db_pool()
        logging.info(f"Crawling finished or stopped. Processed {len(crawled_tokens)} unique properties.")
        logging.info(f"Check the '{JSON_OUTPUT_DIR}' directory for saved JSON files.")

# --- Script Entry Point (Simplified finally block) ---
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Crawling interrupted by user.")
        # Let main() handle the cleanup in its finally block
