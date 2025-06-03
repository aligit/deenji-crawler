# main.py

import argparse
import asyncio
import json
import logging
import os
import random
from pathlib import Path
from urllib.parse import quote

import aiohttp
import asyncpg  # Ensure this is imported
from crawl4ai import (AsyncWebCrawler, BrowserConfig, CacheMode,
                      CrawlerRunConfig)
from dotenv import load_dotenv

from db_utils import close_db_pool, init_db_pool, save_property_to_db
from es_indexer import DivarElasticsearchIndexer  # Import the indexer
from extractor import extract_property_details, transform_for_db
from image_storage import SupabaseStorageManager

# --- command-line argument parsing ---
parser = argparse.ArgumentParser(
    description=(
        "Divar crawler. If TEST_MODE is False and you provide a single argument, "
        "we'll read custom-lists/<listname> and crawl only those IDs. "
        "Otherwise, run the default pagination crawl."
    )
)
parser.add_argument(
    "listname",
    nargs="?",
    help="(optional) name of a file inside custom-lists/ containing one Divar ID per line",
)
args = parser.parse_args()


# --- Configuration ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
DIVAR_SEARCH_API = "https://api.divar.ir/v8/postlist/w/search"
DIVAR_DETAIL_URL_FORMAT = "https://divar.ir/v/{token}"
TARGET_CITY_ID = "1"  # Tehran
PAGES_TO_CRAWL = 5
MAX_CONCURRENT_CRAWLS = 3
JSON_OUTPUT_DIR = "output_json"

# Simple proxy configuration - MOVED TO TOP
PROXY_URL = "http://127.0.0.1:10808"  # Your existing proxy

Path(JSON_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)

# --- Create global indexer instance ---
es_indexer = DivarElasticsearchIndexer()


def load_tokens_from_file(listname: str) -> list[str]:
    """
    Given a listname (e.g. "andarzgu"), open custom-lists/andarzgu,
    read each non-empty line as a Divar token, return a deduped list.
    """
    file_path = Path("custom-lists") / listname
    if not file_path.exists():
        logging.error(f"Custom list not found: {file_path}")
        return []

    tokens = []
    with file_path.open(encoding="utf-8") as f:
        for line in f:
            tok = line.strip()
            if tok:
                tokens.append(tok)

    # Deduplicate while preserving order
    seen = set()
    unique_tokens = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            unique_tokens.append(t)
    return unique_tokens


# --- API Fetch Function ---
async def fetch_divar_listings(session, page=1, last_sort_date_cursor=None):
    """Fetches a page of listings from the Divar API using the last sort_date."""
    payload = {
        "city_ids": [TARGET_CITY_ID],
        "source_view": "CATEGORY",
        "disable_recommendation": False,
        "map_state": {
            "camera_info": {
                "bbox": {
                    "min_latitude": 35.56,
                    "min_longitude": 51.1,
                    "max_latitude": 35.84,
                    "max_longitude": 51.61,
                },
                "place_hash": f"{TARGET_CITY_ID}||real-estate",
                "zoom": 9.8,
            },
            "page_state": "HALF_STATE",
            "interaction": {"list_only_used": {}},
        },
        "search_data": {
            "form_data": {"data": {"category": {"str": {"value": "residential-sell"}}}},
            "server_payload": {
                "@type": "type.googleapis.com/widgets.SearchData.ServerPayload",
                "additional_form_data": {
                    "data": {"sort": {"str": {"value": "sort_date"}}}
                },
            },
        },
    }
    if last_sort_date_cursor:
        server_payload = payload["search_data"]["server_payload"]
        if "additional_form_data" not in server_payload:
            server_payload["additional_form_data"] = {}
        if "data" not in server_payload["additional_form_data"]:
            server_payload["additional_form_data"]["data"] = {}
        server_payload["additional_form_data"]["data"]["last_post_date"] = {
            "str": {"value": last_sort_date_cursor}
        }
        logging.info(f"Adding pagination cursor: {last_sort_date_cursor}")

    logging.info(f"Fetching listings page {page} (cursor: {last_sort_date_cursor})...")
    try:
        async with session.post(DIVAR_SEARCH_API, json=payload, timeout=20) as response:
            response_text = await response.text()
            logging.debug(f"API Response Status: {response.status}")
            response.raise_for_status()
            data = json.loads(response_text)
            logging.info(
                f"Fetched {len(data.get('list_widgets', []))} potential listings from API for page {page}."
            )
            return data
    except aiohttp.ClientResponseError as e:
        logging.error(
            f"API request failed for page {page}: Status {e.status}, Message: {e.message}"
        )
        return None
    except Exception as e:
        logging.error(
            f"Unexpected error fetching listings page {page}: {e}", exc_info=True
        )
        return None


# --- Crawl and Save Function ---
async def crawl_and_save_property(
    crawler,
    db_pool: asyncpg.Pool | None,
    token: str,
    slug: str,
    storage_manager=None,
    api_only: bool = False,
):
    """Crawls a single property, saves data to JSON, and attempts DB save with anti-detection measures"""

    if not token:
        logging.warning("Skipping widget, missing token.")
        return

    if not db_pool:
        logging.error(
            f"[{token}] Database pool is not available. Cannot acquire connection."
        )
        # We'll proceed to crawl but skip DB saving later

    detail_url = DIVAR_DETAIL_URL_FORMAT.format(token=token)
    logging.info(
        f"[{token}] {'API-only test for' if api_only else 'Crawling detail page:'} {detail_url}"
    )

    db_data = None
    extracted_data = None

    try:
        if api_only:
            # API-only testing mode - skip browser crawling
            logging.info(f"[{token}] Running API-only test mode...")
            extracted_data = await extract_property_details(
                "", token, extract_api_only=True
            )

            if extracted_data:
                logging.info(f"[{token}] API-only test successful")
                # Print key extracted data for verification
                print(f"\n=== API Test Results for {token} ===")
                print(f"Area: {extracted_data.get('area')}")
                print(f"Bedrooms: {extracted_data.get('bedrooms')}")
                print(f"Price: {extracted_data.get('price')}")
                print(f"Has Parking: {extracted_data.get('has_parking')}")
                print(f"Has Storage: {extracted_data.get('has_storage')}")
                print(f"Has Balcony: {extracted_data.get('has_balcony')}")
                print(f"Floor Material: {extracted_data.get('floor_material')}")
                print("=" * 50)
            else:
                logging.error(f"[{token}] API-only test failed - no data extracted")

        else:
            # Full crawl with anti-detection measures
            run_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                page_timeout=random.randint(60000, 90000),
                delay_before_return_html=random.uniform(2.0, 4.0),
                scan_full_page=True,
                scroll_delay=random.uniform(0.3, 0.8),
                remove_overlay_elements=True,
                simulate_user=True,
                js_code=[
                    f"await new Promise(resolve => setTimeout(resolve, {random.randint(1000, 3000)}));",
                    "window.scrollTo(0, document.body.scrollHeight);",
                    f"await new Promise(resolve => setTimeout(resolve, {random.randint(500, 1500)}));",
                    "window.scrollTo(0, 0);",
                ],
                wait_for="css:div.kt-page-title, h1[class*='kt-page-title__title'], div.kt-carousel__cell",
                magic=True,
            )

            result = await crawler.arun(url=detail_url, config=run_config)

            if result.success and result.html:
                logging.info(
                    f"[{token}] Crawl successful. Parsing RAW HTML (length: {len(result.html)})..."
                )
                extracted_data = await extract_property_details(result.html, token)

                if extracted_data:
                    db_data = transform_for_db(extracted_data)
                    if not db_data:
                        logging.warning(
                            f"[{token}] Failed to transform data for DB (missing title/id?)."
                        )
                else:
                    logging.warning(
                        f"[{token}] Failed to extract details from HTML for URL {detail_url}."
                    )

                    # IMPROVED: Save problematic HTML for debugging
                    if not extracted_data:
                        debug_filename = Path(JSON_OUTPUT_DIR) / f"{token}_failed.html"
                        try:
                            await asyncio.to_thread(
                                lambda: Path(debug_filename).write_text(
                                    result.html, encoding="utf-8"
                                )
                            )
                            logging.info(
                                f"[{token}] Saved problematic HTML to {debug_filename} for debugging"
                            )
                        except Exception as err:
                            logging.error(f"[{token}] Failed to save debug HTML: {err}")

            elif result.success:
                logging.warning(
                    f"[{token}] Crawl successful but no HTML content found for {detail_url}. Status: {result.status_code}"
                )
            else:
                logging.error(
                    f"[{token}] Failed to crawl detail page {detail_url}: {result.error_message} (Status: {result.status_code})"
                )

    except Exception as e:
        logging.error(
            f"[{token}] Unexpected error during crawl/parse for {detail_url}: {e}",
            exc_info=True,
        )

    # --- Save to JSON (if extraction produced anything) ---
    if extracted_data:
        json_filename = Path(JSON_OUTPUT_DIR) / f"{token}.json"
        try:
            extracted_data_json = json.dumps(
                extracted_data, indent=2, ensure_ascii=False
            )
            await asyncio.to_thread(
                json_filename.write_text, extracted_data_json, encoding="utf-8"
            )
            logging.info(f"[{token}] Saved extracted data to {json_filename}")
        except Exception as json_e:
            logging.error(
                f"[{token}] Failed to save data to JSON file {json_filename}: {json_e}"
            )

    # --- Attempt to Save to DB (if data valid and pool exists) ---
    if db_data and db_pool:
        try:
            async with db_pool.acquire() as db_conn:
                await save_property_to_db(db_conn, db_data)

                # Process and store images if storage manager is available
                if storage_manager:
                    await storage_manager.process_property_images(db_data, db_conn)

                # Index in Elasticsearch INSIDE the connection context
                try:
                    await es_indexer.index_property(db_data, db_conn)
                    logging.info(
                        f"[{token}] Successfully indexed property in Elasticsearch"
                    )
                except Exception as es_error:
                    logging.error(
                        f"[{token}] Error indexing property in Elasticsearch: {es_error}"
                    )

        except Exception as db_e:
            logging.error(
                f"[{token}] Failed to acquire DB connection or save: {db_e}",
                exc_info=True,
            )
    elif db_data and not db_pool:
        logging.error(
            f"[{token}] Database pool is not available. Data saved to JSON only."
        )
    elif not api_only:
        # Only log this if we're not in API test mode
        logging.info(
            f"[{token}] {'No data to save to DB' if not db_data else 'Data processing completed'}"
        )

    # Add a small delay between requests to avoid rate limiting
    if not api_only:
        delay = random.uniform(1.0, 3.0)
        logging.debug(f"[{token}] Waiting {delay:.2f} seconds before next request...")
        await asyncio.sleep(delay)


# --- Main Orchestration ---
async def main():
    """Main orchestration function."""
    db_pool_instance = await init_db_pool()  # Initialize the pool

    load_dotenv()
    supabase_url = os.getenv(
        "SUPABASE_STORAGE_URL", "http://127.0.0.1:54321"
    )  # Local Supabase port
    supabase_key = os.getenv("SUPABASE_ROLE")
    if not supabase_key:
        logging.warning("SUPABASE_KEY not set in environment. Image storage disabled.")
        storage_manager = None
    else:
        storage_manager = SupabaseStorageManager(supabase_url, supabase_key)
        # Initialize storage bucket
        bucket_initialized = await storage_manager.init_bucket()
        if not bucket_initialized:
            logging.error(
                "Failed to initialize storage bucket. Image storage disabled."
            )
            storage_manager = None

    # Initialize Elasticsearch
    try:
        await es_indexer.init_client()
        # Get delete_existing parameter from environment variable (default to False)
        delete_existing = os.getenv("DELETE_ES_INDEXES", "False").lower() == "true"

        await es_indexer.create_indexes(delete_existing=delete_existing)
        logging.info("Elasticsearch initialized successfully")
    except Exception as e:
        logging.error(f"Failed to initialize Elasticsearch: {e}")
        # You can decide whether to continue without Elasticsearch or exit
        # For now, let's continue but with a warning
        logging.warning("Continuing without Elasticsearch indexing...")

    # Test mode configuration
    TEST_MODE = True  # Changed to False to test actual crawling
    TEST_TOKEN = "Aae8wB29"
    API_ONLY_TEST = False

    if TEST_MODE:
        logging.info("=== RUNNING IN TEST MODE ===")

        if API_ONLY_TEST:
            # API calls also use proxy via environment
            os.environ["HTTP_PROXY"] = PROXY_URL
            os.environ["HTTPS_PROXY"] = PROXY_URL
            await crawl_and_save_property(
                None,
                db_pool_instance,
                TEST_TOKEN,
                "test-slug",
                storage_manager=storage_manager,
                api_only=True,
            )
        else:
            browser_config = BrowserConfig(
                browser_type="chromium",
                headless=False,
                proxy=PROXY_URL,  # Add your proxy here
                viewport_width=random.randint(1200, 1400),
                viewport_height=random.randint(800, 1000),
                user_agent="random",
                verbose=True,
            )

            async with AsyncWebCrawler(config=browser_config) as crawler:
                await crawl_and_save_property(
                    crawler,
                    db_pool_instance,
                    TEST_TOKEN,
                    "test-slug",
                    storage_manager=storage_manager,
                    api_only=False,
                )

        logging.info("=== TEST MODE COMPLETED ===")
        # After TEST_MODE finishes, clean up and exit
        if db_pool_instance:
            await close_db_pool()
        await es_indexer.close_client()
        return

    # If TEST_MODE is False, check for a custom‐list argument
    if args.listname:
        tokens = load_tokens_from_file(args.listname)
        if not tokens:
            logging.error("No tokens to crawl. Exiting.")
            # Clean up and return
            if db_pool_instance:
                await close_db_pool()
            await es_indexer.close_client()
            return

        logging.info(
            f"Crawling a custom list of {len(tokens)} tokens from custom-lists/{args.listname}"
        )
        browser_config_main = BrowserConfig(headless=True, verbose=False)
        async with AsyncWebCrawler(config=browser_config_main) as crawler:
            for token in tokens:
                slug = quote(token)  # generate a safe slug; token itself is fine too
                await crawl_and_save_property(
                    crawler,
                    db_pool_instance,
                    token,
                    slug,
                    storage_manager=storage_manager,
                    api_only=False,
                )

        # After finishing the custom list, clean up and exit
        if db_pool_instance:
            await close_db_pool()
        await es_indexer.close_client()
        logging.info("Custom‐list crawling complete. Exiting.")
        return

    # --- If no custom list is provided, proceed with the original pagination logic ---

    # For API calls, also set environment variables
    os.environ["HTTP_PROXY"] = PROXY_URL
    os.environ["HTTPS_PROXY"] = PROXY_URL

    # Anti-detection measures
    # Regular crawling with proxy
    browser_config = BrowserConfig(
        browser_type="chromium",
        headless=True,
        proxy=PROXY_URL,  # Add your proxy here
        viewport_width=random.randint(1200, 1400),
        viewport_height=random.randint(800, 1000),
        user_agent="random",
        verbose=False,
    )

    # Anti-detection crawler run config
    run_config = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        page_timeout=random.randint(25000, 35000),  # Random timeout
        delay_before_return_html=random.uniform(2.0, 4.0),  # Random delay
        mean_delay=random.uniform(5.0, 10.0),  # Random delay between pages
        max_range=random.uniform(2.0, 5.0),  # Random variation
        scan_full_page=True,
        scroll_delay=random.uniform(0.3, 0.8),  # Random scroll delay
        remove_overlay_elements=True,
        simulate_user=True,  # Simulate human behavior
        js_code=[
            f"await new Promise(resolve => setTimeout(resolve, {random.randint(1000, 3000)}));",
            "window.scrollTo(0, document.body.scrollHeight);",
            f"await new Promise(resolve => setTimeout(resolve, {random.randint(500, 1500)}));",
            "window.scrollTo(0, 0);",
        ],
        wait_for="css:div.kt-page-title, h1[class*='kt-page-title__title'], div.kt-carousel__cell",
        magic=True,
    )

    next_page_cursor = None
    crawled_tokens = set()
    total_properties_processed = 0
    browser_config_main = BrowserConfig(headless=True, verbose=False)

    try:
        async with AsyncWebCrawler(
            config=browser_config_main
        ) as crawler, aiohttp.ClientSession() as http_session:

            for page_num in range(1, PAGES_TO_CRAWL + 1):
                listings_data = await fetch_divar_listings(
                    http_session, page=page_num, last_sort_date_cursor=next_page_cursor
                )

                if not listings_data:
                    logging.warning(
                        f"Stopping crawl: No listings data from page {page_num}."
                    )
                    break
                if "list_widgets" not in listings_data:
                    logging.warning(
                        f"API response for page {page_num} missing 'list_widgets'."
                    )
                    break

                property_widgets = [
                    w
                    for w in listings_data.get("list_widgets", [])
                    if w.get("widget_type") == "POST_ROW"
                ]

                if not property_widgets:
                    logging.info(f"No 'POST_ROW' widgets on page {page_num}.")
                    last_sort_date_cursor = None
                    try:
                        last_widget_action_log = listings_data["list_widgets"][-1].get(
                            "action_log", {}
                        )
                        sort_date_cursor = last_widget_action_log["server_side_info"][
                            "info"
                        ]["sort_date"]
                        if sort_date_cursor:
                            next_page_cursor = sort_date_cursor
                    except (KeyError, IndexError, TypeError):
                        pass
                    if not next_page_cursor and page_num < PAGES_TO_CRAWL:
                        logging.warning("No props & no pagination cursor. Stopping.")
                        break
                    continue

                semaphore = asyncio.Semaphore(MAX_CONCURRENT_CRAWLS)
                tasks = []

                async def crawl_with_semaphore_wrapper(widget, current_pool):
                    async with semaphore:
                        widget_data = widget.get("data", {})
                        action_payload = widget_data.get("action", {}).get(
                            "payload", {}
                        )
                        token = action_payload.get("token")
                        web_info = action_payload.get("web_info", {})
                        raw_slug = web_info.get("title", f"property-{token}")
                        safe_slug = quote(raw_slug.replace(" ", "-"))

                        if token and token not in crawled_tokens:
                            crawled_tokens.add(token)
                            await crawl_and_save_property(
                                crawler,
                                current_pool,
                                token,
                                safe_slug,
                                storage_manager=storage_manager,
                            )
                        elif token:
                            logging.debug(f"Token {token} already processed. Skipping.")

                for widget in property_widgets:
                    tasks.append(crawl_with_semaphore_wrapper(widget, db_pool_instance))

                await asyncio.gather(*tasks)
                total_properties_processed = len(crawled_tokens)

                # Determine cursor for NEXT page from the last widget of THIS page
                last_widget = property_widgets[-1]
                try:
                    sort_date_cursor = last_widget["action_log"]["server_side_info"][
                        "info"
                    ]["sort_date"]
                    if sort_date_cursor:
                        next_page_cursor = sort_date_cursor
                    else:
                        next_page_cursor = None
                        logging.warning(
                            f"Could not extract sort_date from last item on page {page_num}."
                        )
                except (KeyError, IndexError, TypeError) as e:
                    next_page_cursor = None
                    logging.error(f"Error extracting sort_date: {e}")

                logging.info(f"Next page cursor for fetch: {next_page_cursor}")
                if not next_page_cursor and page_num < PAGES_TO_CRAWL:
                    logging.warning(f"No next page cursor. Stopping.")
                    break
                await asyncio.sleep(1.5)

    except Exception as e:
        logging.error(
            f"An error occurred during the main crawl loop: {e}", exc_info=True
        )
    finally:
        if db_pool_instance:
            await close_db_pool()
        # Close Elasticsearch client
        await es_indexer.close_client()
        logging.info(
            f"Crawling finished or stopped. Processed {len(crawled_tokens)} unique properties."
        )
        logging.info(f"Check the '{JSON_OUTPUT_DIR}' directory for saved JSON files.")


# --- Script Entry Point ---
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Crawling interrupted by user.")
        # Cleanup is handled in main's finally block
