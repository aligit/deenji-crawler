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
from fetch_viewport_listings import (extract_tokens_from_viewport_response,
                                     fetch_viewport_listings)
from image_storage import SupabaseStorageManager
from viewport_api import load_bbox_config

# --- command-line argument parsing ---
parser = argparse.ArgumentParser(
    description=(
        "Divar crawler with support for custom bounding boxes. "
        "You can provide a list of IDs to crawl and/or a bounding box configuration."
    )
)
parser.add_argument(
    "--list",
    dest="listname",
    help="Name of a file inside custom-lists/ containing one Divar ID per line",
)
parser.add_argument(
    "--bbox",
    dest="bbox_config",
    help="Name of a file inside bbox_configs/ containing bbox values",
)
parser.add_argument(
    "--test",
    action="store_true",
    help="Run in test mode with a single property",
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
async def fetch_divar_listings(
    session, page=1, last_sort_date_cursor=None, bbox_config=None
):
    """Fetches a page of listings from the Divar API using the last sort_date."""
    payload = {
        "city_ids": [TARGET_CITY_ID],
        "source_view": "CATEGORY",
        "disable_recommendation": False,
        "map_state": {
            "camera_info": {
                "bbox": {
                    "min_latitude": 35.74,
                    "min_longitude": 51.30,
                    "max_latitude": 35.74,
                    "max_longitude": 51.31,
                },
                "place_hash": f"{TARGET_CITY_ID}||real-estate",
                "zoom": 14.568499456622654,
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

    # Override bbox values if provided
    if bbox_config:
        payload["map_state"]["camera_info"]["bbox"] = bbox_config
        logging.info(f"Using custom bbox configuration: {bbox_config}")

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
    # Initialize database and storage
    db_pool_instance = await init_db_pool()

    # Initialize storage manager if enabled
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_STORAGE_URL", "http://127.0.0.1:54321")
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
        logging.warning("Continuing without Elasticsearch indexing...")

    # Test mode
    if args.test:
        logging.info("=== RUNNING IN TEST MODE ===")
        TEST_TOKEN = "Aa8EDApT"  # Default test token

        # API calls also use proxy via environment
        os.environ["HTTP_PROXY"] = PROXY_URL
        os.environ["HTTPS_PROXY"] = PROXY_URL

        browser_config = BrowserConfig(
            browser_type="chromium",
            headless=False,
            proxy=PROXY_URL,
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

        # Clean up and exit
        if db_pool_instance:
            await close_db_pool()
        await es_indexer.close_client()
        return

    # Set environment variables for API calls
    os.environ["HTTP_PROXY"] = PROXY_URL
    os.environ["HTTPS_PROXY"] = PROXY_URL

    # Process tokens from a custom list if provided
    tokens_to_crawl = set()
    if args.listname:
        custom_tokens = load_tokens_from_file(args.listname)
        tokens_to_crawl.update(custom_tokens)
        logging.info(
            f"Loaded {len(custom_tokens)} tokens from custom list '{args.listname}'"
        )

    # Process tokens from a bounding box if provided
    if args.bbox_config:
        bbox_config = load_bbox_config(args.bbox_config)
        if not bbox_config:
            logging.error(f"Failed to load bbox configuration: {args.bbox_config}")
            if (
                not tokens_to_crawl
            ):  # Only exit if we don't have any tokens from custom list
                if db_pool_instance:
                    await close_db_pool()
                await es_indexer.close_client()
                return
        else:
            # Fetch properties from viewport API
            async with aiohttp.ClientSession() as http_session:
                viewport_data = await fetch_viewport_listings(http_session, bbox_config)
                viewport_tokens = extract_tokens_from_viewport_response(viewport_data)

                if viewport_tokens:
                    tokens_to_crawl.update(viewport_tokens)
                    logging.info(
                        f"Added {len(viewport_tokens)} tokens from bbox '{args.bbox_config}'"
                    )
                else:
                    logging.error("No tokens found in the specified bounding box")

    # Exit if we don't have any tokens to crawl
    if not tokens_to_crawl:
        logging.error("No tokens to crawl. Exiting.")
        if db_pool_instance:
            await close_db_pool()
        await es_indexer.close_client()
        return

    logging.info(f"Preparing to crawl {len(tokens_to_crawl)} unique tokens")

    # Configure browser for crawling
    browser_config_main = BrowserConfig(
        browser_type="chromium",
        headless=True,
        proxy=PROXY_URL,
        viewport_width=random.randint(1200, 1400),
        viewport_height=random.randint(800, 1000),
        user_agent="random",
        verbose=False,
    )

    # Crawl the tokens
    async with AsyncWebCrawler(config=browser_config_main) as crawler:
        # Use a semaphore to limit concurrent crawls
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_CRAWLS)
        tasks = []

        async def crawl_with_semaphore(token, current_pool):
            async with semaphore:
                slug = quote(token)  # Generate a safe slug
                await crawl_and_save_property(
                    crawler,
                    current_pool,
                    token,
                    slug,
                    storage_manager=storage_manager,
                    api_only=False,
                )

        for token in tokens_to_crawl:
            tasks.append(crawl_with_semaphore(token, db_pool_instance))

        await asyncio.gather(*tasks)

    # Clean up
    if db_pool_instance:
        await close_db_pool()
    await es_indexer.close_client()

    logging.info(
        f"Crawling completed. Processed {len(tokens_to_crawl)} unique properties."
    )
    logging.info(f"Check the '{JSON_OUTPUT_DIR}' directory for saved JSON files.")


# --- Script Entry Point ---
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Crawling interrupted by user.")
        # Cleanup is handled in main's finally block
