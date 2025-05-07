import asyncpg
import os
import logging
import json
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
pool = None

async def init_db_pool():
    """Initializes the database connection pool."""
    global pool
    if pool is None:
        try:
            pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
            logging.info("Database pool initialized.")
        except Exception as e:
            logging.error(f"Error initializing database pool: {e}")
            raise
    return pool

async def close_db_pool():
    """Closes the database connection pool."""
    global pool
    if pool:
        await pool.close()
        logging.info("Database pool closed.")
        pool = None

async def save_property_to_db(conn: asyncpg.Connection, property_data: dict):
    """Calls the import_mongodb_property function in PostgreSQL."""
    external_id = property_data.get("p_external_id") # Get ID early for logging
    try:
        # Ensure JSON fields are correctly formatted
        location_json = json.dumps(property_data.get("p_location")) if property_data.get("p_location") else None
        # Use ensure_ascii=False for Persian text in JSON
        attributes_json = json.dumps(property_data.get("p_attributes", []), ensure_ascii=False)
        image_urls_json = json.dumps(property_data.get("p_image_urls", []))
        highlight_flags_json = json.dumps(property_data.get("p_highlight_flags", []), ensure_ascii=False)
        similar_properties_json = json.dumps(property_data.get("p_similar_properties", []))

        # Handle potential None values for numeric/int fields expected by DB
        price = property_data.get("p_price")
        investment_score = property_data.get("p_investment_score")
        neighborhood_fit_score = property_data.get("p_neighborhood_fit_score")
        rent_to_price_ratio = property_data.get("p_rent_to_price_ratio")

        logging.debug(f"[{external_id}] Calling import_mongodb_property with data...")

        result_id = await conn.fetchval(
            """
            SELECT import_mongodb_property(
                $1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb,
                $8, $9, $10, $11, $12::jsonb, $13::jsonb
            )
            """,
            external_id,
            property_data.get("p_title", "N/A"), # Provide default for title
            property_data.get("p_description"),
            price, # Pass potentially None price
            location_json,
            attributes_json,
            image_urls_json,
            investment_score,
            property_data.get("p_market_trend"),
            neighborhood_fit_score,
            rent_to_price_ratio,
            highlight_flags_json,
            similar_properties_json
        )
        logging.info(f"[{external_id}] Successfully saved/updated property. DB ID: {result_id}")
        return result_id
    except asyncpg.exceptions.UniqueViolationError:
         logging.warning(f"[{external_id}] Property already exists. Skipping or update handled by DB.")
         return None
    except Exception as e:
        logging.error(f"[{external_id}] Error saving property to DB: {e}", exc_info=True) # Add exc_info
        # Log the specific data structure being sent
        loggable_data = {k: (v if len(str(v)) < 200 else str(v)[:197] + '...') # Truncate long strings
                         for k, v in property_data.items()}
        logging.error(f"[{external_id}] Data causing error (truncated): {json.dumps(loggable_data, indent=2, ensure_ascii=False)}")
        return None

# ... (rest of db_utils.py: close_db_pool) ...
