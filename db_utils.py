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
    """
    Calls the import_mongodb_property function in PostgreSQL.

    Args:
        conn: An active asyncpg connection or pool connection.
        property_data: A dictionary containing data matching the function's signature.
                       Keys should match the p_ parameter names.
    """
    try:
        # Ensure JSON fields are correctly formatted
        location_json = json.dumps(property_data.get("p_location")) if property_data.get("p_location") else None
        attributes_json = json.dumps(property_data.get("p_attributes"), ensure_ascii=False) if property_data.get("p_attributes") else None
        image_urls_json = json.dumps(property_data.get("p_image_urls")) if property_data.get("p_image_urls") else None
        highlight_flags_json = json.dumps(property_data.get("p_highlight_flags"), ensure_ascii=False) if property_data.get("p_highlight_flags") else None
        similar_properties_json = json.dumps(property_data.get("p_similar_properties")) if property_data.get("p_similar_properties") else None

        result_id = await conn.fetchval(
            """
            SELECT import_mongodb_property(
                $1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb,
                $8, $9, $10, $11, $12::jsonb, $13::jsonb
            )
            """,
            property_data.get("p_external_id"),
            property_data.get("p_title"),
            property_data.get("p_description"),
            property_data.get("p_price"),
            location_json,
            attributes_json,
            image_urls_json,
            property_data.get("p_investment_score"),
            property_data.get("p_market_trend"),
            property_data.get("p_neighborhood_fit_score"),
            property_data.get("p_rent_to_price_ratio"),
            highlight_flags_json,
            similar_properties_json
        )
        logging.info(f"Successfully saved/updated property {property_data.get('p_external_id')}. DB ID: {result_id}")
        return result_id
    except asyncpg.exceptions.UniqueViolationError:
         logging.warning(f"Property with external_id {property_data.get('p_external_id')} already exists. Skipping or update handled by DB.")
         return None # Or fetch existing ID if needed
    except Exception as e:
        logging.error(f"Error saving property {property_data.get('p_external_id')} to DB: {e}")
        logging.error(f"Data that caused error: {property_data}") # Log the problematic data
        return None
