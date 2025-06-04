# db_utils.py

import json
import logging
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
# Keep module-level variable primarily for closing, but ensure functions get it passed
_pool: asyncpg.Pool | None = None


async def init_db_pool() -> asyncpg.Pool | None:
    """Initializes the database connection pool and returns it."""
    global _pool  # Reference the module-level variable for assignment
    # Prevent re-initialization if already connected
    if _pool is not None and not _pool.is_closing():
        logging.info("Database pool already initialized.")
        return _pool

    if not DATABASE_URL:
        logging.error("DATABASE_URL environment variable not set.")
        _pool = None
        return None

    try:
        logging.info(
            f"Attempting to create database pool for: {DATABASE_URL[:DATABASE_URL.find('@')]}..."
        )
        pool_instance = await asyncpg.create_pool(
            DATABASE_URL, min_size=1, max_size=10, timeout=30
        )
        # Verify connection
        async with pool_instance.acquire() as conn:
            await conn.execute("SELECT 1")
        logging.info("Database pool initialized and connection verified.")
        _pool = pool_instance  # Assign to module-level variable

        return _pool  # Return the created pool
    except (
        asyncpg.exceptions.InvalidConnectionParametersError,
        asyncpg.exceptions.CannotConnectNowError,
        ConnectionRefusedError,
        TimeoutError,
        OSError,
    ) as e:
        logging.error(f"Error initializing database pool: {type(e).__name__} - {e}")
        logging.error(
            "Please check your DATABASE_URL, network connectivity, and Supabase/Postgres server status."
        )
        _pool = None
        return None
    except Exception as e:
        logging.error(
            f"An unexpected error occurred during pool initialization: {e}",
            exc_info=True,
        )
        _pool = None
        return None


async def close_db_pool():
    """Closes the database connection pool."""
    global _pool
    if _pool and not _pool._closed:
        try:
            await _pool.close()
            logging.info("Database pool closed.")
        except Exception as e:
            logging.error(f"Error closing database pool: {e}", exc_info=True)
        finally:
            _pool = None
    else:
        logging.info("Database pool was already closed or not initialized.")


# Updated save_property_to_db function with 27 parameters
async def save_property_to_db(conn: asyncpg.Connection, property_data: dict):
    """Calls the improved insert_property_direct function in PostgreSQL with explicit parameter passing."""
    external_id = property_data.get("p_external_id")
    try:
        # JSON formatting and None handling
        location_json = (
            json.dumps(property_data.get("p_location"))
            if property_data.get("p_location")
            else None
        )
        attributes_json = json.dumps(
            property_data.get("p_attributes", []), ensure_ascii=False
        )
        image_urls_json = json.dumps(property_data.get("p_image_urls", []))
        highlight_flags_json = json.dumps(
            property_data.get("p_highlight_flags", []), ensure_ascii=False
        )
        similar_properties_json = json.dumps(
            property_data.get("p_similar_properties", [])
        )

        # Ensure these are actual numbers or None
        price = property_data.get("p_price")
        price_per_meter = property_data.get("p_price_per_meter")
        bedrooms = property_data.get("p_bedrooms")
        area = property_data.get("p_area")
        year_built = property_data.get("p_year_built")
        investment_score = property_data.get("p_investment_score")
        neighborhood_fit_score = property_data.get("p_neighborhood_fit_score")
        rent_to_price_ratio = property_data.get("p_rent_to_price_ratio")

        # Ensure text values are properly passed
        bathroom_type = property_data.get("p_bathroom_type")
        heating_system = property_data.get("p_heating_system")
        cooling_system = property_data.get("p_cooling_system")
        floor_material = property_data.get("p_floor_material")
        hot_water_system = property_data.get("p_hot_water_system")

        # Ensure boolean values are properly typed
        has_parking = bool(property_data.get("p_has_parking", False))
        has_storage = bool(property_data.get("p_has_storage", False))
        has_balcony = bool(property_data.get("p_has_balcony", False))

        # Geospatial parameters
        longitude = property_data.get("p_longitude")
        latitude = property_data.get("p_latitude")

        logging.debug(f"[{external_id}] Calling insert_property_direct with data...")
        logging.debug(
            f"[{external_id}] Numeric values: price={price}, bedrooms={bedrooms}, area={area}, year_built={year_built}"
        )
        logging.debug(
            f"[{external_id}] Geospatial: longitude={longitude}, latitude={latitude}"
        )

        # Call the stored procedure as before
        result_id = await conn.fetchval(
            """
            SELECT insert_property_direct(
                $1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb,
                $8, $9, $10, $11, $12::jsonb, $13::jsonb,
                $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27
            )
            """,
            external_id,
            property_data.get("p_title", "N/A"),
            property_data.get("p_description"),
            price,
            location_json,
            attributes_json,
            image_urls_json,
            investment_score,
            property_data.get("p_market_trend"),
            neighborhood_fit_score,
            rent_to_price_ratio,
            highlight_flags_json,
            similar_properties_json,
            price_per_meter,
            has_parking,
            has_storage,
            has_balcony,
            bedrooms,
            bathroom_type,
            heating_system,
            cooling_system,
            floor_material,
            hot_water_system,
            area,
            year_built,
            longitude,
            latitude,
        )

        # If we have coordinates and a valid result_id, update the PostGIS geometry
        if result_id and longitude is not None and latitude is not None:
            try:
                await conn.execute(
                    """
                    UPDATE properties 
                    SET geom = ST_SetSRID(ST_MakePoint($1, $2), 4326)
                    WHERE id = $3
                    """,
                    longitude,
                    latitude,
                    result_id,
                )
                logging.debug(
                    f"[{external_id}] Updated geometry for property ID {result_id} "
                    f"with coordinates: {longitude}, {latitude}"
                )
            except Exception as e:
                logging.warning(
                    f"[{external_id}] Failed to update geometry for property ID {result_id}: {e}"
                )
                # Don't fail the whole operation if geometry update fails

        logging.info(
            f"[{external_id}] Successfully saved/updated property. DB ID: {result_id}"
        )
        return result_id

    except asyncpg.exceptions.UniqueViolationError:
        logging.warning(
            f"[{external_id}] Property already exists. Skipping or update handled by DB."
        )
        return None
    except Exception as e:
        logging.error(
            f"[{external_id}] Error saving property to DB: {e}", exc_info=True
        )
        loggable_data = {
            k: (v if len(str(v)) < 200 else str(v)[:197] + "...")
            for k, v in property_data.items()
        }
        logging.error(
            f"[{external_id}] Data causing error (truncated): {json.dumps(loggable_data, indent=2, ensure_ascii=False)}"
        )
        return None
