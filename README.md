# Divar Real Estate Crawler

This project uses `crawl4ai` to scrape real estate listings from Divar.ir for Tehran and store the data in a PostgreSQL database.

## Features

- Fetches listing tokens from Divar's search API.
- Crawls individual property detail pages using `crawl4ai`.
- Parses relevant data (title, description, images, location, attributes, price) from detail page HTML.
- Transforms data and inserts it into a PostgreSQL database using asyncpg.
- Leverages PostgreSQL functions defined in migrations for structured data insertion.
- Includes basic pagination logic for the listing API.
- Uses environment variables for database configuration.

## Setup

1.  **Clone the repository:**

    ```bash
    git clone <your-repo-url>
    cd divar_crawler
    ```

2.  **Create a virtual environment:**

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

    You might need to install Playwright browsers if `crawl4ai` doesn't do it automatically:

    ```bash
    playwright install --with-deps chromium
    ```

4.  **Set up PostgreSQL Database:**

    - Ensure you have PostgreSQL installed and running with PostGIS extension enabled (`CREATE EXTENSION IF NOT EXISTS postgis;`).
    - Apply the provided SQL migration scripts (`20250325*.sql`, `20250326*.sql`, `20250327*.sql`) to create the necessary tables and functions. You can use a tool like `psql` or a migration tool (`flyway`, `alembic`, etc.).

5.  **Configure Environment Variables:**
    - Create a `.env` file in the project root directory.
    - Add your PostgreSQL connection string:
      ```dotenv
      DATABASE_URL=postgresql://YOUR_USER:YOUR_PASSWORD@YOUR_HOST:YOUR_PORT/YOUR_DB_NAME
      ```

## Running the Crawler

Execute the main script:

```bash
python3.12 -m venv venv && source venv/bin/activate && python3.12 main.py
```

## Update real estate properties

When crawling divar the type remains NULL. Use the following function to fix that:

```sql
UPDATE public.properties
SET type =
    CASE
        -- 1. Check for VILA
        WHEN title ILIKE '%ویلا%' OR description ILIKE '%ویلا%' OR
             title ILIKE '%ویلایی%' OR description ILIKE '%ویلایی%'
            THEN 'vila'

        -- 2. Check for APARTMENT (if not already classified as vila)
        WHEN title ILIKE '%آپارتمان%' OR description ILIKE '%آپارتمان%' OR
             title ILIKE '%اپارتمان%' OR description ILIKE '%اپارتمان%' OR -- Common typo/alternative
             title ILIKE '%برج%' OR description ILIKE '%برج%' OR
             title ILIKE '%مجتمع مسکونی%' OR description ILIKE '%مجتمع مسکونی%' OR
             ( (title ILIKE '%واحد%' OR description ILIKE '%واحد%') AND -- "واحد" is a strong indicator for apartment
               NOT (title ILIKE '%ویلا%' OR description ILIKE '%ویلا%') AND -- but ensure it's not a "واحد ویلایی"
               NOT (title ILIKE '%زمین%' OR description ILIKE '%زمین%') -- and not "واحد زمین" (less likely)
             )
            THEN 'apartment'

        -- 3. Check for LAND (if not already classified as vila or apartment)
        WHEN title ILIKE '%زمین%' OR description ILIKE '%زمین%' OR
             title ILIKE '%قطعه زمین%' OR description ILIKE '%قطعه زمین%' OR
             title ILIKE '%قطعه%' OR description ILIKE '%قطعه%' OR -- Often used with زمین
             ( (title ILIKE '%باغ%' OR description ILIKE '%باغ%') AND
               NOT (title ILIKE '%ویلا%' OR description ILIKE '%ویلا%') AND
               NOT (title ILIKE '%آپارتمان%' OR description ILIKE '%آپارتمان%') AND
               NOT (title ILIKE '%اپارتمان%' OR description ILIKE '%اپارتمان%')
             ) OR
             ( (title ILIKE '%باغچه%' OR description ILIKE '%باغچه%') AND
               NOT (title ILIKE '%ویلا%' OR description ILIKE '%ویلا%') AND
               NOT (title ILIKE '%آپارتمان%' OR description ILIKE '%آپارتمان%') AND
               NOT (title ILIKE '%اپارتمان%' OR description ILIKE '%اپارتمان%')
             )
            THEN 'land'

        -- If none of the above, keep it NULL (or set to 'unknown' if you prefer)
        ELSE NULL
    END
WHERE type IS NULL; -- Only update rows where type is currently NULL
```
