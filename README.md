# Divar Real Estate Crawler

This project uses `crawl4ai` to scrape real estate listings from Divar.ir for Tehran and store the data in a PostgreSQL database.

## Features

-   Fetches listing tokens from Divar's search API.
-   Crawls individual property detail pages using `crawl4ai`.
-   Parses relevant data (title, description, images, location, attributes, price) from detail page HTML.
-   Transforms data and inserts it into a PostgreSQL database using asyncpg.
-   Leverages PostgreSQL functions defined in migrations for structured data insertion.
-   Includes basic pagination logic for the listing API.
-   Uses environment variables for database configuration.

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
    *   Ensure you have PostgreSQL installed and running with PostGIS extension enabled (`CREATE EXTENSION IF NOT EXISTS postgis;`).
    *   Apply the provided SQL migration scripts (`20250325*.sql`, `20250326*.sql`, `20250327*.sql`) to create the necessary tables and functions. You can use a tool like `psql` or a migration tool (`flyway`, `alembic`, etc.).

5.  **Configure Environment Variables:**
    *   Create a `.env` file in the project root directory.
    *   Add your PostgreSQL connection string:
        ```dotenv
        DATABASE_URL=postgresql://YOUR_USER:YOUR_PASSWORD@YOUR_HOST:YOUR_PORT/YOUR_DB_NAME
        ```

## Running the Crawler

Execute the main script:

```bash
python main.py
