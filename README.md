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

### Supabase

#### Supabase Storage Integration for Divar Crawler

This section documents how the crawler uses Supabase Storage to store property images fetched from Divar.

##### Overview

The crawler fetches property images from Divar listings and stores them in Supabase Storage, which provides:

- Persistent storage for all property images
- Public URLs for easy access
- Organization by property ID

#### Environment Configuration

To enable Supabase Storage integration, add the following to your `.env` file:

```env
# Database
DATABASE_URL="postgresql://postgres:postgres@localhost:54322/postgres"

# Elasticsearch
ELASTICSEARCH_URL=http://localhost:9200
DELETE_ES_INDEXES=true

# Supabase Storage
SUPABASE_STORAGE_URL=http://127.0.0.1:54321  # Default port for self-hosted Supabase
SUPABASE_KEY=your_anon_key_here              # Public anon key for client use
SUPABASE_ROLE=your_service_role_key_here     # Service role key for admin operations
```

###### Environment Variable Details

- `SUPABASE_STORAGE_URL`: URL of your Supabase instance (http://127.0.0.1:54321 for local development)
- `SUPABASE_KEY`: The anon key from your Supabase project (used for public operations)
- `SUPABASE_ROLE`: The service role key with admin permissions (required for bucket management)

##### How It Works

1. The crawler downloads property images from Divar to temporary local storage
2. Images are uploaded to Supabase Storage in buckets organized by property ID
3. Public URLs for the stored images are saved in the `property_images` database table
4. Original temporary images are deleted after successful upload

##### Setup Instructions

###### 1. Create the Storage Bucket

When running self-hosted Supabase, you need to create a storage bucket for property images. This can be done:

####### Automatically (via code)

The crawler will attempt to create the bucket automatically on startup if it doesn't exist. This requires the `SUPABASE_ROLE` key to have sufficient permissions.

####### Manually (via Supabase UI)

If you prefer to set up manually:

1. Open your Supabase Studio (http://localhost:54323)
2. Navigate to Storage in the left sidebar
3. Click "New Bucket"
4. Name it "property-images"
5. Check "Public bucket" to make images publicly accessible
6. Click "Create bucket"

###### 2. Verify Storage Access

You can verify your storage is correctly configured by running:

```bash
python3 image_storage.py
```

This standalone script will:

- Connect to your Supabase instance
- Create a test bucket if needed
- Upload a test image
- Verify the public URL is accessible

##### In Production

For staging or production environments:

1. Update the environment variables with the appropriate URLs and keys
2. Ensure your Supabase instance has enough storage capacity for your needs
3. Consider setting up bucket lifecycle policies for cleanup of old images

##### Troubleshooting

###### Permission Issues

If you encounter "Unauthorized" or "row-level security policy" errors:

- Ensure you're using the `service_role` key in the `SUPABASE_ROLE` environment variable
- Check that the bucket exists and is accessible
- Verify your Supabase instance is running correctly

###### Storage Access Issues

If you can't access stored images:

- Confirm the bucket is set to "public"
- Check network connectivity between your services
- Verify the Supabase Storage API is running (`docker ps | grep storage-api`)

###### Database Integration Issues

If images are stored but not appearing in your application:

- Check that the `property_images` table exists and has the correct schema
- Ensure the image URLs are being stored correctly in the database
- Verify the property IDs match between storage paths and database records

##### Technical Details

Images are stored with this path structure:

```
property-images/[property_external_id]/[unique_filename]
```

Public URLs follow this format:

```
http://127.0.0.1:54321/storage/v1/object/public/property-images/[property_external_id]/[unique_filename]
```

The crawler optimizes storage by:

- Only storing full-size images (excluding thumbnails)
- Limiting to 3-5 images per property to save space
- Using unique filenames to prevent collisions
