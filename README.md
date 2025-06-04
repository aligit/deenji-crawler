# Divar Real Estate Crawler

This project uses `crawl4ai` to scrape real estate listings from Divar.ir for Tehran and store the data in a PostgreSQL database with Elasticsearch for search and autocomplete functionality.

## Features

- Fetches listing tokens from Divar's search API.
- Supports bounding box (map area) based crawling for targeted neighborhood scraping.
- Crawls individual property detail pages using `crawl4ai`.
- Parses relevant data (title, description, images, location, attributes, price) from detail page HTML.
- Transforms data and inserts it into a PostgreSQL database using asyncpg.
- Leverages PostgreSQL functions defined in migrations for structured data insertion.
- Uses environment variables for database configuration.
- **Elasticsearch integration for fast search and autocomplete functionality.**
- **Supabase Storage integration for image storage.**

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
    - Add your PostgreSQL connection string and Elasticsearch configuration:

      ```dotenv
      DATABASE_URL=postgresql://YOUR_USER:YOUR_PASSWORD@YOUR_HOST:YOUR_PORT/YOUR_DB_NAME
      ELASTICSEARCH_URL=http://localhost:9200
      DELETE_ES_INDEXES=true

      # Supabase Storage (optional)
      SUPABASE_STORAGE_URL=http://127.0.0.1:54321
      SUPABASE_KEY=your_anon_key_here
      SUPABASE_ROLE=your_service_role_key_here
      ```

## Running the Crawler

The crawler supports three main crawling approaches:

1. **Targeted Area Crawling** (recommended): Crawl properties within a specific map area using bounding box coordinates
2. **Custom List Crawling**: Crawl a list of specific property IDs
3. **Test Mode**: Crawl a single property for testing

### Targeted Area Crawling

This approach lets you crawl all properties within a specific geographic area (neighborhood, district, etc.).

#### Step 1: Create a Bounding Box Configuration

Create a bounding box configuration from a Divar URL:

```bash
python create_bbox_config.py --url "https://divar.ir/s/iran/buy-residential?bbox=51.3005066%2C35.7437935%2C51.3155823%2C35.7472916&cities=1%2C1708&map_place_hash=1%2C1708%7C%7Cresidential-sell" --name mantaghe5-pajuhande.json
```

This will create a file in the `bbox_configs/` directory with the coordinates extracted from the URL.

**TIP:** To get this URL:

1. Go to Divar.ir and navigate to the map view
2. Zoom to your desired area
3. Copy the URL from your browser
4. Paste it into the command above

#### Step 2: Run the Crawler with the Bounding Box

```bash
source .env && python3.12 main.py --bbox mantaghe5-pajuhande.json
```

This will crawl all properties within the specified area.

### Custom List Crawling

If you have specific Divar property IDs you want to crawl:

#### Step 1: Create a Custom List File

Create a file in the `custom-lists/` directory with one Divar ID per line:

```
# File: custom-lists/premium-properties.txt
Aa7Mvffn
Aa9cJxBm
Aayo9F4L
AakIWY9p
```

#### Step 2: Run the Crawler with the Custom List

```bash
source .env && python3.12 main.py --list premium-properties.txt
```

### Combined Approach

You can combine both approaches to crawl both a specific area and a custom list:

```bash
source .env && python3.12 main.py --bbox mantaghe5-pajuhande.json --list premium-properties.txt
```

### Test Mode

To test the crawler with a single property:

```bash
source .env && python3.12 main.py --test
```

## Command Line Arguments

| Argument      | Description                                                |
| ------------- | ---------------------------------------------------------- |
| `--bbox FILE` | Name of a file in bbox_configs/ containing map coordinates |
| `--list FILE` | Name of a file in custom-lists/ with Divar IDs             |
| `--test`      | Run in test mode with a single property                    |

## Creating Custom Bounding Boxes

### Method 1: Using create_bbox_config.py (Recommended)

Extract coordinates directly from a Divar map URL:

```bash
python create_bbox_config.py --url "DIVAR_URL_WITH_BBOX_PARAMETER" --name area_name.json
```

### Method 2: Manual Creation

Create a JSON file in `bbox_configs/` with the following structure:

```json
{
  "min_latitude": 35.743794,
  "min_longitude": 51.300506,
  "max_latitude": 35.747293,
  "max_longitude": 51.315583,
  "zoom": 14.568499456622654
}
```

## Post-Processing Real Estate Properties

### Type Classification

After crawling, property types may be NULL. Use this SQL function to update them:

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

## Elasticsearch Autocomplete API

The crawler includes optimized Elasticsearch autocomplete functionality for property types. Here are the different approaches and their use cases:

### Clean Completion Suggestions (Recommended for Autocomplete)

This approach returns **only the suggestion text** with minimal response size (<1KB), perfect for real-time autocomplete:

```bash
# OPTIMIZED: Clean suggestions only - Fast & lightweight
curl -X POST "localhost:9200/divar_properties/_search" \
-H "Content-Type: application/json" \
-d '{
  "size": 0,                    # No document hits (saves bandwidth)
  "_source": false,             # No document sources (faster response)
  "suggest": {
    "property_type_completion": {
      "prefix": "آپ",             # User input prefix
      "completion": {
        "field": "property_type_suggest",
        "size": 10,               # Higher for deduplication
        "contexts": {
          "location": ["تهران"],   # Filter by city
          "stage": ["property_type"]
        }
      }
    }
  }
}' | jq '[.suggest.property_type_completion[0].options[].text] | unique'
```

**Output:** `["آپارتمان"]` (tiny response)

### Example Commands for Different Property Types

**Apartments (آپارتمان):**

```bash
curl -X POST "localhost:9200/divar_properties/_search" \
-H "Content-Type: application/json" \
-d '{
  "size": 0,
  "_source": false,
  "suggest": {
    "property_type_completion": {
      "prefix": "آپ",
      "completion": {
        "field": "property_type_suggest",
        "size": 10,
        "contexts": {
          "location": ["تهران"],
          "stage": ["property_type"]
        }
      }
    }
  }
}' | jq '[.suggest.property_type_completion[0].options[].text] | unique'
```

**Villas (ویلا):**

```bash
curl -X POST "localhost:9200/divar_properties/_search" \
-H "Content-Type: application/json" \
-d '{
  "size": 0,
  "_source": false,
  "suggest": {
    "property_type_completion": {
      "prefix": "وی",
      "completion": {
        "field": "property_type_suggest",
        "size": 10,
        "contexts": {
          "location": ["تهران"],
          "stage": ["property_type"]
        }
      }
    }
  }
}' | jq '[.suggest.property_type_completion[0].options[].text] | unique'
```

**Land (زمین):**

```bash
curl -X POST "localhost:9200/divar_properties/_search" \
-H "Content-Type: application/json" \
-d '{
  "size": 0,
  "_source": false,
  "suggest": {
    "property_type_completion": {
      "prefix": "زم",
      "completion": {
        "field": "property_type_suggest",
        "size": 10,
        "contexts": {
          "location": ["تهران"],
          "stage": ["property_type"]
        }
      }
    }
  }
}' | jq '[.suggest.property_type_completion[0].options[].text] | unique'
```

### Hybrid Approach (Suggestions + Property Previews)

For a Zillow-style experience showing both suggestions and property previews:

```bash
# HYBRID: Suggestions + Limited property hits
curl -X POST "localhost:9200/divar_properties/_search" \
-H "Content-Type: application/json" \
-d '{
  "size": 3,                    # Show 3 property previews
  "_source": ["title", "price", "bedrooms", "area", "image_urls"],  # Essential fields only
  "query": {
    "bool": {
      "must": [
        {"term": {"location.city": "تهران"}},
        {"term": {"property_type.keyword": "آپارتمان"}}
      ]
    }
  },
  "suggest": {
    "property_type_completion": {
      "prefix": "آپ",
      "completion": {
        "field": "property_type_suggest",
        "size": 5,
        "contexts": {
          "location": ["تهران"],
          "stage": ["property_type"]
        }
      }
    }
  }
}'
```

آپارتمان ۲خوابه بین 10000000000 تا 20000000000

```sh
curl -X POST "localhost:9200/divar_properties/_search" \
-H "Content-Type: application/json" \
-d '{
  "size": 5,
  "_source": ["title", "price", "bedrooms", "property_type"],
  "query": {
    "bool": {
      "must": [
        {"term": {"property_type.keyword": "آپارتمان"}},
        {"term": {"bedrooms": 2}}
      ],
      "filter": [
        {
          "range": {
            "price": {
              "lte": 20000000000,
              "gte": 10000000000
            }
          }
        }
      ]
    }
  }
}' | jq '{
  search_criteria: "آپارتمان + ۲خوابه + تا ۱ میلیارد",
  total_found: .hits.total.value,
  results: [.hits.hits[]._source | {
    title,
    price: (.price/1000000000 | tostring + " میلیارد"),
    bedrooms
  }]
}'
```

### Performance Comparison

| Approach                           | Response Size | Use Case                              |
| ---------------------------------- | ------------- | ------------------------------------- |
| **BEFORE:** Full document hits     | 25KB+         | ❌ Too heavy for autocomplete         |
| **AFTER:** Clean suggestions only  | <1KB          | ✅ Perfect for real-time autocomplete |
| **HYBRID:** Suggestions + previews | 5-10KB        | ✅ Rich search experience             |

### Integration Notes

- Use the **clean suggestions** approach for dropdown autocomplete
- Use the **hybrid approach** for search result pages
- The `jq` filter `[.suggest.property_type_completion[0].options[].text] | unique` removes duplicates
- Context filters ensure suggestions are relevant to the selected city
- Higher `size` values (10 vs 5) improve deduplication effectiveness

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
