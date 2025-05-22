# image_storage.py

import asyncio
import logging
import os
import uuid
from pathlib import Path
from urllib.parse import urlparse

import aiohttp


class SupabaseStorageManager:
    def __init__(self, supabase_url, supabase_key, bucket_name="property-images"):
        """
        Initialize Supabase Storage manager for self-hosted instance

        Args:
            supabase_url: Your Supabase URL (http://127.0.0.1:54321 for self-hosted)
            supabase_key: Your Supabase anon/service key
            bucket_name: The bucket to store images in
        """
        self.supabase_url = supabase_url.rstrip("/")
        self.supabase_key = supabase_key
        self.bucket_name = bucket_name
        # For self-hosted Supabase, use this endpoint pattern
        self.storage_url = f"{self.supabase_url}/storage/v1"
        self.headers = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
        }

    async def init_bucket(self):
        """Initialize the storage bucket if it doesn't exist"""
        async with aiohttp.ClientSession() as session:
            # List buckets to check if ours exists
            async with session.get(
                f"{self.storage_url}/bucket", headers=self.headers
            ) as response:
                if response.status == 200:
                    buckets = await response.json()
                    bucket_exists = any(
                        bucket.get("name") == self.bucket_name for bucket in buckets
                    )

                    if not bucket_exists:
                        # Create bucket if it doesn't exist
                        bucket_data = {
                            "id": self.bucket_name,
                            "name": self.bucket_name,
                            "public": True,
                        }
                        async with session.post(
                            f"{self.storage_url}/bucket",
                            headers=self.headers,
                            json=bucket_data,
                        ) as create_response:
                            if create_response.status in (200, 201):
                                logging.info(
                                    f"Created storage bucket: {self.bucket_name}"
                                )
                            else:
                                error_text = await create_response.text()
                                logging.error(
                                    f"Failed to create bucket: {error_text} (Status: {create_response.status})"
                                )
                                return False
                    else:
                        logging.info(f"Bucket '{self.bucket_name}' already exists")

                    # Make sure bucket is public
                    await self._ensure_bucket_public()
                    return True
                else:
                    error_text = await response.text()
                    logging.error(
                        f"Error checking buckets: {error_text} (Status: {response.status})"
                    )
                    return False

    async def _ensure_bucket_public(self):
        """Make sure bucket is set to public access"""
        async with aiohttp.ClientSession() as session:
            # Update bucket to ensure it's public
            bucket_data = {"id": self.bucket_name, "public": True}
            async with session.put(
                f"{self.storage_url}/bucket/{self.bucket_name}",
                headers=self.headers,
                json=bucket_data,
            ) as response:
                if response.status in (200, 201):
                    logging.info(f"Updated bucket '{self.bucket_name}' to be public")
                else:
                    error_text = await response.text()
                    logging.error(f"Failed to update bucket visibility: {error_text}")

    async def download_image(self, image_url, temp_dir="temp_images"):
        """Download image from URL to temporary file"""
        # Create temp directory if it doesn't exist
        os.makedirs(temp_dir, exist_ok=True)

        # Extract filename from URL and clean it
        parsed_url = urlparse(image_url)
        original_filename = Path(parsed_url.path).name

        # Generate a unique filename to avoid collisions
        filename = f"{uuid.uuid4().hex}_{original_filename}"
        local_path = Path(temp_dir) / filename

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(image_url) as response:
                    if response.status == 200:
                        content = await response.read()
                        # Save to temp file
                        await asyncio.to_thread(lambda: local_path.write_bytes(content))
                        return str(local_path)
                    else:
                        logging.error(
                            f"Failed to download {image_url}, status: {response.status}"
                        )
                        return None
        except Exception as e:
            logging.error(f"Error downloading {image_url}: {e}")
            return None

    async def upload_image(self, local_path, property_external_id):
        """Upload image to Supabase Storage and return public URL"""
        if not os.path.exists(local_path):
            logging.error(f"Local file not found: {local_path}")
            return None

        # Generate storage path: property_id/filename
        filename = os.path.basename(local_path)
        storage_path = f"{property_external_id}/{filename}"

        try:
            # Read file content
            with open(local_path, "rb") as f:
                file_content = f.read()

            content_type = "image/jpeg"  # Default to JPEG
            if filename.lower().endswith(".png"):
                content_type = "image/png"
            elif filename.lower().endswith(".gif"):
                content_type = "image/gif"
            elif filename.lower().endswith(".webp"):
                content_type = "image/webp"

            async with aiohttp.ClientSession() as session:
                # Upload the file
                upload_headers = self.headers.copy()
                upload_headers["Content-Type"] = content_type

                async with session.post(
                    f"{self.storage_url}/object/{self.bucket_name}/{storage_path}",
                    headers=upload_headers,
                    data=file_content,
                ) as response:
                    if response.status in (200, 201):
                        # For self-hosted, construct the public URL
                        public_url = f"{self.supabase_url}/storage/v1/object/public/{self.bucket_name}/{storage_path}"
                        logging.info(f"Uploaded image to {public_url}")
                        return public_url
                    else:
                        error_text = await response.text()
                        logging.error(
                            f"Failed to upload {local_path}, status: {response.status}, error: {error_text}"
                        )
                        return None
        except Exception as e:
            logging.error(f"Error uploading {local_path}: {e}")
            return None
        finally:
            # Cleanup temp file
            try:
                os.remove(local_path)
            except:
                pass

    async def process_property_images(self, property_data, db_conn):
        """Process all images for a property: download, upload, and save to DB"""
        if not property_data.get("p_external_id") or not property_data.get(
            "p_image_urls"
        ):
            return

        external_id = property_data["p_external_id"]
        image_urls = property_data["p_image_urls"]

        # Filter out thumbnail images (they have "thumbnail" in the URL)
        full_size_images = [url for url in image_urls if "/thumbnail/" not in url]

        # Limit to first 5 images to avoid overloading storage
        full_size_images = full_size_images[:5]

        logging.info(
            f"[{external_id}] Processing {len(full_size_images)} full-size images"
        )

        # Get property ID from database
        property_id = await db_conn.fetchval(
            "SELECT id FROM properties WHERE external_id = $1", external_id
        )

        if not property_id:
            logging.error(f"[{external_id}] Property not found in database")
            return

        # First, clear existing images for this property to avoid duplicates
        try:
            await db_conn.execute(
                "DELETE FROM property_images WHERE property_id = $1", property_id
            )
            logging.info(f"[{external_id}] Cleared existing images for property")
        except Exception as e:
            logging.error(f"[{external_id}] Error clearing existing images: {e}")

        # Process each image
        for index, image_url in enumerate(full_size_images):
            # Download image
            local_path = await self.download_image(image_url)
            if not local_path:
                continue

            # Upload to Supabase Storage
            storage_url = await self.upload_image(local_path, external_id)
            if not storage_url:
                continue

            # Save to property_images table - without ON CONFLICT clause
            try:
                is_featured = index == 0  # First image is featured
                await db_conn.execute(
                    """
                    INSERT INTO property_images 
                    (property_id, url, is_featured, sort_order, created_at)
                    VALUES ($1, $2, $3, $4, NOW())
                    """,
                    property_id,
                    storage_url,
                    is_featured,
                    index,
                )
                logging.info(
                    f"[{external_id}] Saved image {index+1}/{len(full_size_images)} to database"
                )
            except Exception as e:
                logging.error(f"[{external_id}] Error saving image to database: {e}")
