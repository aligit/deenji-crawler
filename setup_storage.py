# setup_storage.py
import asyncio
import logging
import os

import aiohttp
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


async def create_bucket(supabase_url, supabase_key, bucket_name="property-images"):
    """Create a public bucket in Supabase Storage"""
    storage_url = f"{supabase_url}/storage/v1/bucket"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
    }

    # Payload for creating a public bucket
    bucket_data = {"id": bucket_name, "name": bucket_name, "public": True}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                storage_url, headers=headers, json=bucket_data
            ) as response:
                if response.status in (200, 201):
                    result = await response.json()
                    logging.info(f"✅ Successfully created bucket: {bucket_name}")
                    logging.info(f"Response: {result}")
                    return True
                else:
                    error_text = await response.text()
                    logging.error(
                        f"❌ Failed to create bucket. Status: {response.status}"
                    )
                    logging.error(f"Error: {error_text}")
                    return False

        except Exception as e:
            logging.error(f"❌ Exception creating bucket: {str(e)}")
            return False


async def test_bucket_access(supabase_url, supabase_key, bucket_name="property-images"):
    """Verify the bucket exists and is accessible"""
    storage_url = f"{supabase_url}/storage/v1/bucket/{bucket_name}"
    headers = {"apikey": supabase_key, "Authorization": f"Bearer {supabase_key}"}

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(storage_url, headers=headers) as response:
                if response.status == 200:
                    result = await response.json()
                    logging.info(f"✅ Bucket '{bucket_name}' exists and is accessible")
                    logging.info(f"Bucket details: {result}")
                    return True
                else:
                    error_text = await response.text()
                    logging.error(
                        f"❌ Cannot access bucket '{bucket_name}'. Status: {response.status}"
                    )
                    logging.error(f"Error: {error_text}")
                    return False
        except Exception as e:
            logging.error(f"❌ Exception checking bucket: {str(e)}")
            return False


async def setup_storage_policy(
    supabase_url, supabase_key, bucket_name="property-images"
):
    """Set up a public access policy for the bucket"""
    policy_url = f"{supabase_url}/storage/v1/bucket/{bucket_name}/policy"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
    }

    # Create a policy to allow public read access
    policy_data = {
        "name": "Public Access",
        "definition": {
            "type": "READ",
            "permissions": ["READ"],
            "roles": ["anon"],
            "allow": True,
        },
    }

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                policy_url, headers=headers, json=policy_data
            ) as response:
                if response.status in (200, 201):
                    result = await response.json()
                    logging.info(
                        f"✅ Successfully created public access policy for bucket: {bucket_name}"
                    )
                    return True
                else:
                    error_text = await response.text()
                    logging.error(
                        f"❌ Failed to create policy. Status: {response.status}"
                    )
                    logging.error(f"Error: {error_text}")
                    return False
        except Exception as e:
            logging.error(f"❌ Exception creating policy: {str(e)}")
            return False


async def upload_test_image(supabase_url, supabase_key, bucket_name="property-images"):
    """Upload a simple test image to verify storage is working"""
    # Create a small test image
    test_file = "test_image.jpg"
    with open(test_file, "wb") as f:
        # Create a simple red square as a JPEG
        f.write(
            b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x01\x00H\x00H\x00\x00\xff\xdb\x00C\x00\x08\x06\x06\x07\x06\x05\x08\x07\x07\x07\t\t\x08\n\x0c\x14\r\x0c\x0b\x0b\x0c\x19\x12\x13\x0f\x14\x1d\x1a\x1f\x1e\x1d\x1a\x1c\x1c $.' \",#\x1c\x1c(7),01444\x1f'9=82<.342\xff\xdb\x00C\x01\t\t\t\x0c\x0b\x0c\x18\r\r\x182!\x1c!22222222222222222222222222222222222222222222222222\xff\xc0\x00\x11\x08\x00d\x00d\x03\x01\"\x00\x02\x11\x01\x03\x11\x01\xff\xc4\x00\x1f\x00\x00\x01\x05\x01\x01\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\xff\xc4\x00\xb5\x10\x00\x02\x01\x03\x03\x02\x04\x03\x05\x05\x04\x04\x00\x00\x01}\x01\x02\x03\x00\x04\x11\x05\x12!1A\x06\x13Qa\x07\"q\x142\x81\x91\xa1\x08#B\xb1\xc1\x15R\xd1\xf0$3br\x82\t\n\x16\x17\x18\x19\x1a%&'()*456789:CDEFGHIJSTUVWXYZcdefghijstuvwxyz\x83\x84\x85\x86\x87\x88\x89\x8a\x92\x93\x94\x95\x96\x97\x98\x99\x9a\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xe1\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xf1\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xff\xc4\x00\x1f\x01\x00\x03\x01\x01\x01\x01\x01\x01\x01\x01\x01\x00\x00\x00\x00\x00\x00\x01\x02\x03\x04\x05\x06\x07\x08\t\n\x0b\xff\xc4\x00\xb5\x11\x00\x02\x01\x02\x04\x04\x03\x04\x07\x05\x04\x04\x00\x01\x02w\x00\x01\x02\x03\x11\x04\x05!1\x06\x12AQ\x07aq\x13\"2\x81\x08\x14B\x91\xa1\xb1\xc1\t#3R\xf0\x15br\xd1\n\x16$4\xe1%\xf1\x17\x18\x19\x1a&'()*56789:CDEFGHIJSTUVWXYZcdefghijstuvwxyz\x82\x83\x84\x85\x86\x87\x88\x89\x8a\x92\x93\x94\x95\x96\x97\x98\x99\x9a\xa2\xa3\xa4\xa5\xa6\xa7\xa8\xa9\xaa\xb2\xb3\xb4\xb5\xb6\xb7\xb8\xb9\xba\xc2\xc3\xc4\xc5\xc6\xc7\xc8\xc9\xca\xd2\xd3\xd4\xd5\xd6\xd7\xd8\xd9\xda\xe2\xe3\xe4\xe5\xe6\xe7\xe8\xe9\xea\xf2\xf3\xf4\xf5\xf6\xf7\xf8\xf9\xfa\xff\xda\x00\x0c\x03\x01\x00\x02\x11\x03\x11\x00?\x00\xfe\xfe(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00(\xa2\x8a\x00"
        )

    storage_path = "test/test_image.jpg"
    storage_url = f"{supabase_url}/storage/v1/object/{bucket_name}/{storage_path}"

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "image/jpeg",
    }

    with open(test_file, "rb") as f:
        file_content = f.read()

    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                storage_url, headers=headers, data=file_content
            ) as response:
                if response.status in (200, 201):
                    result = await response.json()
                    public_url = f"{supabase_url}/storage/v1/object/public/{bucket_name}/{storage_path}"
                    logging.info(f"✅ Successfully uploaded test image")
                    logging.info(f"Public URL: {public_url}")

                    # Try to access the public URL
                    async with session.get(public_url) as get_response:
                        if get_response.status == 200:
                            logging.info(f"✅ Successfully accessed public URL")
                        else:
                            logging.error(
                                f"❌ Failed to access public URL. Status: {get_response.status}"
                            )

                    return public_url
                else:
                    error_text = await response.text()
                    logging.error(
                        f"❌ Failed to upload test image. Status: {response.status}"
                    )
                    logging.error(f"Error: {error_text}")
                    return None
        except Exception as e:
            logging.error(f"❌ Exception uploading test image: {str(e)}")
            return None
        finally:
            # Clean up the test file
            if os.path.exists(test_file):
                os.remove(test_file)


async def main():
    load_dotenv()
    supabase_url = os.getenv("SUPABASE_STORAGE_URL", "http://127.0.0.1:54321")
    supabase_key = os.getenv("SUPABASE_ROLE")

    if not supabase_key:
        logging.error("❌ SUPABASE_KEY not set in environment variables.")
        return

    bucket_name = "property-images"

    # Step 1: Create the bucket
    success = await create_bucket(supabase_url, supabase_key, bucket_name)
    if not success:
        logging.warning("Continuing anyway - bucket might already exist...")

    # Step 2: Verify bucket access
    access_ok = await test_bucket_access(supabase_url, supabase_key, bucket_name)
    if not access_ok:
        logging.error("❌ Cannot access bucket. Stopping.")
        return

    # Step 3: Set up public access policy (if needed)
    # await setup_storage_policy(supabase_url, supabase_key, bucket_name)

    # Step 4: Upload a test image
    public_url = await upload_test_image(supabase_url, supabase_key, bucket_name)
    if public_url:
        logging.info(f"✅ Storage setup complete. Test image URL: {public_url}")
    else:
        logging.error("❌ Storage setup failed.")


if __name__ == "__main__":
    asyncio.run(main())
