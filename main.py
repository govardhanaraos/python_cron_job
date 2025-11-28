import os
import sys
import time
import re
import requests
import hashlib
import logging
from pymongo import MongoClient, UpdateOne
from datetime import datetime

# --- CONFIGURATION ---
# Connection String (Best practice: Set this in Render Environment Variables)
MONGO_URI = os.getenv("MONGO_URI", "mongodb://govardhanaraofmuser:Retail546321987@ac-1iddvrw-shard-00-00.mihjnbk.mongodb.net:27017,ac-1iddvrw-shard-00-01.mihjnbk.mongodb.net:27017,ac-1iddvrw-shard-00-02.mihjnbk.mongodb.net:27017/?ssl=true&authSource=admin&replicaSet=atlas-w63i5e-shard-0")
DB_NAME = "GRRadio"  # Change to your actual DB name
CONFIG_COLLECTION = "app_settings"  # Collection to fetch the 'q' value (e.g., 'india')
TARGET_COLLECTION = "radio_garden_channels"
AUDIT_LOG_COLLECTION = "app_audit_log" # New requirement

# --- CUSTOM MONGODB LOGGING HANDLER ---

class MongoHandler(logging.Handler):
    """
    A custom logging handler that writes log records to MongoDB.
    """
    def __init__(self, mongo_client, db_name, collection_name):
        logging.Handler.__init__(self)
        self.db = mongo_client[db_name]
        self.collection = self.db[collection_name]

    def emit(self, record):
        """
        Writes the log record to the MongoDB collection.
        """
        try:
            log_entry = {
                "timestamp": datetime.utcnow(),
                "level": record.levelname,
                "module": record.module,
                "message": record.getMessage(),
                "line": record.lineno,
                "pathname": record.pathname
            }
            self.collection.insert_one(log_entry)
        except Exception as e:
            # Fallback to console print if DB logging fails
            print(f"ERROR: Failed to log to MongoDB: {e}")

# --- HELPER FUNCTIONS ---

def setup_logging(mongo_client):
    """
    Configures logging to output to console and MongoDB.
    """
    logger = logging.getLogger()
    logger.handlers = [] 
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Console Handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # MongoDB Handler
    mh = MongoHandler(mongo_client, DB_NAME, AUDIT_LOG_COLLECTION)
    mh.setFormatter(formatter)
    logger.addHandler(mh)
    
    logging.info("Logging configured for Console and MongoDB Audit.")

def get_deterministic_id(unique_string):
    """
    Generates a consistent integer ID starting with 300.
    """
    hash_object = hashlib.md5(unique_string.encode())
    hex_dig = hash_object.hexdigest()
    int_val = int(hex_dig, 16)
    short_id = str(int_val)[:6]
    return f"300{short_id}"

def should_run_job():
    """
    Option B Logic: Checks if today is a 10th day interval since Epoch.
    """
    days_since_epoch = int(time.time() / 86400)
    return days_since_epoch % 10 == 0

def clean_and_slugify(text):
    """
    Converts text to a URL-friendly slug.
    Required for the new 'page' field.
    """
    if not text:
        return ""
    # Lowercase, remove non-alphanumeric chars (except spaces/hyphens), replace spaces with hyphens
    text = text.lower()
    text = re.sub(r'[^\w\s-]', '', text).strip()
    text = re.sub(r'[-\s]+', '-', text)
    return text

def get_search_queries(db):
    """
    Fetches ALL search query values from the database, handling both country and place searches.
    Returns a list of dictionaries, each defining a task.
    """
    logging.info(f"Fetching search queries from {CONFIG_COLLECTION}...")
    
    # 1. Fetch Country Searches
    country_cursor = db[CONFIG_COLLECTION].find({"config_name": "radio_search"})
    tasks = [
        {"type": "country", "query": doc["query"]} 
        for doc in country_cursor if "query" in doc and doc["query"]
    ]
    
    # 2. Fetch Place Searches
    place_cursor = db[CONFIG_COLLECTION].find({"config_name": "radio_search_by_place"})
    tasks.extend([
        {"type": "place", "query": doc["query"], "country": doc.get("country")} 
        for doc in place_cursor 
        if "query" in doc and doc["query"] and doc.get("country")
    ])
    
    if not tasks:
        logging.warning("No search configurations found in database. Adding default country search: 'india'.")
        tasks.append({"type": "country", "query": "india"})
        
    return tasks

def extract_channel_id_from_url(url_path):
    """
    Parses '/listen/station-name/ID' to return 'ID'.
    """
    try:
        parts = url_path.strip("/").split("/")
        return parts[-1]
    except Exception:
        return None

# --- PROCESS HANDLERS ---

def create_channel_doc(page, channel_unique_id):
    """
    Helper to construct the final MongoDB document structure, including the new 'page' slug.
    """
    custom_id = get_deterministic_id(channel_unique_id)
    
    # Extract fields
    title = page.get("title", "")
    subtitle = page.get("subtitle", "")
    place = page.get("place", {}).get("title", "")
    country = page.get("country", {}).get("title", "")
    
    # Construct the stream and logo URLs
    stream_url = f"https://radio.garden/api/ara/content/listen/{channel_unique_id}/channel.mp3"
    logo_url = f"https://picsum.photos/150/150?random={custom_id}"

    # Construct the 'page' slug (new requirement)
    page_slug = f"{clean_and_slugify(title)}-{clean_and_slugify(subtitle)}-{clean_and_slugify(place)}-{clean_and_slugify(country)}"
    
    return {
        "id": custom_id,
        "name": title,
        "streamUrl": stream_url,
        "logoUrl": logo_url,
        "language": subtitle, 
        "genre": subtitle,
        "state": place,
        "country": country,
        "radio_garden_id": channel_unique_id,
        "page": page_slug, # The new required field
    }

def fetch_and_parse_content(db, content_url, search_term):
    """
    Fetches content from the content API and performs the bulk write.
    """
    logging.info(f"Fetching content from: {content_url}")
    
    try:
        content_resp = requests.get(content_url, timeout=15)
        content_resp.raise_for_status()
        content_data = content_resp.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch content API for '{search_term}': {e}")
        return

    operations = []
    content_list = content_data.get("data", {}).get("content", [])
    
    for section in content_list:
        items = section.get("items", [])
        for item in items:
            page = item.get("page", {})
            
            if page.get("type") == "channel":
                try:
                    raw_url = page.get("url", "")
                    channel_unique_id = extract_channel_id_from_url(raw_url)
                    
                    if not channel_unique_id:
                        continue
                    
                    # Create the full channel document
                    channel_doc = create_channel_doc(page, channel_unique_id)

                    # Create Upsert Operation (Update if exists, Insert if new)
                    operations.append(
                        UpdateOne(
                            {"radio_garden_id": channel_unique_id}, 
                            {"$set": channel_doc}, 
                            upsert=True
                        )
                    )
                except Exception as e:
                    logging.error(f"Error parsing item for '{search_term}': {e}")

    # Execute Bulk Write
    if operations:
        try:
            result = db[TARGET_COLLECTION].bulk_write(operations)
            logging.info(f"DB Update for '{search_term}' completed. Upserted: {result.upserted_count}, Modified: {result.modified_count}, Total Matched: {result.matched_count}")
        except Exception as e:
            logging.error(f"Failed to execute bulk write for '{search_term}': {e}")
    else:
        logging.info(f"No channels found to update for '{search_term}'.")


def process_search(db, task):
    """
    Determines the specific search logic (country or place) and executes it.
    """
    search_term = task["query"]
    search_type = task["type"]
    
    logging.info(f"--- Starting processing for {search_type}: '{search_term}' ---")

    # 1. Call Search API
    search_url = f"https://radio.garden/api/search?s=1&hl=en&q={search_term}"
    
    try:
        search_resp = requests.get(search_url, timeout=15)
        search_resp.raise_for_status()
        search_data = search_resp.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch search API for '{search_term}': {e}")
        return

    # 2. Find the correct Content URL Suffix
    content_url_suffix = None
    hits = search_data.get("hits", {}).get("hits", [])
    
    for hit in hits:
        source = hit.get("_source", {})
        page = source.get("page", {})
        
        # COUNTRY Search Logic (Original)
        if search_type == "country" and source.get("type") == "country":
            content_url_suffix = page.get("url")
            break
            
        # PLACE Search Logic (New Requirement)
        elif search_type == "place" and source.get("type") == "place":
            # Filter by 'country' field from the DB config (case-insensitive match with subtitle)
            expected_country = task.get("country", "").lower()
            hit_country = page.get("subtitle", "").lower()
            
            if expected_country and expected_country == hit_country:
                content_url_suffix = page.get("url")
                break
    
    if not content_url_suffix:
        logging.warning(f"No matching {search_type} page found for query '{search_term}'. Check DB config/API response.")
        return

    # 3. Construct the Content URL and Fetch/Parse
    content_url = f"https://radio.garden/api/ara/content/page/{extract_channel_id_from_url(content_url_suffix)}?s=1&hl=en"
    fetch_and_parse_content(db, content_url, search_term)
        
    logging.info(f"--- Finished processing for {search_type}: '{search_term}' ---")

# --- EXECUTION ENTRY POINT ---

def main_job():
    """
    Main function for the scheduled job.
    """
    try:
        # Establish MongoDB connection once
        mongo_client = MongoClient(MONGO_URI)
        
        # Setup logging to console AND MongoDB
        setup_logging(mongo_client)
        
    except Exception as e:
        # Cannot proceed without a database connection for logging
        print(f"FATAL ERROR: Could not connect to MongoDB. Cannot start job. Error: {e}")
        sys.exit(1)

    db = mongo_client[DB_NAME]
    
    # 1. Handle All Queries (Country and Place)
    search_tasks = get_search_queries(db)
    logging.info(f"Processing a total of {len(search_tasks)} tasks.")
    
    # 2. Iterate and process each task
    for task in search_tasks:
        process_search(db, task)
        
    logging.info("ALL scheduled tasks complete.")


if __name__ == "__main__":
    # OPTION B SCHEDULER CHECK (Ensures job runs only on 10th-day intervals)
    if should_run_job():
        try:
            main_job()
        except Exception as e:
            # Catching any remaining unexpected errors
            print(f"FATAL ERROR: An unexpected error occurred during job execution: {e}")
            logging.error(f"An unexpected error occurred during job execution: {e}")
            sys.exit(1)
    else:
        # If the day condition is not met, exit gracefully
        print("Scheduler Condition Not Met. Skipping execution for today.")
        sys.exit(0)
