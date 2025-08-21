import os
import time
import requests
import logging
import argparse
import yaml
from datetime import date, datetime
from typing import List, Dict, Tuple
import concurrent.futures
from dotenv import load_dotenv
import sys


def load_config(config_path: str) -> Dict:
    """Load YAML configuration file and return as dictionary."""
    with open(config_path, 'r', encoding='utf-8') as file:
        return yaml.safe_load(file)


def setup_environment(config: Dict, env_path: str) -> None:
    """Load .env variables and override with config environment settings."""
    # Load environment variables from .env first
    load_dotenv(env_path)
    
    # Override with config values if provided
    env_overrides = config.get('environment', {}) or {}
    for key, value in env_overrides.items():
        if value:  # Only override if value is not empty
            os.environ[key.upper()] = str(value)


def get_env_variables() -> Dict[str, str]:
    """Retrieve all required environment variables for the scraper."""
    return {
        'BRIGHTDATA_API_KEY': os.getenv("BRIGHTDATA_API_KEY"),
        'AWS_ACCESS_KEY': os.getenv("aws_access_key"),
        'AWS_SECRET_KEY': os.getenv("aws_secret_access_key"),
        'S3_BUCKET': os.getenv("S3_BUCKET"),
        'S3_DIRECTORY': os.getenv("S3_DIRECTORY"),
        'DATASET_ID': os.getenv("DATASET_ID")
    }

def get_safe_max_workers(pool_size=5, max_overflow=10, safety_margin=0.3):
    "Get the max concurrency while considering Airflow and EC2 instance limitations"
    cpu_count = os.cpu_count() or 2
    db_limit = pool_size + max_overflow
    
    # Apply margin so Airflow itself (scheduler, logging, heartbeat) has breathing room
    safe_db_limit = max(1, int(db_limit * (1 - safety_margin)))
    
    # Pick the lower of CPU-based and DB-based limits
    return min(cpu_count * 2, safe_db_limit)

# Initialize global variables (will be set in main)
BRIGHTDATA_API_KEY = None
AWS_ACCESS_KEY = None
AWS_SECRET_KEY = None
S3_BUCKET = None
S3_DIRECTORY = None
DATASET_ID = None
HEADERS = {}
task_status = True

def create_logger(location: str, log_location: str) -> logging.Logger:
    """Create timestamped logger for specific location with file handler."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    location_safe = location.replace(" ", "_")
    log_filename = f"{log_location}/{location_safe}_indeed_{timestamp}.log"
    logger = logging.getLogger(location_safe)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = logging.FileHandler(log_filename, encoding="utf-8")
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


def trigger_brightdata_job(keyword: Dict, logger: logging.Logger) -> Tuple[Dict, str]:
    """Trigger BrightData job for given keyword and return query data with snapshot ID."""
    api_error = 0 # We assume no api error initially
    logger.info(f"Triggering job for: {keyword['keyword_search']}")
    logger.info(f"Full keyword data being sent: {keyword}")
    
    url = "https://api.brightdata.com/datasets/v3/trigger"
    params = {
        "dataset_id": DATASET_ID,
        "include_errors": "true",
        "type": "discover_new",
        "discover_by": "keyword",
    }

    # Mask API key for logging
    masked_headers = HEADERS.copy()
    if 'Authorization' in masked_headers:
        auth_value = masked_headers['Authorization']
        if auth_value.startswith('Bearer '):
            api_key = auth_value[7:]  # Remove 'Bearer ' prefix
            masked_headers['Authorization'] = f"Bearer ...{api_key[-4:]}"

    logger.info(f"API endpoint: {url}")
    logger.info(f"Query params: {params}")
    logger.info(f"Headers: {masked_headers}")
    logger.info(f"JSON payload: {[keyword]}")

    response = requests.post(url, headers=HEADERS, params=params, json=[keyword])
    
    logger.info(f"Response status: {response.status_code}")
    if response.status_code != 200:
        api_error - 1 # This means something went wrong with the API, hence we update this variable as True
        logger.error(f"Response text: {response.text}")
        logger.error(f"Response headers: {dict(response.headers)}")

    try:
        response.raise_for_status()
        result = response.json()
        logger.info(f"API response: {result}")
        if "snapshot_id" not in result:
            api_error - 1 # Snapshot id should be in the response, so if not, then something went wrong with the API call
            raise ValueError(f"Missing snapshot_id in response: {result}")
        snapshot_id = result["snapshot_id"]
        logger.info(f"Snapshot triggered: {snapshot_id}")
    except Exception as e:
        logger.error(f"Response status error: {e}")
        api_error - 1 # Any deviation from the norm with the response output means something went wrong with the API call
        snapshot_id = None
    return keyword, snapshot_id, api_error


def wait_for_snapshot_ready(snapshot_id: str, logger: logging.Logger, poll_interval: int = 15) -> None:
    """Poll BrightData API until snapshot is ready for download."""
    global task_status
    api_error = 0 # Keep track of whether this Brightdata run has an API error
    logger.info(f"Waiting for snapshot {snapshot_id} to be ready...")
    url = f"https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"

    while True:
        try:
            res = requests.get(url, headers=HEADERS)
            res.raise_for_status()
            status = res.json().get("status")
            if status == "ready":
                logger.info(f"Snapshot {snapshot_id} is ready.")
                break
            if status == "failed":
                task_status = False
                api_error = 1 # If the status of the Brightdata run failed, we log it as api error
                logger.info(f"Snapshot {snapshot_id} {status}.")
                break            
            logger.info(f"Snapshot {snapshot_id} not ready with status currently as {status}. Waiting {poll_interval}s...")
            time.sleep(poll_interval)
        except Exception as e:
            api_error = 1
            logger.info(f"Url {url} has the following error: {e}")
            break

    # If there is a discovery error, we log it as api error
    try:
        discovery_error_code = res.json()['discovery_error_codes']
        if discovery_error_code:
            api_error = 1
            logger.info(f"Snapshot {snapshot_id} has discovery error.")
    except:
        pass
    
    return api_error


def deliver_snapshot_to_s3(query: Dict, snapshot_id: str, logger: logging.Logger, today_date: str) -> None:
    """Deliver completed snapshot to S3 bucket with structured filename."""
    api_error = 0
    logger.info(f"Delivering snapshot {snapshot_id} to S3...")
    # today = date.today().isoformat()
    keyword = query["keyword_search"].replace(" ", "_")
    location = query["location"].replace(" ", "_")
    date_posted = query["date_posted"].replace(" ", "_")
    filename_template = f"{keyword}_{location}_{today_date}_{date_posted}"

    url = f"https://api.brightdata.com/datasets/v3/deliver/{snapshot_id}"
    payload = {
        "deliver": {
            "type": "s3",
            "bucket": S3_BUCKET,
            "directory": S3_DIRECTORY + today_date.replace('-', '/'),
            "filename": {
                "template": filename_template,
                "extension": "csv"
            },
            "credentials": {
                "aws-access-key": AWS_ACCESS_KEY,
                "aws-secret-key": AWS_SECRET_KEY
            }
        },
        "compress": False
    }

    try:
        res = requests.post(url, headers=HEADERS, json=payload)
        res.raise_for_status()
        logger.info(f"Snapshot {snapshot_id} delivered as {filename_template}.csv")
    except Exception as e:
        api_error = 1
        logger.info(f"Snapshot {snapshot_id} failed to upload with error: {e}")

    return api_error


def process_job_with_config(job_title: str, location_config: Dict, scraping_params: Dict, env_vars: Dict, logger: logging.Logger, today_date: str, poll_interval: int = 15):
    """Execute complete scraping workflow for a single job title and location."""
    keyword_data = {
        "country": location_config["country"],
        "domain": location_config["domain"],
        "keyword_search": job_title,
        "location": location_config["location_name"],
        "date_posted": scraping_params["date_posted"],
        "posted_by": scraping_params["posted_by"]
    }

    try:
        query, snapshot_id, api_error = trigger_brightdata_job(keyword_data, logger)
        
        if not api_error: # If there is no api error, we proceed with polling from Brightdata
            api_error = wait_for_snapshot_ready(snapshot_id, logger, poll_interval)
        else:
            logger.info(f"Job for '{job_title}' in {location_config['location_name']} had an API request failure.")
        if not api_error: # Only if there is no api error for that brightdata job do we upload to S3 bucket
            api_error = deliver_snapshot_to_s3(query, snapshot_id, logger, today_date)
            if not api_error:
                logger.info(f"Job for '{job_title}' in {location_config['location_name']} completed.")
            else:
                logger.info(f"Job for '{job_title}' in {location_config['location_name']} not uploaded to s3.")
        else:
            logger.info(f"Job for '{job_title}' in {location_config['location_name']} not uploaded to s3.")
    except Exception as e:
        api_error - 1 # If something went wrong with the brightdata pull and/or upload to s3, there was an api related error that needs to be addressed
        logger.error(f"Error with '{job_title}' in {location_config['location_name']}: {e}", exc_info=True)
    
    return api_error

def process_job_with_config_us(job_title: str, location_config: Dict, scraping_params: Dict, env_vars: Dict, logger: logging.Logger, location: str, today_date: str, poll_interval: int = 15):
    """Execute complete scraping workflow for a single job title and city in the United States."""
    keyword_data = {
        "country": location_config["country"],
        "domain": location_config["domain"],
        "keyword_search": job_title,
        "location": location,  # use explicit location param
        "date_posted": scraping_params["date_posted"],
        "posted_by": scraping_params["posted_by"]
    }

    try:
        query, snapshot_id, api_error = trigger_brightdata_job(keyword_data, logger)
        if not api_error: # If there is no api error, we proceed with polling from Brightdata
            query["location"] = query["location"] + ' United States'
            api_error = wait_for_snapshot_ready(snapshot_id, logger, poll_interval)
        else:
            logger.info(f"Job for '{job_title}' in {location} had an API request failure.")
        if not api_error: # Only if there is no api error for that brightdata job do we upload to S3 bucket
            api_error = deliver_snapshot_to_s3(query, snapshot_id, logger, today_date)
            if not api_error:
                logger.info(f"Job for '{job_title}' in {location} completed.")
            else:
                logger.info(f"Job for '{job_title}' in {location} not uploaded to S3.")
        else:
            logger.info(f"Job for '{job_title}' in {location} not uploaded to S3.")
    except Exception as e:
        api_error - 1 # If something went wrong with the brightdata pull and/or upload to s3, there was an api related error that needs to be addressed
        logger.error(f"Error with '{job_title}' in {location}: {e}", exc_info=True)
    
    return api_error

def process_location(location: str, location_config: Dict, job_titles: List[str], scraping_params: Dict, env_vars: Dict, logger: logging.Logger, today_date: str, poll_interval: int = 15):
    """For US scrapes, due to the high number of cities to pull, we run cities in parallel and job titles one by one. For CA and SA scrapes, we run job titles in parallel."""
    api_error_total = 0 # Keep track of the number of API errors
    for job_title in job_titles:
        api_error_total += process_job_with_config_us(job_title, location_config, scraping_params, env_vars, logger, location, today_date, poll_interval)
    return api_error_total

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Job Market Data Scraper')
    parser.add_argument('--config', '-c', default='/home/ubuntu/airflow/scrape_code/config.yaml', 
                       help='Path to configuration file (default: config.yaml)')
    parser.add_argument('--location', '-l', default=0, type=int,
                       help='Location index from config (default: 0)')
    parser.add_argument('--log_location', 
                       help='Log folder location')
    parser.add_argument('--today_date', type=str,
                       help='Date of scrape')
    parser.add_argument('--env_path', '-e', default='/home/ubuntu/airflow/.env',
                        help='Path to .env file')
    parser.add_argument('--job-title', '-j', type=str,
                       help='Override job title (single job for testing)')
    parser.add_argument('--max-workers', '-w', type=int,
                       help='Override max workers')
    parser.add_argument('--test-mode', action='store_true',
                       help='Run in test mode with limited scope')
    args = parser.parse_args()

    # Create logging folder
    os.makedirs(args.log_location, exist_ok=True)

    # Load configuration
    today_date = args.today_date
    config = load_config(args.config)
    setup_environment(config, args.env_path)
    env_vars = get_env_variables()
    
    # Validate required environment variables
    required_vars = ['BRIGHTDATA_API_KEY', 'AWS_ACCESS_KEY', 'AWS_SECRET_KEY', 'S3_BUCKET', 'DATASET_ID']
    missing_vars = [var for var in required_vars if not env_vars[var]]
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {missing_vars}")

    # Update global variables
    BRIGHTDATA_API_KEY = env_vars['BRIGHTDATA_API_KEY']
    AWS_ACCESS_KEY = env_vars['AWS_ACCESS_KEY'] 
    AWS_SECRET_KEY = env_vars['AWS_SECRET_KEY']
    S3_BUCKET = env_vars['S3_BUCKET']
    S3_DIRECTORY = env_vars['S3_DIRECTORY']
    DATASET_ID = env_vars['DATASET_ID']

    HEADERS = {
        "Authorization": f"Bearer {BRIGHTDATA_API_KEY}",
        "Content-Type": "application/json"
    }

    # Get configuration parameters
    scraping_config = config['scraping']
    locations = scraping_config['locations']
    parameters = scraping_config['parameters'][args.location]
    # max_workers = parameters['max_workers']
    poll_interval = parameters['poll_interval']
    
    # Select location (for Airflow, you can pass different location indices)
    if args.location >= len(locations):
        raise ValueError(f"Location index {args.location} out of range. Available: 0-{len(locations)-1}")

    # Obtain the config for selected location and create logger item
    location_config = locations[args.location]
    logger = create_logger(location_config['log_prefix'], args.log_location)

    # Obtain job titles for selected location
    job_titles = scraping_config['job_titles'][location_config['log_prefix']]

    # Calculating the number of job titles there are per location
    num_job_titles = len(job_titles)

    # Calculate the number of location
    if isinstance(location_config['location_name'], list):
        num_locations = len(location_config['location_name'])
    else:
        num_locations = 1

    # Total number of jobs for that Airflow run
    num_jobs_total = num_job_titles * num_locations

    logger.info(f"Starting scraping for {location_config['location_name']} with {len(job_titles)} job titles")
    
    # Override for testing
    if args.job_title:
        job_titles = [args.job_title]
    # if args.max_workers:
    #     max_workers = args.max_workers
    max_workers = get_safe_max_workers(pool_size=5, max_overflow=10)
    if args.test_mode:
        job_titles = job_titles[:1]  # Only first job title
        max_workers = 1

    if location_config['log_prefix'] not in ['US']:
        # Run non US jobs concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(process_job_with_config, job_title, location_config, parameters, env_vars, logger, today_date, poll_interval)
                for job_title in job_titles
            ]
            concurrent.futures.wait(futures)

            results = [future.result() for future in futures]

    else:
        # Run US jobs concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(process_location, location, location_config, job_titles, parameters, env_vars, logger, today_date, poll_interval)
                for location in location_config['location_name']
            ]
            concurrent.futures.wait(futures)

            results = [future.result() for future in futures]

    logger.info("All jobs completed.")
    logger.info(f"Status of jobs are {results}")

    # If over half the brightdata runs have API errors, we declare task has failed
    if sum(results)/num_jobs_total>0.5:
        task_status = False

    # If task_status is False, we declare this .py run as failed
    if not task_status:
        sys.exit(1)
