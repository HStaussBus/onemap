# auth_clients.py

import os
import json
import mygeotab
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import psycopg2
import config # Import our config module

# --- AWS Secrets Manager Client ---
# NOTE: These AWS functions depend on AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
# being correctly set and available as environment variables.
# They are NOT used for Google/DB init in this modified file, but kept for potential other uses.

def get_aws_secrets_manager_client(region="us-east-1"):
    """
    Initializes and returns a Boto3 Secrets Manager client.
    Relies on AWS credentials being available in the environment.
    """
    # Boto3 will automatically look for credentials in the environment
    # (set by Replit Secrets if AWS_ACCESS_KEY_ID/SECRET_ACCESS_KEY are defined there)
    try:
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=region)
        print("INFO: AWS Secrets Manager client initialized.")
        return client
    except NoCredentialsError:
        print("ERROR: Failed to initialize AWS Secrets Manager client - No AWS credentials found in environment.")
        return None # Return None explicitly if credentials are not found
    except Exception as e:
        print(f"ERROR: Failed to initialize AWS Secrets Manager client: {e}")
        return None # Return None on other errors

def get_secret_from_aws(secret_name, secrets_client):
    """
    Fetches a specific secret string from AWS Secrets Manager using a provided client.
    Returns None if the client is invalid, secret not found, or other error occurs.
    """
    if secrets_client is None:
        print(f"ERROR: AWS Secrets client not available to fetch '{secret_name}'.")
        return None
    try:
        print(f"INFO: Attempting to fetch secret '{secret_name}' from AWS...")
        get_secret_value_response = secrets_client.get_secret_value(SecretId=secret_name)
        print(f"INFO: Successfully fetched secret '{secret_name}' from AWS.")
        # Secrets can be string or binary, assume string for this app's needs
        if 'SecretString' in get_secret_value_response:
            return get_secret_value_response['SecretString']
        else:
            print(f"WARNING: Secret '{secret_name}' fetched from AWS is binary, not string.")
            return None # Or handle binary if needed
    except NoCredentialsError:
         print(f"ERROR: No AWS credentials found when trying to fetch secret '{secret_name}'.")
         return None
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == 'ResourceNotFoundException':
            print(f"ERROR: Secret '{secret_name}' not found in AWS Secrets Manager.")
        elif error_code == 'AccessDeniedException':
             print(f"ERROR: Access denied when trying to fetch secret '{secret_name}' from AWS. Check IAM permissions.")
        else:
            print(f"ERROR: AWS ClientError retrieving secret '{secret_name}': {e}")
        return None
    except Exception as e:
        print(f"ERROR: Unexpected error retrieving secret '{secret_name}' from AWS: {e}")
        return None

# --- Google Clients (GSpread, Drive) ---
# Uses lazy initialization: clients are created only when first requested.
_gspread_client = None
_drive_service = None

def _initialize_google_clients():
    """
    Internal function to initialize Google clients once using JSON from env var.
    """
    global _gspread_client, _drive_service

    # Avoid re-initialization if already done
    if _gspread_client and _drive_service:
        return

    print("INFO: Initializing Google clients from environment JSON...")
    # --- MODIFIED PART: Read JSON from Replit Secret (environment variable) ---
    google_creds_json_string = os.environ.get('GOOGLE_CREDS_JSON')
    # --- END MODIFIED PART ---

    if not google_creds_json_string:
        print("ERROR: GOOGLE_CREDS_JSON environment variable not set or empty.")
        _gspread_client = None
        _drive_service = None
        return # Stop initialization

    try:
        # Parse the JSON string from the environment variable
        secret_dict = json.loads(google_creds_json_string)

        # Define required scopes
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

        # Create credentials object
        creds = ServiceAccountCredentials.from_json_keyfile_dict(secret_dict, scope)

        # Authorize clients
        _gspread_client = gspread.authorize(creds)
        _drive_service = build('drive', 'v3', credentials=creds)
        print("INFO: GSpread client and Drive service initialized successfully from environment JSON.")

    except json.JSONDecodeError:
        print("ERROR: Failed to parse GOOGLE_CREDS_JSON from environment variable. Check format.")
        _gspread_client = None
        _drive_service = None
    except Exception as e:
        print(f"ERROR: Failed to initialize Google clients from environment JSON: {e}")
        _gspread_client = None
        _drive_service = None

def get_gspread_client():
    """Returns the initialized GSpread client (initializes on first call)."""
    # Check if clients need initialization (covers case where previous attempt failed)
    if _gspread_client is None or _drive_service is None:
        _initialize_google_clients()
    return _gspread_client

def get_drive_service():
    """Returns the initialized Google Drive service (initializes on first call)."""
    # Check if clients need initialization
    if _gspread_client is None or _drive_service is None:
        _initialize_google_clients()
    return _drive_service

# --- Geotab Client ---
# Uses lazy initialization
_geotab_client = None
def initialize_geotab_client():
    """Initializes and authenticates the Geotab API client (initializes on first call)."""
    global _geotab_client
    if _geotab_client:
        return _geotab_client # Return cached client

    print("INFO: Initializing Geotab client...")
    # Read credentials from config (which reads from environment variables/Replit Secrets)
    username = config.GEOTAB_USERNAME
    password = config.GEOTAB_PASSWORD

    if not username or not password:
        print("ERROR: Geotab username or password not found in config/environment.")
        return None

    try:
        print(f"INFO: Authenticating Geotab user '{username}'...")
        api = mygeotab.API(
            username=username,
            password=password,
            database=config.GEOTAB_DATABASE,
            server=config.GEOTAB_SERVER
        )
        api.authenticate() # Attempt authentication
        _geotab_client = api # Cache the authenticated client
        print("INFO: Geotab client authenticated successfully.")
        return _geotab_client
    except mygeotab.exceptions.AuthenticationException as e:
        # Log specific authentication errors
        print(f"ERROR: Geotab authentication failed: {e}")
        _geotab_client = None # Ensure client is None on failure
        return None
    except Exception as e:
        # Catch other potential errors during initialization
        print(f"ERROR: Unexpected error initializing Geotab client: {e}")
        _geotab_client = None
        return None

# --- Database Connection ---
# Uses lazy loading for credentials
_db_credentials = None
def _load_db_credentials():
    """Loads DB credentials JSON from environment variable (Replit Secret)."""
    global _db_credentials
    if _db_credentials:
        return _db_credentials # Return cached credentials

    print("INFO: Loading Database credentials from environment JSON...")
    # --- MODIFIED PART: Read JSON from Replit Secret (environment variable) ---
    db_secret_json = os.environ.get('DB_CREDS_JSON')
    # --- END MODIFIED PART ---

    if not db_secret_json:
        print("ERROR: DB_CREDS_JSON environment variable not set or empty.")
        return None

    try:
        # Parse the JSON string
        _db_credentials = json.loads(db_secret_json)

        # Validate required keys
        required_keys = ['dbname', 'username', 'password', 'host', 'port']
        if not all(key in _db_credentials for key in required_keys):
             print("ERROR: DB credentials JSON from environment is missing required keys.")
             _db_credentials = None # Invalidate cache
             return None

        print("INFO: Database credentials loaded successfully from environment JSON.")
        return _db_credentials
    except json.JSONDecodeError:
        print("ERROR: Failed to parse DB_CREDS_JSON from environment variable. Check format.")
        _db_credentials = None
        return None
    except Exception as e:
        print(f"ERROR: Unexpected error loading DB credentials from environment: {e}")
        _db_credentials = None
        return None

def get_db_connection():
    """
    Establishes and returns a new connection to the PostgreSQL database.
    Loads credentials on first call if needed. Returns None on failure.
    """
    # Ensure credentials are loaded (or attempted to be loaded)
    db_creds = _load_db_credentials()

    if not db_creds:
        print("ERROR: Cannot establish DB connection without valid credentials.")
        return None

    try:
        # Establish connection using loaded credentials
        conn = psycopg2.connect(
            dbname=db_creds['dbname'],
            user=db_creds['username'],
            password=db_creds['password'],
            host=db_creds['host'],
            port=db_creds['port']
        )
        # Optionally set autocommit or other session parameters here if needed
        # conn.autocommit = True
        print("INFO: Database connection established.")
        return conn
    except psycopg2.Error as e: # Catch specific psycopg2 errors
        print(f"ERROR: DB connection failed (psycopg2 error): {e}")
        return None
    except Exception as e:
        # Catch other potential errors during connection
        print(f"ERROR: Unexpected error establishing DB connection: {e}")
        return None

# Example of how to use the clients (usually done in app.py or other modules)
if __name__ == '__main__':
    print("\n--- Testing Client Initializations ---")

    # Test Geotab
    print("\nTesting Geotab...")
    gt_client = initialize_geotab_client()
    if gt_client: print("Geotab client obtained.")
    else: print("Failed to obtain Geotab client.")

    # Test Google
    print("\nTesting Google Spreadsheets...")
    gs_client = get_gspread_client()
    if gs_client: print("GSpread client obtained.")
    else: print("Failed to obtain GSpread client.")

    print("\nTesting Google Drive...")
    drv_service = get_drive_service()
    if drv_service: print("Drive service obtained.")
    else: print("Failed to obtain Drive service.")

    # Test DB Connection
    print("\nTesting DB Connection...")
    db_conn = get_db_connection()
    if db_conn:
        print("DB connection obtained.")
        db_conn.close() # Close the test connection
        print("DB test connection closed.")
    else:
        print("Failed to obtain DB connection.")

    print("\n--- Initialization Tests Complete ---")