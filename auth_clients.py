# auth_clients.py
import mygeotab
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import boto3
from botocore.exceptions import ClientError
import psycopg2
import json
import config # Import our config module

# --- AWS Secrets Manager Client ---
# Boto3 will automatically use AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
# from the environment variables set by Codespaces.
def get_aws_secrets_manager_client(region="us-east-1"):
    """Initializes and returns a Boto3 Secrets Manager client."""
    try:
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=region)
        print("INFO: AWS Secrets Manager client initialized.")
        return client
    except Exception as e:
        print(f"ERROR: Failed to initialize AWS Secrets Manager client: {e}")
        raise # Re-raise the error

# --- Function to Get Specific Secret from AWS ---
def get_secret_from_aws(secret_name, secrets_client):
    """Fetches a specific secret string from AWS Secrets Manager."""
    if secrets_client is None:
        print(f"ERROR: AWS Secrets client not available to fetch '{secret_name}'.")
        return None
    try:
        get_secret_value_response = secrets_client.get_secret_value(SecretId=secret_name)
        print(f"INFO: Successfully fetched secret '{secret_name}' from AWS.")
        return get_secret_value_response['SecretString']
    except ClientError as e:
        print(f"ERROR: Couldn't retrieve secret '{secret_name}' from AWS: {e}")
        # Depending on the error, you might want different handling
        # e.g., if ResourceNotFoundException, return None vs raising error
        return None # Or raise e

# --- Google Clients (GSpread, Drive) ---
_gspread_client = None
_drive_service = None
_google_creds_json_string = None

def _initialize_google_clients():
    """Internal function to initialize Google clients once."""
    global _gspread_client, _drive_service, _google_creds_json_string

    if _gspread_client and _drive_service:
        return # Already initialized

    secrets_client = get_aws_secrets_manager_client()
    _google_creds_json_string = get_secret_from_aws(config.GOOGLE_SECRETS_NAME, secrets_client)

    if not _google_creds_json_string:
        print("ERROR: Failed to get Google credentials JSON from AWS.")
        return

    try:
        secret_dict = json.loads(_google_creds_json_string)
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(secret_dict, scope)

        _gspread_client = gspread.authorize(creds)
        _drive_service = build('drive', 'v3', credentials=creds)
        print("INFO: GSpread client and Drive service initialized successfully.")

    except Exception as e:
        print(f"ERROR: Failed to initialize Google clients: {e}")
        _gspread_client = None
        _drive_service = None

def get_gspread_client():
    """Returns the initialized GSpread client."""
    if _gspread_client is None:
        _initialize_google_clients()
    return _gspread_client

def get_drive_service():
    """Returns the initialized Google Drive service."""
    if _drive_service is None:
        _initialize_google_clients()
    return _drive_service


# --- Geotab Client ---
_geotab_client = None
def initialize_geotab_client():
    """Initializes and authenticates the Geotab API client."""
    global _geotab_client
    if _geotab_client:
        return _geotab_client

    if not config.GEOTAB_USERNAME or not config.GEOTAB_PASSWORD:
        print("ERROR: Geotab username or password not found in config.")
        return None
    try:
        print(f"INFO: Authenticating Geotab user {config.GEOTAB_USERNAME}...")
        api = mygeotab.API(
            username=config.GEOTAB_USERNAME,
            password=config.GEOTAB_PASSWORD,
            database=config.GEOTAB_DATABASE,
            server=config.GEOTAB_SERVER
        )
        api.authenticate()
        _geotab_client = api
        print("INFO: Geotab client authenticated successfully.")
        return _geotab_client
    except mygeotab.exceptions.AuthenticationException as e:
        print(f"ERROR: Geotab authentication failed: {e}")
        _geotab_client = None
        return None
    except Exception as e:
        print(f"ERROR: Unexpected error initializing Geotab client: {e}")
        _geotab_client = None
        return None

# --- Database Connection ---
_db_credentials = None
def _load_db_credentials():
    """Loads DB credentials from AWS Secrets Manager."""
    global _db_credentials
    if _db_credentials:
        return _db_credentials

    secrets_client = get_aws_secrets_manager_client()
    db_secret_json = get_secret_from_aws(config.DB_SECRETS_NAME, secrets_client)
    if not db_secret_json:
        print("ERROR: Failed to get DB credentials from AWS.")
        return None
    try:
        _db_credentials = json.loads(db_secret_json)
        # Basic validation
        required_keys = ['dbname', 'username', 'password', 'host', 'port']
        if not all(key in _db_credentials for key in required_keys):
             print("ERROR: DB credentials JSON is missing required keys.")
             _db_credentials = None
             return None
        print("INFO: Database credentials loaded.")
        return _db_credentials
    except json.JSONDecodeError:
        print("ERROR: Failed to parse DB credentials JSON.")
        return None
    except Exception as e:
        print(f"ERROR: Unexpected error loading DB credentials: {e}")
        return None


def get_db_connection():
    """Establishes and returns a new connection to the PostgreSQL database."""
    db_creds = _load_db_credentials()
    if not db_creds:
        print("ERROR: Cannot establish DB connection without credentials.")
        return None
    try:
        conn = psycopg2.connect(
            dbname=db_creds['dbname'],
            user=db_creds['username'],
            password=db_creds['password'],
            host=db_creds['host'],
            port=db_creds['port']
        )
        print("INFO: Database connection established.")
        return conn
    except Exception as e:
        print(f"ERROR: DB connection failed: {e}")
        # Consider logging traceback here
        return None