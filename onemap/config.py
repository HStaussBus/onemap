# config.py
import os
import json

# Load secrets from environment variables (set by Codespaces secrets)
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
GEOTAB_USERNAME = os.environ.get('GEOTAB_USERNAME_SECRET')
GEOTAB_PASSWORD = os.environ.get('GEOTAB_PASSWORD_SECRET')
MAPBOX_TOKEN = os.environ.get('MAPBOX_TOKEN')
DB_TABLE_NAME = 'nycsbus_opt_routes'

# Names of secrets stored in AWS Secrets Manager
GOOGLE_SECRETS_NAME = "GoogleServiceCredsGRR"
DB_SECRETS_NAME = "nycsbusSystemsCreds"

# Geotab connection details (can be env vars too if they change)
GEOTAB_DATABASE = 'nycsbus'
GEOTAB_SERVER = 'afmfe.att.com'

# Depot locations (can be loaded from config file or kept here)
DEPOT_LOCS = {
    'Greenpoint': (-73.941033, 40.728215),
    'Conner': (-73.829986, 40.886504),
    'Zerega': (-73.845146, 40.830833),
    'Sharrotts': (-74.241755, 40.539022),
    'Richmond': (-74.128391, 40.638804),
    'Jamaica': (-73.777627, 40.703080)
}

# Google Drive specifics (can be env vars)
DRIVE_ID = '0AFvESHQ9vvAgUk9PVA'
ROOT_FOLDER_ID = '0AFvESHQ9vvAgUk9PVA'

# RAS Sheet IDs (can be env vars)
CURRENT_RAS_SHEET_ID = "1GFwNcv7gdr8KNZO6v2HmeJCde7tE-QqXZh8NzsVQCME"
HISTORICAL_RAS_SHEET_ID = "1ZdD82MMQKn7ofH1YU2yRP6fdenRv13rgsxHvZf-Y0EA"

# Check if essential AWS keys are present (needed for fetching other secrets)
if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
    print("WARNING: AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY environment variable not set.")
    # Depending on your app's needs, you might raise an error here
    # raise EnvironmentError("Missing required AWS credentials in environment.")

print("Config loaded. GEOTAB_USERNAME:", GEOTAB_USERNAME is not None) # Example check