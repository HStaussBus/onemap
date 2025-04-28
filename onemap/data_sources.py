# data_sources.py
import pandas as pd
import datetime
import pytz
import psycopg2
import psycopg2.extras
import traceback
from mygeotab.exceptions import MyGeotabException # Be specific if possible
import config # To get Sheet IDs etc.
import platform
import re
import gspread

# --- Geotab Data ---
# fetch_bus_data function remains the same as the last version (returning df, device_id)
def fetch_bus_data(api_client, bus_number, from_date, to_date):
    """
    Fetches bus data and ensures proper timezone handling.
    Returns a tuple: (DataFrame, device_id | None)
    """
    if not api_client:
         print("ERROR: Geotab API client not provided.")
         return pd.DataFrame(), None
    utc = pytz.UTC # Define utc timezone
    # Ensure input datetimes are timezone-aware UTC
    if from_date.tzinfo is None: from_date = utc.localize(from_date)
    else: from_date = from_date.astimezone(utc)
    if to_date.tzinfo is None: to_date = utc.localize(to_date)
    else: to_date = to_date.astimezone(utc)

    device_id = None
    try:
        print(f"DEBUG: Fetching device info for bus number: {bus_number}")
        device_info = api_client.call("Get", typeName="Device", search={"name": bus_number})
        if not device_info or not isinstance(device_info, list) or len(device_info) == 0 or 'id' not in device_info[0]:
            print(f"WARNING: Device not found or info invalid for bus: {bus_number}. Info: {device_info}")
            return pd.DataFrame(), None

        device_id = device_info[0]["id"]
        print(f"DEBUG: Found Device ID: {device_id} for Bus: {bus_number}")

        print(f"DEBUG: Fetching log records for Device ID: {device_id}")
        log_records = api_client.get("LogRecord", search={"deviceSearch": {"id": device_id}, "fromDate": from_date.isoformat(), "toDate": to_date.isoformat()}) # Use ISO format

        if not log_records:
            print(f"INFO: No log records found for {bus_number} (Device ID: {device_id}) in time range.")
            return pd.DataFrame(), device_id

        df = pd.DataFrame(log_records)
        if "dateTime" not in df.columns:
            print(f"WARNING: 'dateTime' column missing in log records for {bus_number}.")
            return pd.DataFrame(), device_id

        df["dateTime"] = pd.to_datetime(df["dateTime"], errors='coerce', utc=True)
        df.dropna(subset=["dateTime"], inplace=True)
        df_filtered = df[(df["dateTime"] >= from_date) & (df["dateTime"] <= to_date)].copy()

        print(f"Bus {bus_number}: Fetched {len(df)} raw records, {len(df_filtered)} within range {from_date} -> {to_date}")
        return df_filtered, device_id

    except MyGeotabException as e:
         print(f"ERROR: Geotab API error fetching data for {bus_number}: {e}")
         return pd.DataFrame(), None
    except Exception as e:
         print(f"ERROR: Unexpected error fetching Geotab data for {bus_number}: {e}")
         print(traceback.format_exc())
         return pd.DataFrame(), None


# --- RAS Data (Google Sheets) ---

# _get_ras_data_from_sheet helper function remains the same
def _get_ras_data_from_sheet(gspread_client, sheet_id, sheet_name,
                           date_filter_col, date_filter_value, # date_filter_value can be date obj or string
                           route_filter_col, route_value):
    """
    Helper to get RAS data from a specific sheet, checking filter type BEFORE converting.
    """
    am_routes_to_buses = {}
    pm_routes_to_buses = {}
    headers = []
    if not gspread_client: return None, None, None # Guard clause

    try:
        print(f"\nINFO: Accessing sheet_id='{sheet_id}', sheet_name='{sheet_name}'")
        rassheet = gspread_client.open_by_key(sheet_id)
        rasworksheet = rassheet.worksheet(sheet_name)
        all_data = rasworksheet.get_all_values()

        if not all_data or len(all_data) < 2:
            headers = all_data[0] if all_data else []
            print(f"INFO: No data or only headers found in sheet '{sheet_name}'.")
            return {}, {}, pd.DataFrame(columns=headers)

        headers = all_data[0]
        data = all_data[1:]
        rasdf = pd.DataFrame(data, columns=headers).astype(str)
        rasdf.replace(['None', '', '#N/A', 'nan', 'NaT'], pd.NA, inplace=True)

        print(f"\nDEBUG: Initial rasdf shape for sheet '{sheet_name}': {rasdf.shape}")
        print(f"DEBUG: Filtering for Date Col='{date_filter_col}', Date Value='{repr(date_filter_value)}', Route Col='{route_filter_col}', Route Value='{route_value}'")

        if date_filter_col not in rasdf.columns: return None, None, None
        if route_filter_col not in rasdf.columns: return None, None, None

        filtered_by_date = pd.DataFrame(columns=headers)

        # --- Date Filtering Logic ---
        try: # Keep internal try for date logic
            if isinstance(date_filter_value, datetime.date): # Current RAS (passed date object, formatted in calling func)
                day_format = "%#d" if platform.system() == "Windows" else "%-d"
                date_str_to_match_pattern = date_filter_value.strftime(f"%A-{day_format}")
                print(f"DEBUG (Helper-DateObj): Filtering by DATE OBJECT {date_filter_value}. Performing string comparison using pattern: '{date_str_to_match_pattern}'")
                filtered_by_date = rasdf[rasdf[date_filter_col].astype(str).str.strip() == date_str_to_match_pattern].copy()

            elif isinstance(date_filter_value, str): # Historical RAS (passed MM/DD/YYYY string) or Current (Weekday-Day String)
                date_str_input = date_filter_value.strip()
                print(f"DEBUG (Helper-String): Filtering by STRING value: '{date_str_input}'")
                if re.match(r'^[A-Za-z]+-\d{1,2}$', date_str_input): # Current RAS format
                    print(f"DEBUG (Helper-String): Matches Weekday-Day. Direct string comparison.")
                    filtered_by_date = rasdf[rasdf[date_filter_col].astype(str).str.strip() == date_str_input].copy()
                else: # Historical RAS format (assume MM/DD/YYYY etc.)
                    print(f"DEBUG (Helper-String): Does NOT match Weekday-Day. Attempting date logic.")
                    try:
                        target_date = pd.to_datetime(date_str_input, errors='raise').date()
                        print(f"DEBUG (Helper-String): Parsed input string to date: {target_date}. Converting sheet column '{date_filter_col}'...")
                        rasdf_dates_only = rasdf[[date_filter_col]].copy()
                        rasdf_dates_only[date_filter_col] = pd.to_datetime(rasdf_dates_only[date_filter_col], errors='coerce')
                        rasdf_dates_only.dropna(subset=[date_filter_col], inplace=True)
                        if not rasdf_dates_only.empty:
                            matching_indices = rasdf_dates_only[rasdf_dates_only[date_filter_col].dt.date == target_date].index
                            valid_indices = [idx for idx in matching_indices if idx in rasdf.index]
                            filtered_by_date = rasdf.loc[valid_indices].copy()
                            print(f"DEBUG (Helper-String): Found {len(valid_indices)} rows matching converted date.")
                        else: print(f"DEBUG (Helper-String): Sheet column '{date_filter_col}' had no valid dates after conversion.")
                    except Exception as e:
                        print(f"DEBUG (Helper-String): Failed date logic for string '{date_str_input}' ({e}). Falling back to direct string comparison.")
                        filtered_by_date = rasdf[rasdf[date_filter_col].astype(str).str.strip() == date_str_input].copy()
            else: # Unsupported type
                print(f"ERROR (Helper): Unsupported type for date_filter_value: {type(date_filter_value)}")
                return None, None, None

            if filtered_by_date.empty:
                 print(f"DEBUG (Helper): No rows matched date criteria for: {repr(date_filter_value)}.")
                 return {}, {}, pd.DataFrame(columns=headers)

        except Exception as date_err: # Catch errors specifically from date logic
            print(f"ERROR (Helper): During date filtering logic: {date_err}")
            traceback.print_exc(); return None, None, None

        # --- Route Filtering ---
        final_filtered = pd.DataFrame(columns=headers)
        try:
            if filtered_by_date.empty: return {}, {}, pd.DataFrame(columns=headers)
            route_value_stripped = str(route_value).strip()
            final_filtered = filtered_by_date[filtered_by_date[route_filter_col].astype(str).str.strip() == route_value_stripped].copy()
            print(f"DEBUG (Helper): Shape after route filter: {final_filtered.shape}")
        except Exception as route_err:
            print(f"ERROR (Helper): Filtering by route: {route_err}"); return None, None, None

        # --- Process Filtered Data ---
        filtered_rasdf = final_filtered
        if filtered_rasdf.empty: return {}, {}, pd.DataFrame(columns=headers)
        try:
            for _, row_series in filtered_rasdf.iterrows():
                # ... (vehicle processing logic as before) ...
                if not all(col in row_series.index for col in [route_filter_col, 'Trip Type', 'Vehicle#']): continue
                route = str(row_series[route_filter_col]).strip(); am_pm = str(row_series['Trip Type']).strip().upper(); vehicle_number = str(row_series['Vehicle#']).strip()
                if not vehicle_number or vehicle_number.lower() in ('nan', '', 'none', '#n/a'): continue
                if isinstance(vehicle_number, str) and vehicle_number.endswith('.0'): vehicle_number = vehicle_number[:-2]
                if len(vehicle_number) == 3 and vehicle_number.isdigit(): vehicle_number = '0' + vehicle_number
                vehicle_full = 'NT' + vehicle_number if (len(vehicle_number) == 4 and vehicle_number.isdigit()) else vehicle_number
                if not route: continue
                if am_pm == "AM": am_routes_to_buses[route] = vehicle_full
                elif am_pm == "PM": pm_routes_to_buses[route] = vehicle_full
        except Exception as proc_err:
             print(f"ERROR (Helper): During vehicle processing: {proc_err}"); traceback.print_exc(); return None, None, None

        print(f"DEBUG (Helper): AM Vehicles Found: {am_routes_to_buses}")
        print(f"DEBUG (Helper): PM Vehicles Found: {pm_routes_to_buses}")
        return am_routes_to_buses, pm_routes_to_buses, filtered_rasdf

    # --- Outer error handling (Corrected Syntax) ---
    except gspread.exceptions.APIError as e:
        error_details = e.response.json() if hasattr(e, 'response') else {}
        print(f"ERROR: Google Sheets API error accessing sheet ID {sheet_id}, name {sheet_name}: {e}. Details: {error_details}")
        return None, None, None
    except Exception as e:
        print(f"ERROR: Unexpected error in _get_ras_data_from_sheet for {sheet_name}: {e}")
        traceback.print_exc()
        return None, None, None
# --- End of _get_ras_data_from_sheet ---


# --- get_current_ras_data (Remains the same) ---
def get_current_ras_data(gspread_client, input_date_obj, route):
    """ Fetches RAS data from the 'Week Sheet'. Uses Weekday-Day format. """
    if isinstance(input_date_obj, datetime.datetime): input_date_obj = input_date_obj.date()
    elif not isinstance(input_date_obj, datetime.date): return None, None, None
    try:
        day_format = "%#d" if platform.system() == "Windows" else "%-d"
        date_str_to_filter = input_date_obj.strftime(f"%A-{day_format}")
        print(f"DEBUG: Formatted date for current RAS filter: '{date_str_to_filter}'")
        return _get_ras_data_from_sheet(
            gspread_client, config.CURRENT_RAS_SHEET_ID, "Week Sheet",
            date_filter_col='Date', date_filter_value=date_str_to_filter,
            route_filter_col='Route', route_value=route
        )
    except Exception as e: print(f"ERROR in get_current_ras_data: {e}"); return None, None, None

# --- get_historical_ras_data (Remains the same as last corrected version) ---
def get_historical_ras_data(gspread_client, input_date_obj, route):
    """ Fetches historical RAS data. Uses MM/DD/YYYY string format. """
    if isinstance(input_date_obj, datetime.datetime): input_date_obj = input_date_obj.date()
    elif not isinstance(input_date_obj, datetime.date): return None, None, None
    try:
        date_str_mmddyyyy = input_date_obj.strftime("%m/%d/%Y")
        print(f"DEBUG: Formatted date for historical RAS filter: '{date_str_mmddyyyy}'")
        return _get_ras_data_from_sheet(
            gspread_client, config.HISTORICAL_RAS_SHEET_ID, "Archived_RAS",
            date_filter_col='DateID', date_filter_value=date_str_mmddyyyy,
            route_filter_col='Route', route_value=route
        )
    except Exception as e: print(f"ERROR in get_historical_ras_data: {e}"); return None, None, None

# --- OPT Dump Data (PostgreSQL) ---
# get_opt_dump_data function remains the same as the last version
def get_opt_dump_data(db_connection_func, route, date_input):
    """ Fetches OPT Dump data from the database. """
    # ... (Keep implementation from previous version) ...
    conn = None
    try:
        conn = db_connection_func();
        if conn is None: return None
        # Date validation and formatting
        if isinstance(date_input, str):
            try: query_date = datetime.datetime.strptime(date_input, "%Y-%m-%d").date()
            except ValueError:
                try: query_date = datetime.datetime.strptime(date_input, "%m/%d/%Y").date()
                except ValueError: print(f"ERROR: Invalid date str format '{date_input}'."); return None
        elif isinstance(date_input, datetime.datetime): query_date = date_input.date()
        elif isinstance(date_input, datetime.date): query_date = date_input
        else: print(f"ERROR: Invalid date_input type: {type(date_input)}."); return None
        query_date_str = query_date.strftime("%Y-%m-%d")

        print(f"INFO: Querying OPT Dump for Route: '{route}', Date: '{query_date_str}'")
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Query
            sql_query = f"""SELECT * FROM {config.DB_TABLE_NAME} WHERE route = %s AND extraction_date = (SELECT MAX(t.extraction_date) FROM {config.DB_TABLE_NAME} t WHERE t.route = %s AND t.extraction_date <= %s::date)"""
            params = (route, route, query_date_str)
            cur.execute(sql_query, params)
            results = cur.fetchall(); all_rows = [dict(record) for record in results]
        if not all_rows: print(f"INFO: No OPT data found for route '{route}' as of {query_date_str}."); return pd.DataFrame()
        else: print(f"INFO: Fetched {len(all_rows)} OPT rows for route '{route}'."); df = pd.DataFrame(all_rows); return df
    except (Exception, psycopg2.Error) as error: print(f"ERROR: Failed fetching OPT data: {error}"); traceback.print_exc(); return None
    finally:
        if conn is not None:
            try: conn.close(); print("INFO: Database connection closed.")
            except Exception as close_err: print(f"ERROR: Failed to close DB connection: {close_err}")


# --- Google Drive ---
# find_drive_file function - CORRECTED SYNTAX
def _get_folder_id(service, parent_id, name, drive_id_param):
    """Finds a folder by name within a parent folder."""
    try:
        query = (f"'{parent_id}' in parents and name = '{name}' "
                 f"and mimeType = 'application/vnd.google-apps.folder' "
                 f"and trashed = false")
        results = service.files().list(
            q=query, fields="files(id, name)", corpora="drive",
            driveId=drive_id_param, includeItemsFromAllDrives=True,
            supportsAllDrives=True, pageSize=1
        ).execute()
        folders = results.get('files', [])
        if folders:
            # print(f"DEBUG _get_folder_id: Found folder '{name}' (ID: {folders[0]['id']}) inside parent '{parent_id}'.")
            return folders[0]['id']
        else:
            # print(f"DEBUG _get_folder_id: Folder '{name}' not found inside parent '{parent_id}'.")
            return None
    except Exception as e:
        print(f"ERROR searching for folder '{name}' in Drive parent '{parent_id}': {e}")
        return None

# --- Nested Helper Function to Search for PDF ---
# (Keep this function as it was)
def _search_pdf_in_date_folder(service, date_folder_id, route_str, drive_id_param):
    """Searches for a PDF containing the route string within a specific folder."""
    if not date_folder_id:
        print("DEBUG _search_pdf: Skipped search as date_folder_id was None.")
        return None
    try:
        query = (f"'{date_folder_id}' in parents and mimeType = 'application/pdf' "
                 f"and name contains '{route_str}' and trashed = false")
        print(f"DEBUG Drive Search: Querying for PDF with route '{route_str}' in folder '{date_folder_id}'...")
        result = service.files().list(
            q=query, fields="files(id, name, webViewLink)", corpora="drive",
            driveId=drive_id_param, includeItemsFromAllDrives=True,
            supportsAllDrives=True, pageSize=10 # Allow multiple matches
        ).execute()
        files = result.get('files', [])
        if files:
            print(f"INFO: Found DVI file: {files[0]['name']} (ID: {files[0]['id']})")
            return files[0] # Return first match
        else:
            print(f"DEBUG Drive Search: No PDF found for route '{route_str}' in folder '{date_folder_id}'.")
            return None
    except Exception as e:
        print(f"ERROR searching for PDF in date folder '{date_folder_id}': {e}")
        return None

# --- Rewritten Main Function ---
def find_drive_file(drive_service, root_folder_id, depot, date_str_ymd, route, drive_id):
    """
    Finds a specific file in Google Drive, checking both folder structures:
    1. Depot/YYYY-MM/YYYY-MM-DD
    2. Depot/YYYY-MM-DD

    Args:
        drive_service: Authorized Google Drive service instance.
        root_folder_id: The ID of the parent folder containing the depot folders.
        depot: The name of the depot folder (e.g., 'SHARROTTS').
        date_str_ymd: The date string in 'YYYY-MM-DD' format.
        route: The route identifier (string) expected in the filename.
        drive_id: The ID of the Shared Drive to search within.

    Returns:
        A dictionary containing file info (id, name, webViewLink) if found, otherwise None.
    """
    print(f"--- find_drive_file called: Depot='{depot}', Date='{date_str_ymd}', Route='{route}' ---") # Entry point log

    # --- Input Validation and Setup ---
    if not drive_service:
        print("ERROR find_drive_file: Google Drive service not provided.")
        return None
    if not root_folder_id or not drive_id:
        print("ERROR find_drive_file: root_folder_id or drive_id not provided.")
        return None
    try:
        date_obj = datetime.datetime.strptime(date_str_ymd, "%Y-%m-%d")
    except ValueError:
        print(f"ERROR find_drive_file: Invalid date format '{date_str_ymd}'. Use YYYY-MM-DD.")
        return None

    year_month = date_obj.strftime("%Y-%m")
    day_folder = date_obj.strftime("%Y-%m-%d")
    route_str_upper = str(route).upper()
    depot_name_in_drive = depot.upper()
    pdf_info = None # Variable to store the result if found

    # --- Main Search Logic ---
    try:
        print(f"DEBUG find_drive_file: Starting search in Root ('{root_folder_id}') on Drive ('{drive_id}') for Depot '{depot_name_in_drive}'")
        depot_id = _get_folder_id(drive_service, root_folder_id, depot_name_in_drive, drive_id)

        if not depot_id:
            print(f"INFO find_drive_file: Depot folder '{depot_name_in_drive}' not found in root '{root_folder_id}'.")
            return None # Cannot proceed without depot

        # --- Attempt Path 1: Depot -> YYYY-MM -> YYYY-MM-DD ---
        print(f"DEBUG find_drive_file: === Trying Path 1 (Depot -> {year_month} -> {day_folder}) ===")
        month_id = _get_folder_id(drive_service, depot_id, year_month, drive_id)
        if month_id:
            print(f"DEBUG find_drive_file: Path 1: Found month folder '{year_month}' (ID: {month_id}). Looking for day folder '{day_folder}'...")
            date_id_path1 = _get_folder_id(drive_service, month_id, day_folder, drive_id)
            if date_id_path1:
                print(f"DEBUG find_drive_file: Path 1: Found date folder '{day_folder}' (ID: {date_id_path1}). Searching for PDF...")
                pdf_info = _search_pdf_in_date_folder(drive_service, date_id_path1, route_str_upper, drive_id)
                if pdf_info:
                    print("INFO find_drive_file: PDF found via Path 1.")
                    # If found via Path 1, we can return immediately
                    return pdf_info
                else:
                    print(f"INFO find_drive_file: Path 1: PDF not found in '{day_folder}', although day folder exists.")
            else:
                print(f"DEBUG find_drive_file: Path 1: Date folder '{day_folder}' not found inside month folder '{year_month}'.")
        else:
            print(f"DEBUG find_drive_file: Path 1: Month folder '{year_month}' not found inside depot '{depot_name_in_drive}'.")

        # --- Attempt Path 2: Depot -> YYYY-MM-DD ---
        # Only proceed if pdf_info is still None (meaning Path 1 didn't find the file)
        if pdf_info is None:
            print(f"DEBUG find_drive_file: === Trying Path 2 (Depot -> {day_folder}) ===")
            date_id_path2 = _get_folder_id(drive_service, depot_id, day_folder, drive_id)
            if date_id_path2:
                print(f"DEBUG find_drive_file: Path 2: Found date folder '{day_folder}' (ID: {date_id_path2}). Searching for PDF...")
                pdf_info = _search_pdf_in_date_folder(drive_service, date_id_path2, route_str_upper, drive_id)
                if pdf_info:
                    print("INFO find_drive_file: PDF found via Path 2.")
                    # pdf_info now holds the result from Path 2
                else:
                    print(f"INFO find_drive_file: Path 2: PDF not found in '{day_folder}', although day folder exists.")
            else:
                print(f"DEBUG find_drive_file: Path 2: Date folder '{day_folder}' not found directly under depot '{depot_name_in_drive}'.")
        else:
             # This case should technically not be reached if Path 1 returned early, but good for clarity
             print(f"DEBUG find_drive_file: Skipping Path 2 because PDF was already found via Path 1.")


        # --- Final Result ---
        if pdf_info:
            print(f"--- find_drive_file finished: Found file '{pdf_info.get('name', 'N/A')}' ---")
            return pdf_info
        else:
            print(f"--- find_drive_file finished: File not found for route '{route_str_upper}' on date {date_str_ymd} via any path. ---")
            return None

    except Exception as e:
        # Catch-all for unexpected errors during the search process
        print(f"ERROR find_drive_file: Unexpected error during search logic: {e}")
        traceback.print_exc() # Print detailed traceback for debugging
        return None # Return None on unexpected error
# --- End find_drive_file ---
IDLING_RULE_ID = "RuleIdlingId"
SPEEDING_RULE_ID = "RulePostedSpeedingId"

def fetch_safety_exceptions(api, device_id, start_time_utc, end_time_utc):
    """
    Fetches Idling and Speeding exceptions for a specific device and time range.
    Does NOT fetch coordinates.

    Args:
        api: Authenticated Geotab API client object.
        device_id (str): The specific Geotab device ID (e.g., "b179").
        start_time_utc (str): The start timestamp in ISO 8601 UTC format.
        end_time_utc (str): The end timestamp in ISO 8601 UTC format.

    Returns:
        list: A list of dictionaries, each representing a safety event details.
              Example dict:
              {'device_id': 'bXXX', 'rule_name': 'Idling', 'rule_id': 'RuleIdlingId',
               'start_time': '...', 'end_time': '...', 'duration_s': 120.0}
    """

    all_exceptions_raw = [] # Initialize list for raw API results

    # --- Fetch Exceptions (using ExceptionEventSearch structure) ---
    rule_ids_to_fetch = {
        "Idling": IDLING_RULE_ID,
        "Speeding": SPEEDING_RULE_ID
    }

    for rule_name_friendly, rule_id_actual in rule_ids_to_fetch.items():
        print(f"\nFetching {rule_name_friendly} exceptions using ExceptionEventSearch...")
        try:
            device_search = {'id': device_id}
            rule_search = {'id': rule_id_actual}
            exception_search = {
                'deviceSearch': device_search,
                'ruleSearch': rule_search,
                'fromDate': start_time_utc,
                'toDate': end_time_utc
            }
            results = api.get('ExceptionEvent', search=exception_search)
            print(f"-> Found {len(results)} {rule_name_friendly} results.")
            # Add results for processing
            all_exceptions_raw.extend(results)

        except Exception as e:
            print(f"ERROR fetching {rule_name_friendly} exceptions: {e}")

    # --- Process combined results (NO coordinate fetching here) ---
    print(f"\nProcessing {len(all_exceptions_raw)} combined raw exceptions...")
    processed_data = []
    if all_exceptions_raw:
        for exception in all_exceptions_raw:
            device_info = exception.get('device', {})
            rule_info = exception.get('rule', {})
            duration_obj = exception.get('duration')
            event_start_str = exception.get('activeFrom')
            event_end_str = exception.get('activeTo')

            actual_device_id = device_info.get('id') if isinstance(device_info, dict) else None
            actual_rule_id = rule_info.get('id') if isinstance(rule_info, dict) else None

            if not actual_device_id or not actual_rule_id or not event_start_str:
                continue # Skip if essential info missing

            # --- Parse duration ---
            duration_seconds = None
            if isinstance(duration_obj, dict) and 'ticks' in duration_obj:
                try:
                    duration_seconds = duration_obj['ticks'] / 10_000_000
                except Exception:
                    duration_seconds = None # Keep as None if parsing fails

            processed_data.append({
                'device_id': actual_device_id,
                'rule_name': rule_info.get('name') or actual_rule_id, # Use name or fallback to ID
                'rule_id': actual_rule_id,
                'start_time': event_start_str, # Keep as ISO string
                'end_time': event_end_str,     # Keep as ISO string
                'duration_s': duration_seconds
                # No latitude/longitude here
            })
    else:
        print("No raw exceptions were successfully retrieved.")

    print(f"Finished processing. Returning {len(processed_data)} safety event details.")
    return processed_data # Return list of dicts
