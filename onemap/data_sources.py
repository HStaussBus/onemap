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
def find_drive_file(drive_service, root_folder_id, depot, date_str_ymd, route, drive_id):
    """Finds a specific file in Google Drive."""
    if not drive_service: print("ERROR: Google Drive service not provided."); return None
    try: date_obj = datetime.datetime.strptime(date_str_ymd, "%Y-%m-%d")
    except ValueError: print(f"ERROR: Invalid date format '{date_str_ymd}'. Use YYYY-MM-DD."); return None
    year_month = date_obj.strftime("%Y-%m"); day_folder = date_obj.strftime("%Y-%m-%d")

    def get_folder_id(service, parent_id, name, drive_id):
        """Helper to find folder ID by name within a parent."""
        try:
            query = (f"'{parent_id}' in parents and name = '{name}' "
                     f"and mimeType = 'application/vnd.google-apps.folder' "
                     f"and trashed = false")
            results = service.files().list(
                q=query, fields="files(id, name)", corpora="drive",
                driveId=drive_id, includeItemsFromAllDrives=True,
                supportsAllDrives=True, pageSize=1
            ).execute()
            folders = results.get('files', [])
            return folders[0]['id'] if folders else None
        except Exception as e:
            print(f"ERROR searching for folder '{name}' in Drive: {e}")
            return None

    depot_name_in_drive = depot.upper()
    root_folder_id = config.ROOT_FOLDER_ID
    drive_id = config.DRIVE_ID
    if not root_folder_id or not drive_id: print("ERROR: ROOT_FOLDER_ID or DRIVE_ID not set."); return None

    try:
        print(f"DEBUG Drive Search: Starting search in Root ({root_folder_id}) on Drive ({drive_id})")
        depot_id = get_folder_id(drive_service, root_folder_id, depot_name_in_drive, drive_id)
        # *** CORRECTED SYNTAX: Put raise on its own line ***
        if not depot_id:
            raise FileNotFoundError(f"Depot folder '{depot_name_in_drive}' not found in root '{root_folder_id}'.")

        month_id = get_folder_id(drive_service, depot_id, year_month, drive_id)
        # *** CORRECTED SYNTAX: Put raise on its own line ***
        if not month_id:
            raise FileNotFoundError(f"Month folder '{year_month}' not found in depot '{depot_name_in_drive}' (ID: {depot_id}).")

        date_id = get_folder_id(drive_service, month_id, day_folder, drive_id)
        # *** CORRECTED SYNTAX: Put raise on its own line ***
        if not date_id:
            raise FileNotFoundError(f"Date folder '{day_folder}' not found in month '{year_month}' (ID: {month_id}).")

        # Search for PDF file
        route_str_upper = str(route).upper()
        query = (f"'{date_id}' in parents and mimeType = 'application/pdf' "
                 f"and name contains '{route_str_upper}' and trashed = false")
        print(f"DEBUG Drive Search: Querying for PDF with route '{route_str_upper}'...")
        result = drive_service.files().list(
            q=query, fields="files(id, name, webViewLink)", corpora="drive",
            driveId=drive_id, includeItemsFromAllDrives=True,
            supportsAllDrives=True, pageSize=10
        ).execute()
        files = result.get('files', [])

        # *** CORRECTED SYNTAX: Put raise on its own line ***
        if not files:
            raise FileNotFoundError(f"No DVI PDF containing route '{route_str_upper}' found on {date_str_ymd} in folder '{day_folder}'.")

        print(f"INFO: Found DVI file: {files[0]['name']} (ID: {files[0]['id']})")
        return files[0] # return first matching file info dict

    except FileNotFoundError as e:
        print(f"INFO: DVI file search path ended: {e}")
        return None # Return None if not found at any stage
    except Exception as e:
        print(f"ERROR: Unexpected error finding DVI file: {e}")
        traceback.print_exc() # Log full traceback
        return None # Return None on unexpected errors
# --- End find_drive_file ---