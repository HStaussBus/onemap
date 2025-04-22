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

# --- Geotab Data ---
def fetch_bus_data(api_client, bus_number, from_date, to_date):
    """Fetches bus data and ensures proper timezone handling."""
    # (Keep original implementation, but ensure api_client is passed)
    if not api_client:
         print("ERROR: Geotab API client not provided.")
         return pd.DataFrame()

    # Convert naive datetime to UTC timezone-aware datetime
    utc = pytz.UTC
    if from_date.tzinfo is None: 
        from_date = utc.localize(from_date)
    else: 
        from_date = from_date.astimezone(utc) # Ensure it's UTC

    if to_date.tzinfo is None: 
        to_date = utc.localize(to_date)
    else: 
        to_date = to_date.astimezone(utc) # Ensure it's UTC

    try:
        # Fetch the device information
        device_info = api_client.call("Get", typeName="Device", search={"name": bus_number})
        if not device_info:
            print(f"WARNING: Device not found for bus number: {bus_number}")
            return pd.DataFrame()

        device_id = device_info[0]["id"]

        # Fetch log records
        log_records = api_client.get("LogRecord", search={"deviceSearch": {"id": device_id}, "fromDate": from_date, "toDate":             to_date})

        if not log_records:
            print(f"INFO: No log records found for {bus_number} in time range.")
            return pd.DataFrame()

        df = pd.DataFrame(log_records)
        df["dateTime"] = pd.to_datetime(df["dateTime"]).dt.tz_convert("UTC") # Ensure UTC

        # Filter based on the UTC range
        df_filtered = df[(df["dateTime"] >= from_date) & (df["dateTime"] <= to_date)].copy() # Use .copy() to avoid SettingWithCopyWarning

        print(f"Bus {bus_number}: Fetched {len(df)} records, {len(df_filtered)} within range {from_date} -> {to_date}")
        return df_filtered

    except MyGeotabException as e:
         print(f"ERROR: Geotab API error fetching data for {bus_number}: {e}")
         return pd.DataFrame()
    except Exception as e:
         print(f"ERROR: Unexpected error fetching Geotab data for {bus_number}: {e}")
         return pd.DataFrame()


# --- RAS Data (Google Sheets) ---

def _get_ras_data_from_sheet(gspread_client, sheet_id, sheet_name,
                             date_filter_col, date_filter_value, # date_filter_value can be date obj or string
                             route_filter_col, route_value):
    """
    Helper to get RAS data from a specific sheet, with added debugging and robust filtering.

    Args:
        gspread_client: Authenticated gspread client.
        sheet_id (str): Google Sheet file ID.
        sheet_name (str): Name of the specific worksheet.
        date_filter_col (str): Name of the column containing the date to filter on.
        date_filter_value (datetime.date | str): The date object or specific string to filter by.
        route_filter_col (str): Name of the column containing the route ID.
        route_value (str): The route ID string to filter by.

    Returns:
        tuple: (am_routes_to_buses, pm_routes_to_buses, filtered_rasdf)
               Returns (None, None, None) on critical error.
               Returns ({}, {}, pd.DataFrame()) if no data found matching criteria.
    """
    am_routes_to_buses = {}
    pm_routes_to_buses = {}
    headers = [] # Initialize headers

    if not gspread_client:
        print("ERROR: GSpread client not provided to _get_ras_data_from_sheet.")
        return None, None, None

    try:
        print(f"\nINFO: Accessing sheet_id='{sheet_id}', sheet_name='{sheet_name}'")
        rassheet = gspread_client.open_by_key(sheet_id)
        rasworksheet = rassheet.worksheet(sheet_name)
        all_data = rasworksheet.get_all_values()

        if not all_data or len(all_data) < 2: # Need at least header + 1 data row
            print(f"INFO: No data or only headers found in sheet '{sheet_name}'.")
            return {}, {}, pd.DataFrame()

        headers = all_data[0]
        data = all_data[1:]
        rasdf = pd.DataFrame(data, columns=headers)

        # --- Extensive Debug Prints Start ---
        print(f"\nDEBUG: Initial rasdf shape for sheet '{sheet_name}': {rasdf.shape}")
        print(f"DEBUG: Columns: {rasdf.columns.tolist()}")
        print(f"DEBUG: Filtering for Date Col='{date_filter_col}', Date Value='{repr(date_filter_value)}', Route Col='{route_filter_col}', Route Value='{route_value}'")

        if date_filter_col not in rasdf.columns:
            print(f"ERROR: Date filter column '{date_filter_col}' not found in DataFrame!")
            return None, None, None # Cannot proceed

        print(f"DEBUG: First 5 values in '{date_filter_col}':\n{rasdf[date_filter_col].head().to_string()}")

        if route_filter_col not in rasdf.columns:
            print(f"ERROR: Route filter column '{route_filter_col}' not found in DataFrame!")
            return None, None, None # Cannot proceed

        print(f"DEBUG: First 5 values in '{route_filter_col}':\n{rasdf[route_filter_col].head().to_string()}")
        # --- End initial debug prints ---


        # --- Date Conversion and Filtering ---
        filtered_by_date = pd.DataFrame() # Initialize empty DF
        try:
            print(f"DEBUG: Attempting conversion of date column: '{date_filter_col}'")
            original_non_null_dates = rasdf[date_filter_col].notna().sum()

            # Try specific format first, fallback to inference
            try:
              rasdf[date_filter_col] = pd.to_datetime(rasdf[date_filter_col], format='%m/%d/%Y', errors='coerce')
              print("DEBUG: Used specific format '%m/%d/%Y' for date conversion.")
            except ValueError: # Fallback if specific format fails for some rows
              print("DEBUG: Specific format failed, falling back to inference for date conversion.")
              rasdf[date_filter_col] = pd.to_datetime(rasdf[date_filter_col], errors='coerce')

            rasdf.dropna(subset=[date_filter_col], inplace=True) # Drop rows where conversion failed (became NaT)
            print(f"DEBUG: Shape after date conversion & dropna: {rasdf.shape} (Original non-null dates: {original_non_null_dates})")

            if rasdf.empty:
                 print(f"INFO: No rows left after date conversion/dropna for '{date_filter_col}'.")
                 return {}, {}, pd.DataFrame(columns=headers)

            # Filter by date based on the type of date_filter_value
            if isinstance(date_filter_value, datetime.date):
                 print(f"DEBUG: Filtering by date object: {date_filter_value}")
                 # Compare DATE PART only
                 filtered_by_date = rasdf[rasdf[date_filter_col].dt.date == date_filter_value]
            else: # Assume string comparison for current RAS (Weekday- Day format)
                 print(f"DEBUG: Filtering by date string: {date_filter_value}")
                 # Ensure exact match for strings
                 filtered_by_date = rasdf[rasdf[date_filter_col] == date_filter_value]

            print(f"DEBUG: Shape after date filter: {filtered_by_date.shape}")
            if filtered_by_date.empty:
                 print("DEBUG: No rows matched the date filter.")
                 # Return empty structures, not None, as this isn't necessarily an error
                 return {}, {}, pd.DataFrame(columns=headers)

        except Exception as date_err:
             print(f"ERROR converting or filtering date column '{date_filter_col}': {date_err}")
             return None, None, None # Indicate error occurred


        # --- Route Filtering ---
        final_filtered = pd.DataFrame() # Initialize empty DF
        try:
            print(f"DEBUG: Filtering date-filtered data by route: '{route_value}'")
            if route_filter_col not in filtered_by_date.columns:
                 print(f"ERROR: Route column '{route_filter_col}' not found after date filtering.")
                 return None, None, None

            # Add .str.strip() to handle potential whitespace in sheet data or input route_value
            route_value_stripped = str(route_value).strip()
            final_filtered = filtered_by_date[
                filtered_by_date[route_filter_col].astype(str).str.strip() == route_value_stripped
            ].copy() # Use .copy() to avoid SettingWithCopyWarning
            print(f"DEBUG: Shape after route filter: {final_filtered.shape}")

        except Exception as route_err:
            print(f"ERROR filtering by route column '{route_filter_col}': {route_err}")
            return None, None, None

        # --- Assign final filtered DataFrame for processing ---
        filtered_rasdf = final_filtered

        if filtered_rasdf.empty:
            print(f"INFO: No RAS data found for route {route_value} matching date criteria {repr(date_filter_value)} in sheet {sheet_name} after ALL filters.")
            return {}, {}, pd.DataFrame(columns=headers) # Return empty structures

        print(f"DEBUG: Found {len(filtered_rasdf)} row(s) after all filters.")

        # --- Process AM/PM vehicles from the successfully filtered data ---
        # Determine indices based on headers (more robust than hardcoding indices)
        try:
            route_col_idx = headers.index(route_filter_col) # Usually 'Route'
            trip_type_col_idx = headers.index('Trip Type') # Assuming column 'Trip Type' exists
            vehicle_col_idx = headers.index('Vehicle#')    # Assuming column 'Vehicle#' exists
        except ValueError as e:
            print(f"ERROR: Missing expected column in RAS headers: {e}. Headers are: {headers}")
            return None, None, None # Cannot process vehicles

        for _, row_series in filtered_rasdf.iterrows():
             # Convert Series to list or dict to access by index safely
             row = row_series.tolist()
             route = str(row[route_col_idx]).strip()
             am_pm = str(row[trip_type_col_idx]).strip().upper() # Ensure uppercase for comparison
             vehicle_number = str(row[vehicle_col_idx]).strip()

             # Check if vehicle number is valid before processing
             if vehicle_number and vehicle_number.lower() != 'nan' and vehicle_number.lower() != '':
                 # Add leading zero if 3 digits
                 if len(vehicle_number) == 3 and vehicle_number.isdigit():
                     vehicle_number = '0' + vehicle_number

                 # Add 'NT' prefix (Ensure it's appropriate)
                 # Consider if prefix is always NT or depends on data
                 vehicle_full = 'NT' + vehicle_number

                 if am_pm == "AM":
                     am_routes_to_buses[route] = vehicle_full
                 elif am_pm == "PM":
                     pm_routes_to_buses[route] = vehicle_full

        print(f"DEBUG: AM Vehicles Found: {am_routes_to_buses}")
        print(f"DEBUG: PM Vehicles Found: {pm_routes_to_buses}")

        # Return the processed dictionaries and the final filtered DataFrame
        return am_routes_to_buses, pm_routes_to_buses, filtered_rasdf

    except gspread.exceptions.APIError as e:
        print(f"ERROR: Google Sheets API error accessing sheet ID {sheet_id}, name {sheet_name}: {e}")
        return None, None, None
    except Exception as e:
        print(f"ERROR: Unexpected error in _get_ras_data_from_sheet for {sheet_name}: {e}")
        import traceback
        traceback.print_exc() # Print full traceback for unexpected errors
        return None, None, None

# Remember to also have the get_current_ras_data and get_historical_ras_data functions
# in data_sources.py which call this _get_ras_data_from_sheet helper.

def get_current_ras_data(gspread_client, input_date_obj, route):
    """
    Fetches RAS data from the 'Week Sheet' for a specific date.
    It converts the input date object into a 'Weekday-Day' string format
    (e.g., 'Tuesday-22') for filtering against the sheet.

    Args:
        gspread_client: Authenticated gspread client.
        input_date_obj (datetime.date | datetime.datetime): The specific date object
                                                            to filter by.
        route (str): The route ID string to filter by.

    Returns:
        tuple: Result from _get_ras_data_from_sheet.
    """
    # --- Input Validation ---
    if isinstance(input_date_obj, datetime.datetime):
        # Extract date part if a datetime object is passed
        input_date_obj = input_date_obj.date()
    elif not isinstance(input_date_obj, datetime.date):
        # Error if it's not a date or datetime object
        print(f"ERROR: Invalid input_date_obj type for get_current_ras_data. Expected datetime.date or datetime.datetime, got {type(input_date_obj)}")
        # Consider raising TypeError("Input must be a date or datetime object")
        return None, None, None


    day_format = "%#d" if platform.system() == "Windows" else "%-d"
    # Format the date object into the required "Weekday-Day" string
    date_str_to_filter = input_date_obj.strftime(f"%A-{day_format}") # e.g., "Tuesday-22"
    print(date_str_to_filter)

    print(f"INFO: Fetching current RAS data for date: {input_date_obj} (formatted as: '{date_str_to_filter}') and route: {route}")

    # --- Call Helper Function ---
    # Pass the *formatted string* as the date_filter_value
    return _get_ras_data_from_sheet(
        gspread_client,
        config.CURRENT_RAS_SHEET_ID,
        "Week Sheet",
        date_filter_col='Date',       # Column in sheet assumed to contain "Weekday-Day" strings
        date_filter_value=date_str_to_filter, # Pass the formatted string
        route_filter_col='Route',
        route_value=route
    )

def get_historical_ras_data(gspread_client, input_date_str_mmddyyyy, route):
    """Fetches historical RAS data."""
    # Date format in historical sheet is likely date objects or 'MM/DD/YYYY' strings,
    # need to verify actual format. Assuming MM/DD/YYYY string for filter value.
    return _get_ras_data_from_sheet(
        gspread_client,
        config.HISTORICAL_RAS_SHEET_ID,
        "Archived_RAS",
        date_filter_col='DateID', # Assuming column name is 'DateID'
        date_filter_value=input_date_str_mmddyyyy, # Assuming direct match on MM/DD/YYYY
        route_filter_col='Route', # Assuming column name is 'Route'
        route_value=route
    )

# --- OPT Dump Data (PostgreSQL) ---
def get_opt_dump_data(db_connection_func, route, date_input):
    """ Fetches OPT Dump data from the database. """
    # (Keep original implementation of get_opt_data, but rename it,
    # ensure it uses db_connection_func to get/close connection properly)
    conn = None
    try:
        conn = db_connection_func() # Get a connection using the provided function
        if conn is None:
             print("ERROR: Failed to get DB connection for OPT dump.")
             return None

        # ... (rest of the get_opt_data logic using 'conn') ...
        # Ensure the final DataFrame creation/return happens before the finally block

        # --- Validate and Format Date ---
        if isinstance(date_input, str):
            try:
                query_date = datetime.datetime.strptime(date_input, "%Y-%m-%d").date()
            except ValueError:
                try:
                    query_date = datetime.datetime.strptime(date_input, "%m/%d/%Y").date()
                except ValueError:
                     print(f"ERROR: Invalid date str format '{date_input}'. Use YYYY-MM-DD or MM/DD/YYYY.")
                     return None
        elif isinstance(date_input, datetime.date):
            query_date = date_input
        else:
             print("ERROR: Invalid date_input type. Must be date object or string.")
             return None
        query_date_str = query_date.strftime("%Y-%m-%d") # Format for SQL

        # --- Database Operation ---
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # ... (rest of the query building, execution, fetching logic) ...
            # Use config.DB_TABLE_NAME if needed
            # Make sure to use placeholders %s and pass params tuple to cur.execute()
            # Example (simplified):
            base_select = f"SELECT * FROM {config.DB_TABLE_NAME}" # Replace with actual cols
            where_clause = f"""
                WHERE route = %s AND extraction_date = (
                    SELECT MAX(t.extraction_date) FROM {config.DB_TABLE_NAME} t
                    WHERE t.route = %s AND t.extraction_date <= %s
                )"""
            params = (route, route, query_date_str) # Assuming single route for simplicity here
            sql_query = base_select + where_clause
            cur.execute(sql_query, params)
            results = cur.fetchall()
            all_rows = [dict(record) for record in results]
            # ... (handle multiple routes if needed) ...

        if not all_rows:
            print(f"INFO: No OPT data found for route '{route}' as of {query_date_str}.")
            return pd.DataFrame() # Return empty DF
        else:
            print(f"INFO: Fetched {len(all_rows)} OPT rows.")
            df = pd.DataFrame(all_rows)
            return df

    except (Exception, psycopg2.Error) as error:
        print(f"ERROR: Failed fetching OPT data: {error}")
        print(traceback.format_exc())
        return None
    finally:
        if conn is not None:
            conn.close()
            print("INFO: Database connection closed.")



def find_drive_file(drive_service, root_folder_id, depot, date_str_ymd, route, drive_id):
    """Finds a specific file in Google Drive."""
    if not drive_service:
        print("ERROR: Google Drive service not provided to find_drive_file.")
        # Raise an exception or return None, depending on how you want app.py to handle it
        raise ValueError("Drive service is not available")
    # Parse date info - expects YYYY-MM-DD
    try:
        date_obj = datetime.datetime.strptime(date_str_ymd, "%Y-%m-%d")
    except ValueError:
        print(f"ERROR: Invalid date format '{date_str_ymd}' passed to find_drive_file. Use YYYY-MM-DD.")
        raise ValueError("Invalid date format for find_drive_file")

    year_month = date_obj.strftime("%Y-%m")
    day_folder = date_obj.strftime("%Y-%m-%d")
    # filename_prefix = date_obj.strftime("%d%m%y") # Not used in search query

    # --- Keep the rest of your find_drive_file logic here ---
    # Including the get_folder_id helper, folder traversal, and file search
    # Ensure it uses the passed 'drive_service' argument
    # ---

    # Helper to get subfolder ID inside a parent folder (make it local or keep global if needed)
    def get_folder_id(service, parent_id, name, drive_id):
        # ... (your get_folder_id logic) ...
        query = (f"'{parent_id}' in parents and name = '{name}' and mimeType = 'application/vnd.google-apps.folder'")
        results = service.files().list(q=query, fields="files(id, name)", corpora="drive", driveId=drive_id, includeItemsFromAllDrives=True, supportsAllDrives=True).execute()
        folders = results.get('files', [])
        return folders[0]['id'] if folders else None

    # --- Traverse folder tree ---
    # Map depot input (e.g., "Sharrotts") to folder name if different
    depot_name_in_drive = depot.upper() # Adjust if folder names differ, e.g. depot.capitalize()

    # Use config values for root/drive IDs
    root_folder_id = config.ROOT_FOLDER_ID
    drive_id = config.DRIVE_ID

    try:
        depot_id = get_folder_id(drive_service, root_folder_id, depot_name_in_drive, drive_id)
        if not depot_id: raise FileNotFoundError(f"Depot folder '{depot_name_in_drive}' not found.")

        month_id = get_folder_id(drive_service, depot_id, year_month, drive_id)
        if not month_id: raise FileNotFoundError(f"Month folder '{year_month}' not found in depot '{depot_name_in_drive}'.")

        date_id = get_folder_id(drive_service, month_id, day_folder, drive_id)
        if not date_id: raise FileNotFoundError(f"Date folder '{day_folder}' not found in '{year_month}'.")

        # Search for PDF file containing the route
        query = (f"'{date_id}' in parents and mimeType = 'application/pdf' and name contains '{str(route).upper()}'") # Use upper() for consistency
        result = drive_service.files().list(q=query, fields="files(id, name, webViewLink)", corpora="drive", driveId=drive_id, includeItemsFromAllDrives=True, supportsAllDrives=True).execute()
        files = result.get('files', [])

        if not files: raise FileNotFoundError(f"No DVI PDF file found for route '{route}' on {date_str_ymd} in depot '{depot}'.")

        print(f"INFO: Found DVI file: {files[0]['name']}")
        return files[0] # return first matching file info dict

    except FileNotFoundError as e:
        print(f"INFO: DVI file not found: {e}")
        return None # Return None if not found, instead of raising error up to Flask potentially
    except Exception as e:
        print(f"ERROR: Unexpected error finding DVI file: {e}")
        import traceback
        traceback.print_exc()
        return None # Return None on unexpected errors
