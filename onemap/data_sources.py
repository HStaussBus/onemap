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
    Helper to get RAS data from a specific sheet, checking filter type BEFORE converting.
    """
    am_routes_to_buses = {}
    pm_routes_to_buses = {}
    headers = []

    if not gspread_client:
        print("ERROR: GSpread client not provided to _get_ras_data_from_sheet.")
        return None, None, None

    try:
        print(f"\nINFO: Accessing sheet_id='{sheet_id}', sheet_name='{sheet_name}'")
        rassheet = gspread_client.open_by_key(sheet_id)
        rasworksheet = rassheet.worksheet(sheet_name)
        all_data = rasworksheet.get_all_values()

        if not all_data or len(all_data) < 2:
            print(f"INFO: No data or only headers found in sheet '{sheet_name}'.")
            return {}, {}, pd.DataFrame(columns=headers) # Return empty DF with headers if possible

        headers = all_data[0]
        data = all_data[1:]
        # Load as string initially to prevent incorrect type inference
        rasdf = pd.DataFrame(data, columns=headers).astype(str)
        # Replace common non-values with pandas NA
        rasdf.replace(['None', '', '#N/A', 'nan', 'NaT'], pd.NA, inplace=True) # Added 'nan', 'NaT'

        print(f"\nDEBUG: Initial rasdf shape for sheet '{sheet_name}': {rasdf.shape}")
        # print(f"DEBUG: Columns: {rasdf.columns.tolist()}") # Optional verbose logging
        print(f"DEBUG: Filtering for Date Col='{date_filter_col}', Date Value='{repr(date_filter_value)}', Route Col='{route_filter_col}', Route Value='{route_value}'")

        if date_filter_col not in rasdf.columns:
            print(f"ERROR: Date filter column '{date_filter_col}' not found! Columns are: {rasdf.columns.tolist()}")
            return None, None, None
        if route_filter_col not in rasdf.columns:
            print(f"ERROR: Route filter column '{route_filter_col}' not found! Columns are: {rasdf.columns.tolist()}")
            return None, None, None

        # print(f"DEBUG: First 5 values in '{date_filter_col}':\n{rasdf[date_filter_col].head().to_string(index=False)}")
        # print(f"DEBUG: First 5 values in '{route_filter_col}':\n{rasdf[route_filter_col].head().to_string(index=False)}")

        # --- CORRECTED Date Filtering Logic ---
        filtered_by_date = pd.DataFrame(columns=headers) # Initialize empty with original headers

        try:
            # Check the type of the filter value *first*

            # CASE 1: Input is a date object (flexible, kept for robustness)
            if isinstance(date_filter_value, datetime.date):
                day_format = "%#d" if platform.system() == "Windows" else "%-d"
                date_str_to_match = date_filter_value.strftime(f"%A-{day_format}")
                print(f"DEBUG: Filtering by DATE OBJECT {date_filter_value}. Performing direct string comparison using target: '{date_str_to_match}'")
                # Perform direct string comparison against original DataFrame data (treat column as string)
                filtered_by_date = rasdf[
                    rasdf[date_filter_col].astype(str).str.strip() == date_str_to_match
                ].copy()

            # CASE 2: Input is a string
            elif isinstance(date_filter_value, str):
                date_str_input = date_filter_value.strip()
                print(f"DEBUG: Filtering by STRING value: '{date_str_input}'")

                # Check if the string matches the "Weekday-Day" pattern (Current RAS)
                if re.match(r'^[A-Za-z]+-\d+$', date_str_input):
                    print(f"DEBUG: String '{date_str_input}' matches Weekday-Day pattern. Performing ONLY direct string comparison.")
                    # Perform ONLY direct string comparison against original DataFrame data (treat column as string)
                    filtered_by_date = rasdf[
                        rasdf[date_filter_col].astype(str).str.strip() == date_str_input
                    ].copy()
                    # ***** NO pd.to_datetime CONVERSION of rasdf[date_filter_col] here *****
                else:
                    # String does NOT look like "Weekday-Day". Assume it might be MM/DD/YYYY etc (Historical RAS). Attempt date logic.
                    print(f"DEBUG: String '{date_str_input}' does NOT match Weekday-Day pattern. Attempting date logic.")
                    try:
                        # Try parsing the INPUT string as a date (allow inference)
                        target_date = pd.to_datetime(date_str_input, errors='raise').date()
                        print(f"DEBUG: Parsed input string to date: {target_date}. Attempting conversion of sheet column '{date_filter_col}' for comparison.")

                        # Convert sheet column ON A COPY - only if needed
                        rasdf_converted = rasdf[[date_filter_col]].copy()
                        # Let pandas infer format for sheet column conversion here, maybe add dayfirst=True if needed
                        rasdf_converted[date_filter_col] = pd.to_datetime(rasdf_converted[date_filter_col], errors='coerce')
                        rasdf_converted.dropna(subset=[date_filter_col], inplace=True) # Remove rows that failed conversion in sheet

                        if not rasdf_converted.empty:
                            # Filter original DataFrame using indices from converted comparison
                            matching_indices = rasdf_converted[rasdf_converted[date_filter_col].dt.date == target_date].index
                            filtered_by_date = rasdf.loc[matching_indices].copy()
                            print(f"DEBUG: Found {len(matching_indices)} rows matching converted date.")
                        else:
                             print(f"DEBUG: Sheet column '{date_filter_col}' resulted in no valid dates after conversion attempt.")
                             # No match found via date conversion, filtered_by_date remains empty

                    except (ValueError, TypeError) as e:
                        # Failed to parse input string as a date OR failed during sheet conversion/comparison
                        print(f"DEBUG: Failed date logic for string '{date_str_input}' ({e}). Falling back to direct string comparison.")
                        # Fallback: If it wasn't Weekday-Day & not parseable as a standard date, do direct string match anyway
                        filtered_by_date = rasdf[
                            rasdf[date_filter_col].astype(str).str.strip() == date_str_input
                        ].copy()
            else:
                print(f"ERROR: Unsupported type for date_filter_value: {type(date_filter_value)}")
                return None, None, None # Or raise error

            # --- Post-filtering checks ---
            print(f"DEBUG: Shape after date filter section: {filtered_by_date.shape}")
            if filtered_by_date.empty:
                print(f"DEBUG: No rows matched the date filter criteria applied for: {repr(date_filter_value)}.")
                return {}, {}, pd.DataFrame(columns=headers) # Return standard empty result

        except Exception as date_err:
            print(f"ERROR during date filtering logic: {date_err}")
            import traceback
            traceback.print_exc()
            return None, None, None


        # --- Route Filtering ---
        # (Code remains the same)
        final_filtered = pd.DataFrame()
        try:
            print(f"DEBUG: Filtering date-filtered data ({filtered_by_date.shape[0]} rows) by route: '{route_value}' in column '{route_filter_col}'")
            if filtered_by_date.empty:
                print("DEBUG: Skipping route filtering as date filtering yielded no results.")
                return {}, {}, pd.DataFrame(columns=headers)

            if route_filter_col not in filtered_by_date.columns:
                print(f"ERROR: Route column '{route_filter_col}' not found in the date-filtered DataFrame. Columns: {filtered_by_date.columns.tolist()}")
                return None, None, None

            route_value_stripped = str(route_value).strip()
            # Ensure comparison column is treated as string and stripped
            final_filtered = filtered_by_date[
                filtered_by_date[route_filter_col].astype(str).str.strip() == route_value_stripped
            ].copy()
            print(f"DEBUG: Shape after route filter: {final_filtered.shape}")

        except Exception as route_err:
            print(f"ERROR filtering by route column '{route_filter_col}': {route_err}")
            return None, None, None

        # --- Assign final filtered DataFrame for processing ---
        filtered_rasdf = final_filtered
        if filtered_rasdf.empty:
            print(f"INFO: No RAS data found for route {route_value} matching date criteria {repr(date_filter_value)} in sheet {sheet_name} after ALL filters.")
            return {}, {}, pd.DataFrame(columns=headers)

        print(f"DEBUG: Found {len(filtered_rasdf)} row(s) after all filters.")

        try:

            for _, row_series in filtered_rasdf.iterrows():
                 # Check if expected columns exist first
                if not all(col in row_series.index for col in [route_filter_col, 'Trip Type', 'Vehicle#']):
                     print(f"WARN: Skipping row due to missing columns. Index: {_}, Columns: {row_series.index.tolist()}")
                     continue

                route = str(row_series[route_filter_col]).strip()
                am_pm = str(row_series['Trip Type']).strip().upper()
                vehicle_number = str(row_series['Vehicle#']).strip()

                if vehicle_number and vehicle_number.lower() not in ('nan', '', 'none', '#n/a'):
                    if isinstance(vehicle_number, str) and vehicle_number.endswith('.0'): # Handle '1234.0'
                         vehicle_number = vehicle_number[:-2]

                    if len(vehicle_number) == 3 and vehicle_number.isdigit():
                        vehicle_number = '0' + vehicle_number

                    vehicle_full = 'NT' + vehicle_number # Assuming NT prefix is correct

                    if route: # Don't add if route is empty
                       if am_pm == "AM":
                           am_routes_to_buses[route] = vehicle_full
                       elif am_pm == "PM":
                           pm_routes_to_buses[route] = vehicle_full
                    else:
                        print(f"WARN: Skipping row due to empty route. Index: {_}")

        except (KeyError, ValueError, IndexError) as proc_key_err: # Catch potential errors more specifically
             print(f"ERROR: Problem accessing columns during vehicle processing: {proc_key_err}. Check headers/data.")
             # Depending on severity, maybe return partial data or None
             return None, None, None
        except Exception as proc_err:
             print(f"ERROR: Unexpected error during vehicle processing: {proc_err}")
             traceback.print_exc()
             return None, None, None


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
        traceback.print_exc()
        return None, None, None

# --- End of _get_ras_data_from_sheet ---
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
