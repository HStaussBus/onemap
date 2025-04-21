# data_sources.py
import pandas as pd
import datetime
import pytz
import psycopg2
import psycopg2.extras
import traceback
from mygeotab.exceptions import MyGeotabException # Be specific if possible
import config # To get Sheet IDs etc.

# --- Geotab Data ---
def fetch_bus_data(api_client, bus_number, from_date, to_date):
    """Fetches bus data and ensures proper timezone handling."""
    # (Keep original implementation, but ensure api_client is passed)
    if not api_client:
         print("ERROR: Geotab API client not provided.")
         return pd.DataFrame()

    # Convert naive datetime to UTC timezone-aware datetime
    utc = pytz.UTC
    if from_date.tzinfo is None: from_date = utc.localize(from_date)
    else: from_date = from_date.astimezone(utc) # Ensure it's UTC

    if to_date.tzinfo is None: to_date = utc.localize(to_date)
    else: to_date = to_date.astimezone(utc) # Ensure it's UTC

    try:
        # Fetch the device information
        device_info = api_client.call("Get", typeName="Device", search={"name": bus_number})
        if not device_info:
            print(f"WARNING: Device not found for bus number: {bus_number}")
            return pd.DataFrame()

        device_id = device_info[0]["id"]

        # Fetch log records
        log_records = api_client.get("LogRecord", search={"deviceSearch": {"id": device_id}, "fromDate": from_date, "toDate": to_date})

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
def _get_ras_data_from_sheet(gspread_client, sheet_id, sheet_name, date_filter_col, date_filter_value, route_filter_col, route_value):
    """Helper to get RAS data from a specific sheet."""
    if not gspread_client:
        print("ERROR: GSpread client not provided.")
        return None, None, pd.DataFrame()

    try:
        rassheet = gspread_client.open_by_key(sheet_id)
        rasworksheet = rassheet.worksheet(sheet_name)
        all_data = rasworksheet.get_all_values()

        if not all_data: return None, None, pd.DataFrame()

        headers = all_data[0]
        data = all_data[1:]
        rasdf = pd.DataFrame(data, columns=headers)

        # Filter based on provided criteria
        # Ensure types match for filtering if necessary (e.g. dates)
        filtered_rasdf = rasdf[rasdf[date_filter_col] == date_filter_value]
        filtered_rasdf = filtered_rasdf[filtered_rasdf[route_filter_col] == route_value]

        if filtered_rasdf.empty:
             print(f"INFO: No RAS data found for route {route_value} on date {date_filter_value} in sheet {sheet_name}")
             return {}, {}, pd.DataFrame(columns=headers) # Return empty structures

        # Process AM/PM vehicles (Indices based on original code: AM/PM=4, Route=3, Vehicle#=13)
        am_routes_to_buses = {}
        pm_routes_to_buses = {}
        for _, row in filtered_rasdf.iterrows():
            route = row[headers[3]] # Use header name if available, otherwise index
            am_pm = row[headers[4]]
            vehicle_number = str(row[headers[13]]).strip()

            if vehicle_number and vehicle_number != 'nan': # Check for empty/NaN strings
                if len(vehicle_number) == 3: vehicle_number = '0' + vehicle_number
                vehicle_full = 'NT' + vehicle_number

                if am_pm == "AM":
                    am_routes_to_buses[route] = vehicle_full
                elif am_pm == "PM":
                    pm_routes_to_buses[route] = vehicle_full

        return am_routes_to_buses, pm_routes_to_buses, filtered_rasdf

    except gspread.exceptions.APIError as e:
        print(f"ERROR: Google Sheets API error accessing sheet ID {sheet_id}: {e}")
        return None, None, None
    except Exception as e:
        print(f"ERROR: Unexpected error processing RAS sheet {sheet_name}: {e}")
        return None, None, None


def get_current_ras_data(gspread_client, input_date_str_mmddyyyy, route):
    """Fetches current RAS data."""
    # Date format in current sheet is "Weekday- Day" (e.g., "Monday- 14")
    try:
        input_date = datetime.datetime.strptime(input_date_str_mmddyyyy, "%m/%d/%Y")
        formatted_date = input_date.strftime("%A- %-d") # Linux/Mac: %-d; Windows: %#d
    except ValueError:
        print(f"ERROR: Invalid date format for current RAS: {input_date_str_mmddyyyy}")
        return None, None, None

    return _get_ras_data_from_sheet(
        gspread_client,
        config.CURRENT_RAS_SHEET_ID,
        "Week Sheet",
        date_filter_col='Date', # Assuming column name is 'Date'
        date_filter_value=formatted_date,
        route_filter_col='Route', # Assuming column name is 'Route'
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


# --- Google Drive File Finder ---
def find_drive_file_info(drive_service, root_folder_id, depot, date_str, route, drive_id):
    """Finds a specific file in Google Drive."""
    # (Keep original implementation, ensure drive_service is passed)
    if not drive_service:
        print("ERROR: Google Drive service not provided.")
        return None
    try:
        # ... (original folder traversal and file search logic) ...
        # Example call within the function:
        # results = drive_service.files().list(...).execute()
        # ...
        # Parse date info
        date_obj = datetime.datetime.strptime(date_str, "%Y-%m-%d") # Ensure date_str is YYYY-MM-DD
        year_month = date_obj.strftime("%Y-%m")
        day_folder = date_obj.strftime("%Y-%m-%d")
        filename_prefix = date_obj.strftime("%d%m%y") # e.g. 050624

        # Helper to get subfolder ID inside a parent folder (copied from original)
        def get_folder_id(service, parent_id, name, drive_id):
           # ... (original helper code) ...
            query = (
                f"'{parent_id}' in parents and "
                f"name = '{name}' and mimeType = 'application/vnd.google-apps.folder'"
            )
            results = service.files().list(
                q=query, fields="files(id, name)", corpora="drive", driveId=drive_id,
                includeItemsFromAllDrives=True, supportsAllDrives=True
            ).execute()
            folders = results.get('files', [])
            return folders[0]['id'] if folders else None

        # Traverse folder tree (copied from original)
        depot_name = depot.upper()
        depot_id = get_folder_id(drive_service, root_folder_id, depot_name, drive_id)
        if not depot_id: raise FileNotFoundError(f"Depot folder '{depot_name}' not found.")
        # ... (rest of traversal logic from original) ...
        month_id = get_folder_id(drive_service, depot_id, year_month, drive_id)
        if not month_id: raise FileNotFoundError(f"Month folder '{year_month}' not found.")
        date_id = get_folder_id(drive_service, month_id, day_folder, drive_id)
        if not date_id: raise FileNotFoundError(f"Date folder '{day_folder}' not found.")


        # Search for PDF file (copied from original)
        query = (
            f"'{date_id}' in parents and "
            f"mimeType = 'application/pdf' and "
            f"name contains '{route.upper()}'" # Consider case sensitivity if needed
        )
        result = drive_service.files().list(
            q=query, fields="files(id, name, webViewLink)", corpora="drive", driveId=drive_id,
            includeItemsFromAllDrives=True, supportsAllDrives=True
        ).execute()
        files = result.get('files', [])

        if not files: raise FileNotFoundError(f"No PDF file found for route '{route}' on {date_str}.")

        print(f"INFO: Found Drive file: {files[0]['name']}")
        return files[0] # return first matching file info dict

    except FileNotFoundError as e:
        print(f"INFO: Drive file not found: {e}")
        return None
    except Exception as e:
        print(f"ERROR: Unexpected error finding Drive file: {e}")
        return None