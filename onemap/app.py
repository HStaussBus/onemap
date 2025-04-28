# app.py
from flask import Flask, render_template, request, jsonify
import datetime
import traceback
import pandas as pd
import threading
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import config
import auth_clients
import data_sources # Assuming this now contains fetch_safety_exceptions
import processing   # Assuming this now contains annotate_log_records_with_exceptions
import os
import platform
import re

app = Flask(__name__)

# --- Global Variables for Preloaded Data ---
current_ras_df = pd.DataFrame()
historical_ras_df = pd.DataFrame()
ras_data_lock = threading.Lock()

# --- Initialize Clients ---
print("INFO: Initializing clients...")
geotab_client = None
gspread_client = None
drive_service = None
# Make sure MAPBOX_TOKEN is loaded correctly from your config
mapbox_token = getattr(config, 'MAPBOX_TOKEN', None) # Use getattr for safety

try:
    geotab_client = auth_clients.initialize_geotab_client()
    gspread_client = auth_clients.get_gspread_client()
    drive_service = auth_clients.get_drive_service()
    if not mapbox_token:
        print("WARN: MAPBOX_TOKEN not found in config.")
    if not gspread_client:
        print("WARN: GSpread client failed to initialize. RAS preloading will fail.")
    if not geotab_client:
        print("WARN: Geotab client failed to initialize.")
    if not drive_service:
        print("WARN: drive_service failed to initialize.")
    print("INFO: Client initialization attempt complete.")
except Exception as startup_error:
    print(f"FATAL: Error during client initialization: {startup_error}")
# --- End Client Initialization ---


# --- RAS Preloading and Updating Functions ---
# fetch_and_cache_current_ras remains unchanged
def fetch_and_cache_current_ras():
    global current_ras_df
    print(f"INFO: Background task started: Fetching CURRENT RAS data at {datetime.datetime.now()}")
    if not gspread_client: print("ERROR (Background): GSpread client not available."); return
    try:
        print(f"INFO (Background): Accessing CURRENT RAS sheet: {config.CURRENT_RAS_SHEET_ID} / Week Sheet")
        rassheet = gspread_client.open_by_key(config.CURRENT_RAS_SHEET_ID)
        rasworksheet = rassheet.worksheet("Week Sheet")
        all_data = rasworksheet.get_all_values()
        if not all_data or len(all_data) < 1: temp_df = pd.DataFrame()
        else: headers = all_data[0]; data = all_data[1:]; temp_df = pd.DataFrame(data, columns=headers); temp_df = temp_df.astype(str).replace(['None', '', '#N/A', 'nan', 'NaT'], pd.NA)
        with ras_data_lock: current_ras_df = temp_df
        print(f"INFO (Background): Updated CURRENT RAS cache ({len(temp_df)} rows) at {datetime.datetime.now()}")
    except Exception as e: print(f"ERROR (Background): Failed to fetch/cache current RAS data: {e}"); traceback.print_exc()

# HISTORICAL_COLS_TO_KEEP remains unchanged
HISTORICAL_COLS_TO_KEEP = [
    'Route',
    'Date',
    'DateID',
    'Vehicle#',
    'Trip Type',
    'GM | Yard',
    'Active/Inactive',
    'Name',
    'Phone'
]

# fetch_and_cache_historical_ras remains unchanged
def fetch_and_cache_historical_ras():
    global historical_ras_df
    print(f"INFO: Initial load started: Fetching HISTORICAL RAS data at {datetime.datetime.now()}")
    if not gspread_client: print("ERROR (Initial Load): GSpread client not available."); return
    try:
        print(f"INFO (Initial Load): Accessing HISTORICAL RAS sheet: {config.HISTORICAL_RAS_SHEET_ID} / Archived_RAS")
        rassheet = gspread_client.open_by_key(config.HISTORICAL_RAS_SHEET_ID)
        rasworksheet = rassheet.worksheet("Archived_RAS")
        all_data = rasworksheet.get_all_values() # Fetches everything initially

        if not all_data or len(all_data) < 1:
             temp_df = pd.DataFrame()
        else:
            headers = all_data[0]
            data = all_data[1:]
            temp_df = pd.DataFrame(data, columns=headers) # Create full DataFrame

            # --- OPTIMIZATION: Keep only necessary columns ---
            missing_cols = [col for col in HISTORICAL_COLS_TO_KEEP if col not in temp_df.columns]
            if missing_cols:
                print(f"WARNING (Initial Load): Required historical columns not found in sheet: {missing_cols}. Skipping column selection.")
            else:
                print(f"INFO (Initial Load): Selecting required columns: {HISTORICAL_COLS_TO_KEEP}")
                temp_df = temp_df[HISTORICAL_COLS_TO_KEEP] # Reassign temp_df to only include needed columns
            # --- End Optimization ---

            temp_df = temp_df.astype(str).replace(['None', '', '#N/A', 'nan', 'NaT'], pd.NA)

            try:
                mem_usage_mb = temp_df.memory_usage(deep=True).sum() / (1024**2)
                print(f"INFO (Initial Load): HISTORICAL DataFrame memory usage AFTER column selection: {mem_usage_mb:.2f} MB")
            except Exception as mem_err:
                print(f"ERROR: Could not calculate memory usage: {mem_err}")

        with ras_data_lock:
            historical_ras_df = temp_df
        print(f"INFO: Initial load finished: Updated HISTORICAL RAS cache ({len(temp_df)} rows, {len(temp_df.columns)} columns) at {datetime.datetime.now()}")

    except Exception as e:
        print(f"ERROR (Initial Load): Failed to fetch/cache historical RAS data: {e}");
        traceback.print_exc()

# --- Initialize Scheduler and Load Initial Data ---
# Scheduler setup remains unchanged
if gspread_client:
    fetch_and_cache_historical_ras()
    fetch_and_cache_current_ras()
    scheduler = BackgroundScheduler(daemon=True); scheduler.add_job(fetch_and_cache_current_ras, 'interval', minutes=5); scheduler.start()
    print("INFO: APScheduler started for background RAS updates.")
    atexit.register(lambda: scheduler.shutdown())
else: print("ERROR: GSpread client not initialized. Skipping RAS preloading and scheduling.")


# --- Helper Function to Get Depot ---
# get_depot_from_ras remains unchanged
def get_depot_from_ras(ras_df):
    if ras_df is None or ras_df.empty: return None
    yard_col = None; yard_string = None
    if "Assigned Pullout Yard" in ras_df.columns: yard_col = "Assigned Pullout Yard"
    elif "GM | Yard" in ras_df.columns: yard_col = "GM | Yard"
    if not yard_col: return None
    try:
        if not ras_df.empty: yard_string = ras_df[yard_col].iloc[0]
        else: return None
    except IndexError: return None
    if pd.isna(yard_string) or yard_string == '': return None
    yard_string_lower = str(yard_string).lower().strip()
    for depot_name in config.DEPOT_LOCS.keys():
        if depot_name.lower() in yard_string_lower: return depot_name
    return None

# --- Helper to process PRELOADED RAS Data ---
# get_vehicles_from_preloaded_ras remains unchanged from previous version
def get_vehicles_from_preloaded_ras(rasdf, date_obj, route_input):
    am_routes_to_buses = {}
    pm_routes_to_buses = {}
    filtered_rasdf = pd.DataFrame() # Initialize filtered_rasdf here
    final_filtered = pd.DataFrame() # Initialize final_filtered as well
    driver_name = None  # <-- Initialize driver info
    driver_phone = None # <-- Initialize driver info

    # Default return structure
    default_return = {
        'am_buses': am_routes_to_buses, 'pm_buses': pm_routes_to_buses,
        'filtered_data': final_filtered, 'driver_name': driver_name, 'driver_phone': driver_phone
    }

    # Check if input DataFrame is valid
    if rasdf is None or rasdf.empty:
        print("WARN get_vehicles_from_preloaded_ras: Input rasdf is None or empty.")
        return default_return # type: ignore

    # --- Determine if current or historical ---
    today = datetime.date.today()
    current_monday_local = today - datetime.timedelta(days=today.weekday())
    is_current_week = (date_obj >= current_monday_local)

    # --- Define column names based on current/historical ---
    if is_current_week:
        date_filter_col = 'Date'; day_format = "%#d" if platform.system() == "Windows" else "%-d"; date_filter_value = date_obj.strftime(f"%A-{day_format}")
        name_col = 'Name'; phone_col = 'Phone'
        print(f"DEBUG Preload Filter: Current. Filter: Col='{date_filter_col}', Val='{date_filter_value}'. Driver Cols: '{name_col}', '{phone_col}'")
    else:
        date_filter_col = 'DateID'; date_filter_value = date_obj.strftime("%m/%d/%Y") # Keep original format for direct string match if needed
        name_col = 'Name'; phone_col = 'Phone'
        print(f"DEBUG Preload Filter: Historical. Filter: Col='{date_filter_col}', Val='{date_filter_value}'. Driver Cols: '{name_col}', '{phone_col}'")

    # --- Date Filtering ---
    try:
        if date_filter_col not in rasdf.columns:
            print(f"ERROR Preload Filter: Date column '{date_filter_col}' not found in input DataFrame.")
            filtered_rasdf = pd.DataFrame() # Set to empty
        elif is_current_week:
             # Current week filtering (remains the same)
             filtered_rasdf = rasdf[rasdf[date_filter_col].astype(str).str.strip() == date_filter_value].copy()
        else: # Historical
            # Historical filtering using robust date comparison (remains the same)
            target_date_obj = date_obj # The date object from user input '%Y-%m-%d'
            print(f"DEBUG Preload Filter (Hist): Applying robust date filtering for target: {target_date_obj}")

            if date_filter_col not in rasdf.columns:
                print(f"ERROR Preload Filter (Hist): Date column '{date_filter_col}' not found.")
                filtered_rasdf = pd.DataFrame()
            else:
                try:
                    df_to_filter = rasdf.copy()
                    # Ensure the date column is treated as string before conversion
                    df_to_filter[date_filter_col] = df_to_filter[date_filter_col].astype(str)
                    df_to_filter['parsed_date'] = pd.to_datetime(df_to_filter[date_filter_col], errors='coerce')
                    original_count = len(df_to_filter)
                    df_to_filter.dropna(subset=['parsed_date'], inplace=True)
                    dropped_count = original_count - len(df_to_filter)
                    if dropped_count > 0: print(f"WARN Preload Filter (Hist): Dropped {dropped_count} rows due to unparseable dates in '{date_filter_col}'.")

                    if df_to_filter.empty:
                        print("INFO Preload Filter (Hist): No valid dates found after parsing.")
                        filtered_rasdf = pd.DataFrame()
                    else:
                        # Ensure target_date_obj is a date object for comparison
                        if isinstance(target_date_obj, datetime.datetime):
                            target_date_obj = target_date_obj.date()
                        matching_indices = df_to_filter[df_to_filter['parsed_date'].dt.date == target_date_obj].index
                        filtered_rasdf = rasdf.loc[matching_indices].copy()
                        print(f"DEBUG Preload Filter (Hist): Found {len(filtered_rasdf)} rows matching date {target_date_obj}.")

                except Exception as e:
                    print(f"ERROR Preload Filter (Hist): Date processing/filtering failed: {e}"); traceback.print_exc(); filtered_rasdf = pd.DataFrame()
    except Exception as e:
        print(f"ERROR Preload Filter: Date filtering failed: {e}"); traceback.print_exc(); filtered_rasdf = pd.DataFrame()

    # --- Route Filtering ---
    # (remains the same)
    route_filter_col = 'Route'
    if filtered_rasdf.empty:
        print("DEBUG Preload Filter: DataFrame empty after date filter.")
        default_return['filtered_data'] = filtered_rasdf
        return default_return

    try:
        if route_filter_col not in filtered_rasdf.columns:
            print(f"ERROR Preload Filter: Route column '{route_filter_col}' not found.")
            final_filtered = pd.DataFrame()
        else:
            route_value_stripped = str(route_input).strip()
            # Ensure comparison is case-insensitive and handles potential whitespace
            final_filtered = filtered_rasdf[
                filtered_rasdf[route_filter_col].astype(str).str.strip().str.upper() == route_value_stripped.upper()
            ].copy()
            print(f"DEBUG Preload Filter: Shape after route filter for '{route_value_stripped}': {final_filtered.shape}")
    except Exception as e:
        print(f"ERROR Preload Filter: Filtering by route failed: {e}"); final_filtered = pd.DataFrame()

    # --- Extract Driver Info and Process Vehicles from final_filtered ---
    # (remains the same)
    if final_filtered.empty:
        print("INFO Preload Filter: DataFrame empty after route filter.")
    else:
        first_row = final_filtered.iloc[0]
        if name_col in first_row.index and pd.notna(first_row[name_col]):
            driver_name = str(first_row[name_col]).strip()
            if not driver_name or driver_name.lower() == 'nan': driver_name = None
        else: print(f"WARN Preload Filter: Driver name column '{name_col}' not found or is NA.")
        if phone_col in first_row.index and pd.notna(first_row[phone_col]):
            driver_phone = str(first_row[phone_col]).strip()
            # Basic phone number cleaning (optional)
            driver_phone = re.sub(r'\D', '', driver_phone) # Remove non-digits
            if not driver_phone or driver_phone.lower() == 'nan': driver_phone = None
        else: print(f"WARN Preload Filter: Driver phone column '{phone_col}' not found or is NA.")
        print(f"DEBUG Preload Filter: Extracted Driver Name: '{driver_name}', Phone: '{driver_phone}'")

        try:
            required_proc_cols = [route_filter_col, 'Trip Type', 'Vehicle#']
            if not all(col in final_filtered.columns for col in required_proc_cols):
                print(f"WARN Preload Filter: Missing columns for vehicle processing: {required_proc_cols}")
            else:
                for _, row_series in final_filtered.iterrows():
                     route = str(row_series[route_filter_col]).strip(); am_pm = str(row_series['Trip Type']).strip().upper(); vehicle_number = str(row_series['Vehicle#']).strip()
                     if not vehicle_number or vehicle_number.lower() in ('nan', '', 'none', '#n/a', 'na'): continue # Added 'na'
                     # Clean vehicle number more robustly
                     if isinstance(vehicle_number, str):
                         if vehicle_number.endswith('.0'): vehicle_number = vehicle_number[:-2]
                         vehicle_number = re.sub(r'\D', '', vehicle_number) # Remove non-digits
                         if len(vehicle_number) == 3 and vehicle_number.isdigit(): vehicle_number = '0' + vehicle_number
                         if len(vehicle_number) == 4 and vehicle_number.isdigit(): vehicle_full = 'NT' + vehicle_number
                         else: vehicle_full = vehicle_number # Fallback if format is unexpected
                     else: # Handle non-string case if necessary
                         vehicle_full = str(vehicle_number)

                     if not route: continue
                     if am_pm == "AM": am_routes_to_buses[route] = vehicle_full
                     elif am_pm == "PM": pm_routes_to_buses[route] = vehicle_full
        except Exception as proc_err: print(f"ERROR Preload Filter: During vehicle processing: {proc_err}"); traceback.print_exc()

    # --- Return Updated Structure ---
    print(f"DEBUG Preload Filter: Returning AM:{am_routes_to_buses}, PM:{pm_routes_to_buses}, Name:{driver_name}, Phone:{driver_phone}")
    return {
        'am_buses': am_routes_to_buses, 'pm_buses': pm_routes_to_buses,
        'filtered_data': final_filtered, 'driver_name': driver_name, 'driver_phone': driver_phone
    }


# --- Flask Routes ---
@app.route('/')
def index():
    """Serves the main HTML page, passing the Mapbox token."""
    # Ensure mapbox_token is passed correctly
    print(f"DEBUG: Passing mapbox_token to template: {'Yes' if mapbox_token else 'No'}")
    return render_template('index.html', mapbox_token=mapbox_token)


# --- /get_map Route ---
# Fetches initial map data (trace, stops, etc.) for AM and PM trips.
# Relies on preloaded RAS data and fetches GPS/OPT data.
@app.route('/get_map', methods=['POST'])
def get_map_data():
    """ API endpoint using preloaded RAS data to get initial map info. """
    if not geotab_client: return jsonify({"error": "Server configuration error: Geotab client not ready."}), 503
    start_time = datetime.datetime.now(); print(f"\n--- Received /get_map request at {start_time} ---")

    # Initialize all variables that will be populated
    dvi_webview_link = None; optdf_json = [];
    am_routes_to_buses = {}; pm_routes_to_buses = {}
    am_bus_number = None; pm_bus_number = None; am_device_id = None; pm_device_id = None
    # IMPORTANT: Decide on the format for trace data returned by this endpoint.
    # If JavaScript `displayMapData` expects {lat, lon, ts, spd}, use format_gps_trace_simple.
    # If JavaScript expects GeoJSON, use format_gps_trace_geojson.
    # Let's assume the original JS needs the simple format for now.
    am_route_data_list = []; pm_route_data_list = [] # Use simple format for original map
    am_stops_list = []; pm_stops_list = []
    driver_name = "N/A"; driver_phone = "N/A"
    rasdf_filtered_for_request = pd.DataFrame()
    depot = None

    try:
        # 1. Get Input Data
        data = request.get_json();
        if not data: return jsonify({"error": "Invalid request body. JSON expected."}), 400
        route_input = data.get('route'); date_str_ymd = data.get('date')
        if not route_input or not date_str_ymd: return jsonify({"error": "Missing route or date"}), 400
        try: date_obj = datetime.datetime.strptime(date_str_ymd, '%Y-%m-%d').date()
        except ValueError: return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
        print(f"Processing Route: {route_input}, Date: {date_str_ymd}")

        # 2. Access Preloaded RAS Data and Filter
        print("Accessing preloaded RAS data...")
        today = datetime.date.today(); current_monday = today - datetime.timedelta(days=today.weekday())
        use_current_ras = (date_obj >= current_monday)
        print(f"DEBUG: Using {'Current' if use_current_ras else 'Historical'} RAS data for filtering.")

        ras_df_to_filter = pd.DataFrame()
        with ras_data_lock:
            ras_df_to_filter = current_ras_df.copy() if use_current_ras else historical_ras_df.copy()

        if ras_df_to_filter.empty:
            print(f"WARN: Preloaded {'Current' if use_current_ras else 'Historical'} RAS data is empty.")
        else:
            ras_results = get_vehicles_from_preloaded_ras(ras_df_to_filter, date_obj, route_input)
            am_routes_to_buses = ras_results.get('am_buses', {})
            pm_routes_to_buses = ras_results.get('pm_buses', {})
            rasdf_filtered_for_request = ras_results.get('filtered_data', pd.DataFrame())
            driver_name = ras_results.get('driver_name') or "N/A"
            driver_phone = ras_results.get('driver_phone') or "N/A"

            if not rasdf_filtered_for_request.empty:
                 # Assuming add_depot_coords exists and works
                 if hasattr(processing, 'add_depot_coords'):
                     rasdf_with_coords = processing.add_depot_coords(rasdf_filtered_for_request)
                     depot = get_depot_from_ras(rasdf_with_coords)
                 else:
                     print("WARN: processing.add_depot_coords function not found.")
                     depot = get_depot_from_ras(rasdf_filtered_for_request) # Try without coords
            else: print("INFO: No RAS data returned for route/date after filtering.")

        # --- Find DVI Link ---
        if depot:
             print(f"Attempting to find DVI file for Depot: {depot}, Route: {route_input}, Date: {date_str_ymd}")
             try:
                 if drive_service and hasattr(data_sources, 'find_drive_file'):
                     # Ensure DRIVE_ID is loaded from config
                     drive_id = getattr(config, 'DRIVE_ID', None)
                     root_folder_id = getattr(config, 'ROOT_FOLDER_ID', None)
                     if drive_id and root_folder_id:
                         file_info = data_sources.find_drive_file(drive_service, root_folder_id, depot, date_str_ymd, route_input, drive_id)
                         if file_info and isinstance(file_info, dict) and 'webViewLink' in file_info:
                             dvi_webview_link = file_info['webViewLink']
                             print(f"DVI Link Found: {dvi_webview_link}")
                         else: print("INFO: DVI file not found or info invalid.")
                     else: print("WARN: DRIVE_ID or ROOT_FOLDER_ID missing in config.")
                 elif not drive_service: print("WARN: Skipping DVI search because drive_service was not initialized.")
                 else: print("WARN: Skipping DVI search because data_sources.find_drive_file function not found.")
             except Exception as dvi_err: print(f"ERROR searching for DVI file: {dvi_err}")
        else: print(f"INFO: Skipping DVI file search because depot could not be determined. (Depot value: '{depot}')")

        # 3. Fetch OPT Dump Data & Convert to JSON
        print("Fetching OPT Dump data...")
        # Ensure get_opt_dump_data exists
        if hasattr(data_sources, 'get_opt_dump_data') and hasattr(auth_clients, 'get_db_connection'):
            optdf = data_sources.get_opt_dump_data(auth_clients.get_db_connection, route_input, date_obj)
            if optdf is None: optdf = pd.DataFrame(); optdf_json = []
            elif optdf.empty: optdf_json = []
            else:
                 try:
                     print(f"DEBUG: Converting {len(optdf)} OPT rows to JSON...")
                     # Ensure columns exist before conversion
                     time_cols = [col for col in ['sess_beg', 'sess_end'] if col in optdf.columns]
                     for col in time_cols:
                         optdf[col] = pd.to_datetime(optdf[col], errors='coerce').dt.strftime('%H:%M:%S').fillna('')
                     optdf_serializable = optdf.astype(str).replace({'nan': '', 'NaT': '', '<NA>': '', 'None': ''}).fillna('') # Added None
                     optdf_json = optdf_serializable.to_dict(orient='records')
                     print("DEBUG: OPT DataFrame successfully converted to JSON list.")
                 except Exception as json_err: print(f"ERROR: Failed converting OPT DataFrame to JSON: {json_err}"); traceback.print_exc(); optdf_json = []
        else:
            print("WARN: Skipping OPT Dump data fetch: data_sources.get_opt_dump_data or auth_clients.get_db_connection not found.")
            optdf = pd.DataFrame(); optdf_json = []


        # 4. Process OPT Dump for AM/PM Locations
        print("Processing OPT Dump data for maps...")
        # Ensure process_am_pm exists
        if hasattr(processing, 'process_am_pm'):
             am_locations_df, pm_locations_df = processing.process_am_pm(optdf, am_routes_to_buses, pm_routes_to_buses)
        else:
             print("WARN: Skipping OPT processing: processing.process_am_pm function not found.")
             am_locations_df, pm_locations_df = pd.DataFrame(), pd.DataFrame()


        # 5. Prepare Time Inputs for GPS Fetching
        # Define default time windows (adjust as needed)
        am_start_hour, am_start_minute = 4, 0   # Example: 4:00 AM
        am_end_hour, am_end_minute = 12, 0      # Example: 12:00 PM (Noon)
        pm_start_hour, pm_start_minute = 12, 0  # Example: 12:00 PM (Noon)
        pm_end_hour, pm_end_minute = 20, 0      # Example: 8:00 PM

        # 6. Process AM Data (Fetch GPS Trace)
        print("Processing AM Data...")
        am_bus_number = am_routes_to_buses.get(route_input) # Get AM bus from filtered RAS
        if am_bus_number:
             try:
                 am_start_dt = datetime.datetime.combine(date_obj, datetime.time(am_start_hour, am_start_minute)); am_end_dt = datetime.datetime.combine(date_obj, datetime.time(am_end_hour, am_end_minute))
                 # Ensure fetch_bus_data exists
                 if hasattr(data_sources, 'fetch_bus_data'):
                     # Fetch GPS trace data using the bus number and time window
                     am_vehicle_data_df, temp_am_device_id = data_sources.fetch_bus_data(geotab_client, am_bus_number, am_start_dt, am_end_dt)
                     am_device_id = temp_am_device_id # Store the device ID

                     # Format trace data - USE THE SIMPLE FORMAT for original JS
                     if hasattr(processing, 'format_gps_trace_simple'): # Check for the simple formatter
                          am_route_data_list = processing.format_gps_trace_simple(am_vehicle_data_df)
                     elif hasattr(processing, 'format_gps_trace'): # Fallback to original if simple doesn't exist
                          # WARNING: This might return GeoJSON if format_gps_trace was updated!
                          print("WARN: Using processing.format_gps_trace for AM trace, ensure output format matches JS.")
                          am_route_data_list = processing.format_gps_trace(am_vehicle_data_df)
                     else:
                          print("ERROR: No suitable GPS trace formatting function found in processing.py.")
                          am_route_data_list = []

                     # Format stops from OPT data
                     if hasattr(processing, 'format_stops'):
                          am_stops_list = processing.format_stops(am_locations_df, "am_")
                     else:
                          print("WARN: processing.format_stops function not found.")
                          am_stops_list = []

                     print(f"AM Data Processed. Trace points: {len(am_route_data_list)}, Stops: {len(am_stops_list)}, DeviceID: {am_device_id}")
                 else:
                      print("WARN: Skipping AM GPS fetch: data_sources.fetch_bus_data function not found.")
             except Exception as am_err: print(f"ERROR: AM data processing failed: {am_err}"); traceback.print_exc()
        else: print("INFO: No AM vehicle number found in RAS data for this route/date.")


        # 7. Process PM Data (Fetch GPS Trace)
        print("Processing PM Data...")
        pm_bus_number = pm_routes_to_buses.get(route_input) # Get PM bus from filtered RAS
        if pm_bus_number:
             try:
                 pm_start_dt = datetime.datetime.combine(date_obj, datetime.time(pm_start_hour, pm_start_minute)); pm_end_dt = datetime.datetime.combine(date_obj, datetime.time(pm_end_hour, pm_end_minute))
                 # Ensure fetch_bus_data exists
                 if hasattr(data_sources, 'fetch_bus_data'):
                     # Fetch GPS trace data using the bus number and time window
                     pm_vehicle_data_df, temp_pm_device_id = data_sources.fetch_bus_data(geotab_client, pm_bus_number, pm_start_dt, pm_end_dt)
                     pm_device_id = temp_pm_device_id # Store the device ID

                     # Format trace data - USE THE SIMPLE FORMAT for original JS
                     if hasattr(processing, 'format_gps_trace_simple'): # Check for the simple formatter
                         pm_route_data_list = processing.format_gps_trace_simple(pm_vehicle_data_df)
                     elif hasattr(processing, 'format_gps_trace'): # Fallback to original if simple doesn't exist
                         # WARNING: This might return GeoJSON if format_gps_trace was updated!
                         print("WARN: Using processing.format_gps_trace for PM trace, ensure output format matches JS.")
                         pm_route_data_list = processing.format_gps_trace(pm_vehicle_data_df)
                     else:
                         print("ERROR: No suitable GPS trace formatting function found in processing.py.")
                         pm_route_data_list = []


                     # Format stops from OPT data
                     if hasattr(processing, 'format_stops'):
                          pm_stops_list = processing.format_stops(pm_locations_df, "pm_")
                     else:
                          print("WARN: processing.format_stops function not found.")
                          pm_stops_list = []

                     print(f"PM Data Processed. Trace points: {len(pm_route_data_list)}, Stops: {len(pm_stops_list)}, DeviceID: {pm_device_id}")
                 else:
                     print("WARN: Skipping PM GPS fetch: data_sources.fetch_bus_data function not found.")
             except Exception as pm_err: print(f"ERROR: PM data processing failed: {pm_err}"); traceback.print_exc()
        else: print("INFO: No PM vehicle number found in RAS data for this route/date.")

        # --- 8. Return JSON Results ---
        end_time = datetime.datetime.now(); duration = end_time - start_time
        print(f"--- /get_map request completed in {duration.total_seconds():.2f} seconds ---")
        # Return AM/PM traces (simple format), stops, device IDs, DVI link, OPT data, and driver info
        return jsonify({
            "am_map_data": {"vehicle_number": am_bus_number, "device_id": am_device_id, "trace": am_route_data_list, "stops": am_stops_list },
            "pm_map_data": {"vehicle_number": pm_bus_number, "device_id": pm_device_id, "trace": pm_route_data_list, "stops": pm_stops_list },
            "dvi_link": dvi_webview_link or "#",
            "opt_data": optdf_json,
            "driver_name": driver_name,
            "driver_phone": driver_phone
        })

    # --- Top-Level Error Handling ---
    except Exception as e:
        print(f"ERROR: Unhandled exception in /get_map: {e}")
        print(traceback.format_exc())
        return jsonify({
            "error": f"An unexpected server error occurred: {e}",
            "opt_data": [], "am_map_data": {}, "pm_map_data": {}, "dvi_link": "#",
            "driver_name": "N/A", "driver_phone": "N/A"
             }), 500

# ============================================================
# --- Safety Summary Endpoint ---
# ============================================================
@app.route('/get_safety_summary', methods=['POST'])
def get_safety_summary():
    """
    API endpoint to fetch safety exceptions and the corresponding log records (GPS trace)
    for a specific device and time period, then annotates the log records with exception info.
    Returns data in GeoJSON format suitable for the safety layer.
    """
    if not geotab_client:
        return jsonify({"error": "Server configuration error: Geotab client not ready."}), 503

    start_time = datetime.datetime.now()
    print(f"\n--- Received /get_safety_summary request at {start_time} ---")

    try:
        # 1. Get Input Data
        data = request.get_json()
        if not data:
            return jsonify({"error": "Invalid request body. JSON expected."}), 400

        device_id = data.get('device_id')
        date_str_ymd = data.get('date')
        time_period = data.get('time_period') # Expected: "AM", "PM", or "RoundTrip"
        vehicle_number = data.get('vehicle_number') # Needed to fetch correct log records

        # --- Input Validation ---
        errors = []
        if not device_id: errors.append("Missing device_id")
        if not date_str_ymd: errors.append("Missing date")
        if not time_period or time_period not in ["AM", "PM", "RoundTrip"]: errors.append("Missing or invalid time_period (must be AM, PM, or RoundTrip)")
        if not vehicle_number: errors.append("Missing vehicle_number")

        if errors:
             print(f"ERROR /get_safety_summary: Input validation failed: {', '.join(errors)}")
             return jsonify({"error": ", ".join(errors)}), 400

        # 2. Parse Date and Define Time Windows
        try:
            date_obj = datetime.datetime.strptime(date_str_ymd, '%Y-%m-%d').date()
        except ValueError:
            return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        print(f"Processing Safety Summary for Device: {device_id}, Vehicle: {vehicle_number}, Date: {date_str_ymd}, Period: {time_period}")

        # Define time windows (match those used in /get_map)
        am_start_hour, am_start_minute = 4, 0
        am_end_hour, am_end_minute = 12, 0
        pm_start_hour, pm_start_minute = 12, 0
        pm_end_hour, pm_end_minute = 20, 0

        if time_period == "AM":
            start_dt = datetime.datetime.combine(date_obj, datetime.time(am_start_hour, am_start_minute))
            end_dt = datetime.datetime.combine(date_obj, datetime.time(am_end_hour, am_end_minute))
        elif time_period == "PM":
            start_dt = datetime.datetime.combine(date_obj, datetime.time(pm_start_hour, pm_start_minute))
            end_dt = datetime.datetime.combine(date_obj, datetime.time(pm_end_hour, pm_end_minute))
        elif time_period == "RoundTrip":
            start_dt = datetime.datetime.combine(date_obj, datetime.time(am_start_hour, am_start_minute))
            end_dt = datetime.datetime.combine(date_obj, datetime.time(pm_end_hour, pm_end_minute))
        else: # Should not happen due to validation
            return jsonify({"error": "Internal server error: Invalid time_period processing."}), 500

        print(f"Fetching log records and safety exceptions from {start_dt} to {end_dt}")

        # 3. Fetch Log Records (GPS Trace) for the period
        log_records_df = pd.DataFrame()
        log_records_geojson = []
        try:
            # Ensure fetch_bus_data exists
            if not hasattr(data_sources, 'fetch_bus_data'):
                 print("ERROR: data_sources.fetch_bus_data function not found.")
                 return jsonify({"error": "Server configuration error: Log record source unavailable."}), 500

            log_records_df, _ = data_sources.fetch_bus_data(
                geotab_client,
                vehicle_number, # Use vehicle_number from request
                start_dt,
                end_dt
            )

            # Format log records as GeoJSON features - USE THE GEOJSON FORMATTER
            if hasattr(processing, 'format_gps_trace'): # Assuming format_gps_trace now outputs GeoJSON
                 log_records_geojson = processing.format_gps_trace(log_records_df)
                 print(f"Fetched and formatted {len(log_records_geojson)} log records for the period.")
            else:
                 print("ERROR: processing.format_gps_trace (GeoJSON version) function not found.")
                 return jsonify({"error": "Server configuration error: Log record processing unavailable."}), 500

        except Exception as log_fetch_err:
            print(f"ERROR fetching/formatting log records for safety summary: {log_fetch_err}")
            traceback.print_exc()
            return jsonify({"error": f"Failed to retrieve log records for safety summary: {log_fetch_err}"}), 500


        # 4. Fetch Safety Exceptions from Geotab
        raw_exceptions = []
        try:
            # Ensure fetch_safety_exceptions exists
            if not hasattr(data_sources, 'fetch_safety_exceptions'):
                 print("ERROR: 'fetch_safety_exceptions' function not found in data_sources.py")
                 return jsonify({"error": "Server configuration error: Safety data source unavailable."}), 500

            raw_exceptions = data_sources.fetch_safety_exceptions(
                geotab_client,
                device_id, # Use device_id from request
                start_dt,
                end_dt
            )
            print(f"Fetched {len(raw_exceptions)} raw safety exceptions.")

        except Exception as fetch_err:
            print(f"ERROR fetching safety exceptions: {fetch_err}")
            traceback.print_exc()
            # Allow continuing even if exceptions fail, will return unannotated trace
            # return jsonify({"error": f"Failed to retrieve safety data: {fetch_err}"}), 500


        # 5. Annotate Log Records with Exceptions
        annotated_log_records_geojson = []
        try:
            # Ensure annotate_log_records_with_exceptions exists
            if not hasattr(processing, 'annotate_log_records_with_exceptions'):
                 print("ERROR: 'annotate_log_records_with_exceptions' function not found in processing.py")
                 return jsonify({"error": "Server configuration error: Safety data processing unavailable."}), 500

            # Call the function to merge exceptions onto the log record features
            # Pass the GeoJSON formatted log records
            annotated_log_records_geojson = processing.annotate_log_records_with_exceptions(
                log_records_geojson, # The formatted GPS trace (GeoJSON)
                raw_exceptions       # The list of raw exception dicts
            )
            print(f"Annotation complete. Returning {len(annotated_log_records_geojson)} annotated log records.")

        except Exception as annotate_err:
            print(f"ERROR annotating log records with exceptions: {annotate_err}")
            print(traceback.format_exc())
            # If annotation fails, return the original unannotated trace (still GeoJSON)
            annotated_log_records_geojson = log_records_geojson
            # return jsonify({"error": f"Failed to process safety data: {annotate_err}"}), 500


        # 6. Return JSON Results
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        print(f"--- /get_safety_summary request completed in {duration.total_seconds():.2f} seconds ---")

        # Return the list of log record GeoJSON features, now potentially annotated
        return jsonify({
            "annotated_trace": annotated_log_records_geojson
        })

    # --- Top-Level Error Handling ---
    except Exception as e:
        print(f"ERROR: Unhandled exception in /get_safety_summary: {e}")
        print(traceback.format_exc())
        return jsonify({"error": f"An unexpected server error occurred: {e}"}), 500
# ============================================================
# --- End Safety Summary Endpoint ---
# ============================================================


if __name__ == '__main__':
    # Check essential clients before starting
    if not gspread_client: print("FATAL: GSpread client failed initialization. Cannot run.")
    elif not geotab_client: print("FATAL: Geotab client failed initialization. Cannot run.")
    else:
        print("Starting Flask server...")
        # Make sure use_reloader is False if running background scheduler this way
        # Bind to 0.0.0.0 to be accessible externally if needed
        # Set debug=False for production
        app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)

