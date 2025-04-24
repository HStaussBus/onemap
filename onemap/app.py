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
import data_sources
import processing
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
mapbox_token = config.MAPBOX_TOKEN

try:
    geotab_client = auth_clients.initialize_geotab_client()
    gspread_client = auth_clients.get_gspread_client()
    drive_service = auth_clients.get_drive_service()
    if not mapbox_token:
        print("WARN: MAPBOX_TOKEN not found.")
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

# --- MODIFIED Helper to process PRELOADED RAS Data ---
# Added driver name/phone extraction and modified return value
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
        date_filter_col = 'DateID'; date_filter_value = date_obj.strftime("%m/%d/%Y")
        name_col = 'Name'; phone_col = 'Phone'
        print(f"DEBUG Preload Filter: Historical. Filter: Col='{date_filter_col}', Val='{date_filter_value}'. Driver Cols: '{name_col}', '{phone_col}'")

    # --- Date Filtering ---
    try:
        if date_filter_col not in rasdf.columns:
            print(f"ERROR Preload Filter: Date column '{date_filter_col}' not found in input DataFrame.")
            filtered_rasdf = pd.DataFrame() # Set to empty
        elif is_current_week:
             filtered_rasdf = rasdf[rasdf[date_filter_col].astype(str).str.strip() == date_filter_value].copy()
        elif is_current_week:
            # ... (keep current week logic as is) ...
            filtered_rasdf = rasdf[rasdf[date_filter_col].astype(str).str.strip() == date_filter_value].copy()
        else: # Historical
            date_filter_col = 'DateID'
            target_date_obj = date_obj # The date object from user input '%Y-%m-%d'
            print(f"DEBUG Preload Filter (Hist): Applying robust date filtering for target: {target_date_obj}")

            if date_filter_col not in rasdf.columns:
                print(f"ERROR Preload Filter (Hist): Date column '{date_filter_col}' not found.")
                filtered_rasdf = pd.DataFrame()
            else:
                try:
                    # --- Robust Date Conversion and Comparison ---
                    # Work on a copy to avoid modifying the global historical_ras_df implicitly
                    df_to_filter = rasdf.copy()

                    # Convert the DateID column to datetime objects
                    # 'coerce' turns unparseable dates into NaT (Not a Time)
                    df_to_filter['parsed_date'] = pd.to_datetime(df_to_filter[date_filter_col], errors='coerce')

                    # Warn about and remove rows that couldn't be parsed
                    original_count = len(df_to_filter)
                    df_to_filter.dropna(subset=['parsed_date'], inplace=True)
                    dropped_count = original_count - len(df_to_filter)
                    if dropped_count > 0:
                        print(f"WARN Preload Filter (Hist): Dropped {dropped_count} rows due to unparseable dates in '{date_filter_col}'.")

                    if df_to_filter.empty:
                        print("INFO Preload Filter (Hist): No valid dates found after parsing.")
                        filtered_rasdf = pd.DataFrame()
                    else:
                        # Compare the DATE PART ONLY of the parsed dates with the target date object
                        matching_indices = df_to_filter[df_to_filter['parsed_date'].dt.date == target_date_obj].index
                        # Select rows from the original DataFrame using the matching indices
                        filtered_rasdf = rasdf.loc[matching_indices].copy() # Use original rasdf to keep all original columns
                        print(f"DEBUG Preload Filter (Hist): Found {len(filtered_rasdf)} rows matching date {target_date_obj}.")
                    # --- End Robust Date Conversion ---

                except Exception as e:
                    print(f"ERROR Preload Filter (Hist): Date processing/filtering failed: {e}")
                    traceback.print_exc()
                    filtered_rasdf = pd.DataFrame() # Ensure empty on error
    except Exception as e:
        print(f"ERROR Preload Filter (Hist): Date processing/filtering failed: {e}")
        traceback.print_exc()
        filtered_rasdf = pd.DataFrame() # Ensure empty on error

    # --- Route Filtering ---
    route_filter_col = 'Route'
    if filtered_rasdf.empty:
        print("DEBUG Preload Filter: DataFrame empty after date filter.")
        # Update default return with the (empty) filtered_rasdf before returning
        default_return['filtered_data'] = filtered_rasdf
        return default_return

    try:
        if route_filter_col not in filtered_rasdf.columns:
            print(f"ERROR Preload Filter: Route column '{route_filter_col}' not found.")
            final_filtered = pd.DataFrame() # Ensure empty
        else:
            route_value_stripped = str(route_input).strip()
            final_filtered = filtered_rasdf[filtered_rasdf[route_filter_col].astype(str).str.strip() == route_value_stripped].copy()
            print(f"DEBUG Preload Filter: Shape after route filter: {final_filtered.shape}")
    except Exception as e:
        print(f"ERROR Preload Filter: Filtering by route failed: {e}")
        final_filtered = pd.DataFrame() # Ensure empty on error

    # --- Extract Driver Info and Process Vehicles from final_filtered ---
    if final_filtered.empty:
        print("INFO Preload Filter: DataFrame empty after route filter.")
    else:
        # Extract driver info from the first matching row
        first_row = final_filtered.iloc[0]
        if name_col in first_row.index and pd.notna(first_row[name_col]):
            driver_name = str(first_row[name_col]).strip()
            if not driver_name or driver_name.lower() == 'nan': driver_name = None # Use None for easier checks later
        else:
            print(f"WARN Preload Filter: Driver name column '{name_col}' not found or is NA.")

        if phone_col in first_row.index and pd.notna(first_row[phone_col]):
            driver_phone = str(first_row[phone_col]).strip()
            if not driver_phone or driver_phone.lower() == 'nan': driver_phone = None # Use None for easier checks later
        else:
            print(f"WARN Preload Filter: Driver phone column '{phone_col}' not found or is NA.")

        print(f"DEBUG Preload Filter: Extracted Driver Name: '{driver_name}', Phone: '{driver_phone}'")

        # Process Filtered Rows for Vehicle Info (using final_filtered)
        try:
            required_proc_cols = [route_filter_col, 'Trip Type', 'Vehicle#']
            if not all(col in final_filtered.columns for col in required_proc_cols):
                print(f"WARN Preload Filter: Missing columns for vehicle processing.")
            else:
                for _, row_series in final_filtered.iterrows():
                     route = str(row_series[route_filter_col]).strip(); am_pm = str(row_series['Trip Type']).strip().upper(); vehicle_number = str(row_series['Vehicle#']).strip()
                     if not vehicle_number or vehicle_number.lower() in ('nan', '', 'none', '#n/a'): continue
                     if isinstance(vehicle_number, str) and vehicle_number.endswith('.0'): vehicle_number = vehicle_number[:-2]
                     if len(vehicle_number) == 3 and vehicle_number.isdigit(): vehicle_number = '0' + vehicle_number
                     vehicle_full = 'NT' + vehicle_number if (len(vehicle_number) == 4 and vehicle_number.isdigit()) else vehicle_number
                     if not route: continue
                     if am_pm == "AM": am_routes_to_buses[route] = vehicle_full
                     elif am_pm == "PM": pm_routes_to_buses[route] = vehicle_full
        except Exception as proc_err:
            print(f"ERROR Preload Filter: During vehicle processing: {proc_err}"); traceback.print_exc()

    # --- Return Updated Structure ---
    print(f"DEBUG Preload Filter: Returning AM:{am_routes_to_buses}, PM:{pm_routes_to_buses}, Name:{driver_name}, Phone:{driver_phone}")
    return {
        'am_buses': am_routes_to_buses,
        'pm_buses': pm_routes_to_buses,
        'filtered_data': final_filtered, # Return the final filtered df
        'driver_name': driver_name,      # Return extracted info
        'driver_phone': driver_phone     # Return extracted info
    }


# --- Flask Routes ---
@app.route('/')
def index():
    """Serves the main HTML page, passing the Mapbox token."""
    return render_template('index.html', mapbox_token=mapbox_token)


# --- MODIFIED /get_map Route ---
@app.route('/get_map', methods=['POST'])
def get_map_data():
    """ API endpoint using preloaded RAS data. """
    if not geotab_client: return jsonify({"error": "Server configuration error: Geotab client not ready."}), 503
    start_time = datetime.datetime.now(); print(f"\n--- Received /get_map request at {start_time} ---")

    # Initialize all variables that will be populated
    dvi_webview_link = None; optdf_json = [];
    am_routes_to_buses = {}; pm_routes_to_buses = {}
    am_bus_number = None; pm_bus_number = None; am_device_id = None; pm_device_id = None
    am_route_coordinates_list = []; pm_route_coordinates_list = []
    am_stops_list = []; pm_stops_list = []
    driver_name = "N/A" # Default response value
    driver_phone = "N/A"# Default response value
    rasdf_filtered_for_request = pd.DataFrame() # Define before try block
    depot = None # Define before try block

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
            # --- Call updated function and unpack results ---
            ras_results = get_vehicles_from_preloaded_ras(ras_df_to_filter, date_obj, route_input)
            am_routes_to_buses = ras_results.get('am_buses', {})
            pm_routes_to_buses = ras_results.get('pm_buses', {})
            rasdf_filtered_for_request = ras_results.get('filtered_data', pd.DataFrame())
            # Get driver info, provide default 'N/A' if function returned None/empty
            driver_name = ras_results.get('driver_name') or "N/A"
            driver_phone = ras_results.get('driver_phone') or "N/A"
            # -------------------------------------------------

            # Determine Depot (using the filtered data returned by the function)
            if not rasdf_filtered_for_request.empty:
                 # Pass the already filtered DataFrame to add_depot_coords
                 rasdf_with_coords = processing.add_depot_coords(rasdf_filtered_for_request)
                 depot = get_depot_from_ras(rasdf_with_coords) # Use get_depot_from_ras defined in app.py
            else:
                print("INFO: No RAS data returned for route/date after filtering.")

        # --- Find DVI Link ---
        if depot: # Checks if depot was successfully determined
             print(f"Attempting to find DVI file for Depot: {depot}, Route: {route_input}, Date: {date_str_ymd}")
             try:
                 # Ensure drive_service was initialized successfully at startup
                 if drive_service:
                     file_info = data_sources.find_drive_file(drive_service, config.ROOT_FOLDER_ID, depot, date_str_ymd, route_input, config.DRIVE_ID)
                     if file_info and isinstance(file_info, dict) and 'webViewLink' in file_info:
                         dvi_webview_link = file_info['webViewLink']
                         print(f"DVI Link Found: {dvi_webview_link}")
                     else:
                          print("INFO: DVI file not found or info invalid.")
                 else:
                      print("WARN: Skipping DVI search because drive_service was not initialized.")
             except Exception as dvi_err:
                  print(f"ERROR searching for DVI file: {dvi_err}")
        else:
             print(f"INFO: Skipping DVI file search because depot could not be determined. (Depot value: '{depot}')")


        # 3. Fetch OPT Dump Data & Convert to JSON
        # ... (Keep existing OPT Dump logic) ...
        print("Fetching OPT Dump data...")
        optdf = data_sources.get_opt_dump_data(auth_clients.get_db_connection, route_input, date_obj)
        if optdf is None: optdf = pd.DataFrame(); optdf_json = []
        elif optdf.empty: optdf_json = []
        else:
             try:
                 print(f"DEBUG: Converting {len(optdf)} OPT rows to JSON...")
                 for col in ['sess_beg', 'sess_end']:
                     if col in optdf.columns: optdf[col] = pd.to_datetime(optdf[col], errors='coerce').dt.strftime('%H:%M:%S').fillna('')
                 optdf_serializable = optdf.astype(str).replace({'nan': '', 'NaT': '', '<NA>': ''}).fillna('')
                 optdf_json = optdf_serializable.to_dict(orient='records')
                 print("DEBUG: OPT DataFrame successfully converted to JSON list.")
             except Exception as json_err: print(f"ERROR: Failed converting OPT DataFrame to JSON: {json_err}"); traceback.print_exc(); optdf_json = []


        # 4. Process OPT Dump for AM/PM Locations
        # ... (Keep existing processing.process_am_pm call) ...
        print("Processing OPT Dump data for maps...")
        am_locations_df, pm_locations_df = processing.process_am_pm(optdf, am_routes_to_buses, pm_routes_to_buses)


        # 5. Prepare Time Inputs for GPS Fetching
        # ... (Keep existing time logic) ...
        am_start_hour, am_start_minute = 10, 0; am_end_hour, am_end_minute = 16, 0
        pm_start_hour, pm_start_minute = 16, 0; pm_end_hour, pm_end_minute = 23, 59


        # 6. Process AM Data
        # ... (Keep existing AM processing logic) ...
        print("Processing AM Data...")
        if isinstance(am_locations_df, pd.DataFrame) and not am_locations_df.empty:
             try:
                 am_row = am_locations_df.iloc[0]; am_bus_number = am_row.get(f"am_Vehicle#")
                 if am_bus_number:
                     am_start_dt = datetime.datetime.combine(date_obj, datetime.time(am_start_hour, am_start_minute)); am_end_dt = datetime.datetime.combine(date_obj, datetime.time(am_end_hour, am_end_minute))
                     am_vehicle_data_df, temp_am_device_id = data_sources.fetch_bus_data(geotab_client, am_bus_number, am_start_dt, am_end_dt)
                     am_device_id = temp_am_device_id
                     am_route_coordinates_list = processing.format_gps_trace(am_vehicle_data_df)
                     am_stops_list = processing.format_stops(am_locations_df, "am_")
                     print(f"AM Data Processed. Trace points: {len(am_route_coordinates_list)}, Stops: {len(am_stops_list)}, DeviceID: {am_device_id}")
                 else: print("No AM vehicle number found in processed OPT data.")
             except Exception as am_err: print(f"ERROR: AM data processing failed: {am_err}"); traceback.print_exc()
        else: print("INFO: No processed AM location data available.")


        # 7. Process PM Data
        # ... (Keep existing PM processing logic) ...
        print("Processing PM Data...")
        if isinstance(pm_locations_df, pd.DataFrame) and not pm_locations_df.empty:
             try:
                 pm_row = pm_locations_df.iloc[0]; pm_bus_number = pm_row.get(f"pm_Vehicle#")
                 if pm_bus_number:
                     pm_start_dt = datetime.datetime.combine(date_obj, datetime.time(pm_start_hour, pm_start_minute)); pm_end_dt = datetime.datetime.combine(date_obj, datetime.time(pm_end_hour, pm_end_minute))
                     pm_vehicle_data_df, temp_pm_device_id = data_sources.fetch_bus_data(geotab_client, pm_bus_number, pm_start_dt, pm_end_dt)
                     pm_device_id = temp_pm_device_id
                     pm_route_coordinates_list = processing.format_gps_trace(pm_vehicle_data_df)
                     pm_stops_list = processing.format_stops(pm_locations_df, "pm_")
                     print(f"PM Data Processed. Trace points: {len(pm_route_coordinates_list)}, Stops: {len(pm_stops_list)}, DeviceID: {pm_device_id}")
                 else: print("No PM vehicle number found in processed OPT data.")
             except Exception as pm_err: print(f"ERROR: PM data processing failed: {pm_err}"); traceback.print_exc()
        else: print("INFO: No processed PM location data available.")


        # --- 8. Return JSON Results ---
        end_time = datetime.datetime.now(); duration = end_time - start_time
        print(f"--- Request completed in {duration.total_seconds():.2f} seconds ---")
        return jsonify({
            "am_map_data": {"vehicle_number": am_bus_number, "device_id": am_device_id, "trace": am_route_coordinates_list, "stops": am_stops_list },
            "pm_map_data": {"vehicle_number": pm_bus_number, "device_id": pm_device_id, "trace": pm_route_coordinates_list, "stops": pm_stops_list },
            "dvi_link": dvi_webview_link or "#",
            "opt_data": optdf_json,
            # --- ADDED DRIVER INFO ---
            "driver_name": driver_name,
            "driver_phone": driver_phone
            # -------------------------
        })

    # --- Top-Level Error Handling ---
    except Exception as e:
        print(f"ERROR: Unhandled exception in /get_map: {e}")
        print(traceback.format_exc())
        # Return driver info defaults even on top-level error
        return jsonify({
            "error": f"An unexpected server error occurred: {e}",
            "opt_data": [],
            "am_map_data": {}, "pm_map_data": {}, "dvi_link": "#",
            "driver_name": "N/A", "driver_phone": "N/A"
             }), 500


if __name__ == '__main__':
    if not gspread_client: print("FATAL: GSpread client failed initialization. Cannot run.")
    elif not geotab_client: print("FATAL: Geotab client failed initialization. Cannot run.")
    else:
        print("Starting Flask server...")
        # Make sure use_reloader is False if running background scheduler this way
        app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)