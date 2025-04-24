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
# ... (Keep fetch_and_cache_current_ras and fetch_and_cache_historical_ras functions as they were) ...
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

HISTORICAL_COLS_TO_KEEP = [
    'Route',
    'DateID',
    'Vehicle#',
    'Trip Type', # Verify exact name
    'Status'     # Verify exact name
]

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
            # Ensure all required columns actually exist in the headers
            missing_cols = [col for col in HISTORICAL_COLS_TO_KEEP if col not in temp_df.columns]
            if missing_cols:
                print(f"WARNING (Initial Load): Required historical columns not found in sheet: {missing_cols}. Skipping column selection.")
            else:
                print(f"INFO (Initial Load): Selecting required columns: {HISTORICAL_COLS_TO_KEEP}")
                temp_df = temp_df[HISTORICAL_COLS_TO_KEEP] # Reassign temp_df to only include needed columns
            # --- End Optimization ---

            # Clean data (apply this AFTER selecting columns)
            temp_df = temp_df.astype(str).replace(['None', '', '#N/A', 'nan', 'NaT'], pd.NA)

            # Optional: Apply data type optimizations (categories, downcasting) here
            # on the smaller temp_df for further memory savings

            # Measure memory usage AFTER selecting columns
            try:
                mem_usage_mb = temp_df.memory_usage(deep=True).sum() / (1024**2)
                print(f"INFO (Initial Load): HISTORICAL DataFrame memory usage AFTER column selection: {mem_usage_mb:.2f} MB")
            except Exception as mem_err:
                print(f"ERROR: Could not calculate memory usage: {mem_err}")

        # Store the smaller DataFrame globally
        with ras_data_lock:
            historical_ras_df = temp_df

        print(f"INFO: Initial load finished: Updated HISTORICAL RAS cache ({len(temp_df)} rows, {len(temp_df.columns)} columns) at {datetime.datetime.now()}")

    except Exception as e:
        print(f"ERROR (Initial Load): Failed to fetch/cache historical RAS data: {e}");
        traceback.print_exc()

# --- Initialize Scheduler and Load Initial Data ---
# ... (Keep scheduler setup as is) ...
if gspread_client:
    fetch_and_cache_historical_ras()
    fetch_and_cache_current_ras()
    scheduler = BackgroundScheduler(daemon=True); scheduler.add_job(fetch_and_cache_current_ras, 'interval', minutes=5); scheduler.start()
    print("INFO: APScheduler started for background RAS updates.")
    atexit.register(lambda: scheduler.shutdown())
else: print("ERROR: GSpread client not initialized. Skipping RAS preloading and scheduling.")


# --- Helper Function to Get Depot ---
# ... (Keep get_depot_from_ras as is) ...
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
# ... (Keep get_vehicles_from_preloaded_ras as is, including the fix from the previous step) ...
def get_vehicles_from_preloaded_ras(rasdf, date_obj, route_input):
    am_routes_to_buses = {}; pm_routes_to_buses = {}; filtered_rasdf = pd.DataFrame()
    if rasdf is None or rasdf.empty: return am_routes_to_buses, pm_routes_to_buses, filtered_rasdf
    today = datetime.date.today(); current_monday_local = today - datetime.timedelta(days=today.weekday()); is_current_week = (date_obj >= current_monday_local)
    if is_current_week: date_filter_col = 'Date'; day_format = "%#d" if platform.system() == "Windows" else "%-d"; date_filter_value = date_obj.strftime(f"%A-{day_format}"); print(f"DEBUG Preload Filter: Current. Filter: Col='{date_filter_col}', Val='{date_filter_value}'")
    else: date_filter_col = 'DateID'; date_filter_value = date_obj.strftime("%m/%d/%Y"); print(f"DEBUG Preload Filter: Historical. Filter: Col='{date_filter_col}', Val='{date_filter_value}'")
    try:
        if is_current_week: filtered_rasdf = rasdf[rasdf[date_filter_col].astype(str).str.strip() == date_filter_value].copy()
        else: # Historical - attempt date logic first
            try:
                target_date = pd.to_datetime(date_filter_value, errors='raise').date(); rasdf_dates_only = rasdf[[date_filter_col]].copy()
                rasdf_dates_only[date_filter_col] = pd.to_datetime(rasdf_dates_only[date_filter_col], errors='coerce'); rasdf_dates_only.dropna(subset=[date_filter_col], inplace=True)
                if not rasdf_dates_only.empty:
                    matching_indices = rasdf_dates_only[rasdf_dates_only[date_filter_col].dt.date == target_date].index; valid_indices = [idx for idx in matching_indices if idx in rasdf.index]
                    filtered_rasdf = rasdf.loc[valid_indices].copy()
                    print(f"DEBUG Preload Filter (Hist): Found {len(valid_indices)} rows matching converted date.")
                else:
                    print(f"DEBUG Preload Filter (Hist): Date conversion yielded no results. Trying string match.")
                    filtered_rasdf = rasdf[rasdf[date_filter_col].astype(str).str.strip() == date_filter_value].copy()
            except Exception as e:
                print(f"ERROR Preload Filter (Hist): Date logic failed ({e}). Falling back.")
                # Fallback to direct string comparison
                try: # Nested try for fallback
                    filtered_rasdf = rasdf[rasdf[date_filter_col].astype(str).str.strip() == date_filter_value].copy()
                except KeyError:
                     print(f"ERROR Preload Filter (Hist Fallback): Column '{date_filter_col}' missing.")
                     return am_routes_to_buses, pm_routes_to_buses, pd.DataFrame()
    except KeyError: print(f"ERROR Preload Filter: Column '{date_filter_col}' not found."); return am_routes_to_buses, pm_routes_to_buses, pd.DataFrame()
    except Exception as date_filter_err: print(f"ERROR Preload Filter: Date filtering failed {date_filter_err}."); return am_routes_to_buses, pm_routes_to_buses, pd.DataFrame()
    # Route Filtering
    route_filter_col = 'Route';
    if filtered_rasdf.empty: return am_routes_to_buses, pm_routes_to_buses, filtered_rasdf
    try:
        if route_filter_col not in filtered_rasdf.columns: return am_routes_to_buses, pm_routes_to_buses, pd.DataFrame()
        route_value_stripped = str(route_input).strip(); final_filtered = filtered_rasdf[filtered_rasdf[route_filter_col].astype(str).str.strip() == route_value_stripped].copy()
        filtered_rasdf = final_filtered; print(f"DEBUG Preload Filter: Shape after route filter: {filtered_rasdf.shape}")
    except Exception as e: print(f"ERROR Preload Filter: Filtering by route failed: {e}"); return am_routes_to_buses, pm_routes_to_buses, pd.DataFrame()
    # Process Filtered Rows
    if filtered_rasdf.empty: return am_routes_to_buses, pm_routes_to_buses, filtered_rasdf
    try:
        required_proc_cols = [route_filter_col, 'Trip Type', 'Vehicle#']
        if not all(col in filtered_rasdf.columns for col in required_proc_cols): print(f"WARN Preload Filter: Missing columns for processing."); return am_routes_to_buses, pm_routes_to_buses, filtered_rasdf
        for _, row_series in filtered_rasdf.iterrows():
            route = str(row_series[route_filter_col]).strip(); am_pm = str(row_series['Trip Type']).strip().upper(); vehicle_number = str(row_series['Vehicle#']).strip()
            if not vehicle_number or vehicle_number.lower() in ('nan', '', 'none', '#n/a'): continue
            if isinstance(vehicle_number, str) and vehicle_number.endswith('.0'): vehicle_number = vehicle_number[:-2]
            if len(vehicle_number) == 3 and vehicle_number.isdigit(): vehicle_number = '0' + vehicle_number
            vehicle_full = 'NT' + vehicle_number if (len(vehicle_number) == 4 and vehicle_number.isdigit()) else vehicle_number
            if not route: continue
            if am_pm == "AM": am_routes_to_buses[route] = vehicle_full
            elif am_pm == "PM": pm_routes_to_buses[route] = vehicle_full
    except Exception as proc_err: print(f"ERROR Preload Filter: During vehicle processing: {proc_err}"); traceback.print_exc()
    print(f"DEBUG Preload Filter: AM Vehicles Found: {am_routes_to_buses}"); print(f"DEBUG Preload Filter: PM Vehicles Found: {pm_routes_to_buses}")
    return am_routes_to_buses, pm_routes_to_buses, filtered_rasdf


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
    dvi_webview_link = None; optdf_json = []; rasdf_filtered_for_request = None
    am_routes_to_buses = {}; pm_routes_to_buses = {}
    am_bus_number = None; pm_bus_number = None; am_device_id = None; pm_device_id = None
    am_route_coordinates_list = []; pm_route_coordinates_list = []
    am_stops_list = []; pm_stops_list = []
    try:
        # 1. Get Input Data
        data = request.get_json();
        if not data: return jsonify({"error": "Invalid request body. JSON expected."}), 400
        route_input = data.get('route'); date_str_ymd = data.get('date')
        if not route_input or not date_str_ymd: return jsonify({"error": "Missing route or date"}), 400
        try: date_obj = datetime.datetime.strptime(date_str_ymd, '%Y-%m-%d').date()
        except ValueError: return jsonify({"error": "Invalid date format. Use yyyy-MM-dd"}), 400
        print(f"Processing Route: {route_input}, Date: {date_str_ymd}")

        # 2. Access Preloaded RAS Data and Filter
        print("Accessing preloaded RAS data...")
        today = datetime.date.today(); current_monday = today - datetime.timedelta(days=today.weekday())
        use_current_ras = (date_obj >= current_monday)
        print(f"DEBUG: Using {'Current' if use_current_ras else 'Historical'} RAS data for filtering.")
        ras_df_to_filter = pd.DataFrame()
        with ras_data_lock: ras_df_to_filter = current_ras_df.copy() if use_current_ras else historical_ras_df.copy()
        depot = None
        if ras_df_to_filter.empty: print(f"WARN: Preloaded {'Current' if use_current_ras else 'Historical'} RAS data is empty.")
        else:
            am_routes_to_buses, pm_routes_to_buses, rasdf_filtered_for_request = get_vehicles_from_preloaded_ras(ras_df_to_filter, date_obj, route_input)
            if rasdf_filtered_for_request is not None and not rasdf_filtered_for_request.empty:
                 rasdf_with_coords = processing.add_depot_coords(rasdf_filtered_for_request)
                 depot = get_depot_from_ras(rasdf_with_coords)
                 if not am_routes_to_buses and not pm_routes_to_buses: print(f"WARN: Route {route_input} found in preloaded RAS, but no vehicles extracted.")
            else: print("INFO: No RAS data found for route/date in preloaded data.")

        # --- Find DVI Link (CORRECTED SYNTAX) ---
        if depot:
             print(f"Attempting to find DVI file for Depot: {depot}, Route: {route_input}, Date: {date_str_ymd}")
             try:
                 file_info = data_sources.find_drive_file(drive_service, config.ROOT_FOLDER_ID, depot, date_str_ymd, route_input, config.DRIVE_ID)
                 # *** CORRECTED SYNTAX: Put assignment and print on separate lines ***
                 if file_info and isinstance(file_info, dict) and 'webViewLink' in file_info:
                     dvi_webview_link = file_info['webViewLink']
                     print(f"DVI Link Found: {dvi_webview_link}")
                 # *********************************************************************
                 else:
                      print("INFO: DVI file not found or info invalid.")
             except Exception as dvi_err:
                  print(f"ERROR searching for DVI file: {dvi_err}")
        else:
             print("INFO: Skipping DVI file search (Drive service not ready or depot unknown).")
        # --- End Find DVI Link ---

        # 3. Fetch OPT Dump Data & Convert to JSON
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
        print("Processing OPT Dump data for maps...")
        am_locations_df, pm_locations_df = processing.process_am_pm(optdf, am_routes_to_buses, pm_routes_to_buses)

        # 5. Prepare Time Inputs for GPS Fetching
        am_start_hour, am_start_minute = 10, 0; am_end_hour, am_end_minute = 16, 0
        pm_start_hour, pm_start_minute = 16, 0; pm_end_hour, pm_end_minute = 23, 59

        # --- 6. Process AM Data ---
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

        # --- 7. Process PM Data ---
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
            "opt_data": optdf_json
        })

    # --- Top-Level Error Handling ---
    except Exception as e:
        print(f"ERROR: Unhandled exception in /get_map: {e}")
        print(traceback.format_exc())
        return jsonify({"error": f"An unexpected server error occurred: {e}", "opt_data": [] }), 500


if __name__ == '__main__':
    if not gspread_client: print("FATAL: GSpread client failed initialization. Cannot run.")
    elif not geotab_client: print("FATAL: Geotab client failed initialization. Cannot run.")
    else:
        print("Starting Flask server...")
        app.run(debug=True, host='0.0.0.0', port=5000, use_reloader=False)