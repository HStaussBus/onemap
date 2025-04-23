# app.py
from flask import Flask, render_template, request, jsonify
import datetime
import traceback  # For logging errors
import pandas as pd  # Needed for DataFrame operations

# Import necessary functions/modules
import config
import auth_clients
import data_sources
import processing
# import map_plotting # No longer needed

import os

app = Flask(__name__)

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
    if not mapbox_token: print("WARN: MAPBOX_TOKEN not found.")
    # Allow server to start even if some clients fail, but log clearly
    if not gspread_client or not drive_service:
        print("WARN: Google clients (GSpread/Drive) failed to initialize.")
    if not geotab_client:
        print("WARN: Geotab client failed to initialize.")
    print("INFO: Client initialization attempt complete.")
except Exception as startup_error:
    print(f"FATAL: Error during client initialization: {startup_error}")
# --- End Client Initialization ---

# --- Helper Function to Get Depot ---
def get_depot_from_ras(ras_df):
    # ... (Keep implementation as is) ...
    if ras_df is None or ras_df.empty: return None
    yard_col = None; yard_string = None # Initialize
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
# --- End Helper ---


# --- Flask Routes ---
@app.route('/')
def index():
    """Serves the main HTML page, passing the Mapbox token."""
    return render_template('index.html', mapbox_token=mapbox_token)


@app.route('/get_map', methods=['POST'])
def get_map_data():
    """
    API endpoint returning route data as JSON for client-side rendering.
    """
    # Check if essential clients are ready before proceeding
    if not geotab_client or not gspread_client:
        print(f"ERROR /get_map: Geotab or GSpread clients are not ready.")
        return jsonify({"error": "Server configuration error: Core clients not ready."}), 503

    start_time = datetime.datetime.now(); print(f"\n--- Received /get_map request at {start_time} ---")

    # Initialize data holders
    dvi_webview_link = None; optdf_json = []; rasdf = None
    am_routes_to_buses = {}; pm_routes_to_buses = {}
    am_bus_number = None; pm_bus_number = None
    am_device_id = None; pm_device_id = None
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

        # 2. Fetch RAS Data
        print("Fetching RAS data...")
        today = datetime.date.today()
        current_monday = today - datetime.timedelta(days=today.weekday())
        use_current_ras = (date_obj >= current_monday)
        print(f"DEBUG: Date {date_str_ymd} is >= current Monday ({current_monday})? {use_current_ras}. Using {'Current' if use_current_ras else 'Historical'} RAS.")

        result_tuple = None # Initialize result_tuple
        if use_current_ras:
            try: result_tuple = data_sources.get_current_ras_data(gspread_client, date_obj, route_input)
            except Exception as e: print(f"ERROR calling get_current_ras_data: {e}")
        else: # Use Historical RAS
            try: result_tuple = data_sources.get_historical_ras_data(gspread_client, date_obj, route_input)
            except Exception as e: print(f"ERROR calling get_historical_ras_data: {e}")

        # Process result_tuple safely
        if result_tuple and len(result_tuple) == 3: am_routes_to_buses, pm_routes_to_buses, rasdf = result_tuple
        else: print("WARN: RAS data function did not return expected tuple or failed.")
        if am_routes_to_buses is None: am_routes_to_buses = {}
        if pm_routes_to_buses is None: pm_routes_to_buses = {}
        if rasdf is None: rasdf = pd.DataFrame()

        # Determine Depot from RAS data
        depot = None
        if not rasdf.empty:
            rasdf = processing.add_depot_coords(rasdf); depot = get_depot_from_ras(rasdf)
            if not am_routes_to_buses and not pm_routes_to_buses: print(f"WARN: Route {route_input} found in RAS for {date_str_ymd}, but no AM/PM vehicles assigned.")
        else: print("INFO: No RAS data found or fetch failed. Cannot determine depot.")

        # --- Find DVI Link ---
        if drive_service and depot:
            print(f"Attempting to find DVI file for Depot: {depot}, Route: {route_input}, Date: {date_str_ymd}")
            try:
                file_info = data_sources.find_drive_file(drive_service, config.ROOT_FOLDER_ID, depot, date_str_ymd, route_input, config.DRIVE_ID)
                if file_info and isinstance(file_info, dict) and 'webViewLink' in file_info:
                    dvi_webview_link = file_info['webViewLink']
                    print(f"DVI Link Found: {dvi_webview_link}")
                else: print("INFO: DVI file not found or info invalid.")
            except Exception as dvi_err: print(f"ERROR searching for DVI file: {dvi_err}")
        else: print("INFO: Skipping DVI file search (Drive service not ready or depot unknown).")
        # --- End Find DVI Link ---

        # 3. Fetch OPT Dump Data
        print("Fetching OPT Dump data...")
        optdf = data_sources.get_opt_dump_data(auth_clients.get_db_connection, route_input, date_obj)
        if optdf is None: optdf = pd.DataFrame(); optdf_json = [] # Ensure optdf is DataFrame, json is list
        elif optdf.empty: optdf_json = []
        # --- Convert OPT DataFrame to JSON (CORRECTED SYNTAX) ---
        else:
            # *** Moved try block onto new line and indented ***
            try:
                print(f"DEBUG: Converting {len(optdf)} OPT rows to JSON...")
                optdf_serializable = optdf.astype(str).replace({'nan': '', 'NaT': '', '<NA>': ''}).fillna('')
                optdf_json = optdf_serializable.to_dict(orient='records')
                print("DEBUG: OPT DataFrame successfully converted to JSON list.")
            except Exception as json_err:
                print(f"ERROR: Failed converting OPT DataFrame to JSON: {json_err}")
                traceback.print_exc()
                optdf_json = [] # Send empty list on conversion error
        # --- End JSON Conversion ---

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
    if not geotab_client or not gspread_client:
        print("FATAL: Cannot start Flask server - Geotab or GSpread clients failed initialization.")
    else:
        print("Starting Flask server...")
        app.run(debug=True, host='0.0.0.0', port=8080, use_reloader=False)