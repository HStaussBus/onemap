# app.py
from flask import Flask, render_template, request, jsonify
import datetime
import traceback  # For logging errors
import pandas as pd # Needed for DataFrame operations

# Import necessary functions/modules
import config
import auth_clients
import data_sources
import processing
import map_plotting

import os # Make sure os is imported
# --- Temporary Debug Prints ---
# (You can remove these once secrets are confirmed working reliably)
print(f"TEST_VAR value is: {os.environ.get('TEST_VAR')}")
print("--- Checking Secrets Directly ---")
print("Value for AWS_ACCESS_KEY_ID:", os.environ.get('AWS_ACCESS_KEY_ID'))
print("Value for GEOTAB_USERNAME_SECRET:", os.environ.get('GEOTAB_USERNAME_SECRET'))
print("Value for GOOGLE_CREDS_JSON exists:", 'GOOGLE_CREDS_JSON' in os.environ)
print("--- End Checking Secrets ---")
# --- End Temporary Debug Prints ---


app = Flask(__name__)

# --- Initialize Clients (Error handling improved slightly) ---
print("INFO: Initializing clients...")
geotab_client = None
gspread_client = None
drive_service = None
mapbox_token = config.MAPBOX_TOKEN # Assume config loads this

try:
    geotab_client = auth_clients.initialize_geotab_client()
    gspread_client = auth_clients.get_gspread_client()  # Uses lazy initialization
    drive_service = auth_clients.get_drive_service()    # Uses lazy initialization

    if not mapbox_token: print("WARNING: MAPBOX_TOKEN not found in config/environment.")
    if not gspread_client or not drive_service: raise ConnectionError("Google clients (GSpread/Drive) failed to initialize.")
    if not geotab_client: raise ConnectionError("Geotab client failed to initialize.")

    print("INFO: Core clients initialized (or initialization deferred).")

except Exception as startup_error:
     print(f"FATAL: Error during application startup client initialization: {startup_error}")
     # Clients might remain None, causing routes to fail later


# --- Helper Function to Get Depot from RAS DataFrame ---
def get_depot_from_ras(ras_df):
    if ras_df is None or ras_df.empty:
        print("DEBUG get_depot: RAS DataFrame is empty or None.")
        return None
    yard_col = None
    if "Assigned Pullout Yard" in ras_df.columns: yard_col = "Assigned Pullout Yard"
    elif "GM | Yard" in ras_df.columns: yard_col = "GM | Yard"
    if not yard_col:
        print("WARNING: Cannot determine depot, missing Yard column in RAS data.")
        return None
    try: yard_string = ras_df[yard_col].iloc[0]
    except IndexError: print("WARNING: Could not access first row of RAS DataFrame for depot."); return None
    if pd.isna(yard_string) or yard_string == '': print("WARNING: Yard value is empty or NaN in RAS data."); return None
    yard_string_lower = str(yard_string).lower().strip()
    for depot_name in config.DEPOT_LOCS.keys():
        if depot_name.lower() in yard_string_lower:
            print(f"INFO: Determined depot: {depot_name}")
            return depot_name
    print(f"WARNING: Could not match yard string '{yard_string}' to known depots in config.")
    return None

# --- Flask Routes ---
@app.route('/')
def index():
    """Serves the main HTML page."""
    return render_template('index.html')


@app.route('/get_map', methods=['POST'])
def get_map_data():
    """API endpoint to generate maps, find DVI link, and return OPT data."""
    if not all([geotab_client, gspread_client, drive_service, mapbox_token]):
        print(f"ERROR /get_map: One or more required clients are not ready...")
        return jsonify({"error": "Server configuration error: required clients not ready."}), 503

    start_time = datetime.datetime.now()
    print(f"\n--- Received /get_map request at {start_time} ---")

    am_map_html = None
    pm_map_html = None
    dvi_webview_link = None
    optdf_json = None # Initialize
    rasdf = None

    try:
        # 1. Get Input Data
        data = request.get_json()
        route_input = data.get('route')
        date_str_ymd = data.get('date')
        if not route_input or not date_str_ymd: return jsonify({"error": "Missing route or date"}), 400
        try: date_obj = datetime.datetime.strptime(date_str_ymd, '%Y-%m-%d').date()
        except ValueError: return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400
        print(f"Processing Route: {route_input}, Date: {date_str_ymd}")

        # 2. Fetch RAS Data
        print("Fetching RAS data...")
        today = datetime.date.today(); current_monday = today - datetime.timedelta(days=today.weekday())
        #current_ras_date_format = date_obj.strftime("%A- %-d")
        am_routes_to_buses, pm_routes_to_buses = {}, {}
        if date_obj >= current_monday:
            # --- Call Current RAS ---
            # Needs date string in MM/DD/YYYY format for strptime inside it
            try:
                date_str_for_current_ras = date_obj.strftime("%m/%d/%Y")
                print(f"INFO: Calling get_current_ras_data with date string: {date_str_for_current_ras}") # Debug
                # Call function, handle potential None return values if it fails
                result_tuple = data_sources.get_current_ras_data(
                    gspread_client,
                    date_obj, # Pass MM/DD/YYYY string
                    route_input
                )
                # Safely unpack results, assuming function returns tuple (dict, dict, df) or None
                if result_tuple and len(result_tuple) == 3:
                     am_routes_to_buses, pm_routes_to_buses, rasdf = result_tuple
                     if am_routes_to_buses is None: am_routes_to_buses = {} # Ensure dict type
                     if pm_routes_to_buses is None: pm_routes_to_buses = {} # Ensure dict type
                else:
                     print("WARNING: get_current_ras_data did not return expected tuple.")
                     # Keep defaults (empty dicts, None df)

            except Exception as e:
                 print(f"ERROR calling get_current_ras_data: {e}")
                 # Keep defaults (empty dicts, None df)
        else:
            # --- Call Historical RAS ---
            # Assuming historical function correctly takes the date object
            try:
                print(f"INFO: Calling get_historical_ras_data with date object: {date_obj}") # Debug
                # Call function, handle potential None return values
                result_tuple = data_sources.get_historical_ras_data(
                    gspread_client,
                    date_obj, # Pass the date object
                    route_input
                )
                # Safely unpack results
                if result_tuple and len(result_tuple) == 3:
                     am_routes_to_buses, pm_routes_to_buses, rasdf = result_tuple
                     if am_routes_to_buses is None: am_routes_to_buses = {} # Ensure dict type
                     if pm_routes_to_buses is None: pm_routes_to_buses = {} # Ensure dict type
                else:
                     print("WARNING: get_historical_ras_data did not return expected tuple.")
                     # Keep defaults

            except Exception as e:
                 print(f"ERROR calling get_historical_ras_data: {e}")
                 # Keep defaults

        # --- Check results after the if/else block ---
        # print(f"DEBUG: AM routes count: {len(am_routes_to_buses)}")
        # print(f"DEBUG: PM routes count: {len(pm_routes_to_buses)}")
        # print(f"DEBUG: RAS DataFrame is None: {rasdf is None}")


        if rasdf is None: return jsonify({"error": "Failed to retrieve RAS data due to server error."}), 500
        depot = None
        if not rasdf.empty:
            rasdf = processing.add_depot_coords(rasdf)
            depot = get_depot_from_ras(rasdf)
            if not am_routes_to_buses and not pm_routes_to_buses: print(f"WARNING: Route {route_input} found in RAS for {date_str_ymd}, but no AM/PM vehicles assigned.")
        else:
             print("WARNING: No RAS data found for route/date. Cannot determine depot or vehicles.")
             return jsonify({ "am_map": "<div>No Scheduling (RAS) data found.</div>", "pm_map": "<div>No Scheduling (RAS) data found.</div>", "dvi_link": "#", "opt_data": [] })

        # --- Find DVI Link ---
        if drive_service and depot:
            print(f"Attempting to find DVI file for Depot: {depot}, Route: {route_input}, Date: {date_str_ymd}")
            file_info = data_sources.find_drive_file( drive_service, config.ROOT_FOLDER_ID, depot, date_str_ymd, route_input, config.DRIVE_ID )
            if file_info and isinstance(file_info, dict) and 'webViewLink' in file_info:
                dvi_webview_link = file_info['webViewLink']
                print(f"DVI Link Found: {dvi_webview_link}")
            else: print("INFO: DVI file info not found or invalid.")
        else: print("INFO: Skipping DVI file search (Drive service not ready or depot unknown).")
        # --- End Find DVI Link ---

        # 3. Fetch OPT Dump Data
        print("Fetching OPT Dump data...")
        optdf = data_sources.get_opt_dump_data(auth_clients.get_db_connection, route_input, date_obj)

        if optdf is None: return jsonify({"error": "Failed to retrieve route optimization data."}), 500

        # --- Convert OPT DataFrame to JSON (Final Robust Method) ---
        if optdf.empty:
            print("WARNING: No OPT Dump data found for this route/date.")
            optdf_json = [] # Set to empty list for the response
        else:
            try:
                print(f"DEBUG: Converting {len(optdf)} OPT rows to JSON for response (Forcing String).")
                optdf_serializable = optdf.copy()

                # Convert ALL columns to string type BEFORE fillna and to_dict
                for col in optdf_serializable.columns:
                    try:
                        optdf_serializable[col] = optdf_serializable[col].astype(str)
                    except Exception as str_conv_err:
                         print(f"WARN: Could not force column '{col}' to string: {str_conv_err}. Keeping original values which might cause issues.")
                         # Keep original data in this column if conversion fails

                # Replace string representations of nulls ('nan', 'NaT', etc.) and fill any remaining Python Nones
                optdf_serializable = optdf_serializable.replace({'nan': '', 'NaT': '', '<NA>': ''}).fillna('')

                # Convert the now "safe" DataFrame to list of dictionaries
                optdf_json = optdf_serializable.to_dict(orient='records')
                print("DEBUG: OPT DataFrame successfully converted to JSON list.")

            except Exception as json_err:
                print(f"ERROR: Failed during revised OPT DataFrame to JSON conversion: {json_err}")
                traceback.print_exc()
                optdf_json = None # Set to None if conversion fails

        # Handle conversion failure case - send empty list instead of None
        if optdf is not None and not optdf.empty and optdf_json is None:
            print("WARNING: Sending empty OPT data in response due to conversion error.")
            optdf_json = []
        # --- End JSON Conversion ---

        # If OPT data was empty initially, return now (avoids processing maps without data)
        # This check is slightly redundant now as optdf_json is set above, but harmless
        if optdf.empty:
             return jsonify({ "am_map": "<div>No route optimization data found.</div>", "pm_map": "<div>No route optimization data found.</div>", "dvi_link": dvi_webview_link or "#", "opt_data": optdf_json }) # optdf_json is [] here

        # 4. Process OPT Dump for AM/PM Locations (using original optdf)
        print("Processing OPT Dump data for maps...")
        am_locations_df, pm_locations_df = processing.process_am_pm(optdf, am_routes_to_buses, pm_routes_to_buses)

        # 5. Prepare Time Inputs for GPS Fetching
        am_start_hour, am_start_minute = 10, 0; am_end_hour, am_end_minute = 16, 00
        pm_start_hour, pm_start_minute = 16, 0; pm_end_hour, pm_end_minute = 23, 59

        # 6. Process AM Map
        print("Processing AM Map...")
        if am_locations_df is not None and not am_locations_df.empty:
             try:
                 am_row = am_locations_df.iloc[0]; am_bus_number = am_row.get("am_Vehicle#")
                 if am_bus_number:
                     print(f"Fetching AM GPS for Bus: {am_bus_number}")
                     am_start_dt=datetime.datetime.combine(date_obj, datetime.time(am_start_hour, am_start_minute))
                     am_end_dt=datetime.datetime.combine(date_obj, datetime.time(am_end_hour, am_end_minute))
                     am_vehicle_data_df = data_sources.fetch_bus_data(geotab_client, am_bus_number, am_start_dt, am_end_dt)
                     am_route_polyline = processing.convert_to_polyline(am_vehicle_data_df)
                     am_route_data = processing.prepare_route_data(am_row, "am")
                     am_map_obj = map_plotting.plot_route_updated(am_route_data, am_vehicle_data_df, am_route_polyline, mapbox_token)
                     if am_map_obj: am_map_html = am_map_obj._repr_html_(); print("AM Map generated.")
                     else: print("AM Map plotting returned None.")
                 else: print("No AM vehicle number found.")
             except Exception as am_err: print(f"ERROR: AM map processing: {am_err}"); traceback.print_exc()
        else: print("INFO: No processed AM location data available.")

        # 7. Process PM Map
        print("Processing PM Map...")
        if pm_locations_df is not None and not pm_locations_df.empty:
             try:
                 pm_row = pm_locations_df.iloc[0]; pm_bus_number = pm_row.get("pm_Vehicle#")
                 if pm_bus_number:
                     print(f"Fetching PM GPS for Bus: {pm_bus_number}")
                     pm_start_dt=datetime.datetime.combine(date_obj, datetime.time(pm_start_hour, pm_start_minute))
                     pm_end_dt=datetime.datetime.combine(date_obj, datetime.time(pm_end_hour, pm_end_minute))
                     pm_vehicle_data_df = data_sources.fetch_bus_data(geotab_client, pm_bus_number, pm_start_dt, pm_end_dt)
                     pm_route_polyline = processing.convert_to_polyline(pm_vehicle_data_df)
                     pm_route_data = processing.prepare_route_data(pm_row, "pm")
                     pm_map_obj = map_plotting.plot_route_updated(pm_route_data, pm_vehicle_data_df, pm_route_polyline, mapbox_token)
                     if pm_map_obj: pm_map_html = pm_map_obj._repr_html_(); print("PM Map generated.")
                     else: print("PM Map plotting returned None.")
                 else: print("No PM vehicle number found.")
             except Exception as pm_err: print(f"ERROR: PM map processing: {pm_err}"); traceback.print_exc()
        else: print("INFO: No processed PM location data available.")

        # 8. Return Results
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        print(f"--- Request completed in {duration.total_seconds():.2f} seconds ---")

        return jsonify({
            "am_map": am_map_html or "<div>No AM map generated or data available.</div>",
            "pm_map": pm_map_html or "<div>No PM map generated or data available.</div>",
            "dvi_link": dvi_webview_link or "#",
            "opt_data": optdf_json # Use the converted JSON list here (or [] or None)
        })

    except Exception as e:
        print(f"ERROR: Unhandled exception in /get_map: {e}")
        print(traceback.format_exc())
        return jsonify({"error": f"An unexpected server error occurred: {e}", "opt_data": None}), 500
        
if __name__ == '__main__':
    # Keep check for essential clients before running
    if not all([geotab_client, gspread_client, drive_service, mapbox_token]):
        print("FATAL: Cannot start Flask server - essential clients failed initialization during startup.")
    else:
        print("Starting Flask server...")
        app.run(debug=True, host='0.0.0.0', port=5000) # Add use_reloader=False if needed for schedulers