# app.py
from flask import Flask, render_template, request, jsonify
import datetime
import traceback  # For logging errors
import pandas as pd # Needed for get_depot_from_ras helper

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
    # Add more checks here if specific clients are absolutely critical for startup
    if not mapbox_token:
         print("WARNING: MAPBOX_TOKEN not found in config/environment.")
    # Check if primary clients needed for core functionality initialized
    if not gspread_client or not drive_service: # Check Google clients needed for RAS/DVI
        raise ConnectionError("Google clients (GSpread/Drive) failed to initialize.")
    if not geotab_client: # Check Geotab needed for GPS
        raise ConnectionError("Geotab client failed to initialize.")

    print("INFO: Core clients initialized (or initialization deferred).")

except Exception as startup_error:
     print(f"FATAL: Error during application startup client initialization: {startup_error}")
     # Depending on the error, you might want to exit or prevent app run
     # For now, clients might remain None, causing routes to fail later


# --- Helper Function to Get Depot from RAS DataFrame ---
# (Moved here or could be in processing.py/utils.py)
def get_depot_from_ras(ras_df):
    if ras_df is None or ras_df.empty:
        print("DEBUG get_depot: RAS DataFrame is empty or None.")
        return None

    yard_col = None
    # Check column names case-insensitively if needed, but exact match preferred
    if "Assigned Pullout Yard" in ras_df.columns:
        yard_col = "Assigned Pullout Yard"
    elif "GM | Yard" in ras_df.columns:
        yard_col = "GM | Yard"

    if not yard_col:
        print("WARNING: Cannot determine depot, missing Yard column in RAS data.")
        return None

    # Use .iloc[0] carefully, assumes the first row is representative
    try:
        yard_string = ras_df[yard_col].iloc[0]
    except IndexError:
         print("WARNING: Could not access first row of RAS DataFrame for depot.")
         return None

    if pd.isna(yard_string) or yard_string == '':
         print("WARNING: Yard value is empty or NaN in RAS data.")
         return None

    yard_string_lower = str(yard_string).lower().strip()
    # Match against keys defined in config.DEPOT_LOCS
    for depot_name in config.DEPOT_LOCS.keys():
        if depot_name.lower() in yard_string_lower:
            print(f"INFO: Determined depot: {depot_name}")
            return depot_name # Return the key like "Greenpoint"

    print(f"WARNING: Could not match yard string '{yard_string}' to known depots in config.")
    return None

# --- Flask Routes ---
@app.route('/')
def index():
    """Serves the main HTML page."""
    return render_template('index.html')


@app.route('/get_map', methods=['POST'])
def get_map_data():
    """API endpoint to generate maps and find DVI link."""
    # Check if essential clients needed for THIS route are available
    # Note: Checks might differ if not all routes need all clients
    if not all([geotab_client, gspread_client, drive_service, mapbox_token]):
        print(f"ERROR /get_map: One or more required clients are not ready. "+
              f"Geotab:{geotab_client is not None}, GSpread:{gspread_client is not None}, "+
              f"Drive:{drive_service is not None}, Mapbox:{mapbox_token is not None}")
        return jsonify({"error": "Server configuration error: required clients not ready."}), 503

    start_time = datetime.datetime.now()
    print(f"\n--- Received /get_map request at {start_time} ---")

    # Initialize variables for the response
    am_map_html = None
    pm_map_html = None
    dvi_webview_link = None
    rasdf = None # Initialize rasdf to ensure it exists

    try:
        # 1. Get Input Data
        data = request.get_json()
        route_input = data.get('route')
        date_str_ymd = data.get('date')  # Expecting 'YYYY-MM-DD'

        if not route_input or not date_str_ymd:
            return jsonify({"error": "Missing route or date"}), 400

        # Validate and convert date
        try:
            date_obj = datetime.datetime.strptime(date_str_ymd, '%Y-%m-%d').date()
            date_str_mmddyyyy = date_obj.strftime("%m/%d/%Y") # Format needed for historical RAS check if using string
        except ValueError:
             return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        print(f"Processing Route: {route_input}, Date: {date_str_ymd}")

        # 2. Fetch RAS Data (No preloading yet)
        print("Fetching RAS data...")
        today = datetime.date.today()
        current_monday = today - datetime.timedelta(days=today.weekday())
        current_ras_date_format = date_obj.strftime("%A- %-d") # Format for current RAS

        # Reset variables before fetching
        am_routes_to_buses = {}
        pm_routes_to_buses = {}

        if date_obj >= current_monday:
            am_routes_to_buses, pm_routes_to_buses, rasdf = data_sources.get_current_ras_data(
                gspread_client, current_ras_date_format, route_input
            )
        else:
            am_routes_to_buses, pm_routes_to_buses, rasdf = data_sources.get_historical_ras_data(
                gspread_client, date_obj, route_input # Pass date_obj here
            )

        # --- Handle RAS Fetching Results ---
        if rasdf is None:  # Indicates a critical error during fetch/processing
             # Error already printed in data_sources, return server error
             return jsonify({"error": "Failed to retrieve RAS data due to server error."}), 500
        if rasdf.empty:
             print("WARNING: No RAS data found for route/date.")
             # Cannot determine depot or proceed without RAS usually
             # Decide how to respond - maybe return error or empty maps?
             return jsonify({
                 "am_map": "<div>No Scheduling (RAS) data found for this route/date.</div>",
                 "pm_map": "<div>No Scheduling (RAS) data found for this route/date.</div>",
                 "dvi_link": "#"
             }) # Return early

        # Add Depot Coords if RAS data exists
        rasdf = processing.add_depot_coords(rasdf) # Assuming this returns the df

        # Check if vehicles were assigned (important for later steps)
        if not am_routes_to_buses and not pm_routes_to_buses:
            print(f"WARNING: Route {route_input} found in RAS for {date_str_ymd}, but no AM/PM vehicles assigned.")
            # Allow proceeding, maps might be generated without GPS if OPT data exists

        # --- Find DVI Link (using rasdf) ---
        depot = get_depot_from_ras(rasdf) # Call helper function defined above

        if drive_service and depot: # Only try if we have service and a depot name
            print(f"Attempting to find DVI file for Depot: {depot}, Route: {route_input}, Date: {date_str_ymd}")
            # Call the function now located in data_sources
            file_info = data_sources.find_drive_file(
                drive_service,
                config.ROOT_FOLDER_ID, # Get from config
                depot,
                date_str_ymd, # Pass YYYY-MM-DD format
                route_input,
                config.DRIVE_ID # Get from config
            )
            if file_info and isinstance(file_info, dict) and 'webViewLink' in file_info:
                dvi_webview_link = file_info['webViewLink']
                print(f"DVI Link Found: {dvi_webview_link}")
            else:
                # find_drive_file prints INFO/ERROR messages internally now
                print("INFO: DVI file info not found or invalid.")
        else:
            if not drive_service: print("INFO: Skipping DVI file search (Drive service not ready).")
            if not depot: print("INFO: Skipping DVI file search (Depot unknown).")
        # --- End Find DVI Link ---


        # 3. Fetch OPT Dump Data
        print("Fetching OPT Dump data...")
        optdf = data_sources.get_opt_dump_data(auth_clients.get_db_connection, route_input, date_obj)
        if optdf is None: # Error during fetch
            return jsonify({"error": "Failed to retrieve route optimization data."}), 500
        if optdf.empty:
            print("WARNING: No OPT Dump data found.")
            return jsonify({ # Return empty maps if no OPT data
                "am_map": "<div>No route optimization data found.</div>",
                "pm_map": "<div>No route optimization data found.</div>",
                "dvi_link": dvi_webview_link or "#" # Still return DVI link if found
            })

        # 4. Process OPT Dump for AM/PM Locations
        print("Processing OPT Dump data...")
        am_locations_df, pm_locations_df = processing.process_am_pm(
            optdf, am_routes_to_buses, pm_routes_to_buses)
        # am_locations_df/pm_locations_df can be None or empty DF here

        # 5. Prepare Time Inputs for GPS Fetching
        am_start_hour, am_start_minute = 10, 0 # Consider making these configurable
        am_end_hour, am_end_minute = 16, 00
        pm_start_hour, pm_start_minute = 16, 0
        pm_end_hour, pm_end_minute = 23, 59

        # 6. Process AM Map
        print("Processing AM Map...")
        if am_locations_df is not None and not am_locations_df.empty:
            # ... (Your existing logic to fetch GPS, prepare data, plot map) ...
            # Make sure this block correctly sets am_map_html = am_map_obj._repr_html_()
            try:
                am_row = am_locations_df.iloc[0]
                am_bus_number = am_row.get("am_Vehicle#")
                if am_bus_number:
                    print(f"Fetching AM GPS for Bus: {am_bus_number}")
                    am_start_dt = datetime.datetime.combine(date_obj, datetime.time(am_start_hour, am_start_minute))
                    am_end_dt = datetime.datetime.combine(date_obj, datetime.time(am_end_hour, am_end_minute))
                    am_vehicle_data_df = data_sources.fetch_bus_data(geotab_client, am_bus_number, am_start_dt, am_end_dt)
                    am_route_polyline = processing.convert_to_polyline(am_vehicle_data_df)
                    am_route_data = processing.prepare_route_data(am_row, "am")
                    am_map_obj = map_plotting.plot_route_updated(am_route_data, am_vehicle_data_df, am_route_polyline, mapbox_token)
                    if am_map_obj:
                         am_map_html = am_map_obj._repr_html_()
                         print("AM Map generated.")
                    else: print("AM Map plotting returned None.")
                else: print("No AM vehicle number found in processed data.")
            except Exception as am_err: print(f"ERROR: Failed during AM map processing: {am_err}"); traceback.print_exc()
        else: print("INFO: No processed AM location data available to generate map.")

        # 7. Process PM Map
        print("Processing PM Map...")
        if pm_locations_df is not None and not pm_locations_df.empty:
            # ... (Your existing logic to fetch GPS, prepare data, plot map) ...
            # Make sure this block correctly sets pm_map_html = pm_map_obj._repr_html_()
            try:
                pm_row = pm_locations_df.iloc[0]
                pm_bus_number = pm_row.get("pm_Vehicle#")
                if pm_bus_number:
                    print(f"Fetching PM GPS for Bus: {pm_bus_number}")
                    pm_start_dt = datetime.datetime.combine(date_obj, datetime.time(pm_start_hour, pm_start_minute))
                    pm_end_dt = datetime.datetime.combine(date_obj, datetime.time(pm_end_hour, pm_end_minute))
                    pm_vehicle_data_df = data_sources.fetch_bus_data(geotab_client, pm_bus_number, pm_start_dt, pm_end_dt)
                    pm_route_polyline = processing.convert_to_polyline(pm_vehicle_data_df)
                    pm_route_data = processing.prepare_route_data(pm_row, "pm")
                    pm_map_obj = map_plotting.plot_route_updated(pm_route_data, pm_vehicle_data_df, pm_route_polyline, mapbox_token)
                    if pm_map_obj:
                        pm_map_html = pm_map_obj._repr_html_()
                        print("PM Map generated.")
                    else: print("PM Map plotting returned None.")
                else: print("No PM vehicle number found in processed data.")
            except Exception as pm_err: print(f"ERROR: Failed during PM map processing: {pm_err}"); traceback.print_exc()
        else: print("INFO: No processed PM location data available to generate map.")

        # 8. Return Results including DVI Link
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        print(f"--- Request completed in {duration.total_seconds():.2f} seconds ---")

        return jsonify({
            "am_map": am_map_html or "<div>No AM map generated or data available.</div>",
            "pm_map": pm_map_html or "<div>No PM map generated or data available.</div>",
            "dvi_link": dvi_webview_link or "#" # Use '#' as fallback
        })

    except Exception as e:
        # Catch-all for unexpected errors during the request handling
        print(f"ERROR: Unhandled exception in /get_map: {e}")
        print(traceback.format_exc())
        return jsonify({"error": f"An unexpected server error occurred: {e}"}), 500


if __name__ == '__main__':
    # Keep check for essential clients before running
    if not all([geotab_client, gspread_client, drive_service, mapbox_token]):
        print("FATAL: Cannot start Flask server - essential clients failed initialization during startup.")
    else:
        print("Starting Flask server...")
        # Set debug=False for production/stable testing
        # use_reloader=False might be needed if using background schedulers later
        app.run(debug=True, host='0.0.0.0', port=5000) # Add use_reloader=False if needed
