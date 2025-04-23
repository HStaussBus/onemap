# app.py
from flask import Flask, render_template, request, jsonify
import datetime
import traceback # For logging errors

# Import necessary functions/modules
import config
import auth_clients
import data_sources
import processing
import map_plotting
import ssl

context = ssl.create_default_context()

if hasattr(ssl, 'OP_ENABLE_MIDDLEBOX_COMPAT'):
    context.options |= ssl.OP_ENABLE_MIDDLEBOX_COMPAT
else:
    print("OP_ENABLE_MIDDLEBOX_COMPAT not available in this environment, skipping...")

app = Flask(__name__)

# --- Initialize Clients (Consider error handling if initialization fails) ---
print("INFO: Initializing clients...")
geotab_client = auth_clients.initialize_geotab_client()
gspread_client = auth_clients.get_gspread_client() # Uses lazy initialization
drive_service = auth_clients.get_drive_service()   # Uses lazy initialization
# db_connection_func = auth_clients.get_db_connection # Pass function for connection mgmt
mapbox_token = config.MAPBOX_TOKEN

# Check if essential clients initialized successfully
if not all([geotab_client, gspread_client, drive_service, mapbox_token]):
     print("FATAL: One or more essential clients/tokens failed to initialize. Check config and secrets.")
     # You might want to prevent the app from starting or handle this gracefully
     # For now, routes will likely fail if clients are None.


# --- Flask Routes ---
@app.route('/')
def index():
    """Serves the main HTML page."""
    return render_template('index.html')

@app.route('/get_map', methods=['POST'])
def get_map_data():
    """API endpoint to generate and return map HTML."""
    # Check if clients are available
    if not all([geotab_client, gspread_client, drive_service, mapbox_token]):
         return jsonify({"error": "Server configuration error: clients not ready."}), 503 # Service Unavailable

    start_time = datetime.datetime.now()
    print(f"\n--- Received /get_map request at {start_time} ---")

    try:
        # 1. Get Input Data
        data = request.get_json()
        route_input = data.get('route')
        date_str_ymd = data.get('date') # Expecting 'YYYY-MM-DD'

        if not route_input or not date_str_ymd:
            return jsonify({"error": "Missing route or date"}), 400

        # Validate and convert date
        try:
            date_obj = datetime.datetime.strptime(date_str_ymd, '%Y-%m-%d').date()
            date_str_mmddyyyy = date_obj.strftime("%m/%d/%Y") # Format needed for RAS sheets
        except ValueError:
             return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        print(f"Processing Route: {route_input}, Date: {date_str_ymd}")

        # 2. Fetch RAS Data
        print("Fetching RAS data...")
        today = datetime.date.today()
        current_monday = today - datetime.timedelta(days=today.weekday())

        if date_obj >= current_monday:
            am_routes_to_buses, pm_routes_to_buses, rasdf = data_sources.get_current_ras_data(
                gspread_client, date_obj.strftime("%A- %-d"), route_input # Pass correct date format
            )
        else:
             am_routes_to_buses, pm_routes_to_buses, rasdf = data_sources.get_historical_ras_data(
                 gspread_client, date_str_mmddyyyy, route_input # Pass MM/DD/YYYY
             )

        # Handle potential errors from RAS fetching
        if rasdf is None: # Indicates an error occurred during fetch/processing
             return jsonify({"error": "Failed to retrieve RAS data."}), 500
        if rasdf.empty:
             print("WARNING: No RAS data found for route/date.")
             # Decide if this is an error or just means no map can be generated
             # return jsonify({"error": "No RAS data found for this route/date."}), 404

        # Add Depot Coords (assuming rasdf is a DataFrame)
        if not rasdf.empty:
            rasdf = processing.add_depot_coords(rasdf)

        # Check if route was actually found in RAS and buses assigned
        if not am_routes_to_buses and not pm_routes_to_buses:
             print(f"WARNING: Route {route_input} not found in RAS for {date_str_mmddyyyy} or no vehicles assigned.")
             # Return appropriate message if needed
             # return jsonify({"error": f"Route {route_input} not found in scheduling data for {date_str_mmddyyyy}"}), 404


        # 3. Fetch OPT Dump Data
        print("Fetching OPT Dump data...")
        optdf = data_sources.get_opt_dump_data(auth_clients.get_db_connection, route_input, date_obj)
        if optdf is None: # Error during fetch
            return jsonify({"error": "Failed to retrieve route optimization data."}), 500
        if optdf.empty:
            print("WARNING: No OPT Dump data found.")
            # Combine with RAS check? If no OPT, maps likely can't be generated.
            return jsonify({"am_map": "<div>No route optimization data found.</div>",
                           "pm_map": "<div>No route optimization data found.</div>"})


        # 4. Process OPT Dump for AM/PM Locations
        print("Processing OPT Dump data...")
        am_locations_df, pm_locations_df = processing.process_am_pm(optdf, am_routes_to_buses, pm_routes_to_buses)
        # Check if either is None (error) or empty (no data for that period)

        # 5. Prepare Time Inputs for GPS Fetching
        am_start_hour, am_start_minute = 10, 0
        am_end_hour, am_end_minute = 16, 00
        pm_start_hour, pm_start_minute = 16, 0
        pm_end_hour, pm_end_minute = 23, 59

        # 6. Process AM Map
        print("Processing AM Map...")
        am_map_html = None
        if am_locations_df is not None and not am_locations_df.empty:
            try:
                # Ensure we select the correct row if multiple routes were processed,
                # assuming processing returns one row per route/period combo.
                # Using iloc[0] assumes only one relevant row is present.
                am_row = am_locations_df.iloc[0]
                am_bus_number = am_row.get("am_Vehicle#") # Use .get for safety

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
                else: print("No AM vehicle number found.")
            except IndexError:
                 print("ERROR: Could not select AM data row (IndexError).")
            except Exception as am_err:
                 print(f"ERROR: Failed during AM map processing: {am_err}")
                 traceback.print_exc() # Log detailed error
        else:
             print("INFO: No processed AM location data available.")


        # 7. Process PM Map
        print("Processing PM Map...")
        pm_map_html = None
        if pm_locations_df is not None and not pm_locations_df.empty:
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
                else: print("No PM vehicle number found.")
            except IndexError:
                 print("ERROR: Could not select PM data row (IndexError).")
            except Exception as pm_err:
                 print(f"ERROR: Failed during PM map processing: {pm_err}")
                 traceback.print_exc()
        else:
            print("INFO: No processed PM location data available.")


        # 8. Return Results
        end_time = datetime.datetime.now()
        duration = end_time - start_time
        print(f"--- Request completed in {duration.total_seconds():.2f} seconds ---")

        return jsonify({
            "am_map": am_map_html or "<div>No AM map generated or data available.</div>",
            "pm_map": pm_map_html or "<div>No PM map generated or data available.</div>"
        })

    except Exception as e:
        # Catch-all for unexpected errors during the request handling
        print(f"ERROR: Unhandled exception in /get_map: {e}")
        print(traceback.format_exc())
        return jsonify({"error": f"An unexpected server error occurred."}), 500


if __name__ == '__main__':
     # Add check for essential clients before running?
     if not all([geotab_client, gspread_client, drive_service, mapbox_token]):
          print("FATAL: Cannot start Flask server - essential clients failed initialization.")
     else:
          print("Starting Flask server...")
          # Set debug=False for production/stable testing
          app.run(debug=True, host='0.0.0.0', port=5000)
