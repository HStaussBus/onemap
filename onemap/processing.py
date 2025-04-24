# processing.py
import pandas as pd
# from shapely.geometry import LineString # No longer needed for frontend structure
import config # For DEPOT_LOCS
import traceback # For detailed error logging

# def convert_to_polyline(df):
#     """Converts DataFrame points to a Shapely LineString."""
#     # (Keep original implementation if needed elsewhere, but not for this frontend refactor)
#     if not isinstance(df, pd.DataFrame) or df.empty:
#         return None
#     if 'latitude' not in df.columns or 'longitude' not in df.columns:
#         print("ERROR: DataFrame missing 'latitude' or 'longitude' columns for polyline.")
#         return None
#     if len(df) < 2:
#         return None
#     try:
#         # Filter out rows with non-numeric or missing coordinates BEFORE creating points
#         df_numeric = df.dropna(subset=['latitude', 'longitude'])
#         df_numeric['latitude'] = pd.to_numeric(df_numeric['latitude'], errors='coerce')
#         df_numeric['longitude'] = pd.to_numeric(df_numeric['longitude'], errors='coerce')
#         df_numeric.dropna(subset=['latitude', 'longitude'], inplace=True)
#         if len(df_numeric) < 2: return None # Check again after cleaning

#         # Create list of coordinate tuples (lon, lat for Shapely)
#         points = list(zip(df_numeric['longitude'], df_numeric['latitude']))
#         polyline = LineString(points)
#         return polyline
#     except Exception as e:
#         print(f"ERROR: Failed to create polyline: {e}")
#         return None

# --- NEW Function: Format GPS Trace ---
def format_gps_trace(df):
    """
    Converts DataFrame lat/lon/dateTime/speed columns into a list of dictionaries
    suitable for detailed Leaflet plotting (including hover tooltips).
    Returns: [{'lat': float, 'lon': float, 'ts': iso_timestamp_str, 'spd': float_kph}, ...]
    """
    trace_data = []
    if not isinstance(df, pd.DataFrame) or df.empty:
        return trace_data # Return empty list

    required_cols = ['latitude', 'longitude', 'dateTime', 'speed']
    if not all(col in df.columns for col in required_cols):
        missing = [col for col in required_cols if col not in df.columns]
        print(f"ERROR format_gps_trace: DataFrame missing required columns: {missing}.")
        # Decide if you want to return partial data or empty
        # Returning empty is safer if all columns are needed for hover
        return trace_data

    try:
        # Select columns, work on a copy
        df_coords = df[required_cols].copy()

        # Convert coordinates and speed to numeric, dropping invalid rows
        df_coords['latitude'] = pd.to_numeric(df_coords['latitude'], errors='coerce')
        df_coords['longitude'] = pd.to_numeric(df_coords['longitude'], errors='coerce')
        df_coords['speed'] = pd.to_numeric(df_coords['speed'], errors='coerce')
        df_coords.dropna(subset=['latitude', 'longitude', 'speed'], inplace=True) # Speed also required

        # Ensure dateTime is timezone-aware UTC datetime object
        df_coords['dateTime'] = pd.to_datetime(df_coords['dateTime'], errors='coerce', utc=True)
        df_coords.dropna(subset=['dateTime'], inplace=True)

        if df_coords.empty:
             print("WARN format_gps_trace: No valid rows remaining after data cleaning.")
             return trace_data

        # Convert to list of dictionaries
        for _, row in df_coords.iterrows():
             trace_data.append({
                 'lat': row['latitude'],
                 'lon': row['longitude'],
                 # Convert datetime to ISO 8601 string (UTC) for JSON compatibility
                 'ts': row['dateTime'].isoformat(),
                 'spd': row['speed'] # Speed in kph from Geotab
             })

        # print(f"DEBUG format_gps_trace: Formatted {len(trace_data)} trace points with details.") # Optional debug
        return trace_data
    except Exception as e:
        print(f"ERROR format_gps_trace: Failed during detailed coordinate formatting: {e}")
        print(traceback.format_exc())
        return [] # Return empty list on error
# --- NEW Function: Format Stops ---
def format_stops(locations_df, period_prefix):
    """
    Extracts and formats stop data from the processed AM/PM locations DataFrame
    into a list of dictionaries suitable for Leaflet markers.
    """
    stops_list = []
    if locations_df is None or not isinstance(locations_df, pd.DataFrame) or locations_df.empty:
        # print(f"DEBUG format_stops ({period_prefix}): Input locations_df is invalid or empty.") # Optional debug
        return stops_list

    # Define column names based on prefix (e.g., "am_", "pm_")
    student_col = f"{period_prefix}Student Pickups"
    school_col = f"{period_prefix}School Locations"
    student_id_col = f"{period_prefix}Student IDs"
    school_name_col = f"{period_prefix}School Names"
    sess_beg_col = f"{period_prefix}Sess_Beg." # Added Session Begin Time column
    
    

    # Check if essential source columns exist in the DataFrame
    # Note: These columns contain dictionaries themselves.
    required_source_cols = [student_col, school_col, student_id_col, school_name_col, sess_beg_col]
    if not all(col in locations_df.columns for col in required_source_cols):
         missing = [col for col in required_source_cols if col not in locations_df.columns]
         print(f"WARN format_stops ({period_prefix}): Missing source columns in locations_df: {missing}. Some stop info might be incomplete.")
         # Proceed even if some info columns are missing, but coordinates are crucial

    # Iterate through the single row (usually) in locations_df
    for _, route_data in locations_df.iterrows():
        # Process Student Stops
        student_stops_dict = route_data.get(student_col)
        student_ids_dict = route_data.get(student_id_col, {}) or {} # Handle potential None
        

        if isinstance(student_stops_dict, dict):
            for seq_key, coords in student_stops_dict.items():
                try:
                    # Validate sequence key (should be convertible to int ideally)
                    seq = int(seq_key)
                except (ValueError, TypeError):
                    # print(f"WARN format_stops ({period_prefix}): Invalid sequence key '{seq_key}' for student stop. Skipping.") # Optional debug
                    continue # Skip stops with non-integer sequences if that's expected

                # Validate coordinates
                if isinstance(coords, (list, tuple)) and len(coords) == 2:
                    lat, lon = coords
                    # Further check if lat/lon are valid numbers (optional but good)
                    if isinstance(lat, (int, float)) and isinstance(lon, (int, float)) and pd.notna(lat) and pd.notna(lon):
                        pupil_id = student_ids_dict.get(seq_key, 'N/A') # Use original key for ID lookup
                        stops_list.append({
                            "lat": lat,
                            "lon": lon,
                            "type": "student",
                            "sequence": seq, # Store numeric sequence
                            "info": f"Pickup #: {seq}<br>Pupil ID: {pupil_id}" # Example popup info
                        })
                    # else: print(f"WARN format_stops ({period_prefix}): Invalid coordinate values for student seq {seq}: {coords}") # Optional debug
                # else: print(f"WARN format_stops ({period_prefix}): Invalid coordinate structure for student seq {seq}: {coords}") # Optional debug
        # else: print(f"WARN format_stops ({period_prefix}): Column '{student_col}' is not a dictionary or missing.") # Optional debug


        # Process School Stops
        school_stops_dict = route_data.get(school_col)
        school_names_dict = route_data.get(school_name_col, {}) or {} # Handle potential None
        school_times_dict = route_data.get(sess_beg_col, {}) or {} # Handle potential None
        print(f"DEBUG format_stops ({period_prefix}): Received school_stops_dict: {school_stops_dict}")

        if isinstance(school_stops_dict, dict):
            for seq_key, coords in school_stops_dict.items():
                try:
                    seq = int(seq_key) # Treat school sequence keys as numbers too if possible
                except (ValueError, TypeError):
                    # print(f"WARN format_stops ({period_prefix}): Invalid sequence key '{seq_key}' for school stop. Assigning default.") # Optional debug
                    # Assign a default sequence (e.g., 0 or max+1) or skip if keys must be numeric
                    seq = 0 # Default school sequence to 0 if key isn't numeric

                # Validate coordinates
                if isinstance(coords, (list, tuple)) and len(coords) == 2:
                    lat, lon = coords
                    if isinstance(lat, (int, float)) and isinstance(lon, (int, float)) and pd.notna(lat) and pd.notna(lon):
                        school_name = school_names_dict.get(seq_key, 'N/A') # Use original key for lookups
                        sess_beg_time = school_times_dict.get(seq_key)
                        # Format time nicely if possible (assuming it might be datetime object or string)
                        time_str = "N/A"
                        if pd.notna(sess_beg_time):
                             try:
                                 # Attempt conversion if it's not already a formatted string
                                 time_dt = pd.to_datetime(sess_beg_time, errors='coerce')
                                 if pd.notna(time_dt):
                                     time_str = time_dt.strftime("%I:%M %p") # HH:MM AM/PM
                                 else:
                                     time_str = str(sess_beg_time) # Use original string if conversion fails
                             except Exception:
                                 time_str = str(sess_beg_time) # Fallback

                        stops_list.append({
                            "lat": lat,
                            "lon": lon,
                            "type": "school",
                            "sequence": seq, # Use determined sequence
                            "info": f"School: {school_name}<br>Session Begin: {time_str}" # Example popup info
                        })
                    # else: print(f"WARN format_stops ({period_prefix}): Invalid coordinate values for school seq {seq}: {coords}") # Optional debug
                # else: print(f"WARN format_stops ({period_prefix}): Invalid coordinate structure for school seq {seq}: {coords}") # Optional debug
        # else: print(f"WARN format_stops ({period_prefix}): Column '{school_col}' is not a dictionary or missing.") # Optional debug


    # Sort by sequence (optional, but often helpful for display order)
    try:
        # Sort primarily by type ('school' often comes first/last), then sequence
        stops_list.sort(key=lambda x: (0 if x.get('type') == 'school' else 1, x.get('sequence', float('inf'))))
    except Exception as sort_err:
         print(f"WARN format_stops ({period_prefix}): Failed to sort stops: {sort_err}")

    # print(f"DEBUG format_stops ({period_prefix}): Formatted {len(stops_list)} stops.") # Optional debug
    return stops_list

# --- Keep Existing Functions Needed by Backend ---

def add_depot_coords(rasdf):
    """Adds 'Depot Coords' column to RAS DataFrame."""
    # (Keep original implementation, get depotlocs from config)
    # Determine which yard column exists
    yard_col = None
    if "Assigned Pullout Yard" in rasdf.columns: yard_col = "Assigned Pullout Yard"
    elif "GM | Yard" in rasdf.columns: yard_col = "GM | Yard"
    else:
        print("WARN add_depot_coords: No yard column found in rasdf ('Assigned Pullout Yard' or 'GM | Yard').")
        rasdf["Depot Coords"] = None # Add column with None if yard column missing
        return rasdf

    depotlocs = config.DEPOT_LOCS # Use locations from config

    # Function to map yard string to depot coords
    def map_depot_coords(yard_value):
        if pd.isna(yard_value) or yard_value == '': return None
        # Iterate through known depots and check if the name is in the yard string (case-insensitive)
        for depot_name, coords in depotlocs.items():
            if depot_name.lower() in str(yard_value).lower().strip():
                 return coords # Return tuple (lon, lat)
        # print(f"DEBUG add_depot_coords: Yard value '{yard_value}' did not match any known depots.") # Optional debug
        return None # Return None if no match found

    # Apply the mapping function to the yard column
    rasdf["Depot Coords"] = rasdf[yard_col].apply(map_depot_coords)
    # print(f"DEBUG add_depot_coords: Depot Coords column added. Null count: {rasdf['Depot Coords'].isnull().sum()}") # Optional debug
    return rasdf


def process_optdump(optdump, session_type, routes_to_buses):
    """
    Processes the optdump DataFrame for AM or PM sessions.
    Returns a DataFrame ready for stop/location extraction.
    """
    if not isinstance(optdump, pd.DataFrame):
         print(f"ERROR process_optdump ({session_type}): Input optdump is not a DataFrame.")
         return None
    if optdump.empty:
         print(f"INFO process_optdump ({session_type}): Input optdump DataFrame is empty.")
         return pd.DataFrame(columns=["Route"]) # Return structure expected by process_am_pm

    if session_type not in ["AM", "PM"]:
        print(f"ERROR process_optdump: Invalid session_type '{session_type}'. Must be 'AM' or 'PM'.")
        raise ValueError("session_type must be 'AM' or 'PM'") # Raise error here

    prefix = f"{session_type.lower()}_"
    df = optdump.copy() # Work on a copy

    # --- Filter by Session Type ---
    if 'am_pm' not in df.columns:
        print(f"WARN process_optdump ({session_type}): 'am_pm' column not found in OPT data. Cannot filter by session.")
        # Decide how to proceed: maybe assume all data is relevant, or return None?
        # For now, let's assume it might work without this filter if column missing
    else:
        if session_type == "AM":
            df = df[df['am_pm'].astype(str).str.upper() != 'PM ONLY']
        else: # PM
            df = df[df['am_pm'].astype(str).str.upper() != 'AM ONLY']

        if df.empty:
            print(f"INFO process_optdump ({session_type}): No data for {session_type} session after filtering by 'am_pm' column.")
            return pd.DataFrame(columns=["Route"])

    # --- Clean and Filter Data ---
    if 'address' in df.columns:
        df = df[~df['address'].astype(str).str.contains("SEE OPERATIONS", case=False, na=False)]
    else: print(f"WARN process_optdump ({session_type}): 'address' column not found.")

    # Rename columns carefully, check if they exist first
    rename_map = {'address': 'Address', 'route': 'Route', 'pupil_id_no': 'Pupil_Id_No'}
    cols_to_rename = {k:v for k,v in rename_map.items() if k in df.columns}
    if cols_to_rename:
        df.rename(columns=cols_to_rename, inplace=True)

    # Check for School_Code_&_Name before filtering
    if 'School_Code_&_Name' in df.columns:
        df = df[~df['School_Code_&_Name'].astype(str).str.contains("DISMISS", case=False, na=False)]
    else: print(f"WARN process_optdump ({session_type}): 'School_Code_&_Name' column not found.")


    # --- Validate and Convert Coordinates ---
    if 'pupil_lat' not in df.columns or 'pupil_lon' not in df.columns:
        print(f"ERROR process_optdump ({session_type}): Missing required 'pupil_lat' or 'pupil_lon' columns.")
        return None # Cannot proceed without coordinates

    # Ensure lat/lon columns are not empty strings before conversion
    df = df[df["pupil_lat"].astype(str).str.strip() != ""]
    df = df[df["pupil_lon"].astype(str).str.strip() != ""]

    try:
        df['Latitude'] = pd.to_numeric(df['pupil_lat'], errors='coerce')
        df['Longitude'] = pd.to_numeric(df['pupil_lon'], errors='coerce')
        # Drop rows where coordinate conversion failed
        initial_rows = len(df)
        df.dropna(subset=['Latitude', 'Longitude'], inplace=True)
        if len(df) < initial_rows: print(f"WARN process_optdump ({session_type}): Dropped {initial_rows - len(df)} rows due to invalid coordinates.")

        if df.empty:
             print(f"INFO process_optdump ({session_type}): No valid pupil coordinates found after conversion.")
             return pd.DataFrame(columns=["Route"]) # Return empty if all rows dropped
    except Exception as e:
        print(f"ERROR process_optdump ({session_type}): Failed converting pupil coordinates: {e}")
        return None # Return None on conversion error

    # --- Handle Datetime and Sequence Conversions ---
    # Convert time columns safely
    if 'sess_beg' in df.columns:
        df['Sess_Beg.'] = pd.to_datetime(df['sess_beg'], errors='coerce', format='%H:%M:%S').dt.time # Extract time part
    else: print(f"WARN process_optdump ({session_type}): 'sess_beg' column not found.")

    if 'sess_end' in df.columns:
        df['Sess_End'] = pd.to_datetime(df['sess_end'], errors='coerce', format='%H:%M:%S').dt.time # Extract time part
    else: print(f"WARN process_optdump ({session_type}): 'sess_end' column not found.")

    # Convert sequence number safely
    if 'seg_no' in df.columns:
        df['Sequence'] = pd.to_numeric(df['seg_no'], errors='coerce')
        # Optional: Fill NaN sequences with a default (like 0 or -1) if needed, or drop them
        # df['Sequence'] = df['Sequence'].fillna(0).astype(int) # Example: fill with 0
        df.dropna(subset=['Sequence'], inplace=True) # Drop rows with invalid sequence
        df['Sequence'] = df['Sequence'].astype(int) # Convert to integer
    else:
         print(f"WARN process_optdump ({session_type}): 'seg_no' column not found, cannot determine sequence. Stop formatting might be affected.")
         # Add a default sequence column if subsequent logic depends on it
         # df['Sequence'] = 0 # Example: Assign default sequence

    # --- Drop Duplicates ---
    # Ensure required columns for duplicate check exist
    required_dup_cols = ['Route', 'Pupil_Id_No', 'School_Code_&_Name']
    cols_present_for_dup_check = [col for col in required_dup_cols if col in df.columns]
    if len(cols_present_for_dup_check) > 1: # Need at least Route + one other identifier
        print(f"DEBUG process_optdump ({session_type}): Dropping duplicates based on columns: {cols_present_for_dup_check}")
        # Sort before dropping (use Sequence if available and valid)
        sort_cols = ['Route']
        if 'Sequence' in df.columns: sort_cols.append('Sequence')
        if 'Pupil_Id_No' in cols_present_for_dup_check: sort_cols.append('Pupil_Id_No')

        df = df.sort_values(by=sort_cols).drop_duplicates(
            subset=cols_present_for_dup_check, keep="first"
        )
    else:
        print(f"WARN process_optdump ({session_type}): Not enough identifying columns ({cols_present_for_dup_check}) found to safely drop duplicates.")


    # --- Prepare Dictionaries for Stops/Locations ---
    # These dictionaries will store data structured by Route -> Sequence -> Value
    route_coords_dict = {}
    school_coords_dict = {}
    route_students_dict = {}
    school_names_dict = {}
    school_times_dict = {}

    # Check if Sequence column is valid before using it for dictionary keys
    if 'Sequence' in df.columns and pd.api.types.is_numeric_dtype(df['Sequence']):
        # Process Pupil Stops (Sequence != 0)
        # Ensure columns exist before trying to access them in the loop
        pupil_cols_exist = all(c in df.columns for c in ['Route', 'Sequence', 'Latitude', 'Longitude'])
        if pupil_cols_exist:
            pupil_stops = df[df["Sequence"] != 0].sort_values(by=["Route", "Sequence"])
            for _, row in pupil_stops.iterrows():
                route = row["Route"]
                seq = int(row["Sequence"]) # Should be integer now
                lat = row["Latitude"]
                lon = row["Longitude"]
                route_coords_dict.setdefault(route, {})[seq] = (lat, lon)
                # Safely get Pupil ID if column exists
                if 'Pupil_Id_No' in row:
                    route_students_dict.setdefault(route, {})[seq] = row["Pupil_Id_No"]
        else: print(f"WARN process_optdump ({session_type}): Missing columns needed to process pupil stops.")

        # Process School Stops (Sequence == 0)
        school_cols_exist = all(c in df.columns for c in ['Route', 'Sequence', 'Latitude', 'Longitude'])
        if school_cols_exist:
            school_stops = df[df["Sequence"] == 0].sort_values(by=["Route", "Sess_Beg."]) # Sort schools by session time
            for _, row in school_stops.iterrows():
                route = row["Route"]
                seq = 0 # School stops are identified by Sequence == 0
                lat = row["Latitude"]
                lon = row["Longitude"]
                school_coords_dict.setdefault(route, {})[seq] = (lat, lon)
                # Safely get School Name and Time if columns exist
                if 'School_Code_&_Name' in row:
                    cleaned_school_name = str(row["School_Code_&_Name"]).replace("ARRIVE", "").strip()
                    school_names_dict.setdefault(route, {})[seq] = cleaned_school_name
                if 'Sess_Beg.' in row and pd.notna(row['Sess_Beg.']):
                     school_times_dict.setdefault(route, {})[seq] = row["Sess_Beg."]
        else: print(f"WARN process_optdump ({session_type}): Missing columns needed to process school stops.")

    else:
        print(f"WARN process_optdump ({session_type}): Cannot process stops by sequence due to missing/invalid 'Sequence' column.")
        # Implement alternative logic here if needed, e.g., grouping differently

    # --- Convert Dictionaries to DataFrames & Merge ---
    # Create DataFrames from the dictionaries, handling empty cases
    coords_df = pd.DataFrame(route_coords_dict.items(), columns=["Route", f"{prefix}Student Pickups"]) if route_coords_dict else pd.DataFrame(columns=["Route", f"{prefix}Student Pickups"])
    school_df = pd.DataFrame(school_coords_dict.items(), columns=["Route", f"{prefix}School Locations"]) if school_coords_dict else pd.DataFrame(columns=["Route", f"{prefix}School Locations"])
    students_df = pd.DataFrame(route_students_dict.items(), columns=["Route", f"{prefix}Student IDs"]) if route_students_dict else pd.DataFrame(columns=["Route", f"{prefix}Student IDs"])
    schools_name_df = pd.DataFrame(school_names_dict.items(), columns=["Route", f"{prefix}School Names"]) if school_names_dict else pd.DataFrame(columns=["Route", f"{prefix}School Names"])
    school_times_df = pd.DataFrame(school_times_dict.items(), columns=["Route", f"{prefix}Sess_Beg."]) if school_times_dict else pd.DataFrame(columns=["Route", f"{prefix}Sess_Beg."])

    # Check if any stop data was generated
    if coords_df.empty and school_df.empty:
        print(f"INFO process_optdump ({session_type}): No student or school stops processed into dictionaries.")
        # Return empty DF structure but include Route and Vehicle# column if possible
        result_df = pd.DataFrame({'Route': df['Route'].unique()}) if 'Route' in df.columns else pd.DataFrame(columns=['Route'])
        vehicle_col = f"{prefix}Vehicle#"
        result_df[vehicle_col] = result_df["Route"].map(routes_to_buses)
        result_df.dropna(subset=[vehicle_col], inplace=True)
        # Add empty dict columns expected by format_stops
        result_df[f"{prefix}Student Pickups"] = [{} for _ in range(len(result_df))]
        result_df[f"{prefix}School Locations"] = [{} for _ in range(len(result_df))]
        result_df[f"{prefix}Student IDs"] = [{} for _ in range(len(result_df))]
        result_df[f"{prefix}School Names"] = [{} for _ in range(len(result_df))]
        result_df[f"{prefix}Sess_Beg."] = [{} for _ in range(len(result_df))]
        return result_df if not result_df.empty else None


    # Start merging - use outer merge to keep all routes that have either student or school stops
    final_df = pd.merge(coords_df, school_df, on="Route", how="outer")
    # Merge remaining info using left joins onto the combined stop data
    final_df = pd.merge(final_df, students_df, on="Route", how="left")
    final_df = pd.merge(final_df, schools_name_df, on="Route", how="left")
    final_df = pd.merge(final_df, school_times_df, on="Route", how="left")

    # --- Add Vehicle Numbers ---
    vehicle_col = f"{prefix}Vehicle#"
    # Before mapping, ensure routes_to_buses keys match 'Route' column format/case
    # print(f"DEBUG ({prefix}): Routes in final_df before mapping: {final_df['Route'].unique()}")
    # print(f"DEBUG ({prefix}): Available routes_to_buses keys: {list(routes_to_buses.keys())}")
    final_df[vehicle_col] = final_df["Route"].map(routes_to_buses)

    # --- Final Cleanup ---
    # Drop rows where a vehicle couldn't be mapped from RAS
    rows_before_drop = len(final_df)
    final_df.dropna(subset=[vehicle_col], inplace=True)
    if len(final_df) < rows_before_drop:
         print(f"WARN process_optdump ({session_type}): Dropped {rows_before_drop - len(final_df)} rows without matching vehicle assignment.")

    if final_df.empty:
         print(f"INFO process_optdump ({session_type}): No routes remained after merging and requiring vehicle assignment.")
         return None # Return None if no rows remain

    # Fill NaN values in the dictionary columns with empty dicts for consistency
    dict_cols = [f"{prefix}Student Pickups", f"{prefix}School Locations", f"{prefix}Student IDs", f"{prefix}School Names", f"{prefix}Sess_Beg."]
    for col in dict_cols:
         if col in final_df.columns:
              # Apply only to rows where the value is NaN
              final_df[col] = final_df[col].apply(lambda x: {} if pd.isna(x) else x)
         else:
              # Add column with empty dicts if it was missing entirely (e.g., only school stops)
              final_df[col] = [{} for _ in range(len(final_df))]


    print(f"INFO process_optdump ({session_type}): Processing complete. Result shape: {final_df.shape}")
    return final_df


def process_am_pm(optdump, am_routes_to_buses, pm_routes_to_buses):
    """Processes the optdump DataFrame for both AM and PM sessions."""
    # Ensure routes_to_buses are dictionaries
    if am_routes_to_buses is None: am_routes_to_buses = {}
    if pm_routes_to_buses is None: pm_routes_to_buses = {}

    am_locations_df = process_optdump(optdump, "AM", am_routes_to_buses)
    pm_locations_df = process_optdump(optdump, "PM", pm_routes_to_buses)

    # Ensure results are DataFrames, even if empty, for consistent return type
    if am_locations_df is None: am_locations_df = pd.DataFrame()
    if pm_locations_df is None: pm_locations_df = pd.DataFrame()

    return am_locations_df, pm_locations_df


# --- REMOVED prepare_route_data function ---
# def prepare_route_data(route_row, period):
#     """Convert AM/PM prefixed Series to expected input for plot_route_updated."""
#     ... (Removed as formatting is now done differently) ...