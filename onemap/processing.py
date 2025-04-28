# processing.py
import pandas as pd
# from shapely.geometry import LineString # No longer needed for frontend structure
import config # For DEPOT_LOCS
import traceback # For detailed error logging
import datetime
import pytz
import math

# --- NEW Function: Format GPS Trace ---
def parse_timestamp(ts_input, input_key_name):
    dt = None
    if ts_input is None: return None
    if isinstance(ts_input, datetime.datetime): dt = ts_input
    elif isinstance(ts_input, str):
        if not ts_input.strip(): return None
        try:
            temp_ts_input = ts_input
            if temp_ts_input.endswith('Z'): temp_ts_input = temp_ts_input[:-1] + '+00:00'

            # Process fractional seconds and timezone carefully
            if '.' in temp_ts_input:
                parts = temp_ts_input.split('.', 1) # Split only once
                if len(parts) == 2:
                    base_part = parts[0]
                    # *** FIX: Initialize frac_part and tz_part here ***
                    frac_part = parts[1]
                    tz_part = ""
                    # *************************************************

                    # Check for timezone offset attached to fractional seconds
                    if '+' in frac_part:
                        tz_split = frac_part.split('+', 1)
                        frac_part = tz_split[0]
                        tz_part = '+' + tz_split[1]
                    elif '-' in frac_part:
                        # Find the last '-' to handle potential negative offsets or just date parts
                        last_dash_idx = frac_part.rfind('-')
                        # Check if '-' is present and not the only character
                        if last_dash_idx > -1 and len(frac_part) > 1:
                            # Try to split; assume it's a timezone if the part after '-' looks like one
                            potential_tz = frac_part[last_dash_idx+1:]
                            if len(potential_tz) >= 4 and potential_tz.replace(':','').isdigit():
                                 frac_part = frac_part[:last_dash_idx]
                                 tz_part = '-' + potential_tz
                            # else: assume '-' is part of fractional seconds, leave frac_part as is
                        # else: '-' is not present or is the only char, leave frac_part as is

                    # Truncate fractional seconds AFTER separating timezone
                    if len(frac_part) > 6:
                        frac_part = frac_part[:6] # Truncate to microseconds

                    # Reconstruct the string for parsing
                    temp_ts_input = base_part + '.' + frac_part + tz_part
                # else: No fractional part found after '.', use original string

            # Attempt parsing with potentially modified string
            dt = datetime.datetime.fromisoformat(temp_ts_input)

        except (ValueError, TypeError) as iso_err:
            # Fallback parsing if ISO format fails
            # print(f"WARN: ISO parse failed for {input_key_name} ('{ts_input}'): {iso_err}. Trying pandas parse.") # Noisy
            dt = pd.to_datetime(ts_input, errors='coerce', utc=True)
            if isinstance(dt, pd.Timestamp): dt = dt.to_pydatetime() # Convert NaT or Timestamp
    # Handle pandas Timestamp objects explicitly if input wasn't string/datetime
    elif isinstance(ts_input, pd.Timestamp):
         dt = ts_input.to_pydatetime() # Convert pandas Timestamp to python datetime
    else:
        # Final fallback for other types
        dt = pd.to_datetime(ts_input, errors='coerce', utc=True)
        if isinstance(dt, pd.Timestamp): dt = dt.to_pydatetime()

    # Check final result
    if dt is None or pd.isna(dt):
        # print(f"DEBUG parse_timestamp: Result is None/NaT for {input_key_name} ('{ts_input}')")
        return None

    # Ensure timezone is explicitly UTC
    if dt.tzinfo is None:
        dt = pytz.utc.localize(dt) # Assume UTC if naive
    elif str(dt.tzinfo) != 'UTC':
        dt = dt.astimezone(pytz.utc) # Convert to UTC if it has other timezone

    return dt
    # --- End Timestamp Parsing Function ---


# *** CORRECTED format_gps_trace function ***
def format_gps_trace(vehicle_data_df):
    """
    Converts a DataFrame of vehicle GPS data into a list of GeoJSON Point features,
    ensuring each feature has a 'dateTime' property in ISO format.

    Args:
        vehicle_data_df (pd.DataFrame): DataFrame containing GPS data from Geotab.
                                         Expected columns: 'latitude', 'longitude',
                                         'dateTime', 'speed'.

    Returns:
        list: A list of GeoJSON Point features representing the GPS trace.
              Returns an empty list if the input is invalid or empty.
    """
    trace_features = []
    if vehicle_data_df is None or vehicle_data_df.empty:
        print("WARN format_gps_trace: Input DataFrame is None or empty.")
        return trace_features

    required_cols = ['latitude', 'longitude', 'dateTime'] # Speed is optional but nice
    missing_cols = [col for col in required_cols if col not in vehicle_data_df.columns]
    if 'dateTime' in missing_cols:
         print("ERROR format_gps_trace: Crucial 'dateTime' column is missing. Cannot format trace.")
         return trace_features # Cannot proceed without timestamps
    elif missing_cols:
         print(f"WARN format_gps_trace: Missing optional columns: {missing_cols}.")


    print(f"DEBUG format_gps_trace: Formatting {len(vehicle_data_df)} GPS points into GeoJSON...")

    for index, row in vehicle_data_df.iterrows():
        try:
            lat = float(row['latitude'])
            lon = float(row['longitude'])
            speed = row.get('speed', 0) # Use .get for optional speed column
            timestamp_input = row['dateTime'] # Get the timestamp input

            # Parse and validate the timestamp using the helper function
            timestamp_dt = parse_timestamp(timestamp_input, f"format_gps_trace row[{index}]")

            if timestamp_dt is None:
                 print(f"WARN format_gps_trace: Skipping row {index} due to invalid/unparseable timestamp: '{timestamp_input}'")
                 continue

            # *** FIX: Ensure dateTime is added to properties as ISO string ***
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [lon, lat] # GeoJSON format: [longitude, latitude]
                },
                "properties": {
                    "dateTime": timestamp_dt.isoformat(), # Store as ISO string
                    "speed": speed
                    # Add any other relevant properties from the row here if needed
                    # "original_ts": str(timestamp_input) # Optional: keep original for debug
                }
            }
            trace_features.append(feature)

        except (ValueError, TypeError, KeyError) as e:
            # Catch errors during row processing (e.g., non-numeric lat/lon)
            print(f"WARN format_gps_trace: Skipping row {index} due to error: {e} - Data: {row.to_dict()}")
            continue
        except Exception as e:
             # Catch any other unexpected error during row processing
            print(f"ERROR format_gps_trace: Unexpected error processing row {index}: {e} - Data: {row.to_dict()}")
            continue


    print(f"DEBUG format_gps_trace: Successfully formatted {len(trace_features)} points into GeoJSON.")
    return trace_features
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
                        pupil_id_display = pupil_id
                        if pupil_id != 'N/A' and pupil_id is not None:
                            try:
                                # Convert to float first to handle potential decimals ("12345.0")
                                # Then convert to int to truncate the decimal part
                                pupil_id_int = int(float(pupil_id))
                                pupil_id_display = pupil_id_int # Use the integer if conversion worked
                            except (ValueError, TypeError):
                                pass # Keep original display value
                        stops_list.append({
                            "lat": lat,
                            "lon": lon,
                            "type": "student",
                            "sequence": seq, # Store numeric sequence
                            "info": f"Pickup #: {seq}<br>Pupil ID: {pupil_id_display}" # Example popup info
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
                        sess_beg_time = school_times_dict.get(seq_key) # Get the time value
                        time_str = "N/A" # Default

                        if pd.notna(sess_beg_time):
                            formatted_time = None
                            try:
                                # Check if it's already a datetime.time object (from process_optdump)
                                if isinstance(sess_beg_time, datetime.time):
                                    # Format directly using strftime for time objects
                                    # %I is 12-hour, %M is minute, %p is AM/PM
                                    formatted_time = sess_beg_time.strftime("%I:%M %p")
                                else:
                                    # If not a time object, try converting using pandas
                                    time_dt = pd.to_datetime(sess_beg_time, errors='coerce')
                                    if pd.notna(time_dt):
                                        formatted_time = time_dt.strftime("%I:%M %p")
                                    else:
                                        # If conversion fails, use original string representation
                                        time_str = str(sess_beg_time)

                                # If formatting succeeded, remove leading zero from hour if present
                                if formatted_time:
                                    if formatted_time.startswith('0'):
                                         time_str = formatted_time[1:] # Remove leading zero
                                    else:
                                         time_str = formatted_time

                            except Exception as fmt_err:
                                # Fallback to string representation on any formatting error
                                print(f"WARN format_stops ({period_prefix}): Could not format time '{sess_beg_time}': {fmt_err}")
                                time_str = str(sess_beg_time)

                        # Now time_str holds the desired format "8:00 AM" or similar

                        # The rest of the append logic...
                        stops_list.append({
                            "lat": lat,
                            "lon": lon,
                            "type": "school",
                            "sequence": seq, # Use determined sequence
                            "info": f"School: {school_name}<br>Session Begin: {time_str}" # Use the formatted time_str
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
            school_stops = df[df["Sequence"] == 0].sort_values(by=["Route", "Sess_Beg."])
            # Use row.name (the DataFrame index) as a unique key for each school stop
            for unique_school_key, row in school_stops.iterrows(): # Use index as unique_school_key
                route = row["Route"]
                # seq = 0 # No longer need this hardcoded value for the key
                lat = row["Latitude"]
                lon = row["Longitude"]
                # Use unique_school_key for the dictionary
                school_coords_dict.setdefault(route, {})[unique_school_key] = (lat, lon)
                if 'School_Code_&_Name' in row:
                    cleaned_school_name = str(row["School_Code_&_Name"]).replace("ARRIVE", "").strip()
                    # Use unique_school_key for the dictionary
                    school_names_dict.setdefault(route, {})[unique_school_key] = cleaned_school_name
                if 'Sess_Beg.' in row and pd.notna(row['Sess_Beg.']):
                     # Use unique_school_key for the dictionary
                     school_times_dict.setdefault(route, {})[unique_school_key] = row["Sess_Beg."]
        else:
            print(f"WARN process_optdump ({session_type}): Missing columns needed to process school stops.")


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
def annotate_log_records_with_exceptions(log_records_geojson, raw_exceptions):
    """
    Annotates log records with exception details, calculating max speed for speeding events.
    """
    print(f"Starting annotation: {len(log_records_geojson)} logs, {len(raw_exceptions)} exceptions.")
    # Initial checks and default annotation setup (same as before)
    if not log_records_geojson: return []
    if not raw_exceptions:
        print("Annotation skipped: No exceptions provided. Adding default annotations to logs.")
        for feature in log_records_geojson:
            if 'properties' not in feature: feature['properties'] = {}
            feature['properties']['exception_type'] = '--'; feature['properties']['exception_details'] = '--'
        return log_records_geojson

    # --- 1. Pre-parse Log Records for efficient lookup ---
    print("Pre-parsing log records...")
    parsed_logs = []
    for i, feature in enumerate(log_records_geojson):
        props = feature.get('properties', {})
        log_dt_str = props.get('dateTime')
        log_dt = parse_timestamp(log_dt_str, f"Log[{i}]")
        speed_kph = None
        try:
             # Attempt to convert speed, handle None or non-numeric safely
             raw_speed = props.get('speed')
             if raw_speed is not None and not isinstance(raw_speed, str): # Avoid converting strings like 'N/A'
                  speed_kph = float(raw_speed)
                  if math.isnan(speed_kph): # Check for NaN explicitly
                      speed_kph = 0.0
             else:
                  speed_kph = 0.0 # Default if missing or non-numeric
        except (ValueError, TypeError):
             speed_kph = 0.0 # Default on conversion error

        if log_dt: # Only add if timestamp is valid
             parsed_logs.append({'index': i, 'dt': log_dt, 'speed_kph': speed_kph})
        # else: print(f"DEBUG: Skipping log {i} due to invalid timestamp during pre-parsing.")

    if not parsed_logs:
        print("WARN: No valid log records found after pre-parsing timestamps. Cannot annotate.")
        for feature in log_records_geojson: # Still add default annotations
             if 'properties' not in feature: feature['properties'] = {}
             feature['properties']['exception_type'] = '--'; feature['properties']['exception_details'] = '--'
        return log_records_geojson

    # Sort parsed logs by time (optional but can help efficiency later)
    # parsed_logs.sort(key=lambda x: x['dt'])
    print(f"Pre-parsed {len(parsed_logs)} valid log records.")


    # --- 2. Pre-process Exceptions & Calculate Max Speed for Speeding ---
    print("Processing raw exception data and calculating max speeds...")
    processed_exceptions = []
    rule_key = 'rule_name'; start_key = 'start_time'; end_key = 'end_time'; duration_key = 'duration_s'

    for i, exc in enumerate(raw_exceptions):
        if not isinstance(exc, dict): continue
        try:
            rule_name = exc.get(rule_key, 'Unknown Exception')
            start_dt = parse_timestamp(exc.get(start_key), f"Exc[{i}].{start_key}")
            end_dt = parse_timestamp(exc.get(end_key), f"Exc[{i}].{end_key}")

            if start_dt is None or end_dt is None: continue # Skip if times invalid
            if start_dt > end_dt: start_dt, end_dt = end_dt, start_dt # Swap if needed

            duration_val = exc.get(duration_key)
            duration_sec = None
            try:
                 if duration_val is not None and isinstance(duration_val, (int, float)) and not pd.isna(duration_val): duration_sec = float(duration_val)
                 else: duration_sec = (end_dt - start_dt).total_seconds()
            except (ValueError, TypeError): duration_sec = (end_dt - start_dt).total_seconds()

            max_speed_kph = -1.0 # Initialize for max speed calculation
            is_speeding_event = 'speeding' in rule_name.lower()

            # Calculate Max Speed *only* if it's a speeding event
            if is_speeding_event:
                logs_in_event = 0
                for log in parsed_logs:
                    # Check if log time falls within the exception range
                    if start_dt <= log['dt'] <= end_dt:
                        logs_in_event += 1
                        max_speed_kph = max(max_speed_kph, log['speed_kph'])
                # print(f"DEBUG: Speeding Exc[{i}]: Found {logs_in_event} logs in range. Max KPH: {max_speed_kph}") # Debug print

            # Convert max speed to MPH (only if found)
            max_speed_mph = None
            if max_speed_kph >= 0: # Check if max_speed was updated
                 max_speed_mph = int(round(max_speed_kph * 0.621371))

            # Format details string
            details = f"Type: {rule_name}<br>Duration: {duration_sec:.1f} sec"
            if is_speeding_event and max_speed_mph is not None:
                details += f"<br>Maximum Speed: {max_speed_mph} MPH" # Add max speed info

            processed_exceptions.append({
                'start': start_dt, 'end': end_dt, 'type': rule_name,
                'details': details # Store the potentially enhanced details
            })
        except Exception as e:
            print(f"WARN: Unexpected error processing exception at index {i}: {e} - Data: {exc}")
            continue

    if not processed_exceptions:
         print("WARN: No valid exceptions processed after parsing. Adding default annotations to logs.")
         # Add default annotations (same as initial check)
         for feature in log_records_geojson:
             if 'properties' not in feature: feature['properties'] = {}
             feature['properties']['exception_type'] = '--'; feature['properties']['exception_details'] = '--'
         return log_records_geojson

    print(f"Processed {len(processed_exceptions)} valid exceptions. Annotating log records...")

    # --- 3. Annotate Log Records using Processed Exceptions ---
    match_count = 0
    # Use the pre-parsed log list for efficiency
    for log_info in parsed_logs:
        log_index = log_info['index']
        log_dt = log_info['dt']
        feature = log_records_geojson[log_index] # Get the original GeoJSON feature

        # Ensure properties dict exists and set defaults
        if 'properties' not in feature: feature['properties'] = {}
        feature['properties']['exception_type'] = '--'
        feature['properties']['exception_details'] = '--'

        matched_exception = None
        try:
            # Check against all processed exceptions
            for exc in processed_exceptions:
                if exc['start'] <= log_dt <= exc['end']:
                    # Prioritization logic (same as before)
                    current_priority = -1; new_priority = 0
                    exc_type_lower = exc['type'].lower()
                    if 'speeding' in exc_type_lower: new_priority = 2
                    elif 'idling' in exc_type_lower or 'idle' in exc_type_lower: new_priority = 0
                    else: new_priority = 1
                    if matched_exception is not None:
                        matched_type_lower = matched_exception['type'].lower()
                        if 'speeding' in matched_type_lower: current_priority = 2
                        elif 'idling' in matched_type_lower or 'idle' in matched_type_lower: current_priority = 0
                        else: current_priority = 1
                    else: current_priority = -1
                    if new_priority > current_priority: matched_exception = exc

            # Annotate if a match was found
            if matched_exception:
                # Copy the type and pre-formatted details (which includes max speed if applicable)
                feature['properties']['exception_type'] = matched_exception['type']
                feature['properties']['exception_details'] = matched_exception['details']
                match_count += 1

        except Exception as e:
            print(f"WARN: Error processing log record during annotation (Index {log_index}): {e}")
            # Reset properties on error
            feature['properties']['exception_type'] = '--'; feature['properties']['exception_details'] = '--'

    # Add default annotations for logs that were skipped during pre-parsing (if any)
    # This loop is likely redundant if pre-parsing handles all logs, but safe to keep
    for feature in log_records_geojson:
         if 'properties' not in feature: feature['properties'] = {}
         if 'exception_type' not in feature['properties']: # Check if annotation was missed
              feature['properties']['exception_type'] = '--'; feature['properties']['exception_details'] = '--'


    print(f"Annotation process finished. Annotated {match_count} log points based on exception ranges.")
    return log_records_geojson