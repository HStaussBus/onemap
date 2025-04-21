# processing.py
import pandas as pd
from shapely.geometry import LineString
import config # For DEPOT_LOCS

def convert_to_polyline(df):
    """Converts DataFrame points to a Shapely LineString."""
    # (Keep original implementation)
    if not isinstance(df, pd.DataFrame) or df.empty:
        # print("INFO: Cannot create polyline from empty or invalid DataFrame.")
        return None
    if 'latitude' not in df.columns or 'longitude' not in df.columns:
        print("ERROR: DataFrame missing 'latitude' or 'longitude' columns for polyline.")
        return None
    if len(df) < 2:
        # print("INFO: Need at least two points to create a polyline.")
        return None # Return None instead of raising error if 0/1 points

    try:
        # Ensure correct data types
        points = list(zip(df['latitude'].astype(float), df['longitude'].astype(float)))
        polyline = LineString(points)
        return polyline
    except Exception as e:
        print(f"ERROR: Failed to create polyline: {e}")
        return None


def add_depot_coords(rasdf):
    """Adds 'Depot Coords' column to RAS DataFrame."""
    # (Keep original implementation, get depotlocs from config)
    # Determine which yard column exists
    yard_col = None
    if "Assigned Pullout Yard" in rasdf.columns: yard_col = "Assigned Pullout Yard"
    elif "GM | Yard" in rasdf.columns: yard_col = "GM | Yard"
    else:
        print("WARNING: No yard column found in rasdf")
        rasdf["Depot Coords"] = None
        return rasdf

    depotlocs = config.DEPOT_LOCS # Use locations from config

    # Function to map yard string to depot coords
    def map_depot_coords(yard_value):
        if pd.isna(yard_value): return None
        for depot_name, coords in depotlocs.items():
            if depot_name.lower() in str(yard_value).lower(): return coords
        return None

    rasdf["Depot Coords"] = rasdf[yard_col].apply(map_depot_coords)
    return rasdf

def process_optdump(optdump, session_type, routes_to_buses):
    """Processes the optdump DataFrame for AM or PM sessions."""
    # (Keep original implementation)
    if session_type not in ["AM", "PM"]:
        raise ValueError("session_type must be 'AM' or 'PM'")

    prefix = f"{session_type.lower()}_"
    df = optdump.copy()

    # Filter for AM or PM
    if session_type == "AM": df = df[df['am_pm'] != 'PM Only']
    else: df = df[df['am_pm'] != 'AM Only']

    if df.empty:
        print(f"INFO: No data for {session_type} session after filtering.")
        return None

    # Clean and filter data
    df = df[~df['address'].str.contains("SEE OPERATIONS", case=False, na=False)]
    # Rename columns carefully, check if they exist first
    rename_map = {'address': 'Address', 'route': 'Route', 'pupil_id_no': 'Pupil_Id_No'}
    cols_to_rename = {k:v for k,v in rename_map.items() if k in df.columns}
    df.rename(columns=cols_to_rename, inplace=True)

    if 'School_Code_&_Name' in df.columns:
        df = df[~df['School_Code_&_Name'].str.contains("DISMISS", case=False, na=False)]

    if 'pupil_lat' not in df.columns or 'pupil_lon' not in df.columns:
        print("ERROR: Missing pupil lat/lon columns in OPT dump.")
        return None # Cannot proceed without coordinates

    df = df[df["pupil_lat"].notna() & (df["pupil_lat"] != "")] # Ensure non-empty strings too
    # Convert coordinates, handle potential errors
    try:
        df['Latitude'] = pd.to_numeric(df['pupil_lat'], errors='coerce')
        df['Longitude'] = pd.to_numeric(df['pupil_lon'], errors='coerce')
        df.dropna(subset=['Latitude', 'Longitude'], inplace=True) # Drop rows where conversion failed
        if df.empty:
             print(f"INFO: No valid pupil coordinates found after conversion for {session_type}.")
             return None
    except Exception as e:
        print(f"ERROR: Failed converting pupil coordinates: {e}")
        return None

    # Handle datetime conversions carefully
    if 'sess_beg' in df.columns:
        df['Sess_Beg.'] = pd.to_datetime(df['sess_beg'], errors='coerce')
    if 'sess_end' in df.columns:
        df['Sess_End'] = pd.to_datetime(df['sess_end'], errors='coerce')
    if 'seg_no' in df.columns:
        df['Sequence'] = pd.to_numeric(df['seg_no'], errors='coerce')
    else:
         print("WARNING: 'seg_no' column not found, cannot determine sequence.")
         # Decide how to handle - maybe return None or proceed without sequence?
         df['Sequence'] = 0 # Assign default sequence if missing

    # Drop duplicates (ensure required columns exist)
    required_dup_cols = ['Route', 'Pupil_Id_No', 'School_Code_&_Name']
    if all(col in df.columns for col in required_dup_cols):
        df = df.sort_values(by=["Route", "Pupil_Id_No"]).drop_duplicates(
            subset=required_dup_cols, keep="first"
        )

    # Prepare dictionaries (rest of the logic seems okay, relies on renamed columns)
    route_coords_dict = {}
    school_coords_dict = {}
    route_students_dict = {}
    school_names_dict = {}
    school_times_dict = {}

    # Check if Sequence column is valid before using it
    if 'Sequence' in df.columns and pd.api.types.is_numeric_dtype(df['Sequence']):
        pupil_stops = df[df["Sequence"] != 0].sort_values(by=["Route", "Sequence"])
        for _, row in pupil_stops.iterrows():
            route = row["Route"]
            seq = row["Sequence"]
            route_coords_dict.setdefault(route, {})[seq] = (row["Latitude"], row["Longitude"])
            if 'Pupil_Id_No' in row: route_students_dict.setdefault(route, {})[seq] = row["Pupil_Id_No"]

        school_stops = df[df["Sequence"] == 0].sort_values(by=["Route", "Sess_Beg."])
        for _, row in school_stops.iterrows():
            route = row["Route"]
            seq = max(school_coords_dict.get(route, {}).keys(), default=0) + 1 # Assume seq=0 are schools
            school_coords_dict.setdefault(route, {})[seq] = (row["Latitude"], row["Longitude"])
            if 'School_Code_&_Name' in row:
                cleaned_school_name = str(row["School_Code_&_Name"]).replace("ARRIVE", "").strip()
                school_names_dict.setdefault(route, {})[seq] = cleaned_school_name
            if 'Sess_Beg.' in row: school_times_dict.setdefault(route, {})[seq] = row["Sess_Beg."]

    else:
        print("WARNING: Cannot process stops by sequence due to missing/invalid 'Sequence' column.")
        # Implement alternative logic if needed, e.g., process all non-zero seg_no as student stops

    # Convert dictionaries to DataFrames & Merge (rest of the logic is okay)
    coords_df = pd.DataFrame(route_coords_dict.items(), columns=["Route", f"{prefix}Student Pickups"])
    school_df = pd.DataFrame(school_coords_dict.items(), columns=["Route", f"{prefix}School Locations"])
    students_df = pd.DataFrame(route_students_dict.items(), columns=["Route", f"{prefix}Student IDs"])
    schools_name_df = pd.DataFrame(school_names_dict.items(), columns=["Route", f"{prefix}School Names"])
    school_times_df = pd.DataFrame(school_times_dict.items(), columns=["Route", f"{prefix}Sess_Beg."])

    # Start with coords_df and merge others onto it
    if coords_df.empty and not school_df.empty: # Handle cases where only school stops exist
        final_df = school_df
    elif not coords_df.empty:
        final_df = coords_df
        if not school_df.empty: final_df = final_df.merge(school_df, on="Route", how="outer") # Use outer if needed
    else: # Both are empty
        print(f"INFO: No student or school stops processed for {session_type}.")
        return pd.DataFrame(columns=["Route"]) # Return empty DF with at least 'Route'

    # Merge remaining info, using how='left' or 'outer' depending on requirements
    if not students_df.empty: final_df = final_df.merge(students_df, on="Route", how="left")
    if not schools_name_df.empty: final_df = final_df.merge(schools_name_df, on="Route", how="left")
    if not school_times_df.empty: final_df = final_df.merge(school_times_df, on="Route", how="left")

    # Add vehicle numbers and drop rows without a vehicle (last part is okay)
    vehicle_col = f"{prefix}Vehicle#"
    final_df[vehicle_col] = final_df["Route"].map(routes_to_buses)
    final_df.dropna(subset=[vehicle_col], inplace=True)

    # Inside processing.py -> process_optdump function
    # ... right before this line:
    # final_df[f"{prefix}Vehicle#"] = final_df["Route"].map(routes_to_buses)
    # Add these:
    print(f"DEBUG ({prefix}): Routes in final_df before mapping: {final_df['Route'].unique()}")
    print(f"DEBUG ({prefix}): Available routes_to_buses keys: {list(routes_to_buses.keys())}")
    # Now the mapping line:
    final_df[f"{prefix}Vehicle#"] = final_df["Route"].map(routes_to_buses)
    # ... rest of the function ...

    if final_df.empty:
         print(f"INFO: No routes matched vehicle assignments for {session_type}.")
         return None # Return None if no rows remain after vehicle mapping

    return final_df


def process_am_pm(optdump, am_routes_to_buses, pm_routes_to_buses):
    """Processes the optdump DataFrame for both AM and PM sessions."""
    # (Keep original implementation)
    am_locations_df = process_optdump(optdump, "AM", am_routes_to_buses)
    pm_locations_df = process_optdump(optdump, "PM", pm_routes_to_buses)
    return am_locations_df, pm_locations_df


def prepare_route_data(route_row, period):
    """Convert AM/PM prefixed Series to expected input for plot_route_updated."""
    # (Keep original implementation)
    suffix = f"{period}_"
    rename_cols = {
        f"{suffix}Student Pickups": "Student Pickups", f"{suffix}School Locations": "School Locations",
        f"{suffix}Student IDs": "Student Ids", f"{suffix}School Names": "School Names",
        f"{suffix}Sess_Beg.": "Sess_Beg.", f"{suffix}Sess_End": "Sess_End",
        f"{suffix}Vehicle#": "Vehicle#"
    }
    # Only rename keys that actually exist in the input Series/dict
    keys_to_rename = {k: v for k, v in rename_cols.items() if k in route_row}
    # Create a new dict/Series or modify in place depending on input type
    if isinstance(route_row, pd.Series):
         return route_row.rename(keys_to_rename)
    elif isinstance(route_row, dict):
         renamed_dict = route_row.copy()
         for old_key, new_key in keys_to_rename.items():
             renamed_dict[new_key] = renamed_dict.pop(old_key)
         return renamed_dict
    else:
         print("WARNING: Invalid type passed to prepare_route_data, expected Series or dict.")
         return route_row # Return original if type is wrong