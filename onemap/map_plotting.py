# map_plotting.py
import folium
import pandas as pd
# Make sure shapely LineString is importable
# If you haven't installed shapely: pip install Shapely
from shapely.geometry import LineString
from collections import defaultdict
import datetime
import pytz
from datetime import timedelta
# Import folium.features if CustomIcon is used elsewhere, otherwise not needed for this func
# from folium.features import CustomIcon


def plot_route_updated(route_data, vehicle_data, polyline, mapbox_token):
    """
    Generates a Folium map for a specific route using provided data structures,
    with added debugging prints.

    Args:
        route_data (pd.Series or dict): Data for a single route, containing keys like
                                'Route', 'Vehicle#', 'Student Pickups' (dict),
                                'School Locations' (dict), 'Student Ids' (dict),
                                'School Names' (dict), 'Sess_Beg.' (dict).
        vehicle_data (pd.DataFrame | None): Optional DataFrame with vehicle GPS points.
                                           Needs 'latitude', 'longitude', 'dateTime'.
        polyline (shapely.geometry.LineString | None): Pre-calculated polyline for the route.
        mapbox_token (str | None): Mapbox access token for custom tiles.

    Returns:
        folium.Map | None: A Folium map object or None if essential data is invalid.
    """
    # --- Input Validation ---
    if not isinstance(route_data, (pd.Series, dict)):
        print(f"ERROR: Invalid route_data type provided: {type(route_data)}")
        return None
    if isinstance(route_data, pd.Series) and route_data.empty:
        print("ERROR: Empty route_data Series provided.")
        return None
    if isinstance(route_data, dict) and not route_data:
        print("ERROR: Empty route_data dict provided.")
        return None

    # Check for essential dictionary-like keys needed for plotting stops & info
    # Note: Using .get() method below for safer access from dict/Series
    stop_keys = ['Student Pickups', 'School Locations']
    info_keys = ['Student Ids', 'School Names', 'Sess_Beg.']
    required_keys = ['Route', 'Vehicle#'] + stop_keys + info_keys

    # Check if all required keys exist (adjust if using dict vs Series)
    if isinstance(route_data, pd.Series):
        if not all(key in route_data.index for key in required_keys):
            missing = [
                key for key in required_keys if key not in route_data.index
            ]
            print(
                f"ERROR: route_data Series is missing required keys: {missing}"
            )
            return None
    elif isinstance(route_data, dict):
        if not all(key in route_data for key in required_keys):
            missing = [key for key in required_keys if key not in route_data]
            print(
                f"ERROR: route_data dict is missing required keys: {missing}")
            return None

    # Extract data safely using .get() for dictionaries or direct access for Series (already checked)
    try:
        route_id = route_data['Route']
        vehicle = route_data['Vehicle#']
        # Default to empty dict if data is missing, not a dict, or NaN (for Series)
        student_pickups = route_data['Student Pickups'] if isinstance(
            route_data['Student Pickups'], dict) else {}
        school_locations = route_data['School Locations'] if isinstance(
            route_data['School Locations'], dict) else {}
        student_ids = route_data['Student Ids'] if isinstance(
            route_data['Student Ids'], dict) else {}
        school_names = route_data['School Names'] if isinstance(
            route_data['School Names'], dict) else {}
        sess_beg_times = route_data['Sess_Beg.'] if isinstance(
            route_data['Sess_Beg.'], dict) else {}
        # Handle potential NaN from pandas Series if keys exist but value is missing
        if pd.isna(student_pickups): student_pickups = {}
        if pd.isna(school_locations): school_locations = {}
        if pd.isna(student_ids): student_ids = {}
        if pd.isna(school_names): school_names = {}
        if pd.isna(sess_beg_times): sess_beg_times = {}

    except KeyError as e:
        print(f"ERROR: Failed to access expected key in route_data: {e}")
        return None
    except Exception as e:
        print(f"ERROR: Unexpected error extracting data from route_data: {e}")
        return None

    # --- Calculate Map Center ---
    all_coords = list(student_pickups.values()) + list(
        school_locations.values())
    valid_coords = [
        coord for coord in all_coords
        if isinstance(coord, (list, tuple)) and len(coord) == 2
    ]
    avg_lat, avg_lon = 40.7128, -74.0060  # Default coordinates (e.g., NYC)

    if valid_coords:
        avg_lat = sum(lat for lat, lon in valid_coords) / len(valid_coords)
        avg_lon = sum(lon for lat, lon in valid_coords) / len(valid_coords)
        print(
            f"INFO: Centering on average of {len(valid_coords)} stop locations for Route {route_id}."
        )
    elif polyline and not polyline.is_empty:
        center_point = polyline.centroid
        avg_lat, avg_lon = center_point.y, center_point.x
        print(
            f"INFO: No valid stops; centering on polyline centroid for Route {route_id}."
        )
    elif isinstance(
            vehicle_data, pd.DataFrame
    ) and not vehicle_data.empty and 'latitude' in vehicle_data.columns:
        avg_lat = vehicle_data['latitude'].iloc[0]
        avg_lon = vehicle_data['longitude'].iloc[0]
        print(
            f"INFO: No valid stops or polyline; centering on first vehicle data point for Route {route_id}."
        )
    else:
        print(
            f"WARNING: No valid coordinates found for Route {route_id}. Using default center."
        )

    # --- START ADDED DEBUG PRINTS ---
    print("\n--- Debugging plot_route_updated ---")
    print(f"Route: {route_id}, Vehicle: {vehicle}")

    # Check the received vehicle data
    print("DEBUG Plotting: vehicle_data type:", type(vehicle_data))
    if isinstance(vehicle_data, pd.DataFrame) and not vehicle_data.empty:
        print(f"DEBUG Plotting: vehicle_data shape: {vehicle_data.shape}")
        print(
            f"DEBUG Plotting: vehicle_data columns: {vehicle_data.columns.tolist()}"
        )
        # Check if essential columns exist before trying to access head()
        essential_gps_cols = ['latitude', 'longitude', 'dateTime']
        if all(col in vehicle_data.columns for col in essential_gps_cols):
            print(
                f"DEBUG Plotting: vehicle_data head:\n{vehicle_data.head().to_string()}"
            )
        else:
            missing_gps_cols = [
                col for col in essential_gps_cols
                if col not in vehicle_data.columns
            ]
            print(
                f"DEBUG Plotting: vehicle_data missing essential columns: {missing_gps_cols}"
            )
    else:
        print("DEBUG Plotting: vehicle_data is None or empty.")

    # Check the received polyline object
    print(f"DEBUG Plotting: polyline object type: {type(polyline)}")
    print(f"DEBUG Plotting: polyline object representation: {repr(polyline)}")
    is_valid_polyline = (polyline is not None
                         and isinstance(polyline, LineString)
                         and not polyline.is_empty)
    print(
        f"DEBUG Plotting: Is polyline valid for plotting? {is_valid_polyline}")
    # --- END ADDED DEBUG PRINTS ---

    # --- Create Map ---
    m = folium.Map(location=[avg_lat, avg_lon],
                   zoom_start=12,
                   tiles="CartoDB positron")

    # Add Mapbox Layer if token provided
    if mapbox_token:
        try:
            folium.TileLayer(
                tiles=
                f"https://api.mapbox.com/styles/v1/vr00n-nycsbus/clyyoiorc00uu01pe8ttggvhd/tiles/256/{{z}}/{{x}}/{{y}}@2x?access_token={mapbox_token}",
                attr="Mapbox",
                name="Custom Mapbox Style",
                overlay=False,
                control=True).add_to(m)
            folium.LayerControl(position='topright', collapsed=False).add_to(m)
        except Exception as tile_err:
            print(f"WARNING: Failed to add Mapbox tile layer: {tile_err}")
            # Map will still be created with default tiles
    else:
        print("WARNING: No Mapbox token provided; using default map tiles.")

    # --- Helper Functions (defined inside or outside depending on preference) ---
    def offset_duplicates(coord_dict, offset_amount=0.0001):
        """Slightly offsets duplicate coordinates within a dictionary {index: (lat, lon)}."""
        seen = defaultdict(int)
        updated_dict = {}
        if not isinstance(coord_dict, dict): return {}  # Handle non-dict input
        for index, loc in coord_dict.items():
            if not (isinstance(loc, (list, tuple)) and len(loc) == 2):
                continue  # Skip invalid locations
            lat, lon = loc
            coord_key = (round(lat, 6), round(lon, 6))
            count = seen[coord_key]
            if count > 0:
                angle = (count // 2) * (3.14159 / 4)
                direction = 1 if (count % 2 == 1) else -1
                lat += direction * offset_amount * (count % 4 + 1) * 0.707
                lon += direction * offset_amount * (count % 4 + 1) * 0.707
            seen[coord_key] += 1
            updated_dict[index] = (lat, lon)
        return updated_dict

    def create_numbered_marker(map_obj, lat, lon, number, popup_content,
                               color):
        """Creates a numbered circular marker and adds it to the map."""
        icon_html = f"""
        <div style="font-size: 10pt; color: white; font-weight: bold; text-align:center;
                    width:24px; height:24px; line-height:24px; background:{color};
                    border-radius:50%; border: 1px solid #FFFFFF; display:inline-block;">
            {number}
        </div>
        """
        icon = folium.DivIcon(html=icon_html)
        try:
            folium.Marker(location=[lat, lon],
                          icon=icon,
                          popup=folium.Popup(popup_content,
                                             max_width=300)).add_to(map_obj)
        except Exception as marker_err:
            print(
                f"WARNING: Failed to add marker {number} at [{lat}, {lon}]: {marker_err}"
            )

    # --- Plot Student Pickups & School Locations (Markers) ---
    student_pickups_offset = offset_duplicates(student_pickups)
    school_locations_offset = offset_duplicates(school_locations)

    print(f"INFO: Plotting {len(student_pickups_offset)} pickup markers...")
    for num, loc in student_pickups_offset.items():
        if not (isinstance(loc, (list, tuple)) and len(loc) == 2):
            continue  # Skip invalid locations
        lat, lon = loc
        student_id = student_ids.get(num, "N/A")
        popup_content = f"<b>Pickup #:</b> {num}<br><b>Student ID:</b> {student_id}"
        create_numbered_marker(m, lat, lon, num, popup_content, "blue")

    print(f"INFO: Plotting {len(school_locations_offset)} school markers...")
    for num, loc in school_locations_offset.items():
        if not (isinstance(loc, (list, tuple)) and len(loc) == 2): continue
        lat, lon = loc
        school_name = school_names.get(num, "N/A")
        sess_beg = sess_beg_times.get(num, "N/A")
        popup_content = f"<b>School Stop #:</b> {num}<br><b>School Name:</b> {school_name}<br><b>Session Begin:</b> {sess_beg}"
        create_numbered_marker(m, lat, lon, num, popup_content, "red")

    # --- Add Polyline from Input ---
    all_points_for_bounds = []
    all_points_for_bounds.extend(list(student_pickups_offset.values()))
    all_points_for_bounds.extend(list(school_locations_offset.values()))

    print("DEBUG Plotting: Attempting to add polyline...")  # Added debug line
    if is_valid_polyline:  # Use the flag checked earlier
        try:
            # Polyline coords are (lon, lat). Folium needs (lat, lon).
            if hasattr(polyline, 'coords'):
                # Ensure coordinates are valid numbers before conversion
                valid_shapely_coords = [(lat, lon)
                                        for lon, lat in polyline.coords
                                        if isinstance(lon, (int, float))
                                        and isinstance(lat, (int, float))]

                if len(valid_shapely_coords) < 2:
                    print(
                        "DEBUG Plotting: Polyline has fewer than 2 valid numeric coordinate pairs."
                    )
                else:
                    # Convert valid (lon, lat) pairs to (lat, lon) for Folium
                    polyline_coords_for_map = [
                        (lat, lon) for lon, lat in valid_shapely_coords
                    ]

                    print(
                        f"DEBUG Plotting: Adding folium.PolyLine with {len(polyline_coords_for_map)} valid points."
                    )
                    print(
                        f"DEBUG Plotting: First 5 polyline coords for map: {polyline_coords_for_map[:5]}"
                    )  # Print sample coords

                    folium.PolyLine(
                        locations=polyline_coords_for_map,
                        color="grey",
                        weight=3,
                        opacity=0.7,
                    ).add_to(m)
                    # Add these points for bounds fitting as well
                    all_points_for_bounds.extend(polyline_coords_for_map)
            else:
                print(
                    "DEBUG Plotting: Polyline object does not have 'coords' attribute."
                )
        except Exception as poly_err:
            print(
                f"WARNING: Error processing or adding polyline to map: {poly_err}"
            )
            import traceback
            traceback.print_exc()  # Print full traceback for polyline errors
    else:
        print("DEBUG Plotting: Skipping polyline addition (invalid or empty).")

    # --- Add Vehicle Hover Trail ---
    print("DEBUG Plotting: Attempting to add vehicle hover trail..."
          )  # Added debug line
    if isinstance(
            vehicle_data, pd.DataFrame
    ) and not vehicle_data.empty and 'latitude' in vehicle_data.columns and 'longitude' in vehicle_data.columns and 'dateTime' in vehicle_data.columns:
        print(
            f"DEBUG Plotting: Adding hover trail for {len(vehicle_data)} vehicle data points..."
        )
        # Ensure coordinate columns are numeric
        vehicle_data['latitude'] = pd.to_numeric(vehicle_data['latitude'],
                                                 errors='coerce')
        vehicle_data['longitude'] = pd.to_numeric(vehicle_data['longitude'],
                                                  errors='coerce')
        vehicle_data.dropna(subset=['latitude', 'longitude'], inplace=True)

        for _, row in vehicle_data.iterrows():
            try:
                lat = row["latitude"]
                lon = row["longitude"]
                # Safely get speed, default to 0 if missing or not convertible
                speed_kph = pd.to_numeric(row.get("speed", 0), errors='coerce')
                speed_kph = speed_kph if pd.notna(speed_kph) else 0
                speed_mph = round(speed_kph / 1.60934, 1)

                # Ensure dateTime is a datetime object before formatting
                timestamp = row["dateTime"]
                if not isinstance(timestamp, datetime.datetime):
                    timestamp = pd.to_datetime(
                        timestamp, errors='coerce')  # Attempt conversion

                if pd.notna(timestamp):
                    # Adjust timezone if necessary (example: convert UTC to Eastern)
                    try:
                        eastern = pytz.timezone(
                            'America/New_York')  # Import pytz if using TZs
                        if timestamp.tzinfo is None:
                            timestamp = pytz.utc.localize(
                                timestamp)  # Assume UTC if naive
                        timestamp_local = timestamp.astimezone(eastern)
                        timestamp_str = timestamp_local.strftime(
                            "%I:%M %p")  # Format as HH:MM AM/PM
                    except Exception as tz_err:
                        print(
                            f"WARN: Timezone conversion/formatting failed: {tz_err}. Using original."
                        )
                        timestamp_str = str(timestamp)  # Fallback
                else:
                    timestamp_str = "N/A"  # Handle failed conversion

                tooltip_text = f"<b>{vehicle} - Route {route_id}</b><br>Driving {speed_mph} mph at {timestamp_str}"

                folium.CircleMarker(
                    location=(lat, lon),
                    radius=4,
                    color='transparent',
                    fill=True,
                    fill_opacity=0,  # Invisible marker, only tooltip matters
                    tooltip=tooltip_text).add_to(m)
            except Exception as hover_err:
                # Print warning but continue loop for other points
                print(
                    f"WARNING: Could not plot hover marker for row: {row.to_dict()}. Error: {hover_err}"
                )
    else:
        print(
            "DEBUG Plotting: Skipping vehicle hover trail (no valid vehicle data or missing columns)."
        )

    # --- Fit Bounds to All Plotted Elements ---
    valid_points_for_bounds = [
        p for p in all_points_for_bounds
        if isinstance(p, (list, tuple)) and len(p) == 2
        and isinstance(p[0], (int, float)) and isinstance(p[1], (
            int, float)) and pd.notna(p[0]) and pd.notna(p[1])
    ]  # Ensure non-NaN numeric coords

    if len(set(map(
            tuple,
            valid_points_for_bounds))) >= 2:  # Need at least 2 distinct points
        try:
            print(
                f"INFO: Fitting map bounds to {len(valid_points_for_bounds)} valid points..."
            )
            m.fit_bounds(bounds=valid_points_for_bounds,
                         padding=(0.05, 0.05))  # Slightly less padding
        except Exception as bounds_error:
            print(f"WARNING: Could not fit map bounds: {bounds_error}")
    elif len(valid_points_for_bounds) == 1:
        print("INFO: Only one valid point found, centering map on it...")
        m.location = valid_points_for_bounds[0]
        m.zoom_start = 15
    else:
        print(
            "WARNING: No valid points found to fit map bounds. Map remains centered as initially calculated."
        )

    print(f"INFO: Map plotting complete for Route {route_id}.")
    # Return the map object
    return m
