# map_plotting.py
import folium
import pandas as pd
from shapely.geometry import LineString
from collections import defaultdict
from datetime import timedelta

def plot_route_updated(route_data, vehicle_data, polyline, mapbox_token):
    """Generates a Folium map for a specific route."""
    # (Keep original implementation, ensure mapbox_token is passed and used)
    # Ensure mapbox_token is checked/used for the TileLayer
    if not mapbox_token:
         print("ERROR: Mapbox token not provided for plotting.")
         # Decide fallback: use default tiles or return None?
         # return None # Option 1: Fail if no token
         # Option 2: Use default tiles (remove Mapbox TileLayer line)
         pass # Assuming default tiles will be used if token is missing

    # --- Input Validation --- (Copied and refined from original)
    # ... (Keep the validation logic for route_data, stop_cols, info_cols, required_keys) ...
    if isinstance(route_data, pd.DataFrame):
      if route_data.empty: print("ERROR: Empty route_data DataFrame."); return None
    elif isinstance(route_data, pd.Series):
        if route_data.empty: print("ERROR: Empty route_data Series."); return None
    else: print(f"ERROR: Invalid route_data type: {type(route_data)}"); return None

    stop_cols = ['Student Pickups', 'School Locations']
    info_cols = ['Student Ids', 'School Names', 'Sess_Beg.']
    required_keys = ['Route', 'Vehicle#'] + stop_cols + info_cols # Check against the *renamed* keys

    if not all(key in route_data for key in required_keys):
        missing = [key for key in required_keys if key not in route_data]
        print(f"ERROR: route_data is missing required keys after preparation: {missing}")
        return None
    # ... (rest of data extraction, map centering, marker plotting, polyline plotting) ...

    # --- Create Map ---
    # ... (Calculate avg_lat, avg_lon as before) ...
    avg_lat, avg_lon = 40.7128, -74.0060 # Default center
    # ... (calculation logic from original) ...

    m = folium.Map(location=[avg_lat, avg_lon], zoom_start=12, tiles="CartoDB positron")

    # *** Use Mapbox Token for Custom Tiles ***
    if mapbox_token:
        folium.TileLayer(
              tiles=f"https://api.mapbox.com/styles/v1/vr00n-nycsbus/clyyoiorc00uu01pe8ttggvhd/tiles/256/{{z}}/{{x}}/{{y}}@2x?access_token={mapbox_token}",
              attr="Mapbox", name="Custom Mapbox Style", overlay=False, control=True
          ).add_to(m)
        folium.LayerControl(position='topright', collapsed=False).add_to(m)
    else:
        print("WARNING: No Mapbox token; using default map tiles.")


    # ... (Keep the offset_duplicates function) ...
    # ... (Keep the create_numbered_marker function) ...
    # ... (Keep the marker plotting logic for students and schools) ...
    # ... (Keep the polyline plotting logic) ...
    # ... (Keep the vehicle hover trail plotting logic) ...
    # ... (Keep the fit_bounds logic) ...

    print(f"INFO: Map plotting complete for Route {route_data.get('Route', 'N/A')}.")
    return m # Return the map object