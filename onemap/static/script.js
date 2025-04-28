// static/script.js

// --- Leaflet Map Variables ---
let map;
let routeLayer; // Layer for AM/PM traces + hover points
let stopsLayer; // Layer for AM/PM stops
let safetyLayer; // Layer for safety summary (ONLY exception markers/lines)
let depotLayer; // Layer for depot markers
let mapInitialized = false;

// --- Store fetched data ---
let currentAmMapData = null;
let currentPmMapData = null;
let currentOptData = null;
let currentSafetyData = null; // Store annotated trace from safety summary
let currentMapDate = null; // Store the date used for the last fetch
let currentMapRoute = null; // Store the route used for the last fetch
let depotLocations = {}; // Store depot locations

// --- State Variables ---
let isSafetyLayerVisible = false; // State for safety layer visibility

// --- Initialize Map Function ---
function initializeMap() {
    const mapContainer = document.getElementById('mapContainer');
    if (mapInitialized || !mapContainer) {
        console.log("DEBUG: Map initialization skipped (already initialized or container missing).");
        return;
    }
    try {
        map = L.map(mapContainer).setView([40.7128, -74.0060], 11); // Default: NYC
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
            maxZoom: 19,
        }).addTo(map);
        routeLayer = L.layerGroup().addTo(map);
        stopsLayer = L.layerGroup().addTo(map);
        safetyLayer = L.layerGroup().addTo(map); // Layer for safety overlays
        depotLayer = L.layerGroup().addTo(map);
        mapInitialized = true;
        console.log("DEBUG: Leaflet map initialized successfully.");
        addDepotMarkers(); // Add depots after map init
    } catch (err) {
         console.error("DEBUG ERROR: Failed to initialize Leaflet map:", err);
         if (mapContainer) mapContainer.innerHTML = '<p style="color: red; text-align: center; padding: 20px;">Error: Could not load the map.</p>';
         displayError("Failed to initialize the map visualization.");
         mapInitialized = false;
    }
} // End initializeMap

// --- Function to Add Depot Markers ---
function addDepotMarkers() {
    if (!mapInitialized || !map || !depotLayer || Object.keys(depotLocations).length === 0) {
         console.warn("DEBUG WARN: Cannot add depot markers. Map not ready or no depot data available.");
         return;
    }
    depotLayer.clearLayers();
    console.log("DEBUG: Adding depot markers. Data:", depotLocations);
    const homeIcon = L.icon({
        iconUrl: '/static/images/home.png', iconSize: [32, 32],
        iconAnchor: [16, 32], popupAnchor: [0, -32]
    });
    let depotCount = 0;
    for (const depotName in depotLocations) {
        if (Object.prototype.hasOwnProperty.call(depotLocations, depotName)) {
            const coords = depotLocations[depotName];
            if (Array.isArray(coords) && coords.length === 2 && typeof coords[1] === 'number' && typeof coords[0] === 'number' && !isNaN(coords[1]) && !isNaN(coords[0])) {
                const lat = coords[1]; const lon = coords[0];
                try {
                    const marker = L.marker([lat, lon], { icon: homeIcon }).bindPopup(`<b>Depot:</b> ${depotName}`);
                    depotLayer.addLayer(marker); depotCount++;
                } catch (e) { console.error(`DEBUG ERROR adding depot marker for ${depotName}:`, e); }
            } else { console.warn(`DEBUG WARN: Invalid coordinates format for depot ${depotName}:`, coords); }
        }
    }
    console.log(`DEBUG: Added ${depotCount} depot markers.`);
} // End addDepotMarkers


// --- Function to Display Regular Map Data (AM/PM Trace and Stops) ---
function displayMapData(mapData, isAm) {
    if (!mapInitialized || !map) { console.warn("Map not initialized."); return; }

    // Clear route and stops layers. Safety layer is handled by its toggle.
    if (routeLayer) routeLayer.clearLayers();
    if (stopsLayer) stopsLayer.clearLayers();
    // isSafetyLayerVisible state remains unchanged when switching AM/PM
    // updateButtonStates(isAm ? 'AM' : 'PM'); // Called by showAmMap/showPmMap

    const vehicleNum = mapData?.vehicle_number || 'N/A';
    console.log(`DEBUG: Displaying ${isAm ? 'AM' : 'PM'} map data for vehicle: ${vehicleNum}`);
    if (mapData?.trace?.length > 0) console.log("DEBUG: First trace object structure for AM/PM view:", mapData.trace[0]);
    else console.log("DEBUG: No trace data received for AM/PM view.");
    if (mapData?.stops?.length > 0) console.log("DEBUG: First stop object structure for AM/PM view:", mapData.stops[0]);
    else console.log("DEBUG: No stops data received for AM/PM view.");

    if (!mapData || typeof mapData !== 'object') {
        console.log("DEBUG: No valid map data to display.");
        return;
    }

    let bounds = L.latLngBounds([]); let hasData = false;

    // --- Process Route Trace (GeoJSON Features) ---
    if (mapData.trace && Array.isArray(mapData.trace) && mapData.trace.length > 0) {
        const validTraceFeatures = mapData.trace.filter(f => f?.type === 'Feature' && f.geometry?.type === 'Point' && Array.isArray(f.geometry.coordinates) && f.geometry.coordinates.length >= 2 && typeof f.geometry.coordinates[1] === 'number' && typeof f.geometry.coordinates[0] === 'number' && !isNaN(f.geometry.coordinates[1]) && !isNaN(f.geometry.coordinates[0]));
        if (validTraceFeatures.length > 1) {
            const traceCoords = validTraceFeatures.map(f => [f.geometry.coordinates[1], f.geometry.coordinates[0]]);
            try {
                const polyline = L.polyline(traceCoords, { color: '#002447', weight: 4, opacity: 0.8 }).addTo(routeLayer);
                bounds.extend(polyline.getBounds()); hasData = true;
            } catch (e) { console.error("DEBUG ERROR drawing polyline:", e); }
            validTraceFeatures.forEach(feature => { // Add hover points
                try {
                    const props = feature.properties || {}; const lat = feature.geometry.coordinates[1]; const lon = feature.geometry.coordinates[0];
                    const speedKph = (typeof props.speed === 'number' && !isNaN(props.speed)) ? props.speed : 0;
                    const speedMph = Math.round(speedKph * 0.621371); let timeStr = "N/A";
                    if (props.dateTime) { try { timeStr = new Date(props.dateTime).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' }); } catch (timeErr) {} }
                    const tooltipContent = `<b>${vehicleNum}</b><br>Time: ${timeStr}<br>Speed: ${speedMph} MPH`;
                    L.circleMarker([lat, lon], { radius: 3, weight: 0, fillOpacity: 0, interactive: true }).bindTooltip(tooltipContent).addTo(routeLayer);
                } catch(hoverErr) { console.error("DEBUG ERROR adding hover marker:", hoverErr, feature); }
            });
        } else { console.log("DEBUG: Not enough valid trace features for polyline."); }
    } else { console.log("DEBUG: No trace data available."); }

    // --- Add Stop Markers ---
    if (mapData.stops && Array.isArray(mapData.stops) && mapData.stops.length > 0) {
        let addedStopsCount = 0;
        mapData.stops.forEach((stop, index) => {
            const lat = stop?.lat; const lon = stop?.lon; const sequence = stop?.sequence;
            if (typeof lat === 'number' && typeof lon === 'number' && !isNaN(lat) && !isNaN(lon) && sequence !== undefined && sequence !== null) {
                try {
                    const stopType = stop.type || 'student'; const markerText = stopType === 'school' ? 'S' : sequence;
                    let backgroundColor;
                    if (stopType === 'school') { backgroundColor = '#0056b3'; } else { backgroundColor = '#007bff'; }
                    const iconSize = 24;
                    const iconHtml = `<div style="font-size: 10pt; color: white; font-weight: bold; text-align:center; width:${iconSize}px; height:${iconSize}px; line-height:${iconSize}px; background:${backgroundColor}; border-radius:50%; border: 1px solid #FFFFFF; box-shadow: 1px 1px 3px rgba(0,0,0,0.5); display: flex; justify-content: center; align-items: center;">${markerText}</div>`;
                    const numberedIcon = L.divIcon({ html: iconHtml, className: '', iconSize: [iconSize, iconSize], iconAnchor: [iconSize / 2, iconSize / 2] });
                    const marker = L.marker([lat, lon], { icon: numberedIcon });
                    const popupContent = stop.info || `Stop #${sequence}`;
                    if (!stop.info) console.warn(`DEBUG: Stop index ${index} missing 'info' property.`);
                    marker.bindPopup(popupContent); stopsLayer.addLayer(marker);
                    bounds.extend([lat, lon]); addedStopsCount++; hasData = true;
                } catch (e) { console.error(`DEBUG ERROR adding numbered marker at index ${index}:`, e, stop); }
            } else { console.warn(`DEBUG WARN: Skipping invalid stop data at index ${index}:`, stop); }
        });
        console.log(`DEBUG: Added ${addedStopsCount} numbered stop markers.`);
    } else { console.log("DEBUG: No stop data available."); }

    // --- Adjust Map View ---
    try {
        if (hasData && bounds.isValid()) { map.fitBounds(bounds, { padding: [30, 30] }); }
        else if (!hasData) { console.log("DEBUG: No data drawn, keeping default map view."); }
    } catch (e) { console.error("DEBUG ERROR fitting map bounds:", e); if(map) map.setView([40.7128, -74.0060], 11); }

     // --- Automatically refresh safety layer if it's supposed to be visible ---
     if (isSafetyLayerVisible) {
         console.log(`DEBUG: AM/PM view changed to ${isAm ? 'AM' : 'PM'}, refreshing safety layer.`);
         fetchAndDisplaySafetyLayer(isAm ? 'AM' : 'PM'); // Fetch for the current view
     }
} // End displayMapData


// --- Function to Display Safety Layer Data ---
// --- UPDATED safety polyline color and idling marker style ---
function displaySafetyLayer(annotatedTraceGeoJSON) {
    if (!mapInitialized || !map) { console.warn("Map not initialized."); return; }
    if (safetyLayer) safetyLayer.clearLayers(); // Clear only safety layer
    // isSafetyLayerVisible = true; // Set by calling function
    // updateButtonStates(); // Set by calling function

    if (!annotatedTraceGeoJSON || annotatedTraceGeoJSON.length === 0) {
        console.log("No annotated trace data received for safety layer."); return;
    }
    console.log(`DEBUG: Drawing safety overlays for ${annotatedTraceGeoJSON.length} points.`);
    let bounds = L.latLngBounds([]); let exceptionPointsFound = 0;
    let currentSegmentCoords = []; let currentSegmentType = null; let segmentStartInfo = null;

    function showSpeedingPopup(e) { /* keep as is */ const layer = e.target; const content = layer.options.popupContent || "Speeding Event"; L.popup().setLatLng(e.latlng).setContent(content).openOn(map); }

    function processSegmentEnd() {
        // console.log(`DEBUG processSegmentEnd: Ending segment type '${currentSegmentType}' with ${currentSegmentCoords.length} points.`); // Noisy
        if (currentSegmentType === 'speeding' && currentSegmentCoords.length > 1 && segmentStartInfo) {
            console.log(`DEBUG: Drawing speeding segment.`);
            try {
                const popupContent = `<b>Exception</b><br>${segmentStartInfo.details}`; // Details include Max Speed
                const speedingPolyline = L.polyline(currentSegmentCoords, { color: 'red', weight: 8, opacity: 0.85, popupContent: popupContent });
                speedingPolyline.on('click', showSpeedingPopup); speedingPolyline.addTo(safetyLayer);
                bounds.extend(speedingPolyline.getBounds()); exceptionPointsFound++;
            } catch (e) { console.error("DEBUG ERROR drawing speeding segment:", e); }
        } else if (currentSegmentType === 'idling' && currentSegmentCoords.length > 0 && segmentStartInfo) {
            console.log(`DEBUG: Drawing consolidated idling marker.`);
            try {
                const latLng = currentSegmentCoords[0];
                if (Array.isArray(latLng) && latLng.length === 2 && typeof latLng[0] === 'number' && typeof latLng[1] === 'number') {
                    // *** UPDATE Idling marker style ***
                    const marker = L.circleMarker(latLng, {
                        radius: 20, // Increased radius (2x previous)
                        fillColor: 'orange', // New color: orange
                        color: '#cc8400', // Darker orange border
                        weight: 1,
                        opacity: 1,
                        fillOpacity: 0.5 // Keep semi-transparent
                    });
                    // ********************************
                    let popupContent = `<b>Log Point (Start of Idle)</b><br>Time: ${segmentStartInfo.dateTime}`;
                    if (segmentStartInfo.details !== '--') popupContent += `<hr style='margin: 5px 0;'><b>Exception</b><br>${segmentStartInfo.details}`;
                    marker.bindPopup(popupContent); safetyLayer.addLayer(marker);
                    exceptionPointsFound++; if (!bounds.contains(latLng)) { bounds.extend(latLng); }
                } else { console.error("DEBUG ERROR: Invalid latLng for idling marker:", latLng); }
            } catch (e) { console.error("DEBUG ERROR adding idling marker:", e); }
        } else if (currentSegmentType === 'other' && currentSegmentCoords.length > 0 && segmentStartInfo) {
             console.log(`DEBUG: Drawing individual 'other' exception marker.`);
             try {
                 const latLng = currentSegmentCoords[0];
                  if (Array.isArray(latLng) && latLng.length === 2 && typeof latLng[0] === 'number' && typeof latLng[1] === 'number') {
                     const marker = L.circleMarker(latLng, { radius: 5, fillColor: 'orange', color: 'orange', weight: 1, opacity: 1, fillOpacity: 0.8 });
                     let popupContent = `<b>Log Point</b><br>Time: ${segmentStartInfo.dateTime}`;
                     if (segmentStartInfo.details !== '--') popupContent += `<hr style='margin: 5px 0;'><b>Exception</b><br>${segmentStartInfo.details}`;
                     marker.bindPopup(popupContent); safetyLayer.addLayer(marker);
                     exceptionPointsFound++; if (!bounds.contains(latLng)) { bounds.extend(latLng); }
                 } else { console.error("DEBUG ERROR: Invalid latLng for other exception marker:", latLng); }
             } catch (e) { console.error("DEBUG ERROR adding other exception marker:", e); }
        }
        currentSegmentCoords = []; currentSegmentType = null; segmentStartInfo = null; // Reset
    } // End processSegmentEnd

    annotatedTraceGeoJSON.forEach((feature, index) => {
        if (!feature?.type || feature.type !== 'Feature' || !feature.geometry?.type || feature.geometry.type !== 'Point' || !feature.geometry.coordinates || feature.geometry.coordinates.length < 2) return;
        const props = feature.properties || {}; const exceptionType = props.exception_type || '--';
        const lowerCaseException = String(exceptionType).toLowerCase();
        let pointType = null;
        if (lowerCaseException.includes('speeding')) pointType = 'speeding';
        else if (lowerCaseException.includes('idling') || lowerCaseException.includes('idle')) pointType = 'idling';
        else if (exceptionType !== '--') pointType = 'other';

        const coords = feature.geometry.coordinates; const lat = coords[1]; const lon = coords[0];
        if (typeof lat !== 'number' || typeof lon !== 'number' || isNaN(lat) || isNaN(lon)) { console.warn("Skipping point due to invalid lat/lon:", lat, lon); processSegmentEnd(); return; }
        const latLng = [lat, lon];
        let dateTime = props.dateTime || 'N/A';
        if (dateTime !== 'N/A') { try { dateTime = new Date(dateTime).toLocaleString([], { dateStyle: 'short', timeStyle: 'short'}); } catch(e) { dateTime = props.dateTime; } }

        if (pointType !== currentSegmentType) { processSegmentEnd(); currentSegmentType = pointType; if (pointType) { segmentStartInfo = { dateTime: dateTime, details: props.exception_details || '--' }; } }
        if (currentSegmentType) { currentSegmentCoords.push(latLng); }
        if (index === annotatedTraceGeoJSON.length - 1) { processSegmentEnd(); }
    }); // End forEach loop

    console.log(`DEBUG: Added ${exceptionPointsFound} safety overlay elements (markers/lines).`);
    if (exceptionPointsFound === 0) displayError("No specific safety events found for this period.");
    try {
        if (exceptionPointsFound > 0 && bounds.isValid()) map.fitBounds(bounds, { padding: [50, 50], maxZoom: 16 });
        else { console.log("DEBUG: No exception elements drawn or bounds invalid, map view not adjusted."); }
    } catch (e) { console.error("DEBUG ERROR fitting safety map bounds:", e); }
} // End displaySafetyLayer


// --- Function to remove safety layer ---
function removeSafetyLayer() { /* Keep as is */ if (safetyLayer) { safetyLayer.clearLayers(); console.log("DEBUG: Removed safety layer overlays."); } isSafetyLayerVisible = false; updateButtonStates(); }

// --- Function to display errors ---
function displayError(message) { /* Keep as is */ const errorDisplay = document.getElementById('errorDisplay'); if (errorDisplay) { errorDisplay.textContent = message; errorDisplay.style.display = 'block'; } else { console.error("DEBUG ERROR: Cannot display error - errorDisplay element missing."); console.error("Original error message:", message); } }

// --- Function to show/hide loading indicator ---
function showLoading(show) { /* Keep as is */ const loadingIndicator = document.getElementById('loadingIndicator'); const getMapButton = document.getElementById('getMapButton'); const showSafetyButton = document.getElementById('showSafetyButton'); if (loadingIndicator) loadingIndicator.style.display = show ? 'block' : 'none'; if (getMapButton) getMapButton.disabled = show; if (showSafetyButton) showSafetyButton.disabled = show; /* Safety toggles removed */ }

// --- Function to update button active states ---
// --- UPDATED to handle AM/PM toggling correctly ---
function updateButtonStates(activeView = null) { // activeView can be 'AM', 'PM', or null
    const showAmButton = document.getElementById('showAmButton');
    const showPmButton = document.getElementById('showPmButton');
    const showSafetyButton = document.getElementById('showSafetyButton');
    const optLinkElement = document.getElementById('optLink');

    if (!showAmButton || !showPmButton || !showSafetyButton || !optLinkElement) {
        console.error("DEBUG ERROR: One or more button/link elements not found in updateButtonStates.");
        return;
    }

    const mapDataLoaded = currentAmMapData || currentPmMapData;

    // Update AM/PM button states ONLY if activeView is explicitly passed
    // This prevents this function from overriding the state set by showAmMap/showPmMap
    if (activeView === 'AM') {
        showAmButton.classList.add('active');
        showPmButton.classList.remove('active');
    } else if (activeView === 'PM') {
        showAmButton.classList.remove('active');
        showPmButton.classList.add('active');
    }
    // If activeView is null, don't change AM/PM button states here

    // Update Safety Layer button state and text
    if (mapDataLoaded) {
        showSafetyButton.classList.add('ready-to-use');
        showSafetyButton.disabled = false;
        if (isSafetyLayerVisible) {
            showSafetyButton.textContent = "Remove Safety Layer";
            showSafetyButton.classList.add('safety-active');
        } else {
            showSafetyButton.textContent = "Add Safety Layer";
            showSafetyButton.classList.remove('safety-active');
        }
    } else {
        showSafetyButton.classList.remove('ready-to-use', 'safety-active');
        showSafetyButton.textContent = "Add Safety Layer";
        showSafetyButton.disabled = true;
    }

    // Update OPT Link state
    if (mapDataLoaded && currentOptData && currentOptData.length > 0) {
        optLinkElement.classList.add('active');
        optLinkElement.style.opacity = 1;
        optLinkElement.href = "#";
        optLinkElement.style.cursor = 'pointer';
    } else {
        optLinkElement.classList.remove('active');
        optLinkElement.style.opacity = 0.5;
        optLinkElement.removeAttribute('href');
        optLinkElement.style.cursor = 'default';
    }
}


// --- Main execution block after HTML is loaded ---
document.addEventListener('DOMContentLoaded', () => {
    console.log("DEBUG: DOM Loaded");
    // --- Get Depot Locations from Embedded Data ---
    const depotDataElement = document.getElementById('depotData');
    if (depotDataElement) { try { depotLocations = JSON.parse(depotDataElement.textContent || '{}'); console.log("DEBUG: Successfully parsed depot locations from HTML:", depotLocations); } catch (e) { console.error("DEBUG ERROR: Failed to parse depot locations JSON:", e); depotLocations = {}; } } else { console.warn("DEBUG WARN: Depot data element (#depotData) not found in index.html."); }
    // --- Get OTHER DOM References ---
    const getMapButton = document.getElementById('getMapButton'); const routeInput = document.getElementById('routeInput'); const dateInput = document.getElementById('dateInput'); const showAmButton = document.getElementById('showAmButton'); const showPmButton = document.getElementById('showPmButton'); const showSafetyButton = document.getElementById('showSafetyButton'); const errorDisplay = document.getElementById('errorDisplay'); const dviLinkElement = document.getElementById('dviLink'); const optLinkElement = document.getElementById('optLink'); const optTableContainer = document.getElementById('optTableContainer'); const optTableContent = document.getElementById('optTableContent'); const downloadOptButton = document.getElementById('download-opt-btn'); const sidebarTitle = document.getElementById('sidebar-title'); const sidebarVehicleSpan = document.getElementById('sidebar-vehicle'); const sidebarRouteSpan = document.getElementById('sidebar-route'); const sidebarDriverNameSpan = document.getElementById('sidebar-driver-name'); const sidebarDriverPhoneSpan = document.getElementById('sidebar-driver-phone');
    // --- Initialize Map (will call addDepotMarkers) ---
    initializeMap();
    // --- Set Initial Button States ---
    updateButtonStates();

    // --- Event Listener for Get Map Button ---
    getMapButton.addEventListener('click', async () => {
        if (!mapInitialized) { displayError("Map initialization failed. Cannot get map."); return; }
        const route = routeInput.value.trim(); const date = dateInput.value;
        if (!route || !date) { displayError("Please enter both a route and a date."); return; }
        showLoading(true); if(errorDisplay) errorDisplay.style.display = 'none';
        if (routeLayer) routeLayer.clearLayers(); if (stopsLayer) stopsLayer.clearLayers(); if (safetyLayer) safetyLayer.clearLayers();
        isSafetyLayerVisible = false; // Reset safety visibility
        currentAmMapData = null; currentPmMapData = null; currentOptData = null; currentSafetyData = null;
        currentMapDate = date; currentMapRoute = route;
        updateButtonStates('AM'); // Reset to AM active view and update other buttons
        if(map) map.setView([40.7128, -74.0060], 11);
        if (dviLinkElement) { dviLinkElement.href = "#"; dviLinkElement.removeAttribute("target"); dviLinkElement.style.opacity = 0.5; }
        if (optTableContainer) optTableContainer.style.display = 'none'; if (optTableContent) optTableContent.innerHTML = '<p id="opt-placeholder">OPT data will appear here.</p>'; if (downloadOptButton) downloadOptButton.style.display = 'none';
        if (sidebarTitle) sidebarTitle.textContent = 'Trip Details'; if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = '--'; if (sidebarRouteSpan) sidebarRouteSpan.textContent = '--'; if (sidebarDriverNameSpan) sidebarDriverNameSpan.textContent = '--'; if (sidebarDriverPhoneSpan) sidebarDriverPhoneSpan.textContent = '--';

        console.log("DEBUG: Starting fetch to /get_map");
        try {
            const response = await fetch('/get_map', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ route: route, date: date }), });
            if (!response.ok) { let errorMsg = `HTTP error! Status: ${response.status}`; try { const errorData = await response.json(); errorMsg = errorData.error || errorMsg; } catch (e) {} throw new Error(errorMsg); }
            const data = await response.json();
            console.log("DEBUG: Received data from /get_map:", data);
            currentAmMapData = data.am_map_data; currentPmMapData = data.pm_map_data; currentOptData = data.opt_data;
             console.log("DEBUG: Stored currentOptData:", currentOptData);
            const initialVehicle = currentAmMapData?.vehicle_number || currentPmMapData?.vehicle_number || '--';
            const initialRoute = route || '--'; const driverName = data.driver_name || 'N/A'; const driverPhone = data.driver_phone || 'N/A';
            if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = initialVehicle; if (sidebarRouteSpan) sidebarRouteSpan.textContent = initialRoute;
            if (sidebarDriverNameSpan) sidebarDriverNameSpan.textContent = driverName; if (sidebarDriverPhoneSpan) sidebarDriverPhoneSpan.textContent = driverPhone;
            const dviLinkUrl = data.dvi_link;
            if (dviLinkElement && dviLinkUrl && dviLinkUrl !== "#") { dviLinkElement.href = dviLinkUrl; dviLinkElement.target = "_blank"; dviLinkElement.style.opacity = 1; }
            else { if(dviLinkElement) { dviLinkElement.href = "#"; dviLinkElement.removeAttribute("target"); dviLinkElement.style.opacity = 0.5; } }
            // Update button states AFTER data load to enable OPT/Safety buttons
            updateButtonStates('AM'); // Set AM active and enable other buttons
            showAmMap(); // Display AM map by default

        } catch (error) {
            console.error("DEBUG: Error caught during Get Map fetch:", error); displayError(`Failed to load map data: ${error.message}`);
            currentAmMapData = null; currentPmMapData = null; currentOptData = null; currentSafetyData = null; currentMapDate = null; currentMapRoute = null; isSafetyLayerVisible = false;
            if (dviLinkElement) { dviLinkElement.href = "#"; dviLinkElement.removeAttribute("target"); dviLinkElement.style.opacity = 0.5; } if (optTableContainer) optTableContainer.style.display = 'none'; if (optTableContent) optTableContent.innerHTML = '<p id="opt-placeholder">OPT data will appear here.</p>'; if (downloadOptButton) downloadOptButton.style.display = 'none'; if (sidebarTitle) sidebarTitle.textContent = 'Trip Details'; if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = '--'; if (sidebarRouteSpan) sidebarRouteSpan.textContent = '--'; if (sidebarDriverNameSpan) sidebarDriverNameSpan.textContent = '--'; if (sidebarDriverPhoneSpan) sidebarDriverPhoneSpan.textContent = '--';
            if(routeLayer) routeLayer.clearLayers(); if(stopsLayer) stopsLayer.clearLayers(); if(safetyLayer) safetyLayer.clearLayers();
            if(map) map.setView([40.7128, -74.0060], 11);
            updateButtonStates(); // Reset buttons on error
        } finally { showLoading(false); }
    });

    // --- Event Listeners for AM/PM Toggle Buttons ---
    // --- UPDATED to call updateButtonStates explicitly ---
    function showAmMap() {
        if (!mapInitialized || !currentAmMapData) { console.warn("Cannot show AM map, not initialized or no data."); return; }
        // Clear safety layer if switching views while it's active
        if (isSafetyLayerVisible) { if(safetyLayer) safetyLayer.clearLayers(); isSafetyLayerVisible = false; }
        updateButtonStates('AM'); // Set AM active *before* drawing
        displayMapData(currentAmMapData, true); // Draw AM map
        if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = currentAmMapData?.vehicle_number || '--';
        console.log("DEBUG: Switched view to AM Map");
    }
    function showPmMap() {
        if (!mapInitialized || !currentPmMapData) { console.warn("Cannot show PM map, not initialized or no data."); return; }
        // Clear safety layer if switching views while it's active
        if (isSafetyLayerVisible) { if(safetyLayer) safetyLayer.clearLayers(); isSafetyLayerVisible = false; }
        updateButtonStates('PM'); // Set PM active *before* drawing
        displayMapData(currentPmMapData, false); // Draw PM map
        if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = currentPmMapData?.vehicle_number || '--';
        console.log("DEBUG: Switched view to PM Map");
    }
    if (showAmButton) showAmButton.addEventListener('click', showAmMap);
    if (showPmButton) showPmButton.addEventListener('click', showPmMap);

    // --- Event Listener for Safety Layer Toggle Button ---
    if (showSafetyButton) { showSafetyButton.addEventListener('click', () => { if (!currentMapDate || !currentMapRoute) { displayError("Please load a map first using 'Get Map'."); return; } if (isSafetyLayerVisible) { removeSafetyLayer(); } else { const isAmActive = showAmButton.classList.contains('active'); const isPmActive = showPmButton.classList.contains('active'); let periodToFetch = null; if (isAmActive) periodToFetch = 'AM'; else if (isPmActive) periodToFetch = 'PM'; else { displayError("Please select AM or PM view first."); return; } console.log(`DEBUG: Add Safety Layer clicked. Fetching for period: ${periodToFetch}`); fetchAndDisplaySafetyLayer(periodToFetch); } }); }

    // --- Function to Fetch and Display Safety Layer Data ---
    async function fetchAndDisplaySafetyLayer(timePeriod) { /* Keep as is */ console.log(`DEBUG: Fetching safety layer data for period: ${timePeriod}`); if (!currentMapDate || !currentMapRoute) { displayError("Cannot fetch safety data: Base map data missing."); return; } let deviceId = null; let vehicleNumber = null; if (timePeriod === 'AM') { deviceId = currentAmMapData?.device_id; vehicleNumber = currentAmMapData?.vehicle_number; } else if (timePeriod === 'PM') { deviceId = currentPmMapData?.device_id; vehicleNumber = currentPmMapData?.vehicle_number; } if (!deviceId || !vehicleNumber) { displayError(`Cannot fetch safety data for ${timePeriod}. Missing Device ID or Vehicle Number.`); isSafetyLayerVisible = false; if(safetyLayer) safetyLayer.clearLayers(); updateButtonStates(); return; } showLoading(true); try { console.log(`DEBUG: Calling /get_safety_summary with deviceId: ${deviceId}, vehicle: ${vehicleNumber}, date: ${currentMapDate}, period: ${timePeriod}`); const response = await fetch('/get_safety_summary', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ device_id: deviceId, vehicle_number: vehicleNumber, date: currentMapDate, time_period: timePeriod }) }); if (!response.ok) { const errorData = await response.json(); throw new Error(errorData.error || `HTTP error! status: ${response.status}`); } const safetyData = await response.json(); console.log("DEBUG: Received safety data from /get_safety_summary:", safetyData); if (!safetyData || !Array.isArray(safetyData.annotated_trace)) { throw new Error("Received invalid safety data format from server."); } currentSafetyData = safetyData.annotated_trace; displaySafetyLayer(currentSafetyData); isSafetyLayerVisible = true; updateButtonStates(); } catch (error) { console.error("DEBUG ERROR fetching or displaying safety layer:", error); displayError(`Failed to load safety layer: ${error.message}`); if(safetyLayer) safetyLayer.clearLayers(); currentSafetyData = null; isSafetyLayerVisible = false; updateButtonStates(); } finally { showLoading(false); } }

    // --- OPT Link, Download Button Listeners, and Helper Functions ---
    function displayOptTable() { /* Keep as is */ console.log("DEBUG: displayOptTable function called."); if (!optTableContainer || !optTableContent) { console.error("DEBUG ERROR: OPT table container or content element not found."); return; } const downloadBtn = document.getElementById('download-opt-btn'); console.log("DEBUG: Current OPT Data:", currentOptData); if (currentOptData && Array.isArray(currentOptData) && currentOptData.length > 0) { console.log(`DEBUG: Building OPT table with ${currentOptData.length} rows.`); const desiredColumns = [ 'seg_no', 'School_Code_&_Name', 'hndc_code', 'pupil_id_no', 'first_name', 'last_name', 'address', 'zip', 'ph', 'amb_cd', 'sess_beg', 'sess_end', 'med_alert', 'am', 'pm' ]; const availableColumns = desiredColumns.filter(col => currentOptData[0].hasOwnProperty(col)); /* Add Sorting Logic */ const sortedOptData = [...currentOptData].sort((a, b) => { const segA = parseInt(a.seg_no || '9999', 10); const segB = parseInt(b.seg_no || '9999', 10); const nameA = (a['School_Code_&_Name'] || '').toUpperCase(); const nameB = (b['School_Code_&_Name'] || '').toUpperCase(); if (segA === 0 && segB !== 0) return 1; if (segA !== 0 && segB === 0) return -1; if (segA === 0 && segB === 0) { if (nameA.startsWith('ARRIVE') && !nameB.startsWith('ARRIVE')) return -1; if (!nameA.startsWith('ARRIVE') && nameB.startsWith('ARRIVE')) return 1; if (nameA.startsWith('DISMISS') && !nameB.startsWith('DISMISS')) return -1; if (!nameA.startsWith('DISMISS') && nameB.startsWith('DISMISS')) return 1; return 0; } return segA - segB; }); /* End Sorting Logic */ let tableHTML = '<table id="opt-table" border="1" style="width:100%; border-collapse: collapse; font-size: 0.8em;">'; tableHTML += '<thead><tr style="background-color: #f2f2f2;">'; availableColumns.forEach(header => { tableHTML += `<th style="padding: 4px; text-align: left;">${header}</th>`; }); tableHTML += '</tr></thead>'; tableHTML += '<tbody>'; sortedOptData.forEach((row, index) => { /* Use sortedOptData */ const rowStyle = index % 2 === 0 ? '' : 'background-color: #f9f9f9;'; tableHTML += `<tr style="${rowStyle}">`; availableColumns.forEach(columnKey => { const value = row[columnKey] ?? ''; const escapedValue = String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); tableHTML += `<td style="padding: 4px; vertical-align: top;">${escapedValue}</td>`; }); tableHTML += '</tr>'; }); tableHTML += '</tbody></table>'; optTableContent.innerHTML = tableHTML; optTableContainer.style.display = 'block'; if (downloadBtn) downloadBtn.style.display = 'inline-block'; } else { console.log("DEBUG: No OPT data available to display."); optTableContent.innerHTML = '<p style="padding: 10px; font-style: italic; color: #666;">No OPT data details available.</p>'; optTableContainer.style.display = 'block'; if (downloadBtn) downloadBtn.style.display = 'none'; } }
    function downloadTableAsCSV(tableId, filename) { /* Keep as is */ filename = filename || 'download.csv'; const table = document.getElementById(tableId); if (!table) { console.error("DEBUG ERROR: Table not found for CSV download:", tableId); return; } let csv = []; const rows = table.querySelectorAll("tr"); const escapeCSV = function(cellData) { if (cellData == null) { return ''; } let data = cellData.toString().replace(/"/g, '""'); if (data.search(/("|,|\n)/g) >= 0) { data = '"' + data + '"'; } return data; }; for (let i = 0; i < rows.length; i++) { const row = [], cols = rows[i].querySelectorAll("td, th"); for (let j = 0; j < cols.length; j++) { let cellText = (cols[j].textContent || cols[j].innerText || '').trim(); row.push(escapeCSV(cellText)); } csv.push(row.join(",")); } const csvContent = "\uFEFF" + csv.join("\n"); const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' }); const link = document.createElement("a"); if (navigator.msSaveBlob) { navigator.msSaveBlob(blob, filename); } else if (link.download !== undefined) { const url = URL.createObjectURL(blob); link.setAttribute("href", url); link.setAttribute("download", filename); link.style.visibility = 'hidden'; document.body.appendChild(link); link.click(); document.body.removeChild(link); URL.revokeObjectURL(url); } else { console.warn("CSV download method not fully supported, attempting fallback."); window.open('data:text/csv;charset=utf-8,' + encodeURIComponent(csvContent)); } }
    // Add listeners for OPT table display and download
    if (optLinkElement) { optLinkElement.addEventListener('click', (event) => { event.preventDefault(); console.log("DEBUG: Show OPT Information link clicked."); console.log("DEBUG: currentOptData before display:", currentOptData); if (currentOptData && Array.isArray(currentOptData) && currentOptData.length > 0) { displayOptTable(); } else { console.log("DEBUG: No OPT data loaded, not displaying table."); displayError("No OPT data loaded yet. Please use 'Get Map' first."); } }); }
    if (downloadOptButton) { downloadOptButton.addEventListener('click', function() { const routeForFilename = routeInput.value.trim() || 'ROUTE'; const dateForFilename = dateInput.value || 'DATE'; downloadTableAsCSV('opt-table', `opt_${routeForFilename}_${dateForFilename}.csv`); }); }


}); // End DOMContentLoaded


























