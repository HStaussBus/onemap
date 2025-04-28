// static/script.js

// --- Leaflet Map Variables ---
let map;
let routeLayer; // Layer for AM/PM traces + hover points
let stopsLayer; // Layer for AM/PM stops
let safetyLayer; // Layer for safety summary (base polyline + exception markers)
let mapInitialized = false;

// --- Store fetched data ---
let currentAmMapData = null;
let currentPmMapData = null;
let currentOptData = null;
let currentSafetyData = null; // Store annotated trace from safety summary
let currentMapDate = null; // Store the date used for the last fetch
let currentMapRoute = null; // Store the route used for the last fetch

// --- State Variables ---
let currentSafetyPeriod = 'RoundTrip'; // Default period for safety view
let isSafetyViewActive = false; // Track if safety view is active

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
        safetyLayer = L.layerGroup().addTo(map);
        mapInitialized = true;
        console.log("DEBUG: Leaflet map initialized successfully.");
    } catch (err) {
         console.error("DEBUG ERROR: Failed to initialize Leaflet map:", err);
         if (mapContainer) mapContainer.innerHTML = '<p style="color: red; text-align: center; padding: 20px;">Error: Could not load the map.</p>';
         displayError("Failed to initialize the map visualization.");
         mapInitialized = false;
    }
} // End initializeMap


// --- Function to Display Regular Map Data (AM/PM Trace and Stops) ---
// --- UPDATED to handle GeoJSON format for trace data ---
// --- Uses stop.info property for popups ---
function displayMapData(mapData, isAm) {
    if (!mapInitialized || !map) { console.warn("Map not initialized."); return; }

    if (routeLayer) routeLayer.clearLayers();
    if (stopsLayer) stopsLayer.clearLayers();
    if (safetyLayer) safetyLayer.clearLayers();
    isSafetyViewActive = false;
    updateButtonStates();

    const vehicleNum = mapData?.vehicle_number || 'N/A';
    console.log(`DEBUG: Displaying ${isAm ? 'AM' : 'PM'} map data for vehicle: ${vehicleNum}`);
    // Log structure for debugging
    if (mapData?.trace?.length > 0) console.log("DEBUG: First trace object structure for AM/PM view:", mapData.trace[0]);
    else console.log("DEBUG: No trace data received for AM/PM view.");
    if (mapData?.stops?.length > 0) console.log("DEBUG: First stop object structure for AM/PM view:", mapData.stops[0]);
    else console.log("DEBUG: No stops data received for AM/PM view.");

    if (!mapData || typeof mapData !== 'object') {
        console.log("DEBUG: No valid map data to display.");
        return;
    }

    let bounds = L.latLngBounds([]);
    let hasData = false;

    // --- Process Route Trace (Polyline AND Hover Markers) ---
    // *** Assumes mapData.trace is an array of GeoJSON Features ***
    // { "type": "Feature", "geometry": { "type": "Point", "coordinates": [lon, lat] }, "properties": { "dateTime": "...", "speed": ... } }
    if (mapData.trace && Array.isArray(mapData.trace) && mapData.trace.length > 0) {
        const validTraceFeatures = mapData.trace.filter(feature => // Filter for valid GeoJSON Point features
            feature?.type === 'Feature' &&
            feature.geometry?.type === 'Point' &&
            Array.isArray(feature.geometry.coordinates) &&
            feature.geometry.coordinates.length >= 2 &&
            typeof feature.geometry.coordinates[1] === 'number' && // latitude
            typeof feature.geometry.coordinates[0] === 'number' && // longitude
            !isNaN(feature.geometry.coordinates[1]) &&
            !isNaN(feature.geometry.coordinates[0])
        );

        if (validTraceFeatures.length > 1) {
            // Extract [lat, lon] coords for the polyline from GeoJSON features
            const traceCoords = validTraceFeatures.map(feature => [
                feature.geometry.coordinates[1], // latitude
                feature.geometry.coordinates[0]  // longitude
            ]);
            try {
                const polyline = L.polyline(traceCoords, {
                    color: isAm ? '#ff7f0e' : '#1f77b4', // Orange AM, Blue PM
                    weight: 4,
                    opacity: 0.8
                }).addTo(routeLayer); // Add polyline to routeLayer
                bounds.extend(polyline.getBounds());
                hasData = true;
            } catch (e) { console.error("DEBUG ERROR drawing polyline:", e); }

            // Add invisible markers for HOVER effect using the GeoJSON features
            validTraceFeatures.forEach(feature => {
                try {
                    const props = feature.properties || {};
                    const lat = feature.geometry.coordinates[1];
                    const lon = feature.geometry.coordinates[0];
                    const speedKph = (typeof props.speed === 'number' && !isNaN(props.speed)) ? props.speed : 0; // Use speed from properties
                    const speedMph = Math.round(speedKph * 0.621371);
                    let timeStr = "N/A";
                    if (props.dateTime) { // Use dateTime from properties
                        try {
                            timeStr = new Date(props.dateTime).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
                        } catch (timeErr) { console.warn("DEBUG WARN: Could not parse trace timestamp:", props.dateTime, timeErr); }
                    }
                    const tooltipContent = `<b>${vehicleNum}</b><br>Time: ${timeStr}<br>Speed: ${speedMph} MPH`;
                    L.circleMarker([lat, lon], { radius: 3, weight: 0, fillOpacity: 0, interactive: true })
                     .bindTooltip(tooltipContent)
                     .addTo(routeLayer); // Add hover markers to routeLayer
                } catch(hoverErr) { console.error("DEBUG ERROR adding hover marker:", hoverErr, feature); }
            });
        } else { console.log("DEBUG: Not enough valid trace features for polyline."); }
    } else { console.log("DEBUG: No trace data available."); }

    // --- Add Stop Markers (Using DivIcon) ---
    // Assumes mapData.stops is still [{lat:..., lon:..., sequence:..., type:..., info:...}, ...]
    if (mapData.stops && Array.isArray(mapData.stops) && mapData.stops.length > 0) {
        let addedStopsCount = 0;
        mapData.stops.forEach((stop, index) => {
            const lat = stop?.lat;
            const lon = stop?.lon;
            const sequence = stop?.sequence;

            if (typeof lat === 'number' && typeof lon === 'number' && !isNaN(lat) && !isNaN(lon) && sequence !== undefined && sequence !== null) {
                try {
                    const stopType = stop.type || 'student';
                    const markerText = stopType === 'school' ? 'S' : sequence;
                    const backgroundColor = stopType === 'school' ? '#DC3545' : (isAm ? '#ff7f0e' : '#1f77b4');
                    const iconSize = 24;

                    const iconHtml = `<div style="font-size: 10pt; color: white; font-weight: bold; text-align:center; width:${iconSize}px; height:${iconSize}px; line-height:${iconSize}px; background:${backgroundColor}; border-radius:50%; border: 1px solid #FFFFFF; box-shadow: 1px 1px 3px rgba(0,0,0,0.5); display: flex; justify-content: center; align-items: center;">${markerText}</div>`;
                    const numberedIcon = L.divIcon({ html: iconHtml, className: '', iconSize: [iconSize, iconSize], iconAnchor: [iconSize / 2, iconSize / 2] });
                    const marker = L.marker([lat, lon], { icon: numberedIcon });

                    // Use the 'info' property from Python for popup content
                    const popupContent = stop.info || `Stop #${sequence}`;
                    if (!stop.info) {
                        console.warn(`DEBUG: Stop index ${index} missing 'info' property. Stop data:`, stop);
                    }

                    marker.bindPopup(popupContent);
                    stopsLayer.addLayer(marker); // Add stop markers to stopsLayer
                    bounds.extend([lat, lon]);
                    addedStopsCount++;
                    hasData = true;
                } catch (e) { console.error(`DEBUG ERROR adding numbered marker at index ${index}:`, e, stop); }
            } else { console.warn(`DEBUG WARN: Skipping invalid stop data at index ${index}:`, stop); }
        });
        console.log(`DEBUG: Added ${addedStopsCount} numbered stop markers.`);
    } else { console.log("DEBUG: No stop data available."); }

    // --- Adjust Map View ---
    try {
        if (hasData && bounds.isValid()) {
            map.fitBounds(bounds, { padding: [30, 30] });
        } else if (!hasData) {
            console.log("DEBUG: No data drawn, keeping default map view.");
        }
    } catch (e) {
        console.error("DEBUG ERROR fitting map bounds:", e);
        if(map) map.setView([40.7128, -74.0060], 11);
    }
} // End displayMapData


// --- Function to Display Safety Layer Data ---
// --- Handles GeoJSON format from /get_safety_summary ---
// --- Draws base polyline + ONLY exception markers ---
function displaySafetyLayer(annotatedTraceGeoJSON) {
    if (!mapInitialized || !map) { console.warn("Map not initialized."); return; }

    if (routeLayer) routeLayer.clearLayers();
    if (stopsLayer) stopsLayer.clearLayers();
    if (safetyLayer) safetyLayer.clearLayers();
    isSafetyViewActive = true;
    updateButtonStates();

    if (!annotatedTraceGeoJSON || annotatedTraceGeoJSON.length === 0) {
        console.log("No safety trace data received.");
        displayError("No GPS trace data found for the selected safety period.");
        return;
    }

    console.log(`DEBUG: Displaying safety layer with ${annotatedTraceGeoJSON.length} total points.`);
    let bounds = L.latLngBounds([]);
    let hasTraceData = false;
    const traceCoords = [];

    // --- Pass 1: Collect coordinates for the base polyline ---
    annotatedTraceGeoJSON.forEach(feature => {
         if (feature?.geometry?.coordinates && feature.geometry.coordinates.length >= 2) {
             const coords = feature.geometry.coordinates;
             if (typeof coords[1] === 'number' && typeof coords[0] === 'number' && !isNaN(coords[1]) && !isNaN(coords[0])) {
                traceCoords.push([coords[1], coords[0]]);
             } else { console.warn("Skipping invalid coordinates for polyline:", coords); }
         }
    });

    // --- Draw the base polyline ---
    if (traceCoords.length > 1) {
         try {
             const polyline = L.polyline(traceCoords, {
                 color: '#555555', weight: 3, opacity: 0.6
             }).addTo(safetyLayer); // Add polyline to the safety layer
             bounds.extend(polyline.getBounds());
             hasTraceData = true;
         } catch (e) { console.error("DEBUG ERROR drawing safety polyline:", e); }
    } else { console.log("DEBUG: Not enough valid points to draw safety polyline."); }


    // --- Pass 2: Draw styled markers ONLY for points with exceptions ---
    let exceptionPointsFound = 0;
    annotatedTraceGeoJSON.forEach(feature => {
        if (!feature?.type || feature.type !== 'Feature' || !feature.geometry?.type || feature.geometry.type !== 'Point' || !feature.geometry.coordinates || feature.geometry.coordinates.length < 2) {
            return;
        }
        const props = feature.properties || {};
        const exceptionType = props.exception_type || '--';

        if (exceptionType !== '--') { // *** Only draw marker if there IS an exception ***
            exceptionPointsFound++;
            const coords = feature.geometry.coordinates;
            const lat = coords[1]; const lon = coords[0];
            if (typeof lat !== 'number' || typeof lon !== 'number' || isNaN(lat) || isNaN(lon)) {
                 console.warn("Skipping exception marker due to invalid lat/lon:", lat, lon); return;
            }
            const latLng = [lat, lon];
            const exceptionDetails = props.exception_details || '--';
            let dateTime = props.dateTime || 'N/A';

            if (dateTime !== 'N/A') {
                 try { dateTime = new Date(dateTime).toLocaleString([], { dateStyle: 'short', timeStyle: 'short'}); }
                 catch(e) { console.warn("Could not format safety point dateTime:", dateTime); dateTime = props.dateTime; }
            }

            let markerOptions = {};
            const lowerCaseException = String(exceptionType).toLowerCase();
            if (lowerCaseException.includes('speeding')) {
                markerOptions = { radius: 6, fillColor: 'red', color: 'red', weight: 1, opacity: 1, fillOpacity: 0.85 };
            } else if (lowerCaseException.includes('idling') || lowerCaseException.includes('idle')) {
                markerOptions = { radius: 7, fillColor: '#a9a9a9', color: '#808080', weight: 1, opacity: 1, fillOpacity: 0.4 };
            } else {
                markerOptions = { radius: 5, fillColor: 'orange', color: 'orange', weight: 1, opacity: 1, fillOpacity: 0.8 };
            }

            try {
                const marker = L.circleMarker(latLng, markerOptions);
                let popupContent = `<b>Log Point</b><br>Time: ${dateTime}`;
                if (exceptionDetails !== '--') { popupContent += `<hr style='margin: 5px 0;'><b>Exception</b><br>${exceptionDetails}`; }
                marker.bindPopup(popupContent);
                safetyLayer.addLayer(marker); // Add exception marker to safety layer
                if (!bounds.contains(latLng)) { bounds.extend(latLng); }
            } catch (e) { console.error("DEBUG ERROR adding safety marker:", e, feature); }
        } // End if (exceptionType !== '--')
    }); // End forEach loop for markers

    console.log(`DEBUG: Added ${exceptionPointsFound} exception markers.`);

    if (hasTraceData && exceptionPointsFound === 0) {
         displayError("No specific safety events found for this period. Showing full trace.");
    } else if (!hasTraceData && exceptionPointsFound === 0) {
         displayError("No GPS trace or safety events found for this period.");
    }

    // Fit map bounds
    try {
        if ((hasTraceData || exceptionPointsFound > 0) && bounds.isValid()) {
            map.fitBounds(bounds, { padding: [40, 40] });
        } else {
            console.log("DEBUG: No valid polyline or exception points to fit bounds.");
            if(map) map.setView([40.7128, -74.0060], 11);
        }
    } catch (e) {
        console.error("DEBUG ERROR fitting safety map bounds:", e);
        if(map) map.setView([40.7128, -74.0060], 11);
    }
} // End displaySafetyLayer


// --- Function to display errors ---
function displayError(message) {
    const errorDisplay = document.getElementById('errorDisplay');
    if (errorDisplay) {
        errorDisplay.textContent = message;
        errorDisplay.style.display = 'block';
    } else {
        console.error("DEBUG ERROR: Cannot display error - errorDisplay element missing.");
        console.error("Original error message:", message);
    }
}

// --- Function to show/hide loading indicator ---
function showLoading(show) {
    const loadingIndicator = document.getElementById('loadingIndicator');
    const getMapButton = document.getElementById('getMapButton');
    const showSafetyButton = document.getElementById('showSafetyButton');

    if (loadingIndicator) loadingIndicator.style.display = show ? 'block' : 'none';
    if (getMapButton) getMapButton.disabled = show;
    if (showSafetyButton) showSafetyButton.disabled = show;

     const safetyToggles = document.querySelectorAll('#safetyTogglesContainer button');
     safetyToggles.forEach(btn => btn.disabled = show);
}

// --- Function to update button active states ---
function updateButtonStates() {
    const showAmButton = document.getElementById('showAmButton');
    const showPmButton = document.getElementById('showPmButton');
    const showSafetyButton = document.getElementById('showSafetyButton');
    const safetyTogglesContainer = document.getElementById('safetyTogglesContainer');
    const safetyToggleAM = document.getElementById('safetyToggleAM');
    const safetyTogglePM = document.getElementById('safetyTogglePM');
    const safetyToggleRoundTrip = document.getElementById('safetyToggleRoundTrip');

    if (!showAmButton || !showPmButton || !showSafetyButton || !safetyTogglesContainer || !safetyToggleAM || !safetyTogglePM || !safetyToggleRoundTrip) {
        console.error("DEBUG ERROR: One or more button/toggle elements not found in updateButtonStates.");
        return;
    }

    showAmButton.classList.remove('active', 'safety-active');
    showPmButton.classList.remove('active', 'safety-active');
    showSafetyButton.classList.remove('active', 'safety-active');

    if (isSafetyViewActive) {
        showSafetyButton.classList.add('active', 'safety-active');
        safetyTogglesContainer.style.display = 'block';
        safetyToggleAM.classList.toggle('active', currentSafetyPeriod === 'AM');
        safetyTogglePM.classList.toggle('active', currentSafetyPeriod === 'PM');
        safetyToggleRoundTrip.classList.toggle('active', currentSafetyPeriod === 'RoundTrip');
    } else {
        safetyTogglesContainer.style.display = 'none';
        // Active state for AM/PM buttons is handled within showAmMap/showPmMap calls
    }
}


// --- Main execution block after HTML is loaded ---
document.addEventListener('DOMContentLoaded', () => {
    console.log("DEBUG: DOM Loaded");

    // --- Get DOM References ---
    const getMapButton = document.getElementById('getMapButton');
    const routeInput = document.getElementById('routeInput');
    const dateInput = document.getElementById('dateInput');
    const showAmButton = document.getElementById('showAmButton');
    const showPmButton = document.getElementById('showPmButton');
    const showSafetyButton = document.getElementById('showSafetyButton');
    const errorDisplay = document.getElementById('errorDisplay');
    const dviLinkElement = document.getElementById('dviLink');
    const optLinkElement = document.getElementById('optLink');
    const optTableContainer = document.getElementById('optTableContainer');
    const optTableContent = document.getElementById('optTableContent');
    const downloadOptButton = document.getElementById('download-opt-btn');
    const sidebarTitle = document.getElementById('sidebar-title');
    const sidebarVehicleSpan = document.getElementById('sidebar-vehicle');
    const sidebarRouteSpan = document.getElementById('sidebar-route');
    const sidebarDriverNameSpan = document.getElementById('sidebar-driver-name');
    const sidebarDriverPhoneSpan = document.getElementById('sidebar-driver-phone');
    const safetyTogglesContainer = document.getElementById('safetyTogglesContainer');

    // --- Initialize Map ---
    initializeMap();

    // --- Event Listener for Get Map Button ---
    getMapButton.addEventListener('click', async () => {
        if (!mapInitialized) { displayError("Map initialization failed. Cannot get map."); return; }
        const route = routeInput.value.trim(); const date = dateInput.value;
        if (!route || !date) { displayError("Please enter both a route and a date."); return; }
        showLoading(true); if(errorDisplay) errorDisplay.style.display = 'none';
        if (routeLayer) routeLayer.clearLayers(); if (stopsLayer) stopsLayer.clearLayers(); if (safetyLayer) safetyLayer.clearLayers();
        isSafetyViewActive = false; currentSafetyPeriod = 'RoundTrip';
        currentAmMapData = null; currentPmMapData = null; currentOptData = null; currentSafetyData = null;
        currentMapDate = date; currentMapRoute = route;
        updateButtonStates(); if(map) map.setView([40.7128, -74.0060], 11);
        if (dviLinkElement) { dviLinkElement.href = "#"; dviLinkElement.removeAttribute("target"); dviLinkElement.style.opacity = 0.5; }
        if (optTableContainer) optTableContainer.style.display = 'none'; if (optTableContent) optTableContent.innerHTML = '<p id="opt-placeholder">OPT data will appear here.</p>'; if (downloadOptButton) downloadOptButton.style.display = 'none';
        if (sidebarTitle) sidebarTitle.textContent = 'Trip Details'; if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = '--'; if (sidebarRouteSpan) sidebarRouteSpan.textContent = '--'; if (sidebarDriverNameSpan) sidebarDriverNameSpan.textContent = '--'; if (sidebarDriverPhoneSpan) sidebarDriverPhoneSpan.textContent = '--';

        console.log("DEBUG: Starting fetch to /get_map");
        try {
            const response = await fetch('/get_map', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ route: route, date: date }), });
            if (!response.ok) { let errorMsg = `HTTP error! Status: ${response.status}`; try { const errorData = await response.json(); errorMsg = errorData.error || errorMsg; } catch (e) {} throw new Error(errorMsg); }
            const data = await response.json(); console.log("DEBUG: Received data from /get_map:", data);
            currentAmMapData = data.am_map_data; currentPmMapData = data.pm_map_data; currentOptData = data.opt_data;
            const initialVehicle = currentAmMapData?.vehicle_number || currentPmMapData?.vehicle_number || '--';
            const initialRoute = route || '--'; const driverName = data.driver_name || 'N/A'; const driverPhone = data.driver_phone || 'N/A';
            if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = initialVehicle; if (sidebarRouteSpan) sidebarRouteSpan.textContent = initialRoute;
            if (sidebarDriverNameSpan) sidebarDriverNameSpan.textContent = driverName; if (sidebarDriverPhoneSpan) sidebarDriverPhoneSpan.textContent = driverPhone;
            const dviLinkUrl = data.dvi_link;
            if (dviLinkElement && dviLinkUrl && dviLinkUrl !== "#") { dviLinkElement.href = dviLinkUrl; dviLinkElement.target = "_blank"; dviLinkElement.style.opacity = 1; }
            else { if(dviLinkElement) { dviLinkElement.href = "#"; dviLinkElement.removeAttribute("target"); dviLinkElement.style.opacity = 0.5; } }
            showAmMap(); // Default to AM view
        } catch (error) {
            console.error("DEBUG: Error caught during Get Map fetch:", error); displayError(`Failed to load map data: ${error.message}`);
            currentAmMapData = null; currentPmMapData = null; currentOptData = null; currentSafetyData = null; currentMapDate = null; currentMapRoute = null; isSafetyViewActive = false;
            if (dviLinkElement) { dviLinkElement.href = "#"; dviLinkElement.removeAttribute("target"); dviLinkElement.style.opacity = 0.5; } if (optTableContainer) optTableContainer.style.display = 'none'; if (optTableContent) optTableContent.innerHTML = '<p id="opt-placeholder">OPT data will appear here.</p>'; if (downloadOptButton) downloadOptButton.style.display = 'none'; if (sidebarTitle) sidebarTitle.textContent = 'Trip Details'; if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = '--'; if (sidebarRouteSpan) sidebarRouteSpan.textContent = '--'; if (sidebarDriverNameSpan) sidebarDriverNameSpan.textContent = '--'; if (sidebarDriverPhoneSpan) sidebarDriverPhoneSpan.textContent = '--';
            if(routeLayer) routeLayer.clearLayers(); if(stopsLayer) stopsLayer.clearLayers(); if(safetyLayer) safetyLayer.clearLayers();
            if(map) map.setView([40.7128, -74.0060], 11); updateButtonStates();
        } finally { showLoading(false); }
    }); // End getMapButton listener

    // --- Event Listeners for AM/PM Toggle Buttons ---
    function showAmMap() {
        if (!mapInitialized) return;
        if (isSafetyViewActive) { if(safetyLayer) safetyLayer.clearLayers(); isSafetyViewActive = false; }
        if(showAmButton) showAmButton.classList.add('active'); if(showPmButton) showPmButton.classList.remove('active'); if(showSafetyButton) showSafetyButton.classList.remove('active', 'safety-active');
        updateButtonStates(); displayMapData(currentAmMapData, true);
        if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = currentAmMapData?.vehicle_number || '--';
        console.log("DEBUG: Switched view to AM Map");
    }
    function showPmMap() {
        if (!mapInitialized) return;
         if (isSafetyViewActive) { if(safetyLayer) safetyLayer.clearLayers(); isSafetyViewActive = false; }
        if(showPmButton) showPmButton.classList.add('active'); if(showAmButton) showAmButton.classList.remove('active'); if(showSafetyButton) showSafetyButton.classList.remove('active', 'safety-active');
        updateButtonStates(); displayMapData(currentPmMapData, false);
        if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = currentPmMapData?.vehicle_number || '--';
        console.log("DEBUG: Switched view to PM Map");
    }
    if (showAmButton) showAmButton.addEventListener('click', showAmMap);
    if (showPmButton) showPmButton.addEventListener('click', showPmMap);

    // --- Event Listener for Safety Summary Button ---
    if (showSafetyButton) {
        showSafetyButton.addEventListener('click', () => {
            if (!currentMapDate || !currentMapRoute) { displayError("Please load a map first using 'Get Map'."); return; }
            console.log("DEBUG: Safety Summary button clicked. Fetching for period:", currentSafetyPeriod);
            fetchAndDisplaySafetySummary(currentSafetyPeriod);
        });
    }

    // --- Event Listeners for Safety Period Toggles (Using Event Delegation) ---
    if (safetyTogglesContainer) {
        safetyTogglesContainer.addEventListener('click', (event) => {
            const button = event.target.closest('button');
            if (!button || !button.dataset.period) return;
            const selectedPeriod = button.dataset.period;
            if (selectedPeriod && selectedPeriod !== currentSafetyPeriod) {
                console.log(`DEBUG: Safety period changed to: ${selectedPeriod}`);
                currentSafetyPeriod = selectedPeriod;
                fetchAndDisplaySafetySummary(currentSafetyPeriod);
            } else if (selectedPeriod === currentSafetyPeriod) {
                console.log(`DEBUG: Safety period toggle clicked, but period (${selectedPeriod}) hasn't changed.`);
            }
        });
    }

    // --- Function to Fetch and Display Safety Summary Data ---
    async function fetchAndDisplaySafetySummary(timePeriod) {
         console.log(`DEBUG: Fetching safety summary for period: ${timePeriod}`);
         if (!currentMapDate || !currentMapRoute) { displayError("Cannot fetch safety data: Base map data (route/date) is missing."); return; }
         let deviceId = null; let vehicleNumber = null;
         if (timePeriod === 'AM') { deviceId = currentAmMapData?.device_id; vehicleNumber = currentAmMapData?.vehicle_number; }
         else if (timePeriod === 'PM') { deviceId = currentPmMapData?.device_id; vehicleNumber = currentPmMapData?.vehicle_number; }
         else { deviceId = currentAmMapData?.device_id || currentPmMapData?.device_id; vehicleNumber = currentAmMapData?.vehicle_number || currentPmMapData?.vehicle_number; }

         if (!deviceId || !vehicleNumber) {
             const missingInfo = []; if (!deviceId) missingInfo.push("Device ID"); if (!vehicleNumber) missingInfo.push("Vehicle Number");
             const periodInfo = (timePeriod === 'RoundTrip') ? "AM or PM" : timePeriod;
             displayError(`Cannot fetch safety data for ${timePeriod}. Missing ${missingInfo.join(' and ')} for the ${periodInfo} trip.`);
             isSafetyViewActive = true; if(safetyLayer) safetyLayer.clearLayers(); updateButtonStates(); return;
         }

         showLoading(true); isSafetyViewActive = true; updateButtonStates();

         try {
             console.log(`DEBUG: Calling /get_safety_summary with deviceId: ${deviceId}, vehicle: ${vehicleNumber}, date: ${currentMapDate}, period: ${timePeriod}`);
             const response = await fetch('/get_safety_summary', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ device_id: deviceId, vehicle_number: vehicleNumber, date: currentMapDate, time_period: timePeriod }) });
             if (!response.ok) { const errorData = await response.json(); throw new Error(errorData.error || `HTTP error! status: ${response.status}`); }
             const safetyData = await response.json();
             console.log("DEBUG: Received safety data from /get_safety_summary:", safetyData);
             if (!safetyData || !Array.isArray(safetyData.annotated_trace)) { throw new Error("Received invalid safety data format from server."); }
             currentSafetyData = safetyData.annotated_trace; // Store the potentially annotated trace
             displaySafetyLayer(currentSafetyData); // Display the layer (will show base + exceptions)
         } catch (error) {
             console.error("DEBUG ERROR fetching or displaying safety summary:", error); displayError(`Failed to load safety data: ${error.message}`);
             if(safetyLayer) safetyLayer.clearLayers(); currentSafetyData = null; isSafetyViewActive = true; updateButtonStates();
         } finally { showLoading(false); }
     } // End fetchAndDisplaySafetySummary

    // --- OPT Link, Download Button Listeners, and Helper Functions (Keep As Is) ---
    function displayOptTable() { /* ... Keep implementation ... */ if (!optTableContainer || !optTableContent) { return; } const downloadBtn = document.getElementById('download-opt-btn'); if (currentOptData && Array.isArray(currentOptData) && currentOptData.length > 0) { const desiredColumns = [ 'seg_no', 'School_Code_&_Name', 'hndc_code', 'pupil_id_no', 'first_name', 'last_name', 'address', 'zip', 'ph', 'amb_cd', 'sess_beg', 'sess_end', 'med_alert', 'am', 'pm' ]; const availableColumns = desiredColumns.filter(col => currentOptData[0].hasOwnProperty(col)); let tableHTML = '<table id="opt-table" border="1" style="width:100%; border-collapse: collapse; font-size: 0.8em;">'; tableHTML += '<thead><tr style="background-color: #f2f2f2;">'; availableColumns.forEach(header => { tableHTML += `<th style="padding: 4px; text-align: left;">${header}</th>`; }); tableHTML += '</tr></thead>'; tableHTML += '<tbody>'; currentOptData.forEach((row, index) => { const rowStyle = index % 2 === 0 ? '' : 'background-color: #f9f9f9;'; tableHTML += `<tr style="${rowStyle}">`; availableColumns.forEach(columnKey => { const value = row[columnKey] ?? ''; const escapedValue = String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); tableHTML += `<td style="padding: 4px; vertical-align: top;">${escapedValue}</td>`; }); tableHTML += '</tr>'; }); tableHTML += '</tbody></table>'; optTableContent.innerHTML = tableHTML; optTableContainer.style.display = 'block'; if (downloadBtn) downloadBtn.style.display = 'inline-block'; } else { optTableContent.innerHTML = '<p style="padding: 10px; font-style: italic; color: #666;">No OPT data details available.</p>'; optTableContainer.style.display = 'block'; if (downloadBtn) downloadBtn.style.display = 'none'; } }
    function downloadTableAsCSV(tableId, filename) { /* ... Keep implementation ... */ filename = filename || 'download.csv'; const table = document.getElementById(tableId); if (!table) { console.error("DEBUG ERROR: Table not found for CSV download:", tableId); return; } let csv = []; const rows = table.querySelectorAll("tr"); const escapeCSV = function(cellData) { if (cellData == null) { return ''; } let data = cellData.toString().replace(/"/g, '""'); if (data.search(/("|,|\n)/g) >= 0) { data = '"' + data + '"'; } return data; }; for (let i = 0; i < rows.length; i++) { const row = [], cols = rows[i].querySelectorAll("td, th"); for (let j = 0; j < cols.length; j++) { let cellText = (cols[j].textContent || cols[j].innerText || '').trim(); row.push(escapeCSV(cellText)); } csv.push(row.join(",")); } const csvContent = "\uFEFF" + csv.join("\n"); const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' }); const link = document.createElement("a"); if (navigator.msSaveBlob) { navigator.msSaveBlob(blob, filename); } else if (link.download !== undefined) { const url = URL.createObjectURL(blob); link.setAttribute("href", url); link.setAttribute("download", filename); link.style.visibility = 'hidden'; document.body.appendChild(link); link.click(); document.body.removeChild(link); URL.revokeObjectURL(url); } else { console.warn("CSV download method not fully supported, attempting fallback."); window.open('data:text/csv;charset=utf-8,' + encodeURIComponent(csvContent)); } }
    // Add listeners for OPT table display and download
    if (optLinkElement) { optLinkElement.addEventListener('click', (event) => { event.preventDefault(); displayOptTable(); }); }
    if (downloadOptButton) { downloadOptButton.addEventListener('click', function() { const routeForFilename = routeInput.value.trim() || 'ROUTE'; const dateForFilename = dateInput.value || 'DATE'; downloadTableAsCSV('opt-table', `opt_${routeForFilename}_${dateForFilename}.csv`); }); }

}); // End DOMContentLoaded










