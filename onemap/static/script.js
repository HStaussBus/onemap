// static/script.js

// --- Leaflet Map Variables ---
let map;
let routeLayer;       // Layer for AM/PM route polylines
let stopsLayer;       // Layer for AM/PM stop markers
let safetyLayer;      // Layer for safety event markers (speeding, idling)
let mapInitialized = false;

// --- Store fetched data ---
let currentAmMapData = null;
let currentPmMapData = null;
let currentAmDeviceId = null;  // Geotab Device ID for AM vehicle
let currentPmDeviceId = null;  // Geotab Device ID for PM vehicle
let currentDeviceId = null;    // Geotab Device ID for the currently displayed map (AM or PM)
let currentOptData = null;
let currentSafetyDataCache = null; // Cache for fetched safety data

// --- State variable for safety layer visibility ---
let showingSafetyData = false;

// --- Initialize Map Function ---
function initializeMap() {
    const mapContainer = document.getElementById('mapContainer');
    // Prevent re-initialization
    if (mapInitialized || !mapContainer) {
         console.log("DEBUG: Map initialization skipped (already done or container missing).");
         return;
     }
    try {
        map = L.map(mapContainer).setView([40.7128, -74.0060], 11); // Centered on NYC approx
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
            maxZoom: 19,
        }).addTo(map);

        // Initialize layer groups and add them to the map
        routeLayer = L.layerGroup().addTo(map);
        stopsLayer = L.layerGroup().addTo(map);
        safetyLayer = L.layerGroup().addTo(map); // Initialize and add safety layer (initially empty)

        mapInitialized = true;
        console.log("DEBUG: Leaflet map and all layers initialized successfully.");

    } catch (err) {
         console.error("DEBUG ERROR: Failed to initialize Leaflet map:", err);
         if (mapContainer) mapContainer.innerHTML = '<p style="color: red; text-align: center; padding: 20px;">Error: Could not load the map.</p>';
         displayError("Failed to initialize the map visualization.");
         mapInitialized = false; // Ensure flag is false on error
    }
} // End initializeMap


// --- Function to Display Regular Map Data (Route/Stops) ---
function displayMapData(mapData) {
    // Ensure map and layers are ready
    console.log('Data received by displayMapData:', JSON.stringify(mapData, null, 2));
    if (!mapInitialized || !map || !routeLayer || !stopsLayer) {
        console.warn("DEBUG WARN: displayMapData called before map/layers initialized.");
        return;
    }
    const vehicleNum = mapData?.vehicle_number || 'N/A';
    console.log("DEBUG: Displaying map data for vehicle:", vehicleNum);

    // Clear previous route and stop layers before drawing new ones
    routeLayer.clearLayers();
    stopsLayer.clearLayers();

    if (!mapData || typeof mapData !== 'object') {
        console.log("DEBUG: No valid map data provided to displayMapData.");
        return; // Exit if no valid data
    }

    let bounds = L.latLngBounds([]); // To fit map view later
    let hasTraceData = false;
    let hasStopData = false;

    // --- Process Route Trace (Polyline AND Hover Markers) ---
    if (mapData.trace && Array.isArray(mapData.trace) && mapData.trace.length > 0) {
        const validTracePoints = mapData.trace.filter(p => p && typeof p.lat === 'number' && typeof p.lon === 'number' && !isNaN(p.lat) && !isNaN(p.lon));
        if (validTracePoints.length > 1) {
            const traceCoords = validTracePoints.map(p => [p.lat, p.lon]);
            try {
                 const polyline = L.polyline(traceCoords, { color: '#005eff', weight: 4, opacity: 0.8 }).addTo(routeLayer);
                 bounds.extend(polyline.getBounds());
                 hasTraceData = true;
            } catch (e) { console.error("DEBUG ERROR drawing polyline:", e); }

            // Add invisible markers for HOVER effect
            validTracePoints.forEach(p => {
                try {
                    const speedKph = (typeof p.spd === 'number' && !isNaN(p.spd)) ? p.spd : 0;
                    const speedMph = Math.round(speedKph * 0.621371);
                    let timeStr = "N/A";
                    if (p.ts) { try { timeStr = new Date(p.ts).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' }); } catch (timeErr) {} }
                    const tooltipContent = `<b>${vehicleNum}</b><br>Time: ${timeStr}<br>Speed: ${speedMph} MPH`;
                    L.circleMarker([p.lat, p.lon], { radius: 3, weight: 0, fillOpacity: 0, interactive: true })
                     .bindTooltip(tooltipContent)
                     .addTo(routeLayer); // Add to routeLayer
                } catch(hoverErr) { console.error("DEBUG ERROR adding hover marker:", hoverErr, p); }
            });
        }
    } else {
         console.log("DEBUG: No valid trace data found.");
    }


    // --- Add Stop Markers (Student/School) ---
    if (mapData.stops && Array.isArray(mapData.stops) && mapData.stops.length > 0) {
        let addedStopsCount = 0;
        mapData.stops.forEach(stop => {
            // Basic validation of stop data
            if (stop && typeof stop.lat === 'number' && typeof stop.lon === 'number' && !isNaN(stop.lat) && !isNaN(stop.lon) && stop.sequence !== undefined && stop.sequence !== null) {
                try {
                    const stopType = stop.type || 'student';
                    const markerText = stopType === 'school' ? 'S' : stop.sequence; // Use 'S' for school icon text
                    const backgroundColor = stopType === 'school' ? '#DC3545' : '#007BFF'; // Red for school, Blue for student
                    const iconSize = 24;

                    const iconHtml = `
                        <div style="
                            font-size: 10pt; color: white; font-weight: bold; text-align:center;
                            width:<span class="math-inline">\{iconSize\}px; height\:</span>{iconSize}px; line-height:<span class="math-inline">\{iconSize\}px;
background\:</span>{backgroundColor}; border-radius:50%; border: 1px solid #FFFFFF;
                            box-shadow: 1px 1px 3px rgba(0,0,0,0.5); display: flex;
                            justify-content: center; align-items: center;">
                            ${markerText}
                        </div>`;

                    const numberedIcon = L.divIcon({
                        html: iconHtml,
                        iconSize: [iconSize, iconSize],
                        iconAnchor: [iconSize / 2, iconSize / 2] // Center anchor
                    });

                    const marker = L.marker([stop.lat, stop.lon], { icon: numberedIcon });
                    marker.bindPopup(stop.info || `Type: <span class="math-inline">\{stopType\} \#</span>{stop.sequence}`);
                    stopsLayer.addLayer(marker); // Add to stopsLayer
                    bounds.extend([stop.lat, stop.lon]);
                    addedStopsCount++;
                    hasStopData = true;
                } catch (e) { console.error("DEBUG ERROR adding numbered stop marker:", e, stop); }
            } else {
                console.warn("DEBUG WARN: Skipping invalid stop data for numbered marker:", stop);
            }
        });
        console.log(`DEBUG: Added ${addedStopsCount} numbered stop markers.`);
    } else {
        console.log("DEBUG: No stop data available.");
    }

    // --- Adjust Map View ---
    try {
        if (bounds.isValid()) {
            map.fitBounds(bounds, { padding: [30, 30] }); // Add padding
        } else if (hasTraceData || hasStopData) {
            // This case might happen if only one point exists, zoom slightly
            map.setView(bounds.getCenter(), 15);
        } else {
            // Fallback if no data at all
             map.setView([40.7128, -74.0060], 11);
        }
    } catch (e) {
        console.error("DEBUG ERROR adjusting map bounds:", e);
        map.setView([40.7128, -74.0060], 11); // Fallback view
    }
} // End displayMapData


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

// --- Function to Display Safety Data ---
function displaySafetyData(safetyData) {
    if (!mapInitialized || !safetyLayer) {
        console.warn("DEBUG WARN: displaySafetyData called before map/safetyLayer initialized.");
        return;
    }
    safetyLayer.clearLayers(); // Clear previous safety markers

    if (!safetyData) {
         console.log("DEBUG: No safety data provided to displaySafetyData.");
         return;
    }
    console.log("DEBUG: Displaying safety data:", safetyData);

    let addedSpeeding = 0;
    let addedIdling = 0;

    // Display Speeding Markers (Red Circles)
    if (safetyData.speeding_points && Array.isArray(safetyData.speeding_points)) {
        safetyData.speeding_points.forEach(p => {
            if (p && typeof p.lat === 'number' && typeof p.lon === 'number' && !isNaN(p.lat) && !isNaN(p.lon)) {
                try {
                    const speedInfo = p.max_speed_mph !== 'N/A' ? `${p.max_speed_mph} MPH` : 'N/A';
                    const timeInfo = p.timestamp ? new Date(p.timestamp).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' }) : 'N/A';
                    const info = `<b>Speeding Event</b><br>Max Speed: ${speedInfo}<br>Duration: ${p.event_duration || 'N/A'}<br>Time: ${timeInfo}`;
                    L.circleMarker([p.lat, p.lon], {
                        radius: 6,
                        fillColor: "#FF0000", // Red
                        color: "#FFFFFF",     // White border
                        weight: 1,
                        opacity: 1,
                        fillOpacity: 0.7
                    }).bindPopup(info).addTo(safetyLayer);
                    addedSpeeding++;
                } catch(e) { console.error("Error adding speeding marker:", e, p); }
            } else { console.warn("DEBUG WARN: Skipping invalid speeding point data:", p); }
        });
    }

    // Display Idling Markers (Blue Squares using DivIcon)
    if (safetyData.idling_points && Array.isArray(safetyData.idling_points)) {
        safetyData.idling_points.forEach(p => {
             if (p && typeof p.lat === 'number' && typeof p.lon === 'number' && !isNaN(p.lat) && !isNaN(p.lon)) {
                try {
                    const info = `<b>Idling Event</b><br>Duration: ${p.event_duration || 'N/A'}`;
                    // Using a DivIcon square
                     const iconHtml = `<div style="width:10px; height:10px; background-color:#007BFF; border:1px solid white; opacity: 0.8;"></div>`;
                     const idlingIcon = L.divIcon({
                         html: iconHtml,
                         iconSize: [10, 10],
                         iconAnchor: [5, 5] // Center anchor
                     });
                     L.marker([p.lat, p.lon], {icon: idlingIcon}).bindPopup(info).addTo(safetyLayer);
                     addedIdling++;
                } catch(e) { console.error("Error adding idling marker:", e, p); }
            } else { console.warn("DEBUG WARN: Skipping invalid idling point data:", p); }
        });
    }
    console.log(`DEBUG: Displayed ${addedSpeeding} speeding and ${addedIdling} idling markers.`);
} // End displaySafetyData

// --- Function to Hide/Clear Safety Data ---
function hideSafetyData() {
    if(safetyLayer) {
        safetyLayer.clearLayers();
    }
    showingSafetyData = false; // Update state
    if(showSafetyButton){
         showSafetyButton.textContent = 'Show Safety Data'; // Reset button text
         showSafetyButton.classList.remove('active'); // Deactivate button style
    }
    currentSafetyDataCache = null; // Clear cache
    console.log("DEBUG: Safety data hidden.");
} // End hideSafetyData


// --- Main execution block after HTML is loaded ---
document.addEventListener('DOMContentLoaded', () => {
    console.log("DEBUG: DOM Loaded");
    // Get references
    const getMapButton = document.getElementById('getMapButton');
    const routeInput = document.getElementById('routeInput');
    const dateInput = document.getElementById('dateInput');
    const mapContainer = document.getElementById('mapContainer');
    const showAmButton = document.getElementById('showAmButton');
    const showPmButton = document.getElementById('showPmButton');
    const loadingIndicator = document.getElementById('loadingIndicator');
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
    const showSafetyButton = document.getElementById('showSafetyButton'); // Reference safety button

    // Initialize Map (this now also initializes the empty safetyLayer)
    initializeMap();

    // --- Helper function to update currentDeviceId ---
    function updateCurrentDeviceId() {
        const amActive = showAmButton.classList.contains('active');
        currentDeviceId = amActive ? currentAmDeviceId : currentPmDeviceId; // Uses globals set during /get_map fetch
        console.log(`DEBUG: Active Device ID updated to: ${currentDeviceId} (AM Active: ${amActive})`);
    }


    // --- Event Listener for Get Map Button ---
    getMapButton.addEventListener('click', async () => {
        // --- Reset UI Elements ---
        if (!mapInitialized) { displayError("Map initialization failed."); return; }
        const route = routeInput.value.trim(); const date = dateInput.value;
        if (!route || !date) { displayError("Please enter both a route and a date."); return; }

        if(loadingIndicator) loadingIndicator.style.display = 'block';
        if(errorDisplay) errorDisplay.style.display = 'none';
        if(routeLayer) routeLayer.clearLayers();
        if(stopsLayer) stopsLayer.clearLayers();
        hideSafetyData(); // *** Hide safety data when fetching new map data ***
        if(map) map.setView([40.7128, -74.0060], 11); // Reset view

        // Reset sidebar and links
        if (dviLinkElement) { dviLinkElement.href = "#"; dviLinkElement.removeAttribute("target"); dviLinkElement.style.opacity = 0.5; }
        currentOptData = null;
        if (optTableContainer) optTableContainer.style.display = 'none';
        if (optTableContent) optTableContent.innerHTML = '';
        if (downloadOptButton) downloadOptButton.style.display = 'none';
        if (sidebarTitle) sidebarTitle.textContent = 'Trip Details';
        if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = '--';
        if (sidebarRouteSpan) sidebarRouteSpan.textContent = '--';
        if (sidebarDriverNameSpan) sidebarDriverNameSpan.textContent = '--';
        if (sidebarDriverPhoneSpan) sidebarDriverPhoneSpan.textContent = '--';

        // Reset stored data
        currentAmMapData = null; currentPmMapData = null; currentAmDeviceId = null; currentPmDeviceId = null; currentDeviceId = null;
        // --- End Reset UI ---

        console.log("DEBUG: Starting fetch to /get_map (expecting JSON)");
        try {
            const response = await fetch('/get_map', { method: 'POST', headers: { 'Content-Type': 'application/json', }, body: JSON.stringify({ route: route, date: date }), });
            if(loadingIndicator) loadingIndicator.style.display = 'none'; // Hide after fetch attempt
            if (!response.ok) { let errorMsg = `HTTP error! Status: ${response.status} ${response.statusText}`; try { const errorData = await response.json(); errorMsg = errorData.error || errorMsg; } catch (e) {} throw new Error(errorMsg); }
            const data = await response.json();
            console.log("DEBUG: Received data keys:", data ? Object.keys(data) : "null/undefined");

            // Store fetched data globally
            currentAmMapData = data.am_map_data; currentPmMapData = data.pm_map_data; currentOptData = data.opt_data;
            currentAmDeviceId = currentAmMapData?.device_id; // Store device IDs
            currentPmDeviceId = currentPmMapData?.device_id;

            // --- Update Sidebar ---
            const initialVehicle = currentAmMapData?.vehicle_number || currentPmMapData?.vehicle_number || '--';
            const initialRoute = route || '--';
            const driverName = data.driver_name || 'N/A';
            const driverPhone = data.driver_phone || 'N/A';
            if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = initialVehicle;
            if (sidebarRouteSpan) sidebarRouteSpan.textContent = initialRoute;
            if (sidebarDriverNameSpan) sidebarDriverNameSpan.textContent = driverName;
            if (sidebarDriverPhoneSpan) sidebarDriverPhoneSpan.textContent = driverPhone;
            // --- End Sidebar Update ---

            // --- Update DVI Link ---
            const dviLinkUrl = data.dvi_link; console.log("DEBUG: Received DVI Link from backend:", dviLinkUrl);
            if (dviLinkElement && dviLinkUrl && dviLinkUrl !== "#") { console.log("DEBUG: Applying valid DVI link"); dviLinkElement.href = dviLinkUrl; dviLinkElement.target = "_blank"; dviLinkElement.style.opacity = 1; }
            else { if(dviLinkElement) { dviLinkElement.href = "#"; dviLinkElement.removeAttribute("target"); dviLinkElement.style.opacity = 0.5; } }
            // --- End DVI Link ---

            // Display initial map (AM) and set active device ID
            showAmMap(); // This calls displayMapData(currentAmMapData) and updateCurrentDeviceId()

       } catch (error) {
            if(loadingIndicator) loadingIndicator.style.display = 'none';
            console.error("DEBUG: Error caught:", error); displayError(`Failed to load map data: ${error.message}`);
            // Reset state variables on error
            currentAmMapData = null; currentPmMapData = null; currentOptData = null; currentAmDeviceId = null; currentPmDeviceId = null; currentDeviceId = null; hideSafetyData();
            // Reset UI elements on error
            if (dviLinkElement) { dviLinkElement.href = "#"; dviLinkElement.removeAttribute("target"); dviLinkElement.style.opacity = 0.5; }
            if (optTableContainer) optTableContainer.style.display = 'none'; if (optTableContent) optTableContent.innerHTML = ''; if (downloadOptButton) downloadOptButton.style.display = 'none';
            if (sidebarTitle) sidebarTitle.textContent = 'Trip Details'; if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = '--'; if (sidebarRouteSpan) sidebarRouteSpan.textContent = '--'; if (sidebarDriverNameSpan) sidebarDriverNameSpan.textContent = '--'; if (sidebarDriverPhoneSpan) sidebarDriverPhoneSpan.textContent = '--';
            if(routeLayer) routeLayer.clearLayers(); if(stopsLayer) stopsLayer.clearLayers(); if(map) map.setView([40.7128, -74.0060], 11);
       }
    }); // End getMapButton listener


    // --- Event Listeners for AM/PM Toggle Buttons ---
    function showAmMap() {
        if (!mapInitialized) return;
        showAmButton.classList.add('active');
        showPmButton.classList.remove('active');
        displayMapData(currentAmMapData); // Display AM route/stops
        if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = currentAmMapData?.vehicle_number || '--';
        updateCurrentDeviceId(); // Update the active device ID
        hideSafetyData(); // Hide safety data when switching view
        console.log("DEBUG: Switched view to AM Map");
    }
    function showPmMap() {
        if (!mapInitialized) return;
        showPmButton.classList.add('active');
        showAmButton.classList.remove('active');
        displayMapData(currentPmMapData); // Display PM route/stops
        if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = currentPmMapData?.vehicle_number || '--';
        updateCurrentDeviceId(); // Update the active device ID
        hideSafetyData(); // Hide safety data when switching view
        console.log("DEBUG: Switched view to PM Map");
    }
    if (showAmButton && showPmButton) {
        showAmButton.addEventListener('click', showAmMap);
        showPmButton.addEventListener('click', showPmMap);
    }


    // --- Event Listener for Safety Button ---
    if (showSafetyButton) {
        showSafetyButton.addEventListener('click', async () => {
            if (!mapInitialized || !safetyLayer) { displayError("Map not ready."); return; }

            // updateCurrentDeviceId(); // Ensure we know which device ID is active **before** deciding action
            const currentActiveDeviceId = showAmButton.classList.contains('active') ? currentAmDeviceId : currentPmDeviceId;
            const date = dateInput.value;
            console.log(`DEBUG: Safety button clicked. Currently showing: ${showingSafetyData}. Active Device: ${currentActiveDeviceId}`);


            if (showingSafetyData) {
                // === Action: HIDE Safety Data ===
                hideSafetyData();
            } else {
                // === Action: SHOW Safety Data ===
                if (!currentActiveDeviceId) { // Use the locally determined active ID for check
                    displayError("No active vehicle selected. Load AM/PM data first.");
                    return;
                }
                 if (!date) {
                    displayError("Please select a date.");
                    return;
                }

                if(loadingIndicator) loadingIndicator.style.display = 'block';
                if(errorDisplay) errorDisplay.style.display = 'none';
                safetyLayer.clearLayers(); // Clear just in case before fetch

                try {
                    console.log(`DEBUG: Fetching safety data for Device ID: ${currentActiveDeviceId}, Date: ${date}`);
                    const response = await fetch('/get_safety_data', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json', },
                        body: JSON.stringify({ device_id: currentActiveDeviceId, date: date }),
                    });

                    // We check loadingIndicator status *after* await
                    if(loadingIndicator) loadingIndicator.style.display = 'none';

                    if (!response.ok) {
                         let errorMsg = `Safety data error! Status: ${response.status}`;
                         try { const errorData = await response.json(); errorMsg = errorData.error || errorMsg; } catch (e) {}
                         throw new Error(errorMsg);
                    }
                    const safetyData = await response.json();
                    console.log("DEBUG: Received safety data:", safetyData);

                    currentSafetyDataCache = safetyData; // Cache the data
                    displaySafetyData(currentSafetyDataCache); // Display the new data

                    // Update button state and text AFTER successful fetch and display
                    showingSafetyData = true;
                    showSafetyButton.textContent = 'Hide Safety Data';
                    showSafetyButton.classList.add('active'); // Optional: visual cue

                } catch (error) {
                    if(loadingIndicator) loadingIndicator.style.display = 'none'; // Ensure indicator hides on error too
                    console.error("DEBUG: Error fetching/displaying safety data:", error);
                    displayError(`Failed to load safety data: ${error.message}`);
                    hideSafetyData(); // Reset state/button on error
                }
            }
        });
    } // End if (showSafetyButton)


    // --- OPT Link, Download Button Listeners, and Helper Functions ---
    // ... (Keep displayOptTable and downloadTableAsCSV functions as is) ...
    if (optLinkElement) { optLinkElement.addEventListener('click', (event) => { event.preventDefault(); displayOptTable(); }); }
    if (downloadOptButton) { downloadOptButton.addEventListener('click', function() { const routeForFilename = routeInput.value.trim() || 'ROUTE'; const dateForFilename = dateInput.value || 'DATE'; downloadTableAsCSV('opt-table', `opt_${routeForFilename}_${dateForFilename}.csv`); }); }
    function displayOptTable() { /* Keep implementation */ if (!optTableContainer || !optTableContent) { return; } const downloadBtn = document.getElementById('download-opt-btn'); if (currentOptData && Array.isArray(currentOptData) && currentOptData.length > 0) { const desiredColumns = [ 'seg_no', 'School_Code_&_Name', 'hndc_code', 'pupil_id_no', 'first_name', 'last_name', 'address', 'zip', 'ph', 'amb_cd', 'sess_beg', 'sess_end', 'med_alert', 'am', 'pm' ]; let tableHTML = '<table id="opt-table" border="1" style="width:100%; border-collapse: collapse; font-size: 0.8em;">'; tableHTML += '<thead><tr style="background-color: #f2f2f2;">'; desiredColumns.forEach(header => { tableHTML += `<th style="padding: 4px; text-align: left;">${header}</th>`; }); tableHTML += '</tr></thead>'; tableHTML += '<tbody>'; currentOptData.forEach((row, index) => { const rowStyle = index % 2 === 0 ? '' : 'background-color: #f9f9f9;'; tableHTML += `<tr style="${rowStyle}">`; desiredColumns.forEach(columnKey => { const value = row[columnKey] ?? ''; const escapedValue = String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); tableHTML += `<td style="padding: 4px; vertical-align: top;">${escapedValue}</td>`; }); tableHTML += '</tr>'; }); tableHTML += '</tbody></table>'; optTableContent.innerHTML = tableHTML; optTableContainer.style.display = 'block'; if (downloadBtn) downloadBtn.style.display = 'inline-block'; } else { optTableContent.innerHTML = '<p style="padding: 10px; font-style: italic; color: #666;">No OPT data details available.</p>'; optTableContainer.style.display = 'block'; if (downloadBtn) downloadBtn.style.display = 'none'; } }
    function downloadTableAsCSV(tableId, filename) { /* Keep implementation */ filename = filename || 'download.csv'; const table = document.getElementById(tableId); if (!table) { return; } let csv = []; const rows = table.querySelectorAll("tr"); const escapeCSV = function(cellData) { if (cellData == null) { return ''; } let data = cellData.toString().replace(/"/g, '""'); if (data.search(/("|,|\n)/g) >= 0) { data = '"' + data + '"'; } return data; }; for (let i = 0; i < rows.length; i++) { const row = [], cols = rows[i].querySelectorAll("td, th"); for (let j = 0; j < cols.length; j++) { let cellText = cols[j].textContent || cols[j].innerText; row.push(escapeCSV(cellText.trim())); } csv.push(row.join(",")); } const csvContent = "\uFEFF" + csv.join("\n"); const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' }); const link = document.createElement("a"); if (navigator.msSaveBlob) { navigator.msSaveBlob(blob, filename); } else if (link.download !== undefined) { const url = URL.createObjectURL(blob); link.setAttribute("href", url); link.setAttribute("download", filename); link.style.visibility = 'hidden'; document.body.appendChild(link); link.click(); document.body.removeChild(link); URL.revokeObjectURL(url); } else { window.open('data:text/csv;charset=utf-8,' + encodeURIComponent(csvContent)); } }


}); // End DOMContentLoaded