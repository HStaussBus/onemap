// static/script.js

// --- Leaflet Map Variables ---
let map;
let routeLayer;
let stopsLayer;
let mapInitialized = false;

// --- Store fetched data ---
let currentAmMapData = null;
let currentPmMapData = null;
let currentAmDeviceId = null;
let currentPmDeviceId = null;
let currentOptData = null;

// --- Marker Icons (REMOVED - Using DivIcon now) ---

// --- Initialize Map Function ---
function initializeMap() {
    const mapContainer = document.getElementById('mapContainer');
    if (mapInitialized || !mapContainer) { /* ... */ return; }
    try {
        map = L.map(mapContainer).setView([40.7128, -74.0060], 11);
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
            maxZoom: 19,
        }).addTo(map);
        routeLayer = L.layerGroup().addTo(map);
        stopsLayer = L.layerGroup().addTo(map);
        mapInitialized = true;
        console.log("DEBUG: Leaflet map initialized successfully.");
    } catch (err) { /* ... error handling ... */
         console.error("DEBUG ERROR: Failed to initialize Leaflet map:", err);
         if (mapContainer) mapContainer.innerHTML = '<p style="color: red; text-align: center; padding: 20px;">Error: Could not load the map.</p>';
         displayError("Failed to initialize the map visualization.");
    }
} // End initializeMap


// --- Function to Display Map Data (MODIFIED: "S" for Schools) ---
function displayMapData(mapData) {
    if (!mapInitialized || !map) { /* ... */ return; }
    const vehicleNum = mapData?.vehicle_number || 'N/A';
    console.log("DEBUG: Displaying map data for vehicle:", vehicleNum);

    routeLayer.clearLayers();
    stopsLayer.clearLayers();

    if (!mapData || typeof mapData !== 'object') { /* ... */ return; }

    let bounds = L.latLngBounds([]);
    let hasTraceData = false;
    let hasStopData = false;

    // --- Process Route Trace (Polyline AND Hover Markers - Keep as is) ---
    if (mapData.trace && Array.isArray(mapData.trace) && mapData.trace.length > 0) {
        const validTracePoints = mapData.trace.filter(p => p && typeof p.lat === 'number' && typeof p.lon === 'number' && !isNaN(p.lat) && !isNaN(p.lon));
        if (validTracePoints.length > 1) {
            const traceCoords = validTracePoints.map(p => [p.lat, p.lon]);
            try { /* ... Add Polyline ... */ const polyline = L.polyline(traceCoords, { color: '#005eff', weight: 4, opacity: 0.8 }).addTo(routeLayer); bounds.extend(polyline.getBounds()); hasTraceData = true; } catch (e) { console.error("DEBUG ERROR drawing polyline:", e); }
            // Add invisible markers for HOVER effect
            validTracePoints.forEach(p => {
                try { /* ... Add Hover CircleMarkers with Tooltips ... */ const speedKph = (typeof p.spd === 'number' && !isNaN(p.spd)) ? p.spd : 0; const speedMph = Math.round(speedKph * 0.621371); let timeStr = "N/A"; if (p.ts) { try { timeStr = new Date(p.ts).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' }); } catch (timeErr) { } } const tooltipContent = `<b>${vehicleNum}</b><br>Time: ${timeStr}<br>Speed: ${speedMph} MPH`; L.circleMarker([p.lat, p.lon], { radius: 3, weight: 0, fillOpacity: 0, interactive: true }).bindTooltip(tooltipContent).addTo(routeLayer); } catch(hoverErr) { console.error("DEBUG ERROR adding hover marker:", hoverErr, p); }
            });
        }
    }

    // --- Add Stop Markers (MODIFIED: Check for School Type) ---
    if (mapData.stops && Array.isArray(mapData.stops) && mapData.stops.length > 0) {
        let addedStopsCount = 0;
        mapData.stops.forEach(stop => {
            if (stop && typeof stop.lat === 'number' && typeof stop.lon === 'number' && !isNaN(stop.lat) && !isNaN(stop.lon) && stop.sequence !== undefined && stop.sequence !== null) {
                try {
                    const stopType = stop.type || 'student';
                    // *** CHANGE HERE: Use "S" for school, sequence number otherwise ***
                    const markerText = stopType === 'school' ? 'S' : stop.sequence;
                    // ***************************************************************
                    const backgroundColor = stopType === 'school' ? '#DC3545' : '#007BFF'; // Red for school, Blue for student
                    const iconSize = 24;

                    const iconHtml = `
                        <div style="
                            font-size: 10pt; /* Adjusted font size slightly */
                            color: white;
                            font-weight: bold;
                            text-align:center;
                            width:${iconSize}px;
                            height:${iconSize}px;
                            line-height:${iconSize}px; /* Match height for vertical center */
                            background:${backgroundColor};
                            border-radius:50%;
                            border: 1px solid #FFFFFF;
                            box-shadow: 1px 1px 3px rgba(0,0,0,0.5);
                            display: flex; /* Use flexbox for centering */
                            justify-content: center;
                            align-items: center;">
                            ${markerText} </div>`;

                    const numberedIcon = L.divIcon({
                        html: iconHtml,
                        className: '',
                        iconSize: [iconSize, iconSize],
                        iconAnchor: [iconSize / 2, iconSize / 2]
                    });

                    const marker = L.marker([stop.lat, stop.lon], {
                        icon: numberedIcon
                    });

                    marker.bindPopup(stop.info || `Type: ${stopType} #${stop.sequence}`); // Popup still shows sequence number if needed
                    stopsLayer.addLayer(marker);
                    bounds.extend([stop.lat, stop.lon]);
                    addedStopsCount++;
                    hasStopData = true;
                } catch (e) { console.error("DEBUG ERROR adding numbered marker:", e, stop); }
            } else {
                 console.warn("DEBUG WARN: Skipping invalid stop data for numbered marker:", stop);
            }
        });
        console.log(`DEBUG: Added ${addedStopsCount} numbered stop markers.`);
    } else {
        console.log("DEBUG: No stop data available.");
    }

    // --- Adjust Map View (Keep as is) ---
    try {
        if (bounds.isValid()) { /* ... map.fitBounds ... */ map.fitBounds(bounds, { padding: [30, 30] }); }
        else { /* ... map.setView fallback ... */ map.setView([40.7128, -74.0060], 11); }
    } catch (e) { /* ... error handling ... */ map.setView([40.7128, -74.0060], 11); }
} // End displayMapData


// --- Function to display errors ---
function displayError(message) { /* ... keep as is ... */
    const errorDisplay = document.getElementById('errorDisplay');
    if (errorDisplay) { errorDisplay.textContent = message; errorDisplay.style.display = 'block'; }
    else { console.error("DEBUG ERROR: Cannot display error - errorDisplay element missing."); console.error("Original error message:", message); }
}


// --- Main execution block after HTML is loaded ---
document.addEventListener('DOMContentLoaded', () => {
    console.log("DEBUG: DOM Loaded");
    // Get references
    const getMapButton = document.getElementById('getMapButton'); /* ... other refs ... */
    const routeInput = document.getElementById('routeInput'); const dateInput = document.getElementById('dateInput'); const mapContainer = document.getElementById('mapContainer'); const showAmButton = document.getElementById('showAmButton'); const showPmButton = document.getElementById('showPmButton'); const loadingIndicator = document.getElementById('loadingIndicator'); const errorDisplay = document.getElementById('errorDisplay'); const dviLinkElement = document.getElementById('dviLink'); const optLinkElement = document.getElementById('optLink'); const optTableContainer = document.getElementById('optTableContainer'); const optTableContent = document.getElementById('optTableContent'); const downloadOptButton = document.getElementById('download-opt-btn'); const sidebarTitle = document.getElementById('sidebar-title'); const sidebarVehicleSpan = document.getElementById('sidebar-vehicle'); const sidebarRouteSpan = document.getElementById('sidebar-route');

    // Initialize Map
    initializeMap();

    // --- Event Listener for Get Map Button (Keep as is) ---
    getMapButton.addEventListener('click', async () => {
        // ... (keep full implementation: reset UI, fetch, store data, update sidebar, update DVI, call displayMapData, call showAmMap) ...
        if (!mapInitialized) { displayError("Map initialization failed."); return; } const route = routeInput.value.trim(); const date = dateInput.value; if (!route || !date) { displayError("Please enter both a route and a date."); return; } if(loadingIndicator) loadingIndicator.style.display = 'block'; if(errorDisplay) errorDisplay.style.display = 'none'; if(routeLayer) routeLayer.clearLayers(); if(stopsLayer) stopsLayer.clearLayers(); if(map) map.setView([40.7128, -74.0060], 11); if (dviLinkElement) { dviLinkElement.href = "#"; dviLinkElement.removeAttribute("target"); dviLinkElement.style.opacity = 0.5; } currentOptData = null; if (optTableContainer) optTableContainer.style.display = 'none'; if (optTableContent) optTableContent.innerHTML = ''; if (downloadOptButton) downloadOptButton.style.display = 'none'; if (sidebarTitle) sidebarTitle.textContent = 'Trip Details'; if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = '--'; if (sidebarRouteSpan) sidebarRouteSpan.textContent = '--'; currentAmMapData = null; currentPmMapData = null; currentAmDeviceId = null; currentPmDeviceId = null;
        console.log("DEBUG: Starting fetch to /get_map (expecting JSON)");
        try { const response = await fetch('/get_map', { method: 'POST', headers: { 'Content-Type': 'application/json', }, body: JSON.stringify({ route: route, date: date }), }); if(loadingIndicator) loadingIndicator.style.display = 'none'; if (!response.ok) { let errorMsg = `HTTP error! Status: ${response.status} ${response.statusText}`; try { const errorData = await response.json(); errorMsg = errorData.error || errorMsg; } catch (e) {} throw new Error(errorMsg); } const data = await response.json(); console.log("DEBUG: Received data keys:", data ? Object.keys(data) : "null/undefined");
             currentAmMapData = data.am_map_data; currentPmMapData = data.pm_map_data; currentOptData = data.opt_data; currentAmDeviceId = currentAmMapData?.device_id; currentPmDeviceId = currentPmMapData?.device_id;
             const initialVehicle = currentAmMapData?.vehicle_number || currentPmMapData?.vehicle_number || '--'; const initialRoute = route || '--'; if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = initialVehicle; if (sidebarRouteSpan) sidebarRouteSpan.textContent = initialRoute;
             const dviLinkUrl = data.dvi_link; console.log("DEBUG: Received DVI Link from backend:", dviLinkUrl); if (dviLinkElement && dviLinkUrl && dviLinkUrl !== "#") { console.log("DEBUG: Applying valid DVI link"); dviLinkElement.href = dviLinkUrl; dviLinkElement.target = "_blank"; dviLinkElement.style.opacity = 1; } else { if(dviLinkElement) { dviLinkElement.href = "#"; dviLinkElement.removeAttribute("target"); dviLinkElement.style.opacity = 0.5; } }
             displayMapData(currentAmMapData); showAmMap();
         } catch (error) { /* ... Error handling ... */ if(loadingIndicator) loadingIndicator.style.display = 'none'; console.error("DEBUG: Error caught:", error); displayError(`Failed to load map data: ${error.message}`); currentAmMapData = null; currentPmMapData = null; currentOptData = null; currentAmDeviceId = null; currentPmDeviceId = null; if (dviLinkElement) { dviLinkElement.href = "#"; dviLinkElement.removeAttribute("target"); dviLinkElement.style.opacity = 0.5; } if (optTableContainer) optTableContainer.style.display = 'none'; if (optTableContent) optTableContent.innerHTML = ''; if (downloadOptButton) downloadOptButton.style.display = 'none'; if (sidebarTitle) sidebarTitle.textContent = 'Trip Details'; if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = '--'; if (sidebarRouteSpan) sidebarRouteSpan.textContent = '--'; if(routeLayer) routeLayer.clearLayers(); if(stopsLayer) stopsLayer.clearLayers(); if(map) map.setView([40.7128, -74.0060], 11); }
    }); // End getMapButton listener


    // --- Event Listeners for Toggle Buttons (Keep as is) ---
    function showAmMap() { /* ... keep implementation ... */ if (!mapInitialized) return; showAmButton.classList.add('active'); showPmButton.classList.remove('active'); displayMapData(currentAmMapData); if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = currentAmMapData?.vehicle_number || '--'; console.log("DEBUG: Switched view to AM Map"); }
    function showPmMap() { /* ... keep implementation ... */ if (!mapInitialized) return; showPmButton.classList.add('active'); showAmButton.classList.remove('active'); displayMapData(currentPmMapData); if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = currentPmMapData?.vehicle_number || '--'; console.log("DEBUG: Switched view to PM Map"); }
    if (showAmButton && showPmButton) { showAmButton.addEventListener('click', showAmMap); showPmButton.addEventListener('click', showPmMap); }


    // --- OPT Link, Download Button Listeners, and Helper Functions (Keep As Is) ---
    // function displayOptTable() { ... } // Keep as is
    // function downloadTableAsCSV(tableId, filename) { ... } // Keep as is
    if (optLinkElement) { optLinkElement.addEventListener('click', (event) => { event.preventDefault(); displayOptTable(); }); }
    if (downloadOptButton) { downloadOptButton.addEventListener('click', function() { const routeForFilename = routeInput.value.trim() || 'ROUTE'; const dateForFilename = dateInput.value || 'DATE'; downloadTableAsCSV('opt-table', `opt_${routeForFilename}_${dateForFilename}.csv`); }); }
    function displayOptTable() { /* Keep implementation */ if (!optTableContainer || !optTableContent) { return; } const downloadBtn = document.getElementById('download-opt-btn'); if (currentOptData && Array.isArray(currentOptData) && currentOptData.length > 0) { const desiredColumns = [ 'seg_no', 'School_Code_&_Name', 'hndc_code', 'pupil_id_no', 'first_name', 'last_name', 'address', 'zip', 'ph', 'amb_cd', 'sess_beg', 'sess_end', 'med_alert', 'am', 'pm' ]; let tableHTML = '<table id="opt-table" border="1" style="width:100%; border-collapse: collapse; font-size: 0.8em;">'; tableHTML += '<thead><tr style="background-color: #f2f2f2;">'; desiredColumns.forEach(header => { tableHTML += `<th style="padding: 4px; text-align: left;">${header}</th>`; }); tableHTML += '</tr></thead>'; tableHTML += '<tbody>'; currentOptData.forEach((row, index) => { const rowStyle = index % 2 === 0 ? '' : 'background-color: #f9f9f9;'; tableHTML += `<tr style="${rowStyle}">`; desiredColumns.forEach(columnKey => { const value = row[columnKey] ?? ''; const escapedValue = String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;'); tableHTML += `<td style="padding: 4px; vertical-align: top;">${escapedValue}</td>`; }); tableHTML += '</tr>'; }); tableHTML += '</tbody></table>'; optTableContent.innerHTML = tableHTML; optTableContainer.style.display = 'block'; if (downloadBtn) downloadBtn.style.display = 'inline-block'; } else { optTableContent.innerHTML = '<p style="padding: 10px; font-style: italic; color: #666;">No OPT data details available.</p>'; optTableContainer.style.display = 'block'; if (downloadBtn) downloadBtn.style.display = 'none'; } }
    function downloadTableAsCSV(tableId, filename) { /* Keep implementation */ filename = filename || 'download.csv'; const table = document.getElementById(tableId); if (!table) { return; } let csv = []; const rows = table.querySelectorAll("tr"); const escapeCSV = function(cellData) { if (cellData == null) { return ''; } let data = cellData.toString().replace(/"/g, '""'); if (data.search(/("|,|\n)/g) >= 0) { data = '"' + data + '"'; } return data; }; for (let i = 0; i < rows.length; i++) { const row = [], cols = rows[i].querySelectorAll("td, th"); for (let j = 0; j < cols.length; j++) { let cellText = cols[j].textContent || cols[j].innerText; row.push(escapeCSV(cellText.trim())); } csv.push(row.join(",")); } const csvContent = "\uFEFF" + csv.join("\n"); const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' }); const link = document.createElement("a"); if (navigator.msSaveBlob) { navigator.msSaveBlob(blob, filename); } else if (link.download !== undefined) { const url = URL.createObjectURL(blob); link.setAttribute("href", url); link.setAttribute("download", filename); link.style.visibility = 'hidden'; document.body.appendChild(link); link.click(); document.body.removeChild(link); URL.revokeObjectURL(url); } else { window.open('data:text/csv;charset=utf-8,' + encodeURIComponent(csvContent)); } }


}); // End DOMContentLoaded