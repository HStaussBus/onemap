// static/script.js

document.addEventListener('DOMContentLoaded', () => {
    // Log when the script starts running after the HTML is loaded
    console.log("DEBUG: DOM Loaded");

    // Get references to all necessary HTML elements
    const getMapButton = document.getElementById('getMapButton');
    const routeInput = document.getElementById('routeInput');
    const dateInput = document.getElementById('dateInput');
    const amMapContainer = document.getElementById('amMapContainer');
    const pmMapContainer = document.getElementById('pmMapContainer');
    const showAmButton = document.getElementById('showAmButton');
    const showPmButton = document.getElementById('showPmButton');
    const loadingIndicator = document.getElementById('loadingIndicator');
    const errorDisplay = document.getElementById('errorDisplay');
    const dviLinkElement = document.getElementById('dviLink');
    const optLinkElement = document.getElementById('optLink');
    const optTableContainer = document.getElementById('optTableContainer');
    const optTableContent = document.getElementById('optTableContent');
    const downloadOptButton = document.getElementById('download-opt-btn');
    // **** NEW: Get references for sidebar elements ****
    const sidebarTitle = document.getElementById('sidebar-title');
    const sidebarVehicleSpan = document.getElementById('sidebar-vehicle');
    const sidebarRouteSpan = document.getElementById('sidebar-route');
    const sidebarContent = document.getElementById('sidebar-content'); // Reference to the content div if needed

    // --- Variable to store fetched OPT data ---
    let currentOptData = null;

    // --- Debugging: Check if elements were found ---
    if (!getMapButton) { console.error("DEBUG ERROR: Get Map button (id='getMapButton') not found!"); return; }
    if (!routeInput) { console.error("DEBUG ERROR: Route input (id='routeInput') not found!"); }
    if (!dateInput) { console.error("DEBUG ERROR: Date input (id='dateInput') not found!"); }
    if (!amMapContainer) { console.error("DEBUG ERROR: AM Map container (id='amMapContainer') not found!"); }
    if (!pmMapContainer) { console.error("DEBUG ERROR: PM Map container (id='pmMapContainer') not found!"); }
    if (!showAmButton) { console.error("DEBUG ERROR: Show AM button (id='showAmButton') not found!"); }
    if (!showPmButton) { console.error("DEBUG ERROR: Show PM button (id='showPmButton') not found!"); }
    if (!loadingIndicator) { console.warn("DEBUG WARN: Loading indicator (id='loadingIndicator') not found!"); }
    if (!errorDisplay) { console.warn("DEBUG WARN: Error display (id='errorDisplay') not found!"); }
    if (!dviLinkElement) { console.warn("DEBUG WARN: DVI Link element (id='dviLink') not found!"); }
    if (!optLinkElement) { console.error("DEBUG ERROR: OPT Link element (id='optLink') not found!"); } // Should be found in sidebar now
    if (!optTableContainer) { console.error("DEBUG ERROR: OPT Table container (id='optTableContainer') not found!"); }
    if (!optTableContent) { console.error("DEBUG ERROR: OPT Table content div (id='optTableContent') not found!"); }
    if (!downloadOptButton) { console.warn("DEBUG WARN: Download OPT button (id='download-opt-btn') not found!"); }
    // **** NEW: Add checks for sidebar elements ****
    if (!sidebarTitle) { console.warn("DEBUG WARN: Sidebar title (id='sidebar-title') not found!"); }
    if (!sidebarVehicleSpan) { console.warn("DEBUG WARN: Sidebar vehicle span (id='sidebar-vehicle') not found!"); }
    if (!sidebarRouteSpan) { console.warn("DEBUG WARN: Sidebar route span (id='sidebar-route') not found!"); }
    if (!sidebarContent) { console.warn("DEBUG WARN: Sidebar content div (id='sidebar-content') not found!"); }
    // --- End Debugging Checks ---


    // --- Event Listener for Get Map Button ---
    getMapButton.addEventListener('click', async () => {
        console.log("DEBUG: Get Map button clicked");

        const route = routeInput.value.trim();
        const date = dateInput.value;

        if (!route || !date) {
            console.log("DEBUG: Route or Date input missing.");
            displayError("Please enter both a route and a date.");
            return;
        }

        // --- Reset UI before fetch ---
        // Reset DVI link
        if (dviLinkElement) {
            console.log("DEBUG: Resetting DVI link state");
            dviLinkElement.href = "#";
            dviLinkElement.removeAttribute("target");
            dviLinkElement.style.opacity = 0.5;
        } else {
            console.log("DEBUG: Skipping DVI link reset (element not found)");
        }
        // Reset OPT data and hide table
        currentOptData = null;
        if (optTableContainer) optTableContainer.style.display = 'none';
        if (optTableContent) optTableContent.innerHTML = ''; // Clear old table content
        if (downloadOptButton) downloadOptButton.style.display = 'none'; // Hide download button
        console.log("DEBUG: Reset OPT data, table display, and download button");
        // Clear Maps and Sidebar (calls showAmMap internally)
        console.log("DEBUG: Clearing map display and sidebar");
        clearMapDisplay();
        // Show loading indicator (now likely in sidebar)
        if(loadingIndicator) loadingIndicator.style.display = 'block';
        if(errorDisplay) errorDisplay.style.display = 'none'; // Hide previous errors
        // --- End Reset UI ---

        console.log("DEBUG: Starting fetch to /get_map with route:", route, "date:", date);
        try {
            // --- Fetch data from Flask backend ---
            const response = await fetch('/get_map', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', },
                body: JSON.stringify({ route: route, date: date }),
            });
            // --- End of fetch call ---

            console.log("DEBUG: Fetch response received, Status:", response.status, response.statusText);

            // --- UI Update: Hide loading ---
            if(loadingIndicator) loadingIndicator.style.display = 'none';

            // --- Handle HTTP errors ---
            if (!response.ok) {
                let errorMsg = `HTTP error! Status: ${response.status} ${response.statusText}`;
                try {
                    const errorData = await response.json();
                    errorMsg = errorData.error || errorMsg;
                } catch (e) { console.log("DEBUG: Could not parse error response as JSON."); }
                console.error("DEBUG: Fetch response not OK:", errorMsg);
                throw new Error(errorMsg); // Trigger the catch block
            }

            // --- Process successful response ---
            console.log("DEBUG: Attempting response.json()");
            const data = await response.json();
            console.log("DEBUG: Successfully parsed JSON data:", data ? Object.keys(data) : "null/undefined");
            console.log("DEBUG: Received dvi_link:", data?.dvi_link);
            console.log("DEBUG: Received opt_data type:", typeof data?.opt_data, "Length:", Array.isArray(data?.opt_data) ? data.opt_data.length : "N/A");

            // Inject map HTML
            console.log("DEBUG: Setting map innerHTML");
            try {
                if (amMapContainer) {
                    amMapContainer.innerHTML = (typeof data.am_map === 'string' && data.am_map) ? data.am_map : '<p>AM map data not available.</p>';
                } else { console.error("DEBUG ERROR: amMapContainer is null!"); }

                if (pmMapContainer) {
                     pmMapContainer.innerHTML = (typeof data.pm_map === 'string' && data.pm_map) ? data.pm_map : '<p>PM map data not available.</p>';
                } else { console.error("DEBUG ERROR: pmMapContainer is null!"); }
            } catch (innerHtmlError) {
                 console.error("DEBUG: ERROR occurred during map innerHTML assignment:", innerHtmlError);
                 // Don't display map error here, let sidebar show general error if needed
                 // displayError("Failed to display map content.");
            }
            // End of map injection

            // Update DVI Link
            console.log("DEBUG: Attempting to update DVI link");
            const dviLinkUrl = data.dvi_link;
            if (dviLinkElement && dviLinkUrl && dviLinkUrl !== "#") {
                console.log("DEBUG: Applying valid DVI link to element");
                dviLinkElement.href = dviLinkUrl;
                dviLinkElement.target = "_blank";
                dviLinkElement.style.opacity = 1;
            } else {
                console.log("DEBUG: No valid DVI link found or element missing, ensuring reset state");
                if(dviLinkElement) { // Reset again just in case
                     dviLinkElement.href = "#";
                     dviLinkElement.removeAttribute("target");
                     dviLinkElement.style.opacity = 0.5;
                 }
            }
            // End Update DVI Link

            // Store OPT Data
            console.log("DEBUG: Attempting to store OPT data");
            currentOptData = data.opt_data; // Store array (can be null/empty)
            console.log("DEBUG: Stored OPT data:", currentOptData ? `${currentOptData.length} rows` : "None or empty list");
            // End Store OPT Data

            // **** NEW: Populate Sidebar Content ****
            console.log("DEBUG: Populating sidebar");
            if (sidebarTitle) sidebarTitle.textContent = 'Trip Details';

            // **IMPORTANT**: Replace 'data.vehicle_info', 'data.route_info' below
            //              with the ACTUAL keys coming from your Flask backend response!
            //              Use optional chaining `?.` for safety if keys might be missing.
            const vehicleInfo = data?.vehicle_am ?? data?.vehicle_pm ?? '--';
            // Use the route ID processed by the backend if available, otherwise fallback to user input
            const routeInfo = data?.route_info ?? route ?? '--'; // 'route' here is the user input value

            // Update the sidebar elements
            if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = vehicleInfo;
            if (sidebarRouteSpan) sidebarRouteSpan.textContent = routeInfo;

            if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = vehicleInfo;
            if (sidebarRouteSpan) sidebarRouteSpan.textContent = routeInfo;
            // Add more lines here to populate other sidebar elements if you add them
            // e.g., if (sidebarDriverSpan) sidebarDriverSpan.textContent = data?.driver_name || '--';
            // **** END: Populate Sidebar Content ****


            // Set Default View
            console.log("DEBUG: Calling showAmMap() to set default view");
            showAmMap(); // Ensure AM map is shown by default

        } catch (error) {
            // --- Handle fetch or processing errors ---
            if(loadingIndicator) loadingIndicator.style.display = 'none';
            console.error("DEBUG: Error caught in fetch/processing block:", error);
            displayError(`Failed to load map data: ${error.message}`); // Show error in sidebar

            // Reset potentially partially updated states
            if (dviLinkElement) { // Reset DVI
                console.log("DEBUG: Resetting DVI link state due to error");
                dviLinkElement.href = "#";
                dviLinkElement.removeAttribute("target");
                dviLinkElement.style.opacity = 0.5;
            }
            currentOptData = null; // Reset OPT
            if (optTableContainer) optTableContainer.style.display = 'none';
            if (optTableContent) optTableContent.innerHTML = '';
            if (downloadOptButton) downloadOptButton.style.display = 'none';
            console.log("DEBUG: Reset OPT data state due to error");

            // **** NEW: Reset Sidebar Content on Error ****
            console.log("DEBUG: Resetting sidebar content due to error");
            if (sidebarTitle) sidebarTitle.textContent = 'Trip Details';
            if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = '--';
            if (sidebarRouteSpan) sidebarRouteSpan.textContent = '--';
            // Reset any other sidebar elements you added
           // **** END: Reset Sidebar Content on Error ****
        }
    }); // End of getMapButton listener


    // --- Event Listeners for Toggle Buttons ---
    if (showAmButton && showPmButton) {
         showAmButton.addEventListener('click', showAmMap);
         showPmButton.addEventListener('click', showPmMap);
    } else {
         console.error("DEBUG ERROR: AM/PM toggle buttons not found, toggling will not work.");
    }


    // --- Event Listener for OPT Link ---
    if (optLinkElement) {
        optLinkElement.addEventListener('click', (event) => {
            event.preventDefault(); // Prevent default link behavior
            console.log("DEBUG: OPT Info link clicked");
            displayOptTable(); // Call function to generate and show the table
        });
    } else {
         console.error("DEBUG ERROR: OPT Link element not found, cannot attach click listener.");
    }

    // --- Event Listener for Download OPT Button ---
    if (downloadOptButton) { // Check if button exists
        downloadOptButton.addEventListener('click', function() {
            console.log("DEBUG: Download OPT button clicked");
            // Call the download function, passing the table ID generated in displayOptTable
            downloadTableAsCSV('opt-table', 'opt_information.csv');
        });
    } // End of downloadOptButton listener block
    else { // Optional: Add an else block if the button wasn't found earlier
        console.warn("DEBUG WARN: Download OPT button listener not attached because button wasn't found.");
    }


    // --- Helper Functions ---
    function showAmMap() {
        if (amMapContainer && pmMapContainer && showAmButton && showPmButton) {
            amMapContainer.classList.add('active');
            pmMapContainer.classList.remove('active');
            showAmButton.classList.add('active');
            showPmButton.classList.remove('active');
            // **** NEW (Optional): Update sidebar title if needed ****
            // if (sidebarTitle) sidebarTitle.textContent = 'AM Trip Details';
        } else { console.error("DEBUG ERROR: Cannot show AM map - elements missing."); }
    }

    function showPmMap() {
        if (amMapContainer && pmMapContainer && showAmButton && showPmButton) {
            pmMapContainer.classList.add('active');
            amMapContainer.classList.remove('active');
            showPmButton.classList.add('active');
            showAmButton.classList.remove('active');
             // **** NEW (Optional): Update sidebar title if needed ****
            // if (sidebarTitle) sidebarTitle.textContent = 'PM Trip Details';
       } else { console.error("DEBUG ERROR: Cannot show PM map - elements missing."); }
    }

    function clearMapDisplay() {
        // Clear Map Containers
        if (amMapContainer) amMapContainer.innerHTML = '<p>AM Map will appear here.</p>';
        if (pmMapContainer) pmMapContainer.innerHTML = '<p>PM Map will appear here.</p>';
        // Hide Error Display
        if (errorDisplay) errorDisplay.style.display = 'none';
        if (errorDisplay) errorDisplay.textContent = '';

        // **** UPDATED: Clear Sidebar Content ****
        console.log("DEBUG: Clearing sidebar content in clearMapDisplay");
        if (sidebarTitle) sidebarTitle.textContent = 'Trip Details'; // Reset title
        if (sidebarVehicleSpan) sidebarVehicleSpan.textContent = '--'; // Reset vehicle
        if (sidebarRouteSpan) sidebarRouteSpan.textContent = '--'; // Reset route
        // Reset any other sidebar elements you added
        // **** END: Clear Sidebar Content ****

        // Reset view to default (AM)
        showAmMap();
    }

    function displayError(message) {
        // Display error message in the sidebar's errorDisplay element
        if (errorDisplay) {
            errorDisplay.textContent = message;
            errorDisplay.style.display = 'block';
        } else {
            console.error("DEBUG ERROR: Cannot display error message - errorDisplay element missing.");
            console.error("Original error message:", message);
        }
    }

    // --- Function to Generate and Display OPT Table ---
    function displayOptTable() {
        if (!optTableContainer || !optTableContent) {
            console.error("DEBUG ERROR: Cannot display OPT table, container elements missing.");
            return;
        }
        const downloadBtn = document.getElementById('download-opt-btn'); // Refetch or use global const

        if (currentOptData && Array.isArray(currentOptData) && currentOptData.length > 0) {
            console.log("DEBUG: Generating OPT table from stored data");
            // Define columns (ensure keys match actual data keys)
            const desiredColumns = [
                'seg_no', 'School_Code_&_Name', 'hndc_code', 'pupil_id_no',
                'first_name', 'last_name', 'address', 'zip', 'ph', 'amb_cd',
                'sess_beg', 'sess_end', 'med_alert', 'am', 'pm'
            ];

            // (Optional check for missing columns)
            const firstRowKeys = Object.keys(currentOptData[0]);
            const missingColumns = desiredColumns.filter(col => !firstRowKeys.includes(col));
            if (missingColumns.length > 0) {
                console.warn("DEBUG WARN: Some desired columns not found in data:", missingColumns);
            }

            // Build Table HTML
            let tableHTML = '<table id="opt-table" border="1" style="width:100%; border-collapse: collapse; font-size: 0.8em;">';
            // Build Headers
            tableHTML += '<thead><tr style="background-color: #f2f2f2;">';
            desiredColumns.forEach(header => {
                tableHTML += `<th style="padding: 4px; text-align: left;">${header}</th>`;
            });
            tableHTML += '</tr></thead>';
            // Build Body Rows
            tableHTML += '<tbody>';
            currentOptData.forEach((row, index) => {
                const rowStyle = index % 2 === 0 ? '' : 'background-color: #f9f9f9;';
                tableHTML += `<tr style="${rowStyle}">`;
                desiredColumns.forEach(columnKey => {
                    const value = row[columnKey] !== null && row[columnKey] !== undefined ? row[columnKey] : '';
                    const escapedValue = String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
                    tableHTML += `<td style="padding: 4px; vertical-align: top;">${escapedValue}</td>`;
                });
                tableHTML += '</tr>';
            });
            tableHTML += '</tbody></table>';
            // End Table Build

            // Update DOM
            optTableContent.innerHTML = tableHTML;
            optTableContainer.style.display = 'block'; // Show section containing table

            // Show Download button
            if (downloadBtn) {
                downloadBtn.style.display = 'inline-block';
                console.log("DEBUG: Showing Download OPT button");
            }

        } else {
            // Handle no OPT data
            console.log("DEBUG: No OPT data available to display in table.");
            optTableContent.innerHTML = '<p style="padding: 10px; font-style: italic; color: #666;">No OPT data details available for the selected route and date.</p>';
            optTableContainer.style.display = 'block'; // Show section with the message

            // Hide Download button
             if (downloadBtn) {
                downloadBtn.style.display = 'none';
                console.log("DEBUG: Hiding Download OPT button (no data)");
            }
        }
    } // End displayOptTable

    // --- Function to Download Table Data as CSV ---
    function downloadTableAsCSV(tableId, filename) {
        filename = filename || 'download.csv';
        const table = document.getElementById(tableId);
        if (!table) {
            console.error('Table with ID "' + tableId + '" not found.');
            alert('Error: Could not find the table to download.');
            return;
        }
        let csv = [];
        const rows = table.querySelectorAll("tr");
        const escapeCSV = function(cellData) {
            if (cellData == null) { return ''; }
            let data = cellData.toString();
            if (data.search(/("|,|\n)/g) >= 0) {
                data = '"' + data.replace(/"/g, '""') + '"';
            }
            return data;
        };
        for (let i = 0; i < rows.length; i++) {
            const row = [], cols = rows[i].querySelectorAll("td, th");
            for (let j = 0; j < cols.length; j++) {
                let cellText = cols[j].innerText || cols[j].textContent;
                row.push(escapeCSV(cellText.trim()));
            }
            csv.push(row.join(","));
        }
        const csvContent = csv.join("\n");
        const blob = new Blob(["\uFEFF" + csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement("a");
        if (link.download !== undefined) {
            const url = URL.createObjectURL(blob);
            link.setAttribute("href", url);
            link.setAttribute("download", filename);
            link.style.visibility = 'hidden';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(url);
        } else {
            console.warn("Download attribute not supported. Opening data URI.");
            window.open('data:text/csv;charset=utf-8,' + encodeURIComponent("\uFEFF" + csvContent));
        }
    } // End downloadTableAsCSV


}); // End DOMContentLoaded