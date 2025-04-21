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
    if (!optLinkElement) { console.error("DEBUG ERROR: OPT Link element (id='optLink') not found!"); }
    if (!optTableContainer) { console.error("DEBUG ERROR: OPT Table container (id='optTableContainer') not found!"); }
    if (!optTableContent) { console.error("DEBUG ERROR: OPT Table content div (id='optTableContent') not found!"); }
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

        // Reset DVI link state - ONLY if element exists
        if (dviLinkElement) {
            console.log("DEBUG: Resetting DVI link state");
            dviLinkElement.href = "#";
            dviLinkElement.removeAttribute("target");
            dviLinkElement.style.opacity = 0.5;
        } else {
            console.log("DEBUG: Skipping DVI link reset (element not found)");
        }

        // Reset OPT data and hide table
        currentOptData = null; // Reset stored OPT data
        if (optTableContainer) optTableContainer.style.display = 'none'; // Hide table
        if (optTableContent) optTableContent.innerHTML = ''; // Clear old table content
        console.log("DEBUG: Reset OPT data and table display");


        // --- UI Updates: Clear previous state, show loading ---
        console.log("DEBUG: Clearing map display and showing loading indicator.");
        clearMapDisplay(); // Calls showAmMap internally to reset view
        loadingIndicator.style.display = 'block';
        errorDisplay.style.display = 'none'; // Hide previous errors

        console.log("DEBUG: Starting fetch to /get_map with route:", route, "date:", date);
        try {
            // --- Fetch data from Flask backend ---
            const response = await fetch('/get_map', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ route: route, date: date }),
            });
            // --- End of fetch call ---

            console.log("DEBUG: Fetch response received, Status:", response.status, response.statusText);

            // --- UI Update: Hide loading ---
            // Ensure loading indicator exists before hiding
            if(loadingIndicator) loadingIndicator.style.display = 'none';

            // --- Handle HTTP errors ---
            if (!response.ok) {
                // Try to get error message from JSON body, provide fallback
                let errorMsg = `HTTP error! Status: ${response.status} ${response.statusText}`;
                try {
                    const errorData = await response.json();
                    errorMsg = errorData.error || errorMsg;
                } catch (e) {
                    console.log("DEBUG: Could not parse error response as JSON.");
                }
                console.error("DEBUG: Fetch response not OK:", errorMsg);
                throw new Error(errorMsg); // Trigger the catch block
            }

            // --- Process successful response ---
            console.log("DEBUG: Attempting response.json()");
            const data = await response.json();
            // Log received data structure for inspection
            console.log("DEBUG: Successfully parsed JSON data:", data ? Object.keys(data) : "null/undefined");
            // Log specific parts if needed (be careful with large map HTML)
            console.log("DEBUG: Received dvi_link:", data?.dvi_link);
            console.log("DEBUG: Received opt_data type:", typeof data?.opt_data, "Length:", Array.isArray(data?.opt_data) ? data.opt_data.length : "N/A");


            // Inject map HTML - Add detailed logs and checks
            console.log("DEBUG: Setting map innerHTML (Overall Start)");
            try {
                console.log("DEBUG: BEFORE setting AM map innerHTML");
                if (amMapContainer) {
                    // Check if data.am_map is a string before assigning
                    if (typeof data.am_map === 'string') {
                        amMapContainer.innerHTML = data.am_map || '<p>AM map data not available.</p>';
                    } else {
                         console.error("DEBUG ERROR: data.am_map is not a string!", data.am_map);
                         amMapContainer.innerHTML = '<p>Error: Invalid AM map data received.</p>';
                    }
                } else { console.error("DEBUG ERROR: amMapContainer is null!"); }
                console.log("DEBUG: AFTER setting AM map innerHTML");

                console.log("DEBUG: BEFORE setting PM map innerHTML");
                if (pmMapContainer) {
                     // Check if data.pm_map is a string before assigning
                    if (typeof data.pm_map === 'string') {
                        pmMapContainer.innerHTML = data.pm_map || '<p>PM map data not available.</p>';
                    } else {
                         console.error("DEBUG ERROR: data.pm_map is not a string!", data.pm_map);
                         pmMapContainer.innerHTML = '<p>Error: Invalid PM map data received.</p>';
                    }
                } else { console.error("DEBUG ERROR: pmMapContainer is null!"); }
                console.log("DEBUG: AFTER setting PM map innerHTML");
            } catch (innerHtmlError) {
                 console.error("DEBUG: ERROR occurred during innerHTML assignment:", innerHtmlError);
                 displayError("Failed to display map content.");
                 // Optionally stop further processing here if maps are critical
                 // return;
            }
            // End of map injection


            // --- Update DVI Link ---
            console.log("DEBUG: Attempting to update DVI link");
            const dviLinkUrl = data.dvi_link;
            // Check if element exists AND link is valid before updating
            if (dviLinkElement && dviLinkUrl && dviLinkUrl !== "#") {
                console.log("DEBUG: Applying valid DVI link to element");
                dviLinkElement.href = dviLinkUrl;
                dviLinkElement.target = "_blank";
                dviLinkElement.style.opacity = 1;
            } else {
                console.log("DEBUG: No valid DVI link found or element missing");
                // Ensure link remains reset if element exists but link is invalid
                if(dviLinkElement) {
                     dviLinkElement.href = "#";
                     dviLinkElement.removeAttribute("target");
                     dviLinkElement.style.opacity = 0.5;
                }
            }
            // --- End Update DVI Link ---


            // --- Store OPT Data ---
            console.log("DEBUG: Attempting to store OPT data");
            currentOptData = data.opt_data; // Store the received OPT data array (can be null or empty list)
            console.log("DEBUG: Stored OPT data:", currentOptData ? `${currentOptData.length} rows` : "None or empty list");
            // --- End Store OPT Data ---


            // --- Set Default View ---
            console.log("DEBUG: Calling showAmMap() to set default view");
            showAmMap(); // Ensure AM map is shown by default

        } catch (error) {
            // --- Handle fetch or processing errors ---
            if(loadingIndicator) loadingIndicator.style.display = 'none';
            console.error("DEBUG: Error caught in fetch/processing block:", error);
            displayError(`Failed to load map: ${error.message}`);

            // Reset DVI link state on error
             if (dviLinkElement) {
                 console.log("DEBUG: Resetting DVI link state due to error");
                 dviLinkElement.href = "#";
                 dviLinkElement.removeAttribute("target");
                 dviLinkElement.style.opacity = 0.5;
             }
            // Reset OPT data state on error
            currentOptData = null;
            if (optTableContainer) optTableContainer.style.display = 'none';
            if (optTableContent) optTableContent.innerHTML = '';
            console.log("DEBUG: Reset OPT data state due to error");
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
            // event.preventDefault(); // Use if href="#"
            console.log("DEBUG: OPT Info link clicked");
            displayOptTable(); // Call function to generate and show the table
        });
    } else {
         console.error("DEBUG ERROR: OPT Link element not found, cannot attach click listener.");
    }


    // --- Helper Functions ---
    function showAmMap() {
        if (amMapContainer && pmMapContainer && showAmButton && showPmButton) {
            amMapContainer.classList.add('active');
            pmMapContainer.classList.remove('active');
            showAmButton.classList.add('active');
            showPmButton.classList.remove('active');
        } else { console.error("DEBUG ERROR: Cannot show AM map - elements missing."); }
    }

    function showPmMap() {
        if (amMapContainer && pmMapContainer && showAmButton && showPmButton) {
            pmMapContainer.classList.add('active');
            amMapContainer.classList.remove('active');
            showPmButton.classList.add('active');
            showAmButton.classList.remove('active');
         } else { console.error("DEBUG ERROR: Cannot show PM map - elements missing."); }
    }

    function clearMapDisplay() {
        if (amMapContainer) amMapContainer.innerHTML = '<p>AM Map will appear here.</p>';
        if (pmMapContainer) pmMapContainer.innerHTML = '<p>PM Map will appear here.</p>';
        if (errorDisplay) errorDisplay.style.display = 'none';
        if (errorDisplay) errorDisplay.textContent = '';
        // Reset view to default (AM) - includes checks
        showAmMap();
    }

    function displayError(message) {
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

        if (currentOptData && Array.isArray(currentOptData) && currentOptData.length > 0) {
            console.log("DEBUG: Generating OPT table from stored data:", currentOptData);
            const headers = Object.keys(currentOptData[0]);
            let tableHTML = '<table border="1" style="width:100%; border-collapse: collapse; font-size: 0.8em;">';
            tableHTML += '<thead><tr style="background-color: #f2f2f2;">';
            headers.forEach(header => { tableHTML += `<th style="padding: 4px; text-align: left;">${header}</th>`; });
            tableHTML += '</tr></thead>';
            tableHTML += '<tbody>';
            currentOptData.forEach((row, index) => {
                const rowStyle = index % 2 === 0 ? '' : 'background-color: #f9f9f9;';
                tableHTML += `<tr style="${rowStyle}">`;
                headers.forEach(header => {
                    const value = row[header] !== null && row[header] !== undefined ? row[header] : '';
                    const escapedValue = String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
                    tableHTML += `<td style="padding: 4px; vertical-align: top;">${escapedValue}</td>`;
                });
                tableHTML += '</tr>';
            });
            tableHTML += '</tbody></table>';
            optTableContent.innerHTML = tableHTML;
            optTableContainer.style.display = 'block';
        } else {
            console.log("DEBUG: No OPT data available to display in table.");
            optTableContent.innerHTML = '<p>No OPT data details available for the selected route and date.</p>';
            optTableContainer.style.display = 'block';
        }
    }

}); // End DOMContentLoaded