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
    const downloadOptButton = document.getElementById('download-opt-btn'); // <<< ADD THIS


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
    if (!downloadOptButton) { console.warn("DEBUG WARN: Download OPT button (id='download-opt-btn') not found!"); } // <<< ADD THIS CHECK
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

        if (downloadOptButton) downloadOptButton.style.display = 'none'; // Hide it
        // if (downloadOptButton) downloadOptButton.disabled = true; // Or disable it
        console.log("DEBUG: Reset OPT download button display");



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
    if (downloadOptButton) { // Check if button exists
     downloadOptButton.addEventListener('click', function() {
        console.log("DEBUG: Download OPT button clicked");
        // Call the download function, passing the table ID generated in displayOptTable
        downloadTableAsCSV('opt-table', 'opt_information.csv');
     });
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
        // --- Function to Generate and Display OPT Table ---
    function displayOptTable() {
            if (!optTableContainer || !optTableContent) {
                console.error("DEBUG ERROR: Cannot display OPT table, container elements missing.");
                return;
            }
            const downloadBtn = document.getElementById('download-opt-btn');

            if (currentOptData && Array.isArray(currentOptData) && currentOptData.length > 0) {
                console.log("DEBUG: Generating OPT table from stored data");

                // **** 1. DEFINE Your Desired Columns and Order ****
                // List the exact keys from your data objects you want to include,
                // in the order you want them to appear.
                // Example: Adjust this array to your needs.
                const desiredColumns = [
                    'seg_no',
                    'School_Code_&_Name',
                    'hndc_code',
                    'pupil_id_no',
                    'first_name',
                    'last_name',
                    'address',
                    'zip',
                    'ph',
                    'amb_cd',
                    'sess_beg',
                    'sess_end',
                    'med_alert',
                    'am',
                    'pm'
                ];
              
                const firstRowKeys = Object.keys(currentOptData[0]);
                const missingColumns = desiredColumns.filter(col => !firstRowKeys.includes(col));
                if (missingColumns.length > 0) {
                     console.warn("DEBUG WARN: Some desired columns not found in data:", missingColumns);
                     // Decide how to handle this - maybe filter desiredColumns or show a message?
                     // For now, we'll proceed with potentially empty cells for missing keys.
                }


                // **** 2. Build Table using desiredColumns ****
                // Add id="opt-table" for the download function to find it
                let tableHTML = '<table id="opt-table" border="1" style="width:100%; border-collapse: collapse; font-size: 0.8em;">';

                // --- Build Headers ---
                tableHTML += '<thead><tr style="background-color: #f2f2f2;">';
                // Iterate through your desiredColumns array for headers
                desiredColumns.forEach(header => {
                    tableHTML += `<th style="padding: 4px; text-align: left;">${header}</th>`;
                });
                tableHTML += '</tr></thead>';

                // --- Build Body Rows ---
                tableHTML += '<tbody>';
                currentOptData.forEach((row, index) => { // Iterate through each data row object
                    const rowStyle = index % 2 === 0 ? '' : 'background-color: #f9f9f9;';
                    tableHTML += `<tr style="${rowStyle}">`;
                    // Iterate through your desiredColumns array again to control cell order and content
                    desiredColumns.forEach(columnKey => {
                        // Get the value from the current data row using the key from desiredColumns
                        const value = row[columnKey] !== null && row[columnKey] !== undefined ? row[columnKey] : '';
                        // Escape the value for HTML display
                        const escapedValue = String(value).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
                        tableHTML += `<td style="padding: 4px; vertical-align: top;">${escapedValue}</td>`;
                    });
                    tableHTML += '</tr>';
                });
                tableHTML += '</tbody></table>';
                // **** End Table Build ****


                optTableContent.innerHTML = tableHTML; // Display the table
                optTableContainer.style.display = 'block'; // Show the container

                // Show/Enable the download button
                if (downloadBtn) {
                    downloadBtn.style.display = 'inline-block';
                    console.log("DEBUG: Showing Download OPT button");
                }

            } else {
                // Handle case where there is no OPT data
                console.log("DEBUG: No OPT data available to display in table.");
                optTableContent.innerHTML = '<p style="padding: 10px; font-style: italic; color: #666;">No OPT data details available for the selected route and date.</p>';
                optTableContainer.style.display = 'block';

                 // Hide/Disable the download button
                 if (downloadBtn) {
                    downloadBtn.style.display = 'none';
                    console.log("DEBUG: Hiding Download OPT button (no data)");
                }
            }
        }
    // --- Function to Download Table Data as CSV ---
    function downloadTableAsCSV(tableId, filename) {
        // Ensure filename is provided, default if not
        filename = filename || 'download.csv'; // Sets a default filename

        const table = document.getElementById(tableId);
        if (!table) {
            // Check if the table exists
            console.error('Table with ID "' + tableId + '" not found.');
            alert('Error: Could not find the table to download.');
            return; // Stop if table not found
        }

        let csv = []; // Array to hold each row of CSV data
        const rows = table.querySelectorAll("tr"); // Get all table rows (header and body)

        // --- Helper function to properly escape data for CSV format ---
        const escapeCSV = function(cellData) {
            if (cellData == null) { // Handle null or undefined values
               return '';
            }
            let data = cellData.toString(); // Ensure data is a string
            // If data contains comma, newline, or double quote, needs escaping
            if (data.search(/("|,|\n)/g) >= 0) {
                // Enclose in double quotes and escape existing double quotes by doubling them
                data = '"' + data.replace(/"/g, '""') + '"';
            }
            return data; // Return processed data
        };

        // --- Loop through each row (<tr>) in the table ---
        for (let i = 0; i < rows.length; i++) {
            const row = [], cols = rows[i].querySelectorAll("td, th"); // Get cells (<td> or <th>) in the current row

            // --- Loop through each cell in the current row ---
            for (let j = 0; j < cols.length; j++) {
                // Extract text content (innerText usually reflects displayed text better)
                let cellText = cols[j].innerText || cols[j].textContent;
                // Escape the text and add it to the current row array
                row.push(escapeCSV(cellText.trim()));
            }

            // Join the cells in the row with commas and add to the main csv array
            csv.push(row.join(","));
        } // --- End of row loop ---

        // **** THIS IS THE CRITICAL LINE that defines csvContent ****
        // Join all the processed rows together with newline characters
        const csvContent = csv.join("\n");
        // **** Make sure this line exists and is spelled correctly ****

        // --- Trigger the file download ---
        // Create a Blob (binary large object) with the CSV data.
        // Include BOM (\uFEFF) for better Excel compatibility with UTF-8.
        const blob = new Blob(["\uFEFF" + csvContent], { type: 'text/csv;charset=utf-8;' });

        // Create a temporary anchor element (link) to trigger download
        const link = document.createElement("a");

        // Check if the browser supports the 'download' attribute
        if (link.download !== undefined) {
            // Create a temporary URL for the blob object
            const url = URL.createObjectURL(blob);
            link.setAttribute("href", url); // Set link's href to the blob URL
            link.setAttribute("download", filename); // Set the desired filename
            link.style.visibility = 'hidden'; // Make the link invisible
            document.body.appendChild(link); // Add the link to the document
            link.click(); // Programmatically click the link to start download
            document.body.removeChild(link); // Remove the link from the document
            URL.revokeObjectURL(url); // Release the blob URL resource
        } else {
            // Fallback for older browsers that don't support 'download'
            console.warn("Download attribute not supported. Opening data URI.");
            // This might open the CSV in a new tab instead of downloading
            window.open('data:text/csv;charset=utf-8,' + encodeURIComponent("\uFEFF" + csvContent));
        }
    } // --- End of downloadTableAsCSV function ---

}); // End DOMContentLoaded