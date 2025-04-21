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

    // --- Debugging: Check if elements were found ---
    if (!getMapButton) { console.error("DEBUG ERROR: Get Map button (id='getMapButton') not found!"); return; } // Stop if main button missing
    if (!routeInput) { console.error("DEBUG ERROR: Route input (id='routeInput') not found!"); }
    if (!dateInput) { console.error("DEBUG ERROR: Date input (id='dateInput') not found!"); }
    if (!amMapContainer) { console.error("DEBUG ERROR: AM Map container (id='amMapContainer') not found!"); }
    if (!pmMapContainer) { console.error("DEBUG ERROR: PM Map container (id='pmMapContainer') not found!"); }
    if (!showAmButton) { console.error("DEBUG ERROR: Show AM button (id='showAmButton') not found!"); }
    if (!showPmButton) { console.error("DEBUG ERROR: Show PM button (id='showPmButton') not found!"); }
    if (!loadingIndicator) { console.warn("DEBUG WARN: Loading indicator (id='loadingIndicator') not found!"); } // Warn, don't stop
    if (!errorDisplay) { console.warn("DEBUG WARN: Error display (id='errorDisplay') not found!"); } // Warn, don't stop
    if (!dviLinkElement) { console.warn("DEBUG WARN: DVI Link element (id='dviLink') not found!"); } // Warn, don't stop
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

        // --- UI Updates: Clear previous state, show loading ---
        console.log("DEBUG: Clearing map display and showing loading indicator.");
        clearMapDisplay(); // Assuming this function exists and works
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

            console.log("DEBUG: Fetch response received, Status:", response.status, response.statusText);

            // --- UI Update: Hide loading ---
            loadingIndicator.style.display = 'none';

            // --- Handle HTTP errors ---
            if (!response.ok) {
                // Try to get error message from JSON body, provide fallback
                let errorMsg = `HTTP error! Status: ${response.status} ${response.statusText}`;
                try {
                    const errorData = await response.json();
                    errorMsg = errorData.error || errorMsg;
                } catch (e) {
                    console.log("DEBUG: Could not parse error response as JSON.");
                    // Use the status text if JSON parsing fails
                }
                console.error("DEBUG: Fetch response not OK:", errorMsg);
                throw new Error(errorMsg); // Trigger the catch block
            }

            // --- Process successful response ---
            console.log("DEBUG: Attempting response.json()");
            const data = await response.json();
            console.log("DEBUG: Successfully parsed JSON data:", data); // Log the actual data received

            // Inject map HTML
            console.log("DEBUG: Setting map innerHTML");
            amMapContainer.innerHTML = data.am_map || '<p>AM map data not available.</p>';
            pmMapContainer.innerHTML = data.pm_map || '<p>PM map data not available.</p>';

            // --- Update DVI Link ---
            console.log("DEBUG: Attempting to update DVI link");
            const dviLinkUrl = data.dvi_link; // Get the link
            console.log("DEBUG: Received dvi_link value from JSON:", dviLinkUrl); // Log the value received

            // Check if element exists AND link is valid before updating
            if (dviLinkElement && dviLinkUrl && dviLinkUrl !== "#") {
                console.log("DEBUG: Applying valid DVI link to element");
                dviLinkElement.href = dviLinkUrl;
                dviLinkElement.target = "_blank";
                dviLinkElement.style.opacity = 1;
                // console.log("DVI link updated:", dviLinkUrl); // Original log
            } else {
                console.log("DEBUG: No valid DVI link found in data or DVI element missing in HTML.");
                // Ensure link remains reset if element exists but link is invalid
                if(dviLinkElement) {
                     dviLinkElement.href = "#";
                     dviLinkElement.removeAttribute("target");
                     dviLinkElement.style.opacity = 0.5;
                }
            }
            // --- End Update DVI Link ---

            console.log("DEBUG: Calling showAmMap() to set default view");
            showAmMap(); // Ensure AM map is shown by default

        } catch (error) {
            // --- Handle fetch or processing errors ---
            loadingIndicator.style.display = 'none';
            console.error("DEBUG: Error caught in fetch/processing block:", error); // Enhanced Error Log
            displayError(`Failed to load map: ${error.message}`);

            // Ensure DVI link is reset on error too
             if (dviLinkElement) {
                 dviLinkElement.href = "#";
                 dviLinkElement.removeAttribute("target");
                 dviLinkElement.style.opacity = 0.5;
             }
        }
    }); // End of getMapButton listener

    // --- Event Listeners for Toggle Buttons ---
    if (showAmButton && showPmButton) { // Check if buttons exist before adding listeners
         showAmButton.addEventListener('click', showAmMap);
         showPmButton.addEventListener('click', showPmMap);
    } else {
         console.error("DEBUG ERROR: AM/PM toggle buttons not found, toggling will not work.");
    }


    // --- Helper Functions ---
    function showAmMap() {
        // Check elements exist before modifying classes
        if (amMapContainer && pmMapContainer && showAmButton && showPmButton) {
            amMapContainer.classList.add('active');
            pmMapContainer.classList.remove('active');
            showAmButton.classList.add('active');
            showPmButton.classList.remove('active');
            // console.log("DEBUG: Switched view to AM Map"); // Optional log
        } else {
             console.error("DEBUG ERROR: Cannot show AM map - one or more required elements missing.");
        }
    }

    function showPmMap() {
         // Check elements exist before modifying classes
        if (amMapContainer && pmMapContainer && showAmButton && showPmButton) {
            pmMapContainer.classList.add('active');
            amMapContainer.classList.remove('active');
            showPmButton.classList.add('active');
            showAmButton.classList.remove('active');
            // console.log("DEBUG: Switched view to PM Map"); // Optional log
         } else {
             console.error("DEBUG ERROR: Cannot show PM map - one or more required elements missing.");
         }
    }

    function clearMapDisplay() {
        // Check elements exist before modifying innerHTML
        if (amMapContainer) amMapContainer.innerHTML = '<p>AM Map will appear here.</p>';
        if (pmMapContainer) pmMapContainer.innerHTML = '<p>PM Map will appear here.</p>';
        if (errorDisplay) errorDisplay.style.display = 'none';
        if (errorDisplay) errorDisplay.textContent = '';
        // Reset view to default (AM) - checks are inside showAmMap
        showAmMap();
    }

    function displayError(message) {
        // Check element exists before modifying
        if (errorDisplay) {
            errorDisplay.textContent = message;
            errorDisplay.style.display = 'block';
        } else {
             console.error("DEBUG ERROR: Cannot display error message - errorDisplay element missing.");
             console.error("Original error message:", message); // Log error anyway
        }
        // Optional: Clear maps on error
        // if (amMapContainer) amMapContainer.innerHTML = '';
        // if (pmMapContainer) pmMapContainer.innerHTML = '';
    }

}); // End DOMContentLoaded