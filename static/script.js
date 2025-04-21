document.addEventListener('DOMContentLoaded', () => {
    const showMapButton = document.getElementById('showMapButton');
    const routeInput = document.getElementById('routeInput');
    const dateInput = document.getElementById('dateInput');
    const amMapContainer = document.getElementById('amMapContainer');
    const pmMapContainer = document.getElementById('pmMapContainer');
    const showAmButton = document.getElementById('showAmButton');
    const showPmButton = document.getElementById('showPmButton');
    const loadingIndicator = document.getElementById('loadingIndicator');
    const errorDisplay = document.getElementById('errorDisplay');

    // --- Event Listener for Show Map Button ---
    showMapButton.addEventListener('click', async () => {
        const route = routeInput.value.trim();
        const date = dateInput.value; // HTML5 date input value is 'YYYY-MM-DD'

        if (!route || !date) {
            displayError("Please enter both a route and a date.");
            return;
        }

        // --- UI Updates: Clear previous state, show loading ---
        clearMapDisplay();
        loadingIndicator.style.display = 'block';

        try {
            // --- Fetch data from Flask backend ---
            const response = await fetch('/get_map', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ route: route, date: date }), // Send route and date as JSON
            });

            // --- UI Update: Hide loading ---
            loadingIndicator.style.display = 'none';

            // --- Handle HTTP errors ---
            if (!response.ok) {
                 // Try to parse error message from server, otherwise use status text
                const errorData = await response.json().catch(() => ({ error: `Server error: ${response.statusText}` }));
                throw new Error(errorData.error || `HTTP error! Status: ${response.status}`);
            }

            // --- Process successful response ---
            const data = await response.json();

            // Inject map HTML received from Flask into the containers
            amMapContainer.innerHTML = data.am_map || '<p>AM map data not available for this route/date.</p>';
            pmMapContainer.innerHTML = data.pm_map || '<p>PM map data not available for this route/date.</p>';

            // Ensure the default view (AM map) is shown correctly after loading
            showAmMap();

        } catch (error) {
            // --- Handle fetch/processing errors ---
            loadingIndicator.style.display = 'none';
            console.error('Error fetching or processing map data:', error);
            displayError(`Failed to load map: ${error.message}`);
        }
    });

    // --- Event Listeners for Toggle Buttons ---
    showAmButton.addEventListener('click', showAmMap);
    showPmButton.addEventListener('click', showPmMap);

    // --- Helper Functions ---
    function showAmMap() {
        // Make AM container visible, hide PM
        amMapContainer.classList.add('active');
        pmMapContainer.classList.remove('active');
        // Update button active states
        showAmButton.classList.add('active');
        showPmButton.classList.remove('active');
    }

    function showPmMap() {
        // Make PM container visible, hide AM
        pmMapContainer.classList.add('active');
        amMapContainer.classList.remove('active');
         // Update button active states
        showPmButton.classList.add('active');
        showAmButton.classList.remove('active');
    }

     function clearMapDisplay() {
         // Reset map containers to initial text
         amMapContainer.innerHTML = '<p>AM Map will appear here.</p>';
         pmMapContainer.innerHTML = '<p>PM Map will appear here.</p>';
         // Hide error messages
         errorDisplay.style.display = 'none';
         errorDisplay.textContent = '';
         // Reset view to default (AM)
         showAmMap();
     }

     function displayError(message) {
         errorDisplay.textContent = message;
         errorDisplay.style.display = 'block';
         // Decide if you want to clear map content on error
         // amMapContainer.innerHTML = '';
         // pmMapContainer.innerHTML = '';
     }

}); // End DOMContentLoaded