document.addEventListener('DOMContentLoaded', function() {
    fetchInventoryListings();
});

async function fetchInventoryListings() {
    try {
        const response = await fetch('/listings');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        displayListings(data.listings); // Assuming the API returns an object with a 'listings' array
    } catch (error) {
        console.error('Error fetching inventory listings:', error);
        displayError('Failed to load inventory listings. Please try again later.');
    }
}

function displayListings(listings) {
    const container = document.getElementById('listings-container');
    container.innerHTML = ''; // Clear previous content

    if (listings.length === 0) {
        container.innerHTML = '<p>No listings available.</p>';
        return;
    }

    listings.forEach(listing => {
        const listingDiv = document.createElement('div');
        listingDiv.className = 'listing';

        // Create elements for title, price, and image
        const title = document.createElement('h2');
        title.textContent = listing['item-name'] || 'No title available';

        const price = document.createElement('p');
        price.textContent = `Price: $${listing.price || 'N/A'}`;

        const image = document.createElement('img');
        image.src = listing.image_url || 'placeholder.jpg'; // Use a placeholder if no image URL
        image.alt = listing['item-name'] || 'Product image';
        image.style.width = '150px'; // Set a fixed width for images

        // Append elements to the listing div
        listingDiv.appendChild(title);
        listingDiv.appendChild(price);
        listingDiv.appendChild(image);

        // Append the listing div to the container
        container.appendChild(listingDiv);
    });
}

function displayError(message) {
    const errorDiv = document.getElementById('error-message');
    errorDiv.textContent = message;
    errorDiv.style.display = 'block';
}
