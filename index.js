async function loadListings() {
    const container = document.getElementById("listings-container");
    const errorMessage = document.getElementById("error-message");

    try {
        const response = await fetch("http://127.0.0.1:8000/listings");
        if (!response.ok) {
            throw new Error(`HTTP error! Status: ${response.status}`);
        }
        const data = await response.json();
        container.innerHTML = ""; // clear existing content

        if (data.count === 0) {
            container.innerHTML = "<p>No listings found.</p>";
            return;
        }

        data.listings.forEach(item => {
            const div = document.createElement("div");
            div.style.border = "1px solid #ccc";
            div.style.padding = "10px";
            div.style.marginBottom = "10px";
            div.innerHTML = `
                <h2>${item.title_clean || "No Title"}</h2>
                <p>Author/Brand: ${item.author_or_brand || "N/A"}</p>
                <p>Category: ${item.category_clean || "N/A"}</p>
                <p>Price: ${item.price ? "$" + item.price : "N/A"}</p>
                <img src="${item.image_url || ''}" alt="Product Image" width="150">
                <p>${item.summary || ""}</p>
            `;
            container.appendChild(div);
        });

    } catch (error) {
        errorMessage.style.display = "block";
        errorMessage.textContent = `Error loading listings: ${error.message}`;
    }
}

// Call the function to load listings when page loads
window.onload = loadListings;
