import requests
import json
from auth import get_access_token

HOST = "sellingpartnerapi-na.amazon.com"
MARKETPLACE = "ATVPDKIKX0DER"
TEST_ASIN = "1664461604"

def print_strings_recursively(obj, prefix=""):
    """Recursively print all string values in a nested dict/list."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_prefix = f"{prefix}.{k}" if prefix else k
            print_strings_recursively(v, new_prefix)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            new_prefix = f"{prefix}[{i}]"
            print_strings_recursively(item, new_prefix)
    elif isinstance(obj, str):
        print(f"{prefix}: {obj}")

def fetch_catalog_item_raw(token, asin):
    url = f"https://{HOST}/catalog/2020-12-01/items/{asin}"
    headers = {
        "x-amz-access-token": token,
        "Accept": "application/json",
    }
    # Only include images to avoid 400 errors
    params = {
        "marketplaceIds": MARKETPLACE,
        "includedData": "images"
    }

    response = requests.get(url, headers=headers, params=params)

    print("\n--- RAW REQUEST ---")
    print("URL:", response.request.url)

    print("\n--- RAW RESPONSE ---")
    print("STATUS:", response.status_code)
    print("BODY:", response.text[:1000] + "...\n")  # limit output length

    # Parse JSON payload
    data = response.json()
    payload = data.get("payload", data)

    print("\n--- RECURSIVE STRING DUMP ---")
    print_strings_recursively(payload)

    # --- Extract MAIN image ---
    images = payload.get("Images") or payload.get("images") or []
    main_image = ""
    if images and isinstance(images, list):
        for img_group in images:
            if img_group.get("marketplaceId") == MARKETPLACE:
                img_list = img_group.get("images", [])
                if img_list:
                    for img in img_list:
                        if img.get("variant") == "MAIN":
                            main_image = img.get("link", "")
                            break
                    if main_image:
                        break

    print("\n--- MAIN IMAGE ---")
    print(main_image if main_image else "No main image found.")

    return payload

def main():
    token = get_access_token()
    fetch_catalog_item_raw(token, TEST_ASIN)

if __name__ == "__main__":
    main()
