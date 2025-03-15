import requests
import os
from dotenv import load_dotenv

load_dotenv()

class Geocoding:
    def call(self, parent: str, address: str) -> any:
        # Prepare the URL and parameters
        url = 'https://maps.googleapis.com/maps/api/geocode/json'
        params = {
            'address': address,
            'key': os.getenv("API_KEY")
        }
        
        # Make the GET request
        response = requests.get(url, params=params)
        
        # Check for a successful response
        if response.status_code == 200:
            # Parse and return the JSON response
            res = response.json()
            return self.extract_location_details(parent,res)
        else:
            # Handle the case when the API request fails
            print("Error: Unable to get a response from Google Maps API.")
            return None

    def extract_location_details(parent: str, data):
        details = []
        
        for result in data.get("results", []):
            # Extract address components
            address_components = result.get("address_components", [])
            county_full = None
            for component in address_components:
                if "administrative_area_level_2" in component["types"]:
                    county_full = component["long_name"]
            
            # Extract geometry
            geometry = result.get("geometry", {})
            
            # Extract location type and fallback
            location_type = geometry.get("location_type")
            if location_type == "ROOFTOP":
                location_type = "ROOFTOP"
            elif location_type == "RANGE_INTERPOLATED":
                location_type = "RANGE_INTERPOLATED"
            elif location_type == "GEOMETRIC_CENTER":
                location_type = "GEOMETRIC_CENTER"
            elif location_type == "APPROXIMATE":
                location_type = "APPROXIMATE"
            else:
                location_type = "UNKNOWN"
            
            # Collect the extracted details for each result
            details.append({
                f"{parent}_county_full": county_full,
                f"{parent}_state": next((comp["short_name"] for comp in address_components if "administrative_area_level_1" in comp["types"]), None),
            })
        
        return details

            
