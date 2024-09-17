

Open both files and change the `location` variable to your target location. Ensure the location information is added to location_data.json

For each location, ensure you run the annualcrops file first to get crop information, then variedconditions file next to access physical conditions for the crop.

have a config.json file of this structure:
{
  "project": {
    "title": "Your Google Project Title"
  },
  "api_keys": {
    "api_key_1": "USDA API key",
    "api_key_2": "USDA API key"
  }
}
