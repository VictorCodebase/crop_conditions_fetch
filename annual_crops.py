import requests
import json
import logging
import csv
from tqdm import tqdm
import pandas as pd
from geopy.distance import geodesic
import time

def log_print(message):
    logging.info(message)
    print(message)

with open('location_data.json', 'r') as file:
    locations = json.load(file)
with open('config.json', 'r') as file:
    keys = json.load(file)
#! Change location here
location = locations['Willacy']

# Initialize logging
logging.basicConfig(filename='crop_logs.txt', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_crop_data(api_key, state_fips, county_fips, year):
    try:
        urls = [
            f"http://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=CENSUS&sector_desc=CROPS&group_desc=VEGETABLES&agg_level_desc=COUNTY&state_fips_code={state_fips}&county_code={county_fips}&year__GE={year}&format=json",
            f"http://quickstats.nass.usda.gov/api/api_GET/?key={api_key}&source_desc=CENSUS&sector_desc=CROPS&group_desc=FRUIT%20%26%20TREE%20NUTS&agg_level_desc=COUNTY&state_fips_code={state_fips}&county_code={county_fips}&year__GE={year}&format=json",
        ]

        crop_productions = {}

        for url in urls:
            for attempt in range(5):  # Retry logic in case its a network issue
                try:
                    log_print(f"Making API request for {url.split('group_desc=')[1].split('&')[0]}")
                    response = requests.get(url)
                    response.raise_for_status()  
                    if response.status_code == 200:
                        log_print(f"API request for {url.split('group_desc=')[1].split('&')[0]} successful")
                        data = response.json().get('data', [])
                        for record in data:
                            crop = record['commodity_desc']
                            try:
                                production = int(record['Value'].replace(",", "")) if record['Value'] != "(D)" else 0
                                if crop in crop_productions:
                                    crop_productions[crop] += production
                                else:
                                    crop_productions[crop] = production
                            except ValueError:
                                logging.error(f"Error in get_production: {record['Value']}")
                        break
                except requests.exceptions.RequestException as e:
                    log_print(f"API request failed (attempt {attempt + 1}): {e}")
                    time.sleep(2 ** attempt)  # Exponential backoff
            else:
                log_print(f"Failed to fetch data after multiple attempts for URL: {url}")
                raise Exception(f"Failed to fetch data after multiple attempts for URL: {url}")

        return crop_productions
    except Exception as e:
        logging.error(f"Error in get_crop_data: {e}")
        raise

def simulate_crop_distribution(crop_productions, total_blocks, min_ratio=0.01):
    crops_per_block = []
    total_production = sum(crop_productions.values())

    for crop, production in crop_productions.items():
        if crop == "VEGETABLE TOTALS":
            continue

        proportion = production / total_production
        if proportion >= min_ratio:
            blocks_for_crop = int(proportion * total_blocks)
            crops_per_block.extend([crop] * blocks_for_crop)

    remaining_blocks = total_blocks - len(crops_per_block)
    if remaining_blocks > 0:
        crops_list = [crop for crop in crop_productions.keys() if crop != "VEGETABLE TOTALS"]
        crops_per_block.extend(crops_list[:remaining_blocks])

    return crops_per_block

def divide_roi_into_blocks(top_left, bottom_right, block_side):
    blocks = []
    lat, lon = top_left
    end_lat, end_lon = bottom_right

    lat_distance = geodesic((lat, lon), (end_lat, lon)).meters
    lon_distance = geodesic((lat, lon), (lat, end_lon)).meters
    num_lat_blocks = int(lat_distance // block_side)
    num_lon_blocks = int(lon_distance // block_side)

    lat_step = (top_left[0] - bottom_right[0]) / num_lat_blocks
    lon_step = (bottom_right[1] - top_left[1]) / num_lon_blocks

    for i in range(num_lat_blocks):
        for j in range(num_lon_blocks):
            block_top_left = (lat - i * lat_step, lon + j * lon_step)
            blocks.append(block_top_left)

    return blocks

def main():
    location_name = location['name']  
    county_fips = location['county_fips']
    state_fips = location['state_fips']
    top_left = location['coord_format_2']['top_left']  # Approximate coordinates for top-left of the ROI
    bottom_right = location['coord_format_2']['bottom_right']  # Approximate coordinates for bottom-right of the ROI
    year = '2020'
    block_side = 1000
    api_key = keys['api_keys']['api_key_1'] 

    try:
        crop_productions = get_crop_data(api_key, state_fips, county_fips, year)

        logging.info("Crop production data retrieved successfully")
        print("Crop production data retrieved:")
        for crop, production in crop_productions.items():
            print(f"{crop}: {production}")
            logging.info(f"{crop}: {production}")

        blocks = divide_roi_into_blocks(top_left, bottom_right, block_side)
        crops_per_block = simulate_crop_distribution(crop_productions, len(blocks))

        block_coordinates = [{'longitude': lon, 'latitude': lat} for lat, lon in blocks]

        max_samples = None  # Set to None to get the whole dataset. Change the value to limit samples.

        for attempt in range(5):  # Retry logic
            try:
                with open(f'{year}_crops_{location_name}.csv', 'w', newline='') as csvfile:
                    fieldnames = ['longitude', 'latitude', 'crop']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()

                    if max_samples:
                        blocks = blocks[:max_samples]

                    for i, block in enumerate(tqdm(blocks, desc="Processing blocks")):
                        latitude, longitude = block
                        crop = crops_per_block[i % len(crops_per_block)]
                        writer.writerow({'longitude': longitude, 'latitude': latitude, 'crop': crop})

                log_print("CSV file created successfully")
                break
            except Exception as e:
                logging.error(f"Failed to write CSV file: {e}")
                print(f"Failed to write CSV file: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff
        else:
            print("Failed to write CSV file after multiple attempts")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()


# def calculate_optimized_blocks(file_path, location_name):
#     df = pd.read_csv(file_path)

#     # Count Frequency of each crop
#     crop_counts = df['crop'].value_counts()

#     # Function to determine the sampling interval based on crop frequency
#     def get_sampling_params(count):
#         base_interval = 2
#         max_samples = 100

#         # Compute the interval using a logarithmic scale for slower growth
#         interval = math.ceil(base_interval * math.log(count + 1))

#         # Limit the maximum number of samples
#         samples = min(math.ceil(count / interval), max_samples)

#         return interval, samples
    
#     # Create a list to store sampled data
#     sampled_data = []
#     sampled_coordinates = []

#     # Apply adaptive sampling
#     for crop, count in crop_counts.items():
#         interval, samples = get_sampling_params(count)
#         crop_df = df[df['crop'] == crop]

#         # Sample rows based on computed interval
#         sampled_crop_df = crop_df.iloc[::interval]

#         # Add sampled data to the list
#         sampled_data.append(sampled_crop_df)

#         # Add sampled coordinates to the JSON list
#         for _, row in sampled_crop_df.iterrows():
#             sampled_coordinates.append({
#                 "longitude": row['longitude'],
#                 "latitude": row['latitude']
#             })

#     sampled_df = pd.concat(sampled_data, ignore_index=True)
#     with open(f'{location_name}_block_coordinates.json', 'w') as json_file:
#         json.dump(sampled_coordinates, json_file, indent=4)
