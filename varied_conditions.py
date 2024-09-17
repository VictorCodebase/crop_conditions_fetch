import ee
import pandas as pd
import json
from tqdm import tqdm
from datetime import datetime
import logging
import os

# Initialize logging
logging.basicConfig(filename='sampled_data_logs.txt', level=logging.INFO, format='%(asctime)s - %(message)s')


# Required files (config and location_data loaded here)
with open('location_data.json', 'r') as file:
    locations = json.load(file)
with open('config.json', 'r') as file:
    keys = json.load(file)


#! Enter all location details here:

location = locations['willacy']

location_name = location['name'] # format: State_County
year = 2020
GEE_project_name = keys['title'] #!!! Dont forget to change
roi = location['coord_format_2']
#! Extra controls (mostly for testing)

scale = 1000 #How big each sample will be (It does not influence the number of samples, the smaller the faster the script, the lesser comprehensive the result. The bigger, the more likely to get overlaps in samples, the slower the script)
number_of_months = 12 # starting month. To get one month, (1 + starting month) to get 2 months, 12 to get all months
starting_month = 1 # chnage this to have the script start running from a certain month
sample_size = None # Number of samples taken for each month. Use None to get all samples 

'''
mini tutorial
 - having starting month as 1, and number of months as 1 will give you data for only one month.
 - if you change the scale here, ensure you change it in the annual_crops.py file for accuracy
'''
#! Now run the script 🚀

print("Greetings from Mark!!! \n")
print("Authenticating...")


try:
    ee.Initialize(project=GEE_project_name)
except Exception as e:
    ee.Authenticate()
    ee.Initialize()

logging.info("Authenticated successfully")
print("Authentication successful")

roi = ee.Geometry.Rectangle(roi)

srtm = ee.Image('USGS/SRTMGL1_003')
elevation = srtm.select('elevation')
slope = ee.Terrain.slope(elevation)

# Function to get the start and end dates of a month
def get_month_date_range(year, month):
    start_date = datetime(year, month, 1).strftime('%Y-%m-%d')
    if month == 12:
        end_date = datetime(year + 1, 1, 1).strftime('%Y-%m-%d')
    else:
        end_date = datetime(year, month + 1, 1).strftime('%Y-%m-%d')
    return start_date, end_date

year = 2020  # Change this to get the desired year

# Load crop data from CSV
crop_data = pd.read_csv(f'{year}_crops_{location_name}.csv')
crop_data.set_index(['longitude', 'latitude'], inplace=True)

# Load crop analysis data from JSON
with open(f'{location_name}_crop_analysis.json') as f:
    crop_analysis = json.load(f)

# Convert crop analysis to a dictionary for faster lookup
crop_analysis_dict = {item['crop']: item for item in crop_analysis}

# number_of_months = 2  # 12 for all the months
# sample_size = 40 # Use None to get the whole dataset, or specify the number of samples per month

max_temperature = ''
min_temperature = ''
for month in range(starting_month, (number_of_months + 1)):
    # Example of another environmental dataset
    start_date, end_date = get_month_date_range(year, month)
    dataset = ee.ImageCollection('NASA/GLDAS/V021/NOAH/G025/T3H').filterBounds(roi).filterDate(start_date, end_date)
    monthly_mean = dataset.mean()
    temperature = monthly_mean.select('Tair_f_inst')
    transpiration = monthly_mean.select('Evap_tavg')
    windSpeed = monthly_mean.select('Wind_f_inst')
    humidity = monthly_mean.select('Qair_f_inst')

    if (max_temperature == '' and min_temperature == ''):
        max_temperature = dataset.select('Tair_f_inst').max()
        min_temperature = dataset.select('Tair_f_inst').min()

    # Kelvin to Celsius conversion
    temperature = temperature.subtract(273.15)
    max_temperature = max_temperature.subtract(273.15)
    min_temperature = min_temperature.subtract(273.15)

    #Lets have month one dataset have constant conditions
    # if month == 1:
    multi_band_image = temperature.rename('Temperature') \
    .addBands(max_temperature.rename('Max_Temperature')) \
    .addBands(min_temperature.rename('Min_Temperature')) \
    .addBands(transpiration.rename('Transpiration')) \
    .addBands(windSpeed.rename('Wind_Speed')) \
    .addBands(humidity.rename('Humidity'))\
    .addBands(elevation.rename('Elevation'))\
    .addBands(slope.rename('Slope'))
    #TODO: Find a way of saving the elevation and slope info for other months to copy
    # else:
    #     # Create a multi-band image combining the datasets
    #     multi_band_image = temperature.rename('Temperature') \
    #         .addBands(max_temperature.rename('Max_Temperature')) \
    #         .addBands(min_temperature.rename('Min_Temperature')) \
    #         .addBands(transpiration.rename('Transpiration')) \
    #         .addBands(windSpeed.rename('Wind_Speed')) \
    #         .addBands(humidity.rename('Humidity'))

    # Function to get sample data
    def get_sample_data(coords, scale):
        point = ee.Geometry.Point(coords)
        sample = multi_band_image.sample(region=point, scale=scale, numPixels=1, geometries=True).first().getInfo()
        return sample

    # Read coordinates from a JSON file
    with open(f'{location_name}_block_coordinates.json') as f:
        data = json.load(f)
        coordinates = [(d['longitude'], d['latitude']) for d in data]

    data = []
    logging.info('Processing coordinates...')

    if sample_size:
        coordinates = coordinates[:sample_size]

    print(f'Processing data for month {month}...')
    # scale = 1000  # Increase the scale to reduce accuracy and computational load
    for coord in tqdm(coordinates):
        sample = get_sample_data(coord, scale)
        if sample:
            props = sample['properties']
            crop = crop_data.loc[coord].crop if coord in crop_data.index else None
            plant_month = crop_analysis_dict[crop]['planting_month'] if crop in crop_analysis_dict else None
            growing_duration = crop_analysis_dict[crop]['growing_duration_months'] if crop in crop_analysis_dict else None

            data.append({
                'Longitude': coord[0],
                'Latitude': coord[1],
                'Temperature': props.get('Temperature'),
                'Max_Temperature': props.get('Max_Temperature'),
                'Min_Temperature': props.get('Min_Temperature'),
                'Transpiration': props.get('Transpiration'),
                'Wind_Speed': props.get('Wind_Speed'),
                'Humidity': props.get('Humidity'),
                'Elevation': props.get('Elevation'),
                'Slope': props.get('Slope'),
                'Crop': crop,
                'Plant_Month': plant_month,
                'Growing_Duration': growing_duration

            })
            logging.info(f'Processed coordinates: {coord}')

    # Convert to DataFrame and save to CSV
    df = pd.DataFrame(data)

    output_CSV = f'{location_name}_{year}_{month}.csv'
    df.to_csv(output_CSV, index=False)
    logging.info(f'{location_name} varying data for month {month}, {year} saved to CSV.')


    #! Repair dataset to ensure no missing values
    print(f"\n Verifying and cleaning CSV file")
    logging.info("Verifying and cleaning...")

    #load the CSV file
    df_main = pd.read_csv(f'{location_name}_{year}_{month}.csv')
    #then the crops CSV
    df_crops = pd.read_csv(f'{year}_crops_{location_name}.csv')

    with open(f'{location_name}_crop_analysis.json') as f:
        crop_info = json.load(f)

    # Convert the crop info JSON into a dictionary for easy lookup
    crop_info_dict = {entry['crop']: entry for entry in crop_info}

    # Create a dictionary from the crop CSV with coordinates as keys
    crop_coords_dict = {(row['longitude'], row['latitude']): row['crop'] for _, row in df_crops.iterrows()}

    # Function to fill missing data
    def fill_missing_data(row):
        if pd.isna(row['Crop']):
            coord = (row['Longitude'], row['Latitude'])
            if coord in crop_coords_dict:
                crop = crop_coords_dict[coord]
                row['Crop'] = crop
                if crop in crop_info_dict:
                    row['Plant_Month'] = crop_info_dict[crop]['planting_month']
                    row['Growing_Duration'] = crop_info_dict[crop]['growing_duration_months']
        return row

    # Apply the function to the DataFrame
    df_main = df_main.apply(fill_missing_data, axis=1)
     # Save the updated DataFrame to a new CSV file
    output_directory = f'{location_name}_data'
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    df_main.to_csv(os.path.join(output_directory, output_CSV), index=False)

    print("Missing data filled and saved to new CSV file.")
    logging.info(f'{location_name} data cerified')



