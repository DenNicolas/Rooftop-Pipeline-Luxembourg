import subprocess
import sys
def install_packages(package_list):
    for package in package_list:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            print(f"Package '{package}' installed successfully.")
        except subprocess.CalledProcessError:
            print(f"Failed to install package '{package}'.")

# List of packages you want to install
packages_to_install = [
    'leafmap', 'matplotlib', 'geopandas', 'pandas', 'numpy',
    'folium', 'shapely', 'scipy', 'geopy', 'lxml', 'triangle'
]

# Install the packages
install_packages(packages_to_install)

# Import necessary packages
from lxml import etree
from tqdm import tqdm
from shapely.geometry import Polygon
from shapely import wkt
from utils.geo_utils import Building, ns_bldg, ns_citygml, ns_gml, convert_3D_2D, add_missing_addresses_to_rooftopdata, add_lux_adress_data
import geopandas as gpd
import pandas as pd
import glob
import os
import multiprocessing
from multiprocessing import Pool



def run_preprocesing_pipeline(source_file):
    # Configuration
  

    # Path for saving the processed .gml files in .geojson format
    OUTPUT_FILE_PATH = "C:/Users/csaz6689/Desktop/CityGML-Preprocessing-Automated/data/GeoJSON/"+source_file[83:-4]+".geojson"




    gml_file_paths = [source_file]


    rooftop_data = {}
    building_data = {}

    check_sum = 0

    for idx, file_path in enumerate(gml_file_paths):
        
        print("******")
        print(f"Start processing {file_path} - {idx+1}/{len(gml_file_paths)}")
        
        try:
            # Read in file and build tree
            CITYGML = etree.parse(file_path) 
            root = CITYGML.getroot()
            
        except Exception as e:
            # Some files have corrupted XML codes; these files are skipped
            print(f'File {file_path} cannot be parsed due to XMLSyntaxError: {e}') 
        
        city_objects = []
        buildings = []
        
        # Create a list of all the objects in the gml file
        for obj in root.getiterator('{%s}cityObjectMember'% ns_citygml):
            city_objects.append(obj)
            
        # Create a list of all the buildings in the gml file 
        for city_object in city_objects:
            for child in city_object.getchildren():
                if child.tag == '{%s}Building' %ns_bldg:
                    buildings.append(child)
                    
        print(f'There are {len(buildings)} Building(s) in this CityGML file.')
        
        # Iterate over building classes (see geo_utils BuildingClass for more information)
        building_classes = []
        check_sum += len(buildings)
        
        for building in buildings:
            identifier = building.attrib['{%s}id' %ns_gml]
            print(identifier)
            building_classes.append(Building(building, identifier))
            
        print("Create building dictionary")
        for building_class in tqdm(building_classes):
            building_data[building_class.id] = {
                            'Building_ID': building_class.id,
                            'City': getattr(building_class, 'city', 'No data'),
                            'Street': getattr(building_class, 'streetName', 'No data'),
                            'StreetNumber': getattr(building_class, 'streetNumber', 'No data'),
                            'Gemeindeschluessel': getattr(building_class, 'gemeindeschluessel', 'No data'),
                            'RoofData': getattr(building_class, 'roofdata', 'No data'),
                            'WallData': getattr(building_class, 'walldata', 'No data'),
                            'GroundData': getattr(building_class, 'grounddata', 'No data'),
                            'Datenquelle_Dachhoehe': getattr(building_class, 'datenquelle_dachhoehe', 'No data'),
                            'DatenquelleBodenhoehe': getattr(building_class, 'datenquelle_bodenhoehe', 'No data'),
                            'DatenquelleLage': getattr(building_class, 'datenquelle_lage', 'No data'),
                            'BuildingFunction': getattr(building_class, 'bldg_function', 'No data'),
                            'RooftopType': getattr(building_class, 'bldg_roofType', 'No data'),
                            'MeasuredHeight': getattr(building_class, 'bldg_measuredHeight', 'No data'),
                            'SourceFile': file_path.split("/")[-1]
            }
            
        print("Create rooftop dictionary")
        # Create rooftopDictionary with relevant rooftop information
        for building_key in tqdm(building_data):
            
            for roof_key in building_data[building_key]["RoofData"]:
                
                roof = building_data[building_key]["RoofData"][roof_key]
                
                rooftop_data[roof_key] = {
                            'Building_ID': building_data[building_key]['Building_ID'],
                            'City': building_data.get('City', 'No data'),
                            'Street': building_data.get('Street', 'No data'),
                            'StreetNumber': building_data.get('StreetNumber', 'No data'),
                            'RooftopType': building_data.get('RooftopType', 'No data'),
                            'Gemeindeschluessel': building_data.get('Gemeindeschluessel', 'No data'),
                            'RoofTopID': roof_key,
                            'Area': roof.get('area', 'No data'),
                            'Azimuth': roof.get('azimuth', 'No data'),
                            'Tilt': roof.get('tilt', 'No data'),
                            'RooftopPolygon': roof.get('polygon', 'No data'),
                            'Source_file': file_path.split("/")[-1]
                }

    # Convert dictionaries to dataframes
    building_data_df = pd.DataFrame(building_data).transpose()
    rooftop_data_df = pd.DataFrame(rooftop_data).transpose()
    rooftop_data_df['RooftopPolygon'] = rooftop_data_df['RooftopPolygon'].apply(Polygon)

    # Create 2D rooftop polygons
    rooftop_data_df['RooftopPolygon_2d'] = convert_3D_2D(rooftop_data_df['RooftopPolygon'])

    # Check sum to validate quality of extraction
    print(f'There should be {check_sum} buildings available in the dataframe. There are {len(rooftop_data_df.Building_ID.unique())} buildings available.')

    # God awful code to add missing addresses to rooftop data

    def convert_df_to_gdf_add_lux_adresses(rooftop_df):
        
        rooftop_df = rooftop_df.rename(columns = {'RooftopPolygon_2d':'geometry', 'Gemeindeschluessel':'PostalCode'})
        gdf = gpd.GeoDataFrame(rooftop_df, geometry='geometry', crs="EPSG:4326")
        
        # Only use relevant columns
        gdf = gdf[['Area', 'Azimuth', 'Building_ID', 'City',
                'PostalCode', 'RoofTopID', 'RooftopType',
                'Street', 'StreetNumber', 'Tilt', 'geometry']]
        
        # Add luxembourgish address data
        gdf = add_lux_adress_data(gdf)
        
        # For missing addresses find the nearest address in dataframe 
        gdf = add_missing_addresses_to_rooftopdata(gdf)
        
        # Drop 'centroid' column
        gdf = gdf.drop(['centroid'], axis=1)
        
        return gdf

    gdf = convert_df_to_gdf_add_lux_adresses(rooftop_data_df)

    # Create GeoJSON
    gdf.to_file(OUTPUT_FILE_PATH, driver="GeoJSON")

# End of method definition
def process_file(file_path):
    try:
        run_preprocesing_pipeline(file_path)
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")

def main(directory_path):
    files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.gml')]
    num_processes = multiprocessing.cpu_count()
    with Pool(processes=num_processes) as pool:
        # Map the process_file function to all files
        pool.map(process_file, files)

if __name__ == "__main__":
    directory_path = 'C:/Users/csaz6689/Desktop/CityGML-Preprocessing-Automated/raw_files/'
    main(directory_path)
    
