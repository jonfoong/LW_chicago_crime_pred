# transform data - clean and preprocess

# imports
import pandas as pd
from pandas_gbq import read_gbq

from google.cloud import bigquery
from google.oauth2 import service_account


import geopandas as gpd
from shapely.geometry import Point
import os
from chicago_crime.params import *



def add_missing_communities(project_id: str,
                            credentials: service_account.Credentials
                            ) -> None:
    """
    This function add missing communities to the raw dataset
    based on a destrict information, contained in a geoJSON file,
    and updates a dataset chicago_crime_temp in a GCloud

    File source:
    https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-Community-Areas-current-/cauq-8yn6

    """
    # TODO: do we need exc handling?

    # Instantiate BigQuery Object
    client = bigquery.Client(project_id, credentials)

    # Clean chicago_crime_temp tab
    query_truncate_vs = """
    TRUNCATE TABLE `wagon-bootcamp-428814.chicago_crime.chicago_crime_temp`;
    """
    temp_truncate_job = client.query(query_truncate_vs)
    temp_truncate_job.result()

    # Copy raw data to temporary tab from main chicago tab
    query_data_to_temp = f"""
    INSERT INTO `wagon-bootcamp-428814.chicago_crime.chicago_crime_temp`
    SELECT * FROM `wagon-bootcamp-428814.chicago_crime.chicago_crime_tab`
    """

    update_job = client.query(query_data_to_temp)
    update_job.result()
    print(f"🍀 Transferred raw data into intermediate table")

    # Load raw data from BQ
    query_missing_communities = f"""
        SELECT Date, CAST(Latitude AS FLOAT64) as Latitude, CAST(Longitude AS FLOAT64) as Longitude, ID
        FROM `wagon-bootcamp-428814.chicago_crime.chicago_crime_tab`
        WHERE `Community Area` IS NULL
        {QUERY_NROWS}
        """

    df_spatial = read_gbq(query_missing_communities, credentials = credentials)
    print(f"✅ Loaded {df_spatial.shape[0]} missing communities to Dataframe")

    # Load the geoJSON file
    current_dir = os.path.dirname(__file__)
    json_file_path = os.path.join(current_dir, 'communities_geodata.geojson')

    communities = gpd.read_file(json_file_path)
    communities.drop(labels = ['community', 'area', 'shape_area', 'perimeter',
                               'area_numbe', 'comarea_id', 'comarea', 'shape_len' ], axis = 1)

    ## Conversions:
    # Convert the lat/lon to shapely Point objects
    df_spatial['geometry'] = df_spatial.apply(lambda row: Point(row['Longitude'], row['Latitude']), axis=1)

    # Convert the DataFrame to a GeoDataFrame
    points_gdf = gpd.GeoDataFrame(df_spatial, geometry='geometry')

    # Ensure both GeoDataFrames use the same CRS
    points_gdf.set_crs(epsg=4326, inplace=True)
    communities.to_crs(epsg=4326, inplace=True)

    # Perform the spatial join
    assigned_gdf = gpd.sjoin(points_gdf, communities, how = 'left', predicate='within')

    print(f"Number of NA's in 'Community': {assigned_gdf['area_num_1'].isna().sum()}")
    print(f"Number of NA's in 'Latitude': {assigned_gdf['Latitude'].isna().sum()}")

    # Adjust DataType for Date
    assigned_gdf['Date'] = pd.to_datetime(assigned_gdf['Date'])

    # Load updated communities to the GCloud
    write_mode = "WRITE_TRUNCATE"
    job_config = bigquery.LoadJobConfig(write_disposition=write_mode)
    tab = 'wagon-bootcamp-428814.chicago_crime.communities_temp'

    job = client.load_table_from_dataframe(assigned_gdf[['ID','area_num_1' ]], tab, job_config=job_config)
    job.result()

    # UPDATE tab on the cloud
    update_query = f"""
    UPDATE `wagon-bootcamp-428814.chicago_crime.chicago_crime_temp` AS destination
    SET destination.`Community Area` = temp.area_num_1
    FROM `wagon-bootcamp-428814.chicago_crime.communities_temp` AS temp
    WHERE temp.ID = destination.ID
    """
    update_job = client.query(update_query)
    update_job.result()  # Wait for the query to finish

    ### Delete temp table for intermediate communities storage
    query_drop_temp = f"""
    DROP TABLE `wagon-bootcamp-428814.chicago_crime.communities_temp`
    """
    job_drop = client.query(query_drop_temp)
    job_drop.result()

    print(f"✅ Missing communities added successfully")


def clean_data_frame(df: pd.DataFrame) -> pd.DataFrame :

    #sets the columns to the right data type
    df['Date_day'] = pd.to_datetime(df['Date_day']).dt.date
    #df['Community Area'] = pd.to_numeric(df['Community Area'])

    #creates a range of dates for the full data frame
    full_date_range = pd.date_range(start=df['Date_day'].min(), end=df['Date_day'].max(), freq='D')

    #Creates a new data frame with the missing dates
    community_areas = df['Community Area'].unique()
    complete_df = pd.MultiIndex.from_product([full_date_range, community_areas], names=['Date_day', 'Community Area']).to_frame(index=False)
    complete_df['Date_day'] = pd.to_datetime(complete_df['Date_day']).dt.date


    #merges the old data frame with the new one
    merged_df = pd.merge(complete_df, df, on=['Date_day', 'Community Area'], how='left')
    merged_df['crime_count'] = merged_df['crime_count'].fillna(0)
    merged_df.sort_values(by=['Date_day', 'Community Area'], inplace=True)

    #sets the date as the index of the dt
    merged_df.set_index('Date_day', inplace=True)

    #final step to drop the lines where we dont have values
    merged_df = merged_df.dropna(subset=['Community Area'])

    print(f"✅ Data cleaned")
    return merged_df
