# transform data - clean and preprocess
# imports
import pandas as pd
from google.cloud import bigquery
import geopandas as gpd
from shapely.geometry import Point
from chicago_crime.params import *



def add_missing_communities() -> None:
    """
    This function add missing communities to the raw dataset
    based on a destrict information, contained in a geoJSON file,
    and updates a dataset chicago_crime_temp in a GCloud

    File source:
    https://data.cityofchicago.org/Facilities-Geographic-Boundaries/Boundaries-Community-Areas-current-/cauq-8yn6

    """

    # Instantiate BigQuery Object
    client = bigquery.Client()

    # Clean chicago_crime_temp tab
    query_truncate_vs = f"""
    TRUNCATE TABLE `{GCP_PROJECT}.{BQ_DATASET}.chicago_crime_temp`;
    """
    client.query_and_wait(query_truncate_vs)

    # Copy raw data to temporary tab from main chicago tab
    query_data_to_temp = f"""
    INSERT INTO `{GCP_PROJECT}.{BQ_DATASET}.chicago_crime_temp`
    SELECT * FROM `{GCP_PROJECT}.{BQ_DATASET}.chicago_crime_tab`
    """

    client.query_and_wait(query_data_to_temp)
    print(f"🍀 Transferred raw data into intermediate table")

    # Load raw data from BQ
    query_missing_communities = f"""
        SELECT Date, CAST(Latitude AS FLOAT64) as Latitude, CAST(Longitude AS FLOAT64) as Longitude, ID
        FROM `{GCP_PROJECT}.{BQ_DATASET}.chicago_crime_tab`
        WHERE `Community Area` IS NULL
        """

    df_spatial = client.query_and_wait(query_missing_communities).to_dataframe()
    print(f"✅ Loaded {df_spatial.shape[0]} missing communities to Dataframe")

    # Load the geoJSON file
    json_file_path = 'chicago_crime/ml_logic/communities_geodata.geojson'

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
    # rename
    assigned_gdf = assigned_gdf.rename(columns = {"area_num_1": "Community Area"})

    # Load updated communities to the GCloud
    write_mode = "WRITE_TRUNCATE"
    job_config = bigquery.LoadJobConfig(write_disposition=write_mode)
    tab = f'{GCP_PROJECT}.{BQ_DATASET}.communities_temp'

    job = client.load_table_from_dataframe(assigned_gdf[['ID','Community Area' ]], tab, job_config=job_config)
    job.result()

    # UPDATE tab on the cloud
    update_query = f"""
    UPDATE `{GCP_PROJECT}.{BQ_DATASET}.chicago_crime_temp` AS destination
    SET destination.`Community Area` = temp.`Community Area`
    FROM `{GCP_PROJECT}.{BQ_DATASET}.communities_temp` AS temp
    WHERE temp.ID = destination.ID
    """
    client.query_and_wait(update_query)

    ### Delete temp table for intermediate communities storage
    query_drop_temp = f"""
    DROP TABLE `{GCP_PROJECT}.{BQ_DATASET}.communities_temp`
    """
    client.query_and_wait(query_drop_temp)
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

    # rename
    merged_df = merged_df.rename(columns = {"Community Area": "community_area"})

    #final step to drop the lines where we dont have values
    merged_df = merged_df.dropna(subset=['community_area'])

    print(f"✅ Data cleaned")
    return merged_df
