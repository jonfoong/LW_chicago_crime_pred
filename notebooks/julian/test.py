import pandas as pd

def merge_main_holidays():

    #calls the main data frame
    project_id = "wagon-bootcamp-428814"
    query = """
    SELECT
        DATE(`Date`) AS Date_day,
        `Community Area`,
        COUNT(*) AS crime_count
    FROM
        `wagon-bootcamp-428814.chicago_crime.chicago_crime_tab`
    GROUP BY
        Date_day, `Community Area`
    ORDER BY
        Date_day, `Community Area`
    """
    df_main = pd.read_gbq(query, project_id=project_id, dialect='standard')

    #calls the holidays data frame
    query2 = """
    SELECT *
    FROM
        `wagon-bootcamp-428814.chicago_public_hols.chicago_public_hols`
    """

    df_holidays = pd.read_gbq(query2, project_id=project_id, dialect='standard')

    #creates a new column where if there's a holiday then we will have 1
    df_holidays['is_holiday'] = 1
    df_holidays = df_holidays.rename(columns={'Date': 'Date_day'})

    #merged the 2 data frames
    df_merged = df_main.merge(df_holidays[['Date_day', 'is_holiday']], on='Date_day', how='left')

    #fills with 0's the rest of the days the normal days
    df_merged['is_holiday'] = df_merged['is_holiday'].fillna(0).astype(int)

    #set's the Date_day as the index
    df_merged = df_merged.set_index('Date_day')

    #create's the column day_of_week by name of the day
    df_merged['day_of_week'] = df_merged.index.day_name()

    #assigns the right number to the day of the week
    day_mapping = {
    'Monday': 0,
    'Tuesday': 1,
    'Wednesday': 2,
    'Thursday': 3,
    'Friday': 4,
    'Saturday': 5,
    'Sunday': 6
    }
    df_merged['day_of_week'] = df_merged['day_of_week'].map(day_mapping)

    return df_merged
