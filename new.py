from datetime import date
from chicago_crime.ml_logic import *


""" NEED TO SCALER! """


def get_forecast(date):
    today = date.today()
    model=model.load_model()
    df = extract.load_postproc_data()

    t=today
    forecast_list=[]

    while t < date:
        1. create sequence of crime counts
        [ crime_count(t-365), crime_count(t-364) , ... crime_count(t)  ]
        2. for all community areas
        3. stack them together : X
         load model
        4. y=model.predict(X)
        5. crime_count(t+1)=y
        6. forecast_list.append(y)
        7. t= t+1


        X.shape needs to be (1,365,77)
    return forecast_list



    # X_list = []
    X_scaled_list = []


    areas = np.sort([int(i) for i in df.community_area.unique()])

    for area in areas:

        df_com = df.query(f"community_area=='{str(area)}'")

        # turn crimes into list
        crime_count_list = list(df_com["crime_count"])

        # create sequences
        sequence_length = 365
        X= np.array( [-365:] )

        #scaler? to get X_scaled

        # append to lists

        X_scaled_list.append(X_scaled)


    X_scaled = np.array(X_scaled_list).transpose(1, 2, 0)

