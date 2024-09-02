from tensorflow.keras.models import Sequential
from tensorflow.keras.optimizers import Adam
from tensorflow.keras import layers
from tensorflow.keras.callbacks import EarlyStopping

from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error

import numpy as np



# initialize model

def initialize_model(sequence_length, n_communities):

    model = Sequential()

    model.add(layers.GRU(units=32, activation='relu', input_shape=(sequence_length, n_communities), return_sequences=True))
    model.add(layers.GRU(units=32, activation='relu', return_sequences=True))
    model.add(layers.GRU(units=16, activation='relu', return_sequences=True))
    model.add(layers.GRU(units=8, activation='relu'))

    model.add(layers.Dense(16, activation="relu"))
    model.add(layers.Dense(n_communities, activation="linear"))

    return model


# compile model

def compile_model(model,
                  loss = 'mae',
                  learning_rate = 0.0005):

    model.compile(loss=loss, optimizer=Adam(learning_rate=learning_rate))

    return model

# data splitting

def data_split(df, sequence_length = 7, train_prop = 0.9, test_prop = 0.2):

    X_test_list = []
    X_test_scaled_list = []
    y_test_list = []
    X_train_list = []
    y_train_list = []
    X_val_list = []
    y_val_list = []

    areas = np.sort([int(i) for i in df.community_area.unique()])

    for area in areas:

        df_com = df.query(f"community_area=='{str(area)}'")

        # turn crimes into list
        crime_count_list = list(df_com["crime_count"])

        # create sequences
        X, y = [], []
        for i in range(sequence_length, len(crime_count_list)):
            X.append(crime_count_list[i-sequence_length:i])
            y.append(crime_count_list[i])

        X = np.array(X)
        y = np.array(y)

        # train_test_split
        train_rows = int(len(X) * train_prop)  # 90% for training

        X_train_full, X_test, y_train_full, y_test = X[:train_rows], X[train_rows:], y[:train_rows], y[train_rows:]

        # Further split the training data into training and validation sets
        X_train, X_val, y_train, y_val = train_test_split(X_train_full, y_train_full, test_size=test_prop, shuffle = False)

        # scale
        scaler=MinMaxScaler().fit(X_train)
        X_train_scaled=scaler.transform(X_train)
        X_test_scaled=scaler.transform(X_test)
        X_val_scaled=scaler.transform(X_val)

        # append to lists

        X_test_scaled_list.append(X_test_scaled)
        X_test_list.append(X_test)
        y_test_list.append(y_test)
        X_train_list.append(X_train_scaled)
        y_train_list.append(y_train)
        X_val_list.append(X_val_scaled)
        y_val_list.append(y_val)

    X_test_scaled = np.array(X_test_scaled_list).transpose(1, 2, 0)
    X_test = np.array(X_test_list).transpose(1, 2, 0)
    y_test = np.array(y_test_list).T
    X_train = np.array(X_train_list).transpose(1, 2, 0)
    y_train = np.array(y_train_list).T
    X_val = np.array(X_val_list).transpose(1, 2, 0)
    y_val = np.array(y_val_list).T

    return X_test_scaled, X_test, y_test, X_train, y_train, X_val, y_val

# train model

def train_model(model,
                X_train,
                y_train,
                X_val,
                y_val,
                epochs = 10,
                batch_size = 16,
                patience = 5):

    es = EarlyStopping(patience=patience)

    history = model.fit(X_train, y_train,
                        epochs=epochs, verbose=1,
                        batch_size= batch_size,
                        validation_data=(X_val, y_val),
                        callbacks=[es])

    return history, model

def get_metrics(model, X_test_scaled, X_test, y_test):

    # get test mae
    y_pred=model.predict(X_test_scaled)
    test_mae = mean_absolute_error(y_test, y_pred)

    # get base mae
    y_base = X_test[:, -1, :]
    base_mae = mean_absolute_error(y_test, y_base)

    return test_mae, base_mae
