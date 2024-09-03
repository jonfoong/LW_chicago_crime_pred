from tensorflow.keras.models import Sequential
from tensorflow.keras.optimizers import Adam
from tensorflow.keras import layers


# initialize model

def initialize_model(sequence_length):

    model = Sequential()
    model.add(layers.Conv1D( filters=64, kernel_size=6, input_shape=(sequence_length, 1)))
    model.add(layers.GRU(units=32, activation='relu'
                        #, return_sequences=True
                        ))
    model.add(layers.Dense(16, activation="relu"))
    model.add(layers.Dense(1, activation="linear"))

    return model


# compile model

def compile_model(model, 
                  loss = 'mae',
                  learning_rate = 0.001):

    model.compile(loss=loss, optimizer=Adam(learning_rate=learning_rate))

    return model

# train model

def train_model(model,
                X_train,
                y_train,
                X_val,
                y_val,
                epochs,
                batch_size):

    history = model.fit(X_train, y_train,
                        epochs=epochs, verbose=1, 
                        batch_size= batch_size,
                        validation_data=(X_val, y_val))
    
    return history, model

