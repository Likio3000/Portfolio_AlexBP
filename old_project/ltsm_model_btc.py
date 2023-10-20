import numpy as np
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping
import pickle

def create_sequences(X, y, time_steps=60):
    Xs, ys = [], []
    for i in range(len(X) - time_steps):
        Xs.append(X[i:(i+time_steps)])
        ys.append(y[i+time_steps])
    return np.array(Xs), np.array(ys)

def train_evaluate_lstm_model(data, time_steps=60, epochs=40, batch_size=32):
    # Set time as index
    data.set_index('time', inplace=True)

    # Select relevant numeric columns
    columns_to_scale = data.columns.drop('target_close')

    # Normalize and scale the data
    scaler = MinMaxScaler()
    scaled_data = data.copy()
    scaled_data[columns_to_scale] = scaler.fit_transform(data[columns_to_scale])

    # Split the data into train and test sets
    train_size = int(len(scaled_data) * 0.8)
    train_data = scaled_data[:train_size]
    test_data = scaled_data[train_size:]

    X_train = train_data.drop(columns=['target_close']).values
    y_train = train_data['target_close'].values
    X_test = test_data.drop(columns=['target_close']).values
    y_test = test_data["target_close"].values

    X_train_seq, y_train_seq = create_sequences(X_train, y_train, time_steps)
    X_test_seq, y_test_seq = create_sequences(X_test, y_test, time_steps)

    # Build and train the LSTM model
    model = Sequential()
    model.add(LSTM(units=64, return_sequences=True, input_shape=(X_train_seq.shape[1], X_train_seq.shape[2])))
    model.add(Dropout(0.2))
    model.add(LSTM(units=32, return_sequences=True))
    model.add(Dropout(0.2))
    model.add(LSTM(units=32))
    model.add(Dropout(0.2))
    model.add(Dense(units=1))

    # Define a custom learning rate
    custom_learning_rate = 0.001

    # Define early stopping
    early_stopping = EarlyStopping(monitor='val_loss', patience=5)

    model.compile(optimizer=Adam(learning_rate=custom_learning_rate), loss='mean_absolute_error')
    history = model.fit(X_train_seq, y_train_seq, epochs=epochs, batch_size=batch_size, validation_split=0.2, callbacks=[early_stopping])

    # Evaluate the model on the test data
    predictions = model.predict(X_test_seq)
    mse = np.mean((y_test_seq - predictions.flatten())**2)
    print(f"Mean Squared Error: {mse}")

    # Save the model
    model.save('lstm_neural_network/lstm_model.h5')

    # Save the scaler objects to .pkl files
    with open('lstm_neural_network/lstm_scaler.pkl', 'wb') as f:
        pickle.dump(scaler, f)

    return model, history, predictions, X_test_seq, y_test_seq
