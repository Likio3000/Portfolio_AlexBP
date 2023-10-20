import os
import pandas as pd
from data_preprocessor import preprocess_data
from ltsm_model_btc import train_evaluate_lstm_model
from regression_model_btc import train_regression

# Create 'regression_model' directory if it doesn't exist
if not os.path.exists('regression_model'):
    os.makedirs('regression_model')

# Read the raw data
raw_data = pd.read_csv("raw_data/BYBIT_BTC_DATA.csv")

# Preprocess the data using the provided script
preprocessed_data = preprocess_data(raw_data)

# Train LSTM and export it
time_steps = 6  # 60
epochs = 3  # 20
batch_size = 10  # 35
model, history, predictions, X_test_seq, y_test_seq = train_evaluate_lstm_model(preprocessed_data,
                                                                                time_steps, epochs, batch_size)

# Save training history to a CSV file
history_df = pd.DataFrame(history.history)
history_df.to_csv("lstm_neural_network/lstm_training_history.csv", index=False)

# Save test predictions to a CSV file
predictions_df = pd.DataFrame({"true_values": y_test_seq.flatten(), "predictions": predictions.flatten()})
predictions_df.to_csv("lstm_neural_network/lstm_test_predictions.csv", index=False)

# Train regression model and export it
avg_mse, avg_r2 = train_regression()

print("Average Mean Squared Error (regression):", avg_mse)
print("Average R^2 Score (regression):", avg_r2)
