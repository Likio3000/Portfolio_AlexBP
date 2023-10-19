import pandas as pd
import numpy as np
import logging
from utils.utils import export_df_head_to_csv, read_csv_to_dataframe

df = read_csv_to_dataframe("project2/processed_data/liquidations24h.csv")
export_df_head_to_csv(
    num_rows=5,
    base_directory="project2",
    folder_name="heads_csv",
    file_name="liquidations_head",
    df=df,
)
