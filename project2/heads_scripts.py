import pandas as pd
import os
import logging
from typing import Union
from pathlib import Path

# Initialize logging
logging.basicConfig(level=logging.INFO)

def export_df_head_to_csv(df: pd.DataFrame, num_rows: int = 5, base_directory: Union[str, Path] = '..', 
                          folder_name: str = 'heads_csv', file_name: str = 'df_head.csv') -> str:
    """
    Export the head of a DataFrame to a CSV file.
    
    Parameters:
        df (pd.DataFrame): The DataFrame to export.
        num_rows (int): Number of rows to include in the head. Default is 5.
        base_directory (Union[str, Path]): The base directory for the folder. Default is one level up ('..').
        folder_name (str): The name of the folder to save the CSV in. Default is 'heads_csv'.
        file_name (str): The name of the CSV file. Default is 'df_head.csv'.
        
    Returns:
        str: The path where the CSV was saved.
    """
    try:
        # Create the complete folder path
        folder_path = Path(base_directory) / folder_name
        
        # Create folder if it doesn't exist
        folder_path.mkdir(parents=True, exist_ok=True)
        
        # Create the initial complete file path
        file_path = folder_path / file_name
        
        # Generate a new file name if file already exists to avoid overwriting
        counter = 1
        while file_path.exists():
            file_name_without_extension = file_path.stem
            extension = file_path.suffix
            new_file_name = f"{file_name_without_extension}_{counter}{extension}"
            file_path = folder_path / new_file_name
            counter += 1
        
        # Export the head of the DataFrame to CSV
        df.head(num_rows).to_csv(file_path, index=False)
        
        logging.info(f"Head of DataFrame has been exported to {file_path}")
        return str(file_path)
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None

# Sample DataFrame
sample_df = pd.DataFrame({
    'Column1': range(1, 11),
    'Column2': list('abcdefghij')
})

# Example usage
export_df_head_to_csv(sample_df, num_rows=5, base_directory='..', folder_name='heads_csv', file_name='sample_head.csv')
