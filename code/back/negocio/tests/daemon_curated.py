from utilidades.i90zip import i90ZIP
import datetime
from datetime import datetime
from datetime import timedelta
import schedule
import time
import logging
import os
import pandas as pd
import re
import config
from typing import List, Dict, Any
from zipfile import BadZipFile
import pretty_errors
import json


def setup_logging(config):
    """Initialize logging.

    Sets up logging configuration based on the provided configuration dictionary.
    This function reads the log filename, level, and format from the config and
    initializes the logging module accordingly. The logs are written to a specified
    file, allowing for detailed tracking of the application's operations and errors.

    Args:
        config (dict): A dictionary containing logging configuration, including
                       'filename', 'level', and 'format' keys.

    """
    log_file = config['logging']['filename']
    
    # Check if the log file already exists
    if os.path.exists(log_file):
        pass

    else:
        logging.basicConfig(
            filename=config['logging']['filename'], #use logging and filename parameters in yml config
            level=getattr(logging, config['logging']['level'].upper()), #INFO level included for logs in the log file
            format='%(asctime)s %(name)s %(levelname)s:%(message)s'
        )

def create_new_folder (new_folder_path:str) -> None :
    """
    Create a new folder at the specified path if it doesn't already exist.

    Parameters:
        new_folder_path (str): The path where the new folder should be created.
    """
    try:
        if not os.path.exists(new_folder_path):
            # Attempt to create the new directory
            os.makedirs(new_folder_path)
            print(f"Created new folder: {new_folder_path}")
        
        else:
            # If the folder already exists, inform the user
            print(f"Folder already exists: {new_folder_path}")

    except OSError as e:
        # Catch OSError exceptions (e.g., permission denied, file system error)
        logging.error(f"Error creating directory '{new_folder_path}': {e}")

    except Exception as e:
        # Catch any other exceptions that were not anticipated
        logging.error(f"An unexpected error occurred: {e}")

def save_csv_to_parquet(parquet_filepath: str, raw_csv_filepath: str) -> None:
    """
    Save a CSV file to a Parquet file.

    Args:
        parquet_filepath (str): The path where the Parquet file should be saved.
        raw_csv_filepath (str): The path of the raw CSV file that will be saved to parquet.
    """
    try:
        # Check if the parquet file already exists
        if os.path.exists(parquet_filepath):
            # If it exists, read it to append more data
            master_df = pd.read_parquet(parquet_filepath)
            logging.info(f"Existing parquet file '{parquet_filepath}' loaded.")
        else:
            # Otherwise, start with an empty DataFrame
            master_df = pd.DataFrame()
            logging.info("Parquet file does not exist, starting with an empty DataFrame.")
        
        try:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(raw_csv_filepath)
            # Append the CSV DataFrame to the master DataFrame
            master_df = pd.concat([master_df, df], ignore_index=True)

            filename = os.path.basename(raw_csv_filepath) #getting filename
            logging.info(f"Processed CSV file: {filename}")

            # Write the combined DataFrame back to the Parquet file
            master_df.to_parquet(parquet_filepath, engine='pyarrow')
            logging.info(f"Successfully updated parquet file: {parquet_filepath}")

        except pd.errors.EmptyDataError:
            logging.warning(f"Skipping empty CSV file: {filename}")
        except Exception as e:
            logging.error(f"Error processing CSV file '{filename}': {str(e)}")

    except FileNotFoundError:
        logging.error(f"File {filename} not found: {raw_csv_filepath}")
    except PermissionError:
        logging.error(f"Permission denied when accessing file: {raw_csv_filepath}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")

def save_zip_to_parquet(month_path_curated:str, month_path_raw: str, filename: str)-> None:
    """
    Save a zip file to a Parquet file.

    Args:
        month_path_curated (str): The path where the Parquet file should be saved.
        month_path_raw (str): The path of the raw zip file that will be saved to parquet.
        filename (str): The name of the zip file.
    """
    fichero_config = r"C:\Users\joaquin.costa\Escritorio\UP Tacker\main\config.yml"

    try:
        if not filename:
            raise ValueError("Filename cannot be empty or None")

        files = [filename]  # put filename in a list
        zip_path = os.path.join(month_path_raw, filename)

        if not os.path.exists(zip_path): #check if full file path exists
            raise FileNotFoundError(f"Zip file not found: {zip_path}")

        try:
            #obj = i90zip(config.fichero_config, month_path_raw)
            obj = i90ZIP(fichero_config, month_path_raw) #para pruebas en local

        except Exception as e:
            logging.error("Error creating i90 object")

        else:
            print("Succesfully created i90 object")
            try:
                obj.generar_ficheros(files, month_path_curated, month_path_raw, lista_uprog=None)
            except BadZipFile:
                raise BadZipFile(f"The file {filename} is not a valid zip file or is corrupted")
            except Exception as e:
                raise Exception(f"Error processing zip file: {str(e)}")
            else: 
                logging.info(f"Successfully processed zip file: {filename}")

    except FileNotFoundError as e:
        logging.error(str(e))
    except BadZipFile as e:
        logging.error(str(e))
    except ValueError as e:
        logging.error(str(e))
    except Exception as e:
        logging.error(f"Unexpected error occurred: {str(e)}")

def load_json_processed_files()-> dict:
    """
    Load the processed files JSON.

    Returns:
        dict: The processed files JSON.
    """
    failed_message = "Failed to load processed files json"

    try:
        #processed_files_json_path = config.processed_files_log['filename']  # getting the processed files json path from config.py
        processed_files_json_path = "C:\\Users\\joaquin.costa\\Escritorio\\UP Tacker\\daemon logs\\processed_files.json" #pruebas en local

        if os.path.exists(processed_files_json_path): #check if processed files json exists
            with open(processed_files_json_path, 'r') as f: #open processed files json
                logging.info(f"Processed files log found at {processed_files_json_path}")
                processed_files_json = json.load(f)#load processed files json
                return processed_files_json
        else:
            raise FileNotFoundError(f"Processed files log not found at {processed_files_json_path}")
        
    except FileNotFoundError as e:
        logging.error(str(e))
        return failed_message
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON: {str(e)}")
        return failed_message
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        return failed_message

def update_json_processed_files(filename: str, json_processed_files: dict) -> None:
    """
    Update the processed files JSON with a new processed file.

    Args:
        processed_files_json (dict): The current state of processed files.
        filename (str): The name of the processed file.

    Returns:
        None
    """
    json_filepath_local = "C:\\Users\\joaquin.costa\\Escritorio\\UP Tacker\\daemon logs\\processed_files.json"

    try:
        # Extract year from filename
        year = extract_year_from_filename(filename)
        indicator = extract_indicator_from_filename(filename)

        if year:
            # Update the processed files JSON
            if indicator == 'i90':
                if year not in json_processed_files['i90']:
                    json_processed_files['i90'][year] = []
                json_processed_files['i90'][year].append(filename)
            else:
                if year not in json_processed_files['prices'][indicator]:
                    json_processed_files['prices'][indicator][year] = []
                json_processed_files['prices'][indicator][year].append(filename)

            # Save the updated JSON
            #with open(config.processed_files_log['filename'], 'w') as f:
                #json.dump(json_processed_files, f, indent=2)
            
            with open(json_filepath_local, 'w') as f:
                json.dump(json_processed_files, f, indent=2)
            
            logging.info(f"Updated processed files log with {filename} for {indicator}")
        else:
            logging.warning(f"Could not extract year from filename: {filename}")

    except KeyError as e:
        logging.error(f"KeyError: {str(e)}. Check if the indicator is correctly defined in the JSON structure.")
    except IOError as e:
        logging.error(f"IOError: {str(e)}. Could not write to the processed files log.")
    except Exception as e:
        logging.error(f"Unexpected error updating processed files log: {str(e)}")

def extract_year_from_filename(filename: str) -> str:
    """
    Extract the year from a filename.

    Args:
        filename (str): The filename to extract the year from.

    Returns:
        str: The extracted year, or None if no year is found.
    """
    try:
        # Try to match the year in the format "YYYY-MM-DD"
        match = re.search(r'(\d{4})-\d{2}-\d{2}', filename)
        if match:
            return match.group(1)
        
        # Try to match the year in the format "YYYYMMDD"
        match = re.search(r'(\d{4})\d{4}', filename)
        if match:
            return match.group(1)
        
        # If no match is found, return None
        return print("No match found when extracting year from filename")
    
    except Exception as e:
        logging.error(f"Error extracting year from filename '{filename}': {str(e)}")
        return print("Error extracting year from filename")

def extract_indicator_from_filename(filename: str) -> str:
    """
    Extract the indicator from a filename.

    Args:
        filename (str): The filename to extract the indicator from.

    Returns:
        str: The extracted indicator, or None if no indicator is found.
    """
    try:
        if filename.startswith("I90"):
            return "i90"
        elif "_precios_diario" in filename:
            return "diario"
        elif "_precios_intradiario" in filename:
            return "intradiario"
        elif "_precios_rr" in filename:
            return "rr"
        elif "_precios_secundaria" in filename:
            return "afrr"
        elif "_precios_terciaria" in filename:
            return "mfrr"
        else:
            logging.warning(f"No match found when extracting indicator from filename: {filename}")
            return print("No match found when extracting indicator from filename")
    except Exception as e:
        logging.error(f"Error extracting indicator from filename '{filename}': {str(e)}")
        return print("Error extracting indicator from filename")

def check_is_processed(filename: str, json_processed_files: dict) -> bool:
    """
    Check if a file has been processed.

    Args:
        file (str): The filename to check.
        processed_files_json (dict): The JSON containing information about processed files.

    Returns:
        bool: True if the file has been processed, False otherwise.
    """
    # Extract year and indicator from the filename
    year = extract_year_from_filename(filename)
    indicator = extract_indicator_from_filename(filename)

    # Determine which category to check based on the indicator
    if indicator == 'i90':
        files_for_year = json_processed_files.get('i90', {}).get(year, [])
    else:
        files_for_year = json_processed_files.get('prices', {}).get(indicator, {}).get(year, [])
    
    # Check if the file is in the list of processed files for its year and indicator
    is_processed = filename in files_for_year

    if is_processed:
        print(f"File {filename} has been processed")
    else:
        print(f"File {filename} has NOT been processed")

    # Explicitly return the boolean result
    return is_processed

def process_raw_files(indicator_dir_raw: str) -> None: 
    """
    Process raw files for a given indicator.

    Args:
        indicator_dir_raw (str): The path to the raw files for a given indicator.

    Returns:
        None
    """
    #indicator dir raw ejemplo: "...\UP Tacker\raw\ESIOS\AFRR"
    json_processed_files = load_json_processed_files()
    year_folder_lst = os.listdir(indicator_dir_raw) #list of years in the directory indicator directory ej: [2022, 2023]
    print(f"Data for the following years was found: {year_folder_lst}")

    for year_folder in year_folder_lst:  #for every year in the folder lst ej:2022,2023, etc

        year_path_raw = os.path.join(indicator_dir_raw, year_folder) #construct year path ej: "C:\Users\joaquin.costa\Escritorio\UP Tacker\raw\ESIOS\AFRR\2023"
        print(f"Iterating through {year_folder} in {indicator_dir_raw}")

        raw_file_lst = os.listdir(year_path_raw) #list of files in the year path raw directory ej: "2022-04-13_precios_diario.csv" or "I90DIA_20230315.zip"
        print(f"{len(raw_file_lst)} files found in {year_path_raw}")

        #create the directory to save curated files in the curated folder by replacing raw str with curated str
        year_path_curated = year_path_raw.replace("raw", "curated") #construct year path curated ej: "C:\Users\joaquin.costa\Escritorio\UP Tacker\curated\ESIOS\AFRR\2023"
        create_new_folder(year_path_curated)
        
        for file in raw_file_lst: #for every file in file list ["2022-01-01_precios_diario.csv", "2022-01-02_precios_diario.csv"]
            print(f"Processing {file}")
            is_processed = check_is_processed(file, json_processed_files)

            if not is_processed: #if file has not been processed i.e. is_processed = False
                if file.endswith(".csv"): #if file is price file (raw csv)
                    filepath_raw = os.path.join(year_path_raw, file) #construct full file path ej: "C:(...)\curated\ESIOS\AFRR\2023\2022-01-01_precios_diario.csv"
                    price_filename = re.sub(r"\d{4}-\d{2}-\d{2}_(.*?)\.csv$", r"\1", file) #removing the date portion from the file name  ej: "precios_diario"
                    parquet_filename = price_filename + ".parquet" #use this to create name of curated parquet filename "precios_diario.parquet
                    parquet_filepath = os.path.join(year_path_curated, parquet_filename) #creating full parquet path ej: "(...)\curated\ESIOS\AFRR\2023\precios_diario.parquet"

                    #saving csv to parquet
                    try: 
                        save_csv_to_parquet(parquet_filepath, filepath_raw)
                        update_json_processed_files(json_processed_files, file)
                        logging.info(f"Successfully updated processed files log with {file}")
                        #print(f"Successfully updated processed files log with {file}")
                        #print(f"Successfully saved price file: {file}, to {parquetpath_curated}")

                    except Exception as e: 
                        logging.error(f"An error occurred processing .csv file ({file}) to .parquet: {str(e)}")
                    else: 
                        logging.info(f"Successfully saved price file: {file}, to {parquet_filepath}")
                
                elif file.endswith(".zip"): #if file is i90 zip (raw zip)
                    #saving zip to parquet
                    try: 
                        save_zip_to_parquet(year_path_curated, year_path_raw, file)
                        update_json_processed_files(json_processed_files, file)
                        logging.info(f"Successfully updated processed files log with {file}")
                        #print(f"Successfully updated processed files log with {file}")
                        #print(f"Successfully saved i90 file: {file}, to {year_path_curated}")
                    
                    except Exception as e: 
                        logging.error(f"An error occurred processing .zip file ({file}) to .parquet: {str(e)}")
                    else: 
                        logging.info(f"Successfully saved i90 file: {file}, to {year_path_curated}")   

                else:
                    logging.warning(f"Unsupported file type: {file}")
            else:
                logging.info(f"File {file} has already been processed. Skipping.")
                #print(f"File {file} has already been processed. Skipping.")

    logging.info("Finished processing all files.")
    #print("Finished processing all files.")
                    
def main(raw_dir_lst: List[str]) -> None:
    #Proceso de curated
    curated_time = "23.59"

    ##CURATED PROCESS##
    for raw_dir in raw_dir_lst: #iterate through indicator
        schedule.every().day.at(curated_time).do(process_raw_files, raw_dir)
                                    
    while True: #set infinite loop
        schedule.run_pending() #run all pending jobs
        time.sleep(1) #sleep for 1 second 

if __name__ == "__main__": 
    diario_raw_dir = "C:\\Users\\joaquin.costa\\Escritorio\\UP Tacker\\data\\raw\\OMIE\\DIARIO"
    intradiario= "raw"
    rr = "raw"
    afrr = "raw"
    mfrr = "raw"
    i90_raw_dir = "C:\\Users\\joaquin.costa\\Escritorio\\UP Tacker\\data\\raw\\ESIOS\\i90"
    
    dir_lst = [diario_raw_dir, intradiario, rr, afrr, mfrr, i90_raw_dir] #directory list

    #process_raw_files(diario_raw_dir)
    #process_raw_files(i90_raw_dir)
"""
    json_ = load_json_processed_files()

    print(json_)

    test_files = ["2022-01-01_precios_diario.csv", "2021-01-01_precios_diario.csv", "2021-02-02_precios_diario.csv", "2023-01-01_precios_intradiario.csv",
                   "2023-01-01_precios_rr.csv", "2023-01-01_precios_secundaria.csv", "2023-01-01_precios_terciaria.csv", "I90DIA_20230315.zip"]
    
    for file in test_files:
        is_processed = check_is_processed(file, json_)
        if not is_processed:
            print(f"File {file} has NOT been processed")
            update_json_processed_files(file, json_)

        else: print(f"File {file} has been processed")

    print(json_)

# Convert the dictionary to a JSON string with pretty formatting
json_string = json.dumps(json_, indent=4, sort_keys=True)

print(json_string)
"""















