from utilidades.i90zip import i90ZIP
from utilidades.esios import ESIOS
from utilidades.omie import OMIE
import datetime
import pandas as pd
from datetime import datetime
from datetime import timedelta
import schedule
import time
import logging
import os
import re
from typing import List, Dict
from zipfile import BadZipFile
import pretty_errors
import config
import json
import unittest
import pyarrow
import glob



def descargador_ultimo_i90(fichero_config, carpeta_raw:str , start_date:str, n:int, list_date:List[str]) -> None:
    """
    Download the latest i90 file from the ESIOS website.

    Args:
        fichero_config (str): The path to the config file.
        carpeta_raw (str): The path to the raw data folder.
        dl_dates (list): A list of dates to be downloaded.

    Returns:
        None
    """
    #try: 
    print("Actualizando....")

    #getting dates
    #today =  datetime.now().date() # todays date in yyyy-mm-dd format
    #fecha_ultimo_i90 = today -  timedelta(days=90)
    #year = str(fecha_ultimo_i90.year) #extracting year string to save in corresponding folder
    #month = str(fecha_ultimo_i90.month) #extracting month string to save in folder
    #fecha_ultimo_i90_str= fecha_ultimo_i90.strftime("%Y-%m-%d") #converting object to string

    #appendng dates to dl list
    #dl_dates_lst.append(fecha_ultimo_i90_str) #append todays date to download list

        
    if list_date == None:
        list_date = []
        for i in range(n):
            print(i)
            current_date = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=i)
            list_date.append(current_date.strftime("%Y-%m-%d"))  


    for date in list_date: #dl_dates_lst:
        #try:

        year = datetime.strptime(date, "%Y-%m-%d").year

        #creating output path
        subdirectories = os.path.join("ESIOS", "i90",  str(year))
        carpeta_out_i90 = os.path.join(carpeta_raw, subdirectories) # Carpeta donde van a ir los ficheros zip ej:"...\UP Tacker\data\raw\ESIOS\i90\2023\12"

        # Check if the directory exists
        if not os.path.exists(carpeta_out_i90):
            # Create the directory if it doesn't exist
            try:
                os.makedirs(carpeta_out_i90)
                print(f"Directory '{carpeta_out_i90}' created successfully.")
            except Exception as e:
                print(f"An error occurred while creating the directory: {e}")
        else:
            print(f"Directory '{carpeta_out_i90}' already exists. Saving file in the existing path.")

        obj = i90ZIP(fichero_config, carpeta_out_i90) #yml file and carpeta out donde estan los ficheros zip (i.e.# ej:"...\UP Tacker\data\raw\ESIOS\i90\2023\12")
        obj.descargar_fichero(datetime.strptime(date,"%Y-%m-%d"), carpeta_out_i90)   #saves file as  'I90DIA_' + fecha.strftime("%Y%m%d") + ".zip"   
        logging.info(f'Data successfully retrieved for {date} i90') 
        #dl_dates_lst.remove(date) #remove the date from the dowload dates list

        #except Exception as e:
        #    logging.info(f"An error occurred when dowloading the data for {fecha_ultimo_i90} i90 on {today}")

    #except Exception as e:
    #    logging.error(f"An unexpected error occurred: {str(e)}") 
    #    logging.exception("Full traceback:")

def download_prices(market_key: str, download_function: callable, base_path: str, dl_dates_dct: Dict[str, List[str]]) -> None:
    """Download prices for a given market.

    This function downloads market prices for specific dates, saves them to
    corresponding directories based on the market type, and updates the list
    of dates to be downloaded. It constructs directory paths based on the
    market type and year, ensuring the directory exists before saving files.
    Errors are logged if any operation fails.

    Args:
        market_key (str): The key representing the market type (e.g., "diario").
        download_function (callable): Function used to download prices for the specified market.
        base_path (str): The base directory path for saving files.
        dl_dates (dict): Dictionary containing dates to be downloaded for each market.

    """

    for date in dl_dates_dct[market_key]: #dl_dates_dct.get(market_key, []):
        try:

            # Extract year from the start date

            date_obj = datetime.strptime(date, "%Y-%m-%d")#convert date string to date object
            year = date_obj.strftime("%Y") #extracting year ie.2022
            month = date_obj.strftime("%m") #extracting month as two digit number ie. 04


            # Define the folder path and file name based on the market key
            folder_path = os.path.join(base_path, "OMIE", market_key.capitalize(), year) #market key folders  have to be capitalized
            if market_key == "diario":
                file_name = f'{date}_precios_diario.csv'
            elif market_key == "intradiario":
                file_name = f'{date}_precios_intradiario.csv'
            elif market_key == "rr":
                file_name = f'{date}_precios_rr.csv'
            elif market_key == "afrr":
                file_name = f'{date}_precios_secundaria.csv'
            elif market_key == "mfrr":
                file_name = f'{date}_precios_terciaria.csv'
            else:
                raise ValueError("Unsupported market key")

            #create folder if it doesnt exist
            create_new_folder(folder_path)

            # Define the full file path with the file name
            file_path = os.path.join(folder_path, file_name)

            # Download prices using the specified function
            prices = download_function(date, date)

            if prices.empty: #check if prices is a DF or if its empty
                logging.error(f"No data available for {market_key} for {date}.")

            else:
                prices.to_csv(file_path, index = False)
                print(f"File saved to '{file_path}'")

                #remove date from dowload queue 
                #dl_dates_dct[market_key].remove(date)
                print(f"Removed {date} from {market_key} scheduled downloads.")

        except Exception as e:
            logging.error(f"Failed to download prices for {market_key} for {date}: {str(e)}")
            logging.exception("Full traceback:")

def descargador_precios(carpeta_raw:str , start_date:str, n:int, list_date:List[str] ) -> None:
    """Download ESIOS & OMIE prices.

    This function sets the start and end dates for downloading prices, appends
    the current date to a list for each market type, and utilizes the
    download_prices function to download data for various market types. The
    function orchestrates the process of retrieving and organizing market
    prices into directories based on the date and market type.

    Args:
        carpeta_raw (str): The base directory where raw data should be saved.
        dl_dates (dict): Dictionary containing dct of dates for each market type.

    """

    try: 
        dl_dates_dct = { "diario": [], "intradiario": [],  "rr": [], "afrr": [], "mfrr": []} 

        if list_date == None:
            list_date = []
            for i in range(n):
                print(i)
                current_date = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=i)
                list_date.append(current_date.strftime("%Y-%m-%d"))  
        
        market_key_lst = ["diario", "intradiario", "rr", "afrr", "mfrr"]
        for current_date in list_date:                                
            for market_key in market_key_lst:            
                dl_dates_dct[market_key].append(current_date) #append start date to the list of dates for each market type


        esios = ESIOS() # Start ESIOS class
        omie = OMIE() #start OMIE class 

        # Use the download_prices function for each market type
        logging.info("Retrieving DIARIO prices")
        download_prices("diario", omie.download_precio_diario, carpeta_raw, dl_dates_dct)

        logging.info("Retrieving INTRADIARIO prices")
        download_prices("intradiario", omie.download_precio_intradiario, carpeta_raw,dl_dates_dct)

        logging.info("Retrieving RR prices")
        download_prices("rr", esios.download_precios_balance_rr, carpeta_raw, dl_dates_dct)

        logging.info("Retrieving AFRR prices")
        download_prices("afrr", esios.download_precios_secundaria, carpeta_raw, dl_dates_dct)

        logging.info("Retrieving MFRR prices")
        download_prices("mfrr", esios.download_precios_terciaria_media_ponderada, carpeta_raw, dl_dates_dct)

    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}") 
        logging.exception("Full traceback:")

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

def save_csv_to_parquet(parquet_filepath: str, raw_csv_filepath: str) -> bool:
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

        else: #if the parquet file does not exist
            #start with an empty DataFrame
            master_df = pd.DataFrame()
            logging.info("Parquet file does not exist, starting with an empty DataFrame.")
        
        try:
            # Read the CSV file into a DataFrame
            df = pd.read_csv(raw_csv_filepath)

            # Append the CSV DataFrame to the master DataFrame
            master_df = pd.concat([master_df, df], ignore_index=True)
            master_df = master_df.drop_duplicates()

            filename = os.path.basename(raw_csv_filepath) #getting filename
            logging.info(f"Processed CSV file: {filename}")

            # Write the combined DataFrame back to the Parquet file
            master_df.to_parquet(parquet_filepath, engine='pyarrow')
            logging.info(f"Successfully updated parquet file: {parquet_filepath}")
            return True

        except pd.errors.EmptyDataError:
            logging.warning(f"Skipping empty CSV file: {filename}")
            return False
        
        except OSError as e:
            logging.error(f"OS error when writing Parquet file {parquet_filepath}: {str(e)}")
            return False
        
        except pd.errors.ParserError as e:
            logging.error(f"Error parsing CSV file {raw_csv_filepath}: {str(e)}")
            return False

    except FileNotFoundError:
        logging.error(f"File {filename} not found: {raw_csv_filepath}")
        return False

    except PermissionError:
        logging.error(f"Permission denied when accessing file: {raw_csv_filepath}")
        return False

    except Exception as e:
        logging.error(f"An unexpected error occurred during while saving csv to parquet: {str(e)}")
        return False

def save_zip_to_parquet(year_path_curated:str, year_path_raw: str, filename: str)-> bool:
    """
    Save a zip file to a Parquet file.

    Args:
        year_path_curated (str): The path where the Parquet file should be saved.
        year_path_raw (str): The path of the raw zip file that will be saved to parquet.
        filename (str): The name of the zip file.
    """
    #fichero_config = r"C:\Users\joaquin.costa\Escritorio\UP Tacker\main\config.yml"

    try:
        if not filename:
            raise ValueError("Filename cannot be empty or None")

        try:
            obj = i90ZIP(config.fichero_config, year_path_raw)
            #obj = i90ZIP(fichero_config, year_path_raw) #para pruebas en local

        except Exception as e:
            logging.error("Error creating i90 object")
            return False

        else:
            print("Succesfully created i90 object")
            try:
                files = [filename]  # put filename in a list
                obj.generar_ficheros(files, year_path_curated, year_path_raw, lista_uprog=None)
                logging.info(f"Successfully processed zip file: {filename}")
                return True
                
            except BadZipFile:
                raise BadZipFile(f"The file {filename} is not a valid zip file or is corrupted")

    except FileNotFoundError as e:
        logging.error(str(e))
        return False

    except BadZipFile as e:
        logging.error(str(e))
        return False
    
    except ValueError as e:
        logging.error(str(e))
        return False

    except Exception as e:
        logging.error(f"Unexpected error occurred during while saving zip to parquet: {str(e)}")
        return False
    
def load_json_processed_files()-> dict:
    """
    Load the processed files JSON.

    Returns:
        dict: The processed files JSON.
    """

    try:
        processed_files_json_path = config.processed_files_log['filedir'] + "\\" + config.processed_files_log['filename']  # getting the processed files json path from config.py
        #processed_files_json_path = "C:\\Users\\joaquin.costa\\Escritorio\\UP Tacker\\daemon logs\\processed_files.json" #pruebas en local

        if os.path.exists(processed_files_json_path): #check if processed files json exists
            with open(processed_files_json_path, 'r') as f: #open processed files json
                logging.info(f"Processed files log found at {processed_files_json_path}")
                processed_files_json = json.load(f)#load processed files json
                return processed_files_json
        else:
            raise FileNotFoundError(f"Processed files log not found at {processed_files_json_path}")
        
    except FileNotFoundError as e:
        logging.error(f"Failed to load JSON: {str(e)}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Error decoding JSON: {str(e)}")
        return {}
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        return {}

def extract_year_from_filename(filename: str) -> str:
    """
    Extract the year from a filename.

    Args:
        filename (str): The filename to extract the year from.

    Returns:
        int: The extracted year as an integer, or None if no year is found.
    """
    try:
        # Try to match the year in the format "YYYY-MM-DD"
        match = re.search(r'^(\d{4})', filename)
        if match:
            year = match.group(1)
            logging.info(f"Successfully extracted year from filename: {year}")
            return year

        # Try to match the year in the format "YYYYMMDD" for i90 files 
        match = re.search(r'_(\d{4})', filename)
        if match:
            year = match.group(1)
            logging.info(f"Successfully extracted year from filename: {year}")
            return year 
        
        if match == None:
            logging.warning(f"No match found when extracting year from filename: {filename}")
            return None
        
    except Exception as e:
        logging.error(f"Error extracting year from filename '{filename}': {str(e)}")
        return None

def extract_indicator_from_filename(filename: str) -> str:
    """
    Extract the indicator from a filename.

    Args:
        filename (str): The filename to extract the indicator from.

    Returns:
        str: The extracted indicator, or None if no indicator is found.
    """
    try:
        if "I90D" in filename:
            indicador = "i90"
            return indicador
        elif "_precios_diario" in filename:
            indicador = "diario"
            return indicador
        elif "_precios_intradiario" in filename:
            indicador = "intradiario"
            return indicador
        elif "_precios_rr" in filename:
            indicador = "rr"
            return indicador
        elif "_precios_secundaria" in filename:
            indicador = "afrr"
            return indicador
        elif "_precios_terciaria" in filename:
            indicador = "mfrr"
            return indicador
        else:
            logging.warning(f"No match found when extracting indicator from filename: {filename}")
            return None
    except Exception as e:
        logging.error(f"Error extracting indicator from filename '{filename}': {str(e)}")
        return None

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
    indicador = extract_indicator_from_filename(filename)

    if (year or indicador) is None:
        return False
    else:
        files_for_year = json_processed_files.get(indicador, {}).get(year)
        print(f"files_for_year: {files_for_year}")
    
    if files_for_year is None:
        #print(f"No files found for {year} under {indicador} that have been processed")
        return False
    elif not files_for_year:
        #print(f"List is empty for {year} under {indicador} that have been processed")
        return False
    else:
        # Check if the file is in the list of processed files for its year and indicator
        is_processed = filename in files_for_year

    if is_processed:
        print(f"File {filename} ALREADY processed")
        logging.info(f"File {filename} ALREADY processed")
    else:
        print(f"File {filename} still NEEDS to be processed")
        logging.info(f"File {filename} still NEEDS to be processed")

    # Explicitly return the boolean result
    return is_processed

def update_json_processed_files(json_processed_files: dict, filename: str, year: int, indicador: str) -> bool:
    """
    Update the processed files JSON with a new processed file. Writes the new processed file to the "processed files" JSON file.

    Args:
        json_processed_files (dict): The current state of processed files.
        filename (str): The name of the processed file.
        year (int): The year to update in the processed files.
        indicador (str): The category/indicator to update (e.g., 'i90', 'diario').

    Returns:
        None
    """
    try:
        json_filepath = config.processed_files_log['filedir'] + "\\" + config.processed_files_log['filename']
        
        # Read existing JSON file if it exists
        if os.path.exists(json_filepath):
            with open(json_filepath, 'r') as f:
                existing_data = json.load(f)
                #no need to specify else since the json filepath will always be created to start daemon

        # Update the data
        if year and indicador: #handle in case year or indicador are None
            if year not in existing_data[indicador]:
                existing_data[indicador][year] = []
            if filename not in existing_data[indicador][year]:
                existing_data[indicador][year].append(filename)
        else:
            logging.warning(f"Year or indicador are None: {year}, {indicador}")
            return False

        # Write the updated data back to the file
        with open(json_filepath, 'w') as f:
            json.dump(existing_data, f, indent=2)

        logging.info(f"Updated processed files log with {filename} for {indicador} in {year}.")
        return True

    except Exception as e:
        logging.error(f"Error updating processed files log: {str(e)}")
        return False

def process_raw_files(fichero_config, carpeta_raw_dir_lst:List[str] , start_date:str, n:int, list_date:List[str]) -> None: 
    """
    Process raw files for a given indicator.

    Args:
        indicator_dir_raw (str): The path to the raw files for a given indicator.
                                 Example: "C:/Users/username/project/data/raw/ESIOS/i90"

    Returns:
        None
    """    
    # Fechas que vamos a procesar
    if list_date == None:
        list_date = []
        for i in range(n):
            print(i)
            current_date = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=i)
            list_date.append(current_date.strftime("%Y-%m-%d"))  
            # Para los i90
            current_date = datetime.strptime(start_date, "%Y-%m-%d") - timedelta(days=i) - timedelta(days=90)
            list_date.append(current_date.strftime("%Y-%m-%d"))  

    # Buscamos los ficheros de esas fechas
    for date in list_date:        
        year = datetime.strptime(date, "%Y-%m-%d").year
        for carpeta_raw in carpeta_raw_dir_lst:
            year_path_raw = os.path.join(carpeta_raw, str(year))    

            patron1 = datetime.strptime(date, "%Y-%m-%d").strftime("%Y%m%d")
            patron2 = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")

            archivos = glob.glob(year_path_raw + "/*")
            archivos_filtrados = [archivo for archivo in archivos if any(glob.fnmatch.fnmatch(archivo, f"*{patron}*") for patron in [patron1, patron2])]

            for file in archivos_filtrados:                
                print(file)

                conversion_function = save_csv_to_parquet if file.endswith(".csv") else save_zip_to_parquet if file.endswith(".zip") else None
                full_filepath_raw = os.path.join(year_path_raw, file)

                if os.path.exists(full_filepath_raw):

                    year_path_curated = year_path_raw.replace("raw", "curated")
                    create_new_folder(year_path_curated)

                    if file.endswith(".csv"):
                        price_filename = re.sub(r"\d{4}-\d{2}-\d{2}_(.*?)\.csv$", r"\1", file) #remove date from filename ex: 2023-01-01_precios_secundaria.csv -> precios_secundaria
                        parquet_filename = price_filename + ".parquet" #ex: precios_secundaria.parquet
                        
                        parquet_filepath = os.path.join(year_path_curated, os.path.basename(parquet_filename))
                        print(f"Parquet filepath: {parquet_filepath}")

                        conversion_successful = conversion_function(parquet_filepath, full_filepath_raw)                        
                    else:  
                        conversion_successful = conversion_function(year_path_curated, year_path_raw, file)
                        pass
                        

            

