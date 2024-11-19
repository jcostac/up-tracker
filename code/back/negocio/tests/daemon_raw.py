from utilidades.i90zip import i90ZIP
from utilidades.esios import ESIOS
from utilidades.omie import OMIE
import datetime
from datetime import datetime
from datetime import timedelta
import schedule
import time
import time
import logging
import os
import pandas as pd
from typing import List, Dict

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
    logging.basicConfig(
        filename=config['logging']['filename'], #use logging and filename parameters in yml config
        level=getattr(logging, config['logging']['level'].upper()), #INFO level included for logs in the log file
        format='%(asctime)s %(name)s %(levelname)s:%(message)s'
    )

def descargador_ultimo_i90(fichero_config, carpeta_raw, dl_dates: List[str]) -> None:
    """Download the latest i90.

    This function retrieves the latest i90 data, calculates the date 90 days
    before the current date, and appends it to a download list. It creates a
    directory structure organized by year if it doesn't exist, initializes an
    i90 object, and downloads data for each date in the download list, logging
    success or failure for each operation.

    Args:
        fichero_config (str): The path to the configuration file for the i90 download.
        carpeta_raw (str): The base directory where raw data should be saved. ej: r"C:\Users\joaquin.costa\Escritorio\UP Tacker\data\raw"
        dl_dates (list): A list of dates for which data needs to be downloaded.

    """
    try: 
        print("Actualizando....")

        #getting dates
        today =  datetime.now().date() # todays date in yyyy-mm-dd format
        fecha_ultimo_i90 = today -  timedelta(days=90)
        year = str(fecha_ultimo_i90.year) #extracting year string to save in corresponding folder
        month = str(fecha_ultimo_i90.month) #extracting month string to save in folder
        fecha_ultimo_i90_str= fecha_ultimo_i90.strftime("%Y-%m-%d") #converting object to string

        #appendng dates to dl list
        dl_dates.append(fecha_ultimo_i90_str) #append todays date to download list

        #creating output path
        subdirectories = os.path.join("ESIOS", "i90",  year)
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
            

        #creating i90 object to begin download
        obj = i90ZIP(fichero_config, carpeta_out_i90) #yml file and carpeta out donde estan los ficheros zip (i.e.# ej:"...\UP Tacker\data\raw\ESIOS\i90\2023\12")

        for date in dl_dates:
            try:
                obj.descargar_fichero(date, carpeta_out_i90)   #saves file as  'I90DIA_' + fecha.strftime("%Y%m%d") + ".zip"   
                logging.info(f'Data successfully retrieved for {fecha_ultimo_i90} i90') 
                dl_dates.remove(date) #remove the date from the dowload dates list

            except Exception as e:
                logging.info(f"An error occurred when dowloading the data for {fecha_ultimo_i90} i90 on {today}")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}") 

def download_prices(market_key, download_function , base_path: str, dl_dates: Dict[str]) -> None:
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
    for date in dl_dates.get(market_key, []):
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

            # Ensure the directory exists, create if it does not
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
                print(f"Directory '{folder_path}' created successfully.")

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
                dl_dates[market_key].remove(date)
                print(f"Removed {date} from {market_key} scheduled downloads.")

        except Exception as e:
            logging.error(f"Failed to download prices for {market_key} for {date}: {str(e)}")

def descargador_precios (carpeta_raw:str , dl_dates: Dict[str]) -> None:
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
    #settign start and end date for download_preciso fucntions to use 
    start_date =  datetime.now().date() #today´s date (prices will be retrieved at 23:59)

    #converting dates to strings
    start_date = start_date.strftime("%Y-%m-%d")

    # Start dates are appended to the download date dictionary
    dl_dates["diario"].append(start_date)
    dl_dates["intradiario"].append(start_date)
    dl_dates["rr"].append(start_date)
    dl_dates["afrr"].append(start_date)
    dl_dates["mfrr"].append(start_date)

    print(dl_dates)

    esios = ESIOS() # Start ESIOS class
    omie = OMIE() #start OMIE class 

    # Use the download_prices function for each market type
    logging.info("Retrieving DIARIO prices")
    download_prices("diario", OMIE.download_precio_diario, carpeta_raw, dl_dates)

    logging.info("Retrieving INTRADIARIO prices")
    download_prices("intradiario", OMIE.download_precio_intradiario, carpeta_raw,dl_dates)

    logging.info("Retrieving RR prices")
    download_prices("rr", ESIOS.download_precios_balance_rr, carpeta_raw, dl_dates)

    logging.info("Retrieving AFRR prices")
    download_prices("afrr", ESIOS.download_precios_secundaria, carpeta_raw, dl_dates)

    logging.info("Retrieving MFRR prices")
    download_prices("mfrr", ESIOS.download_precios_terciaria, carpeta_raw, dl_dates)

def main(fichero_config, carpeta_raw):
    """Main function.

    This is the entry point for the application. It initializes the list and
    dictionary of dates to be downloaded for various markets, schedules daily
    tasks for downloading i90 and market prices, and runs an infinite loop to
    process scheduled tasks continuously. The function ensures log directories
    exist and manages the overall workflow of downloading and saving market
    data.

    Args:
        fichero_config (str): Path to the configuration file for i90 download.
        carpeta_raw (str): Base directory for saving raw data.
    
    """

    #list of dates  to download for i90, useful to redownload failed dates
    dl_dates_lst = [] 

    #dictionary of dates to download for each market in ESIOS/OMIE, useful to redwonload failed dates
    dl_dates_dct = { "diario": [], "intradiario": [],  "rr": [], "afrr": [], "mfrr": []} 

    schedule.every().day.at("23:59").do(descargador_ultimo_i90 (fichero_config, carpeta_raw, dl_dates_lst))
    schedule.every().day.at("23:59").do(descargador_precios(carpeta_raw, dl_dates_dct))

    while True: #set infinite loop
        schedule.run_pending() #run all pending jobs
        time.sleep(1) #sleep for 1 second 

if __name__ == "__main__": 
    #logging confguration in yml format
    config = {
    'logging': {
       'filename': r"C:\Users\joaquin.costa\Escritorio\UP Tacker\dameon logs\daemon_descarga_logs.log", #ruta para el log del demonio
       'level': 'INFO', #nivel de mensajes que van a ser loggeados 
        'format': '%(asctime)s %(name)s %(levelname)s:%(message)s'
    }}
     
    #Set up  daemon log
    setup_logging(config)

    #configuration and out  directories that need to be passed to main daemon function
    fichero_config = r"C:\Users\joaquin.costa\Escritorio\UP Tacker\main\config.yml"
    carpeta_raw =r"C:\Users\joaquin.costa\Escritorio\UP Tacker\raw"

    # Ensure log directory exists
    #os.makedirs(r"C:\Users\joaquin.costa\Escritorio\UP Tacker\dameon logs\daemon_descarga_logs.log", exist_ok=True)

    dl_dates_dct = { "diario": [], "intradiario": [],  "rr": [], "afrr": [], "mfrr": []}

    descargador_precios(carpeta_raw, dl_dates_dct)
