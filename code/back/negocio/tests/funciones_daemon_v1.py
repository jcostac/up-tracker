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

def descargador_ultimo_i90(fichero_config, carpeta_raw, dl_dates):
    """Download the latest i90.

    This function retrieves the latest i90 data, calculates the date 90 days
    before the current date, and appends it to a download list. It creates a
    directory structure organized by year if it doesn't exist, initializes an
    i90 object, and downloads data for each date in the download list, logging
    success or failure for each operation.

    Args:
        fichero_config (str): The path to the configuration file for the i90 download.
        carpeta_raw (str): The base directory where raw data should be saved.
        dl_dates (list): A list of dates for which data needs to be downloaded.

    """
    """
    #getting dates
    today =  datetime.now().date() # todays date in yyyy-mm-dd format
    fecha_ultimo_i90 = today -  timedelta(days=90)
    annio = str(fecha_ultimo_i90.year) #extracting year string to save in corresponding folder
    #fecha_ultimo_i90 = fecha_ultimo_i90.strftime("%Y-%m-%d") #converting object to string

    #appendng dates to dl list
    dl_dates.append(fecha_ultimo_i90) #append todays date to download list

    #creating output path
    subdirectories = os.path.join("ESIOS", "i90",  annio)
    carpeta_out_i90 = os.path.join(carpeta_raw, subdirectories)

    obj = i90ZIP(fichero_config, carpeta_out_i90) 
    for date in dl_dates:            
        obj.descargar_fichero(date, carpeta_out_i90)    
    """


    try: 
        print("Actualizando....")

        #getting dates
        today =  datetime.now().date() # todays date in yyyy-mm-dd format
        fecha_ultimo_i90 = today -  timedelta(days=90)
        annio = str(fecha_ultimo_i90.year) #extracting year string to save in corresponding folder
        #fecha_ultimo_i90 = fecha_ultimo_i90.strftime("%Y-%m-%d") #converting object to string

        #appendng dates to dl list
        dl_dates.append(fecha_ultimo_i90) #append todays date to download list

        #creating output path
        subdirectories = os.path.join("ESIOS", "i90",  annio)
        carpeta_out_i90 = os.path.join(carpeta_raw, subdirectories)

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
        obj = i90ZIP(fichero_config, carpeta_out_i90) 

        for date in dl_dates:
            try:
                print(carpeta_out_i90)
                obj.descargar_fichero(date, carpeta_out_i90)     
                logging.info(f'Data successfully retrieved for {fecha_ultimo_i90} i90 in')
                dl_dates.remove(date) #remove the date from the dowload dates list

            except Exception as e:
                logging.info(f"An error occurred when dowloading the data for {fecha_ultimo_i90} i90 on {today}")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}") 

def download_prices(market_key, download_function, base_path, dl_dates):
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
            # Calculate end date based on existing date value
            end_date = datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)
            end_date_str = end_date.strftime("%Y-%m-%d")

            # Download prices using the specified function
            prices = download_function(date, end_date_str)

            # Extract year from the start date
            start_date = datetime.strptime(date, "%Y-%m-%d")
            year = start_date.strftime("%Y")

            # Define the folder path and file name based on the market key
            folder_path = os.path.join(base_path, "OMIE", market_key.capitalize(), year)
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

            # Save the prices to the file
            with open(file_path, 'w') as file:
                file.write(prices)
            print(f"File saved to '{file_path}'")

            #remove date from dowload queue 
            dl_dates[market_key].remove(date)
            print(f"Removed {date} from {market_key} scheduled downloads.")

        except Exception as e:
            logging.error(f"Failed to download prices for {market_key} from {date} to {end_date_str}: {str(e)}")

def descargador_precios (carpeta_raw, dl_dates):
    """Download ESIOS & OMIE prices.

    This function sets the start and end dates for downloading prices, appends
    the current date to a list for each market type, and utilizes the
    download_prices function to download data for various market types. The
    function orchestrates the process of retrieving and organizing market
    prices into directories based on the date and market type.

    Args:
        carpeta_raw (str): The base directory where raw data should be saved.
        dl_dates (dict): Dictionary containing lists of dates for each market type.

    """
    #settign start and end date for download_preciso fucntions to use 
    start_date =  (datetime.now()-timedelta(days=0)).date() #today´s date (prices will be retrieved at 23:59)
    annio = str(start_date.year) #extracting year of download

    #converting dates to strings
    start_date = start_date.strftime("%Y-%m-%d")


    # Start dates are appended to the download date dictionary
    dl_dates["diario"].append(start_date)
    dl_dates["intradiario"].append(start_date)
    dl_dates["rr"].append(start_date)
    dl_dates["afrr"].append(start_date)
    dl_dates["mfrr"].append(start_date)    
    #dl_dates["secundaria"].append(start_date)
    #dl_dates["terciaria"].append(start_date)

    esios = ESIOS() # Start ESIOS class
    omie = OMIE() #start OMIE class 

    # Use the download_prices function for each market type
    download_prices("diario", omie.download_precio_diario, carpeta_raw, dl_dates)
    download_prices("intradiario", omie.download_precio_intradiario, carpeta_raw,dl_dates)
    download_prices("rr", esios.download_precios_balance_rr, carpeta_raw, dl_dates)
    download_prices("afrr", esios.download_precios_secundaria, carpeta_raw, dl_dates)
    download_prices("mfrr", esios.download_precios_terciaria, carpeta_raw, dl_dates)
