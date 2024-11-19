import json
import os
from utilidades.i90zip import i90ZIP
from zipfile import BadZipFile

def update_json_processed_files(filename: str, json_processed_files: dict, year: int, indicador: str) -> None:
    """
    Update the processed files JSON with a new processed file.

    Args:
        filename (str): The name of the processed file.
        json_processed_files (dict): The current state of processed files.
        year (int): The year to update in the processed files.
        indicador (str): The category/indicator to update (e.g., 'i90', 'diario').

    Returns:
        None
    """
    print(year, indicador)
    print(f"BEFORE UPDATING json_processed_files: {json_processed_files}")

    try:
        if year and indicador:
            # Ensure the year exists for the indicator
            if year not in json_processed_files[indicador]:
                json_processed_files[indicador][year] = []
            
            # Append the filename if it's not already there
            if filename not in json_processed_files[indicador][year]:
                json_processed_files[indicador][year].append(filename)


            #Save the updated JSON
            with open(r"C:\Users\joaquin.costa\Escritorio\Git Repos\up-tracker\data\logs\processed_files.json", 'w') as f:
                json.dump(json_processed_files, f, indent=2)

            #logging.info(f"Updated processed files log with {filename} for {indicador} in {year}.")
        
        else:
            #logging.warning(f"Could not update processed files log: year or indicador parameter is missing for {filename}.")
            print(f"Could not update processed files log: year or indicador parameter is missing for {filename}.")

    except KeyError as e:
        #logging.error(f"KeyError: {str(e)}. Check if the indicator is correctly defined in the JSON structure.")
        print(f"KeyError: {str(e)}. Check if the indicator is correctly defined in the JSON structure.")
    except IOError as e:
        #logging.error(f"IOError: {str(e)}. Could not write to the processed files log.")
        print(f"IOError: {str(e)}. Could not write to the processed files log.")
    except Exception as e:
        #logging.error(f"Unexpected error updating processed files log: {str(e)}")
        print(f"Unexpected error updating processed files log: {str(e)}")

def save_zip_to_parquet(year_path_curated:str, year_path_raw: str, filename: str)-> None:
    """
    Save a zip file to a Parquet file.

    Args:
        month_path_curated (str): The path where the Parquet file should be saved.
        month_path_raw (str): The path of the raw zip file that will be saved to parquet.
        filename (str): The name of the zip file.
    """
    fichero_config = r"C:\Users\joaquin.costa\Escritorio\Git Repos\up-tracker\code\back\utilidades\config.yml"
    try:
        if not filename:
            raise ValueError("Filename cannot be empty or None")

        files = [filename]  # put filename in a list
        zip_path = os.path.join(year_path_raw, filename)

        if not os.path.exists(zip_path): #check if full file path exists
            raise FileNotFoundError(f"Zip file not found: {zip_path}")

        try:
            #obj = i90ZIP(config.fichero_config, month_path_raw)
            obj = i90ZIP(fichero_config, year_path_raw) #para pruebas en local

        except Exception as e:
            #logging.error("Error creating i90 object")
            print("Error creating i90 object")
        else:
            print("Succesfully created i90 object")
            try:
                obj.generar_ficheros(files, year_path_curated, year_path_raw, lista_uprog=None)

            except BadZipFile:
                #raise BadZipFile(f"The file {filename} is not a valid zip file or is corrupted")
                print(f"The file {filename} is not a valid zip file or is corrupted")
            except Exception as e:
                #raise Exception(f"Error processing zip file: {str(e)}")
                print(f"Error processing zip file: {str(e)}")
            else: 
                print(f"Successfully processed zip file: {filename}")

    except FileNotFoundError as e:
        print(str(e))

    except BadZipFile as e:
        print(str(e))

    except ValueError as e:
        print(str(e))

    except Exception as e:
        print(f"Unexpected error occurred: {str(e)}")


json_processed_files = {
        'i90': {},
        'diario': {},
        'intradiario': {},  
        'rr': {},
        'afrr': {},
        'mfrr': {}
    }

year_path_curated = r"C:\Users\joaquin.costa\Escritorio\Git Repos\up-tracker\data\curated\ESIOS\i90\2024"

year_path_raw = r"C:\Users\joaquin.costa\Escritorio\Git Repos\up-tracker\data\raw\ESIOS\i90\2024"

save_zip_to_parquet(year_path_curated, year_path_raw, "I90DIA_20240613.zip")

