import requests
import json
from datetime import datetime
from dateutil import tz
from datetime import timedelta
import pytz
import pandas as pd
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()
from datetime import datetime
from lxml import objectify
from lxml import etree
from lxml import objectify, etree
import requests, zipfile
from io import StringIO
from io import BytesIO
from dateutil import tz
import datetime
from datetime import datetime
from datetime import timedelta
import yaml
import io
import glob
import os 
import pretty_errors
import io
from openpyxl import load_workbook

class i90ZIP:

    def __init__(self,fichero_config, carpeta_ficheros_zip_i90):
        """
        Initializes the i90ZIP class.

        Parameters:
        - fichero_config (str): Path to the YAML configuration file.
        - carpeta_ficheros_zip_i90 (str): Path to the directory containing ZIP files.

        Reads the configuration file and stores the parameters.
        """ 
        self.fichero_config = fichero_config #ruta  de entrada del fichero yml 
        self.carpeta_ficheros_zip_i90 = carpeta_ficheros_zip_i90 #ruta donde se guardan las descargas zip

        with open(self.fichero_config, 'r') as cfile:
            #abrir y leer yml file y renombrar como cfile 
            self.params = yaml.load(cfile,Loader=yaml.FullLoader) #self.params contiene los key-value pairs del fichero yml 
            #retrieve the value for the key HOJAS, otherwise, return an empty dicitionary {}
            self.HOJAS =  self.params.get('HOJAS',{}) 
            
    def obtener_numero_periodos(self, date):
        """
        Determines the number of periods for a given date.

        Parameters:
        - date (str): The date to check in the format 'YYYY-MM-DD'.

        Returns:
        - int: The number of periods (either 23, 24, or 25) based on the date.
        """
        out = 24 #define default otuput if the date passed is not a special date
        special_dates = {
            '2014-03-30': 23, '2014-10-26': 25,
            '2015-03-29': 23, '2015-10-25': 25,
            '2016-03-27': 23, '2016-10-30': 25,
            '2017-03-26': 23, '2017-10-29': 25,
            '2018-03-25': 23, '2018-10-28': 25,
            '2019-03-31': 23, '2019-10-27': 25,
            '2020-03-29': 23, '2020-10-25': 25,
            '2021-03-28': 23, '2021-10-31': 25,
            '2022-03-27': 23, '2022-10-30': 25,
            '2023-03-26': 23, '2023-10-29': 25
            }
        return special_dates.get(date, out) #searach for the date in the di ctionary, if the key containing the date is not found, return the default value 

    def descargar_fichero(self, fecha, ruta_destino = None):
        """
        Downloads a ZIP file for the given date and saves it to the specified destination.

        Parameters:
        - fecha (datetime): The date for which the ZIP file is to be downloaded.
        - ruta_destino (str): The destination path where the ZIP file will be saved.

        Constructs the URL based on the date, downloads the file, and saves it with a name
        based on the date.
        """
        try: 
            zip_file_url = "https://api.esios.ree.es/archives/34/download?date_type=datos&end_date={fecha}T23%3A59%3A59%2B00%3A00&locale=es&start_date={fecha}T00%3A00%3A00%2B00%3A00"
            zip_file_url = zip_file_url.replace("{fecha}",fecha.strftime("%Y-%m-%d")) #replacing fecha in the zip url
            print(zip_file_url)
            r = requests.get(zip_file_url, stream=True) #retreiving zip file url contents
            #z = zipfile.ZipFile(BytesIO(r.content))

            if ruta_destino != None:  #si una ruta de salida particular se especifica
                file_name = 'I90DIA_' + fecha.strftime("%Y%m%d") + ".zip" # EJ:"I90DIA_20240601.zip"
                # Construct the full file path
                file_path = os.path.join(ruta_destino, file_name)
                with open(file_path,"wb") as file: #opening a new file to save the zip file url contents 
                    file.write(r.content) #writing the content of the retrieved zip file
                    
            else: #si no, solo regresar datos S
               return r 
        
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while making the HTTP request: {e}")
        except IOError as e:
            print(f"An error occurred while writing the file: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


    def descargar_ficheros(self,  fecha_ini, fecha_fin):
        """
        Downloads ZIP files for each day within a specified date range and saves them to the specified destination.

        Parameters:
        - fecha_ini (datetime): The initial date of the range.
        - fecha_fin (datetime): The final date of the range.
        - ruta_destino (str): The destination path where the ZIP files will be saved.
        """

        # Definir la fecha inicial y la fecha final, if fecha inical is most recent
        if fecha_ini == "Most recent":
            fecha_actual =  datetime.now()
            fecha_inicial = fecha_actual -  timedelta(days=90)
            fecha_inicial.strftime("%Y-%m-%d") #converting object to string
        else: 
            fecha_inicial = datetime.strftime(fecha_ini, "%Y-%m-%d") 


        fecha_final = datetime.strftime(fecha_fin, "%Y-%m-%d") #has to be at least 90 days in the past


        while fecha_inicial <= fecha_final:
            print(fecha_actual) 
            #descargar cada fichero con la fecha defiida por el bucle usando la fucnión descargar fichero anterior  
            self.descargar_fichero(fecha_inicial)
            #al final de cada descarga se le suma un día a la fecha de descarga
            fecha_actual += timedelta(days=1) 

    def leer_fichero_zip(self, hojas, filename, data, lista_uprog = None,):
            """
            This function takes a ZIP file containing an Excel file, extracts the data,
            processes it according to specified configurations, and appends the processed
            data to the provided sheet configurations.

            Parameters:
            - lista_uprog (list): A list of UPROG values to filter the data.
            - hojas (list): A list of dictionaries containing sheet configurations.
            Each dictionary should contain the following keys:
                - 'NOMBRE' (str): The name of the sheet to be parsed.
                - 'START_HEADER' (int): The row number (1-based index) where the headers are located.
                - 'total_columns' (list): A list of all expected column names in the DataFrame.
                - 'valid_columns' (list): A list of columns to be kept for processing.
                - 'index_columns' (list): A list of columns to be used as the index.
                - 'COLUMNAS_APILADAS' (list): A list of new column names after stacking.
                - 'DATA' (list): A list to store the processed DataFrames.
            - filename_zip (str): The name of the ZIP file, used to extract the date.
            - data (bytes): The byte content of the ZIP file.

            Returns:
            - hojas (list): The updated list of dictionaries with processed data appended.
            """            
            try:
                if "I90DIA" in filename:
                    #remove the "I90DIA" and "zip" from the file name
                    #For example, if filename_zip is "I90DIA_20230315.zip", it becomes "2023-03-15"
                    #then we can parse the date, which is in year, month, day format    
                    date = datetime.strptime(filename.replace("I90DIA_","").replace(".xls",""),"%Y%m%d").strftime("%Y-%m-%d") 
                
                elif "I90DAY" in filename: #if file startwith I90DAY
                    date = datetime.strptime(filename.replace("I90DAY_","").replace(".xls",""),"%Y%m%d").strftime("%Y-%m-%d")

                else:
                    raise ValueError (f"Unsupported filename format: {filename}. Expected filename to contain 'I90DIA' or 'I90DAY'.")

            except Exception as e: 
                print("An errror occured while attempting to parse the date from the filename")  


            #obtain number of hours in the given date (this is useful for dates in which daylight saving kicks in)
            n_hours = self.obtener_numero_periodos(date)
            
            # Determine the file format and read accordingly
            if filename.endswith('.xls'):
                # For .xls files
                xl = pd.ExcelFile(io.BytesIO(data), engine='xlrd')
            elif filename.endswith('.xlsx'):
                # For .xlsx files
                xl = pd.ExcelFile(io.BytesIO(data), engine='openpyxl')
            else:                
                raise ValueError(f"Unsupported file format: {filename}")
                    
            index = 0 #begin index at 0 
            for hoja in hojas: #iterate though every hoja of the hojas variable passed (hojas will have the yml file structure)

                #depending on the number of hours of the given date, we are gonna create a list of quarter hourly periods
                if n_hours == 24:
                    list_hours = list(map(str, range(1, 25))) #full set of hours 
                    list_hours_qh = list(map(str, range(1, 97)))
                elif n_hours == 23:
                    list_hours = list(map(str, [1, 2] + list(range(4, 25)))) #we are skipping hour number "3"
                    list_hours_qh = list(map(str, range(1, 93)))
                else:
                    list_hours = list(map(str, [1, 2] + ['3a', '3b'] + list(range(4, 25)))) #here, hour 3 is subdivided into "3a" and "3b"
                    list_hours_qh = list(map(str, range(1, 101)))  #converting quarter hourly string list to integer lsit using map function (same for every n-hours case)
                    
                #parse the sheet with name matching "hoja['NOMBRE']" in the excel file we created from the zip file contents and create a df
                df = xl.parse(hoja['NOMBRE']).reset_index()     

                if len(df) > 0: #proceed with wrangling only if data frame is not empty 

                    #print(len(df))
                    df.columns = df.iloc[hoja['START_HEADER']-1]
                    df = df.iloc[hoja['START_HEADER']:]         
                    #print(str(len(list(df.columns))))
                    num_columnas = len(list(df.columns))
                    
                    if num_columnas > 90:
                        list_hours = list_hours_qh
                    
                    df.columns = hoja['total_columns'] + list_hours                                                        
                    df = df[hoja['valid_columns'] + list_hours]
                    
                    if lista_uprog != None and len(lista_uprog)>0 : #if lista uprog is passed and is not empty
                        df = df[df['UPROG'].isin(lista_uprog)==True]

                    df = df.set_index(hoja['valid_columns'])                
                    df = df.stack().reset_index()
                    df.columns = hoja['valid_columns'] + hoja['COLUMNAS_APILADAS']
                    df['FECHA'] = date
                        
                    data = {'HORA': list_hours}
                    df_left = pd.DataFrame.from_dict(data)
                    df_left['FECHA'] = date
                    
                
                    df = pd.merge(df_left, df, how='left', on=['FECHA', 'HORA'])

                    hojas[index]['DATA'].append(df)

                index = index + 1

                #except Exception as e:
                #    print("ERROR: " + str(hoja['NOMBRE']))
                #    print(e)

                
            return hojas

    def extraer_datos(self, filename, carpeta_salida, carpeta_ficheros_zip, lista_uprog = None):
        """
        Extracts and processes data from ZIP files containing Excel files, then saves the processed data in a PARQUET file.

        This function performs the following steps:
        1. Reads configuration parameters from a YAML file.
        2. Opens a ZIP file for the specified year.
        3. Iterates over each file in the ZIP archive, reads its contents, and processes the data based on the configuration.
        4. Filters and reshapes the data, then appends the processed data to the respective sheet configurations.
        5. Concatenates the processed data for each sheet configuration and saves the result as parquet files in the specified output directory.

        Parameters:
        - lista_uprog (list): A list of UPROG values to filter the data.
        - filename (str): The name of the zip file that will be processed
        - carpeta_salida (str): The output directory where the processed files will be saved.

        Returns:
        - None
        """

        with open(self.fichero_config, 'r') as cfile:
            self.params = yaml.load(cfile,Loader=yaml.FullLoader)
            self.HOJAS =  self.params.get('HOJAS',{})

        #read zipfile
        print(carpeta_ficheros_zip)
        ruta = os.path.join(carpeta_ficheros_zip, filename)
        print(ruta)
        f = zipfile.ZipFile(ruta, "r")
    
        files_in_zip = f.namelist()

        i = 0
        for file in files_in_zip:
            print(file)
            data = f.read(file)            
            if lista_uprog != None:   
                self.HOJAS = self.leer_fichero_zip(self.HOJAS, file, data, lista_uprog=lista_uprog)  
            else:
                self.HOJAS = self.leer_fichero_zip(self.HOJAS, file, data)             
            
            i = i + 1
            #if i >= 1: break
            
                    
        for hoja in self.HOJAS:
            if hoja['DATA']:
                df_final = pd.concat(hoja['DATA'])   
                ruta = os.path.join(carpeta_salida, f"{filename}_{hoja['CONCEPTO']}_temp.parquet")         
                #df_final.to_csv(carpeta_salida + str(annio) + "_" + hoja['CONCEPTO'] + ".csv", index=False, sep=";")
                #df_final.to_parquet(carpeta_salida + filename + "_" + hoja['CONCEPTO'] + "_temp.parquet", index=False,compression='gzip')
                df_final.to_parquet(ruta, index=False,compression='gzip')    
            else: 
                print(f"No data found for {hoja['CONCEPTO']}")

    def unir_datos(self, files, carpeta_salida):
        """
        Combines and consolidates processed data from multiple years into single PARQUET files.

        This function performs the following steps:
        1. Defines a list of specific files to be combined.
        2. For each file, iterates over the list of years, reads the corresponding parquet files, and appends the data to a list.
        3. Concatenates the data for each file type and saves the result as a consolidated parquet file.
        4. Specifically handles program files by adding a 'PROGRAMA' column and consolidating them into a single parquet file.

        Parameters:
        - lista_annios (list): A list of years to process.
        - carpeta_salida (str): The output directory where the combined files will be saved.

        Returns:
        - None
        """

        print("Uniendo datos...")
        FICHEROS = ["PRE_RES_MD","PRE_RR","PRE_TER_DES_TR","PROG_GES_DESV","PROG_RR","PROG_SEC","PROG_TERC","RESULT_RES","RESULT_TIEMPO_REAL"]
        for fichero in FICHEROS:
            frames = []
            ruta_final = os.path.join(carpeta_salida, f"{str(fichero)}.parquet")
            
            # Check if the final parquet file already exists
            if os.path.exists(ruta_final):
                existing_df = pd.read_parquet(ruta_final)
                frames.append(existing_df)
            
            for filename in files:
                ruta_temp = os.path.join(carpeta_salida, f"{filename}_{str(fichero)}_temp.parquet")
                if os.path.exists(ruta_temp):
                    df = pd.read_parquet(ruta_temp)
                    frames.append(df)
                else:
                    print(f"File not found: {ruta_temp}")
            
            if frames:
                df_fichero = pd.concat(frames, ignore_index=True)
                df_fichero = df_fichero.drop_duplicates()  # Remove any potential duplicates
                df_fichero.to_parquet(ruta_final, index=False, compression='gzip')
            else:
                print(f"No data to combine for {fichero}")

        FICHEROS = ["PROG_PBF","PROG_PVP","PROG_PHF1","PROG_PHF2","PROG_PHF3","PROG_PHF4","PROG_PHF5","PROG_PHF6","PROG_PHF7"]
        frames = []
        ruta_final = os.path.join(carpeta_salida, "PROGRAMAS.parquet")
        
        # Check if the PROGRAMAS.parquet file already exists
        if os.path.exists(ruta_final):
            existing_df = pd.read_parquet(ruta_final)
            frames.append(existing_df)
        
        for fichero in FICHEROS:
            for filename in files:
                ruta_temp = os.path.join(carpeta_salida, f"{filename}_{str(fichero)}_temp.parquet")
                if os.path.exists(ruta_temp):
                    df = pd.read_parquet(ruta_temp)
                    df['PROGRAMA'] = fichero.replace("PROG_","")
                    frames.append(df)
                else:
                    print(f"File not found: {ruta_temp}")
        
        if frames:
            df_fichero = pd.concat(frames, ignore_index=True)
            df_fichero = df_fichero.drop_duplicates()  # Remove any potential duplicates
            df_fichero.to_parquet(ruta_final, index=False, compression='gzip')
        else:
            print("No data to combine for PROGRAMAS")

        FICHEROS = ["PROG_P48"]
        frames = []
        ruta_final = os.path.join(carpeta_salida, "P48.parquet")
        
        # Check if the P48.parquet file already exists
        if os.path.exists(ruta_final):
            existing_df = pd.read_parquet(ruta_final)
            frames.append(existing_df)
        
        for fichero in FICHEROS:
            #print(fichero)
            for filename in files:
                ruta_temp = os.path.join(carpeta_salida, f"{filename}_{str(fichero)}_temp.parquet")
                if os.path.exists(ruta_temp):
                    df = pd.read_parquet(ruta_temp)
                    df['PROGRAMA'] = fichero.replace("PROG_","")
                    frames.append(df)
                else:
                    print(f"File not found: {ruta_temp}")
        
        if frames:
            df_fichero = pd.concat(frames, ignore_index=True)
            df_fichero = df_fichero.drop_duplicates()  # Remove any potential duplicates
            df_fichero.to_parquet(ruta_final, index=False, compression='gzip')
        else:
            print("No data to combine for P48")

    def borrar_ficheros_temporales(self, carpeta_salida): 
        file_lst = os.listdir(carpeta_salida)
        for file in file_lst:
            if file.endswith("_temp.parquet"):
                 # Construct the full path to the file
                file_path = os.path.join(carpeta_salida, os.path.basename(file))
                # Remove the file
                os.remove(file_path)

    def generar_ficheros(self, files, carpeta_salida, carpeta_ficheros_zip, lista_uprog = None): #lista de UPs opcional 
        for filename in files:
            if lista_uprog == None:
                self.extraer_datos(filename, carpeta_salida, carpeta_ficheros_zip)
            else: 
                self.extraer_datos(filename, carpeta_salida, carpeta_ficheros_zip, lista_uprog)

        self.unir_datos(files,carpeta_salida)
        self.borrar_ficheros_temporales(carpeta_ficheros_zip) #(carpeta_salida)


if __name__ == '__main__':

    fichero_config =  "C:\\Users\\joaquin.costa\\Escritorio\\Git Repos\\up-tracker\\code\\back\\utilidades\\config.yml" #ruta entrada ficher yml 
    carpeta_ficheros_zip_i90 = "C:\\Users\\joaquin.costa\\Escritorio\\Git Repos\\up-tracker\\data\\raw\\ESIOS\\i90" #ruta donde se guardan los zip 
    carpeta_salida = "C:\\Users\\joaquin.costa\\Escritorio\\Git Repos\\up-tracker\\data\\curated\\ESIOS\\i90\\2024"
    carpeta_entrada = "C:\\Users\\joaquin.costa\\Escritorio\\Git Repos\\up-tracker\\data\\raw\\ESIOS\\i90\\2024"
    #lista_uprog = [] #["AGUG","AGUB"] #if list is empty, select all UPS
    #carpeta_salida = r"C:\Users\joaquin.costa\Escritorio\UP Tacker\curated" +  str(year) + r"\\" #carpeta salida fichero parquet

    obj = i90ZIP(fichero_config, carpeta_ficheros_zip_i90)
    #obj.generar_ficheros(["I90DIA_20240613.zip"], carpeta_salida, carpeta_entrada)
    #print(obj)

    obj.generar_ficheros(["I90DIA_20240614.zip"], carpeta_salida, carpeta_entrada)
    #print(obj)
    fichero = obj.descargar_fichero()
    #obj.generar_ficheros([year], carpeta_salida)
    print("------------")

