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
import os 
#import io

class ESIOS:
    """
    The ESIOS class interacts with the ESIOS API to fetch energy market data,
    processes it, and stores it in a database.
    
    Attributes:
        token (str): API token for authenticating requests to the ESIOS API.
        db (str): Database connection string.
        ruta (str): Path to save files.
        engine (sqlalchemy.engine.Engine): SQLAlchemy engine for database interactions.
    """
    def __init__(self):
        self.token = "df72d87995436ae960ef8e319af2da024aad7c81450d405752dc45199b020cf6"
        self.db = "mysql+mysqldb://pedro:invesyde@192.168.32.9:3306/prevision_desvios"

        try:
            #self.ruta = "\\\\urki\\proyectos\\INVESYDE\\FicherosP48\\"
            self.ruta = "E:\\clientes\\simulyde\\Morfeo\\data\\FicherosP48\\"
            self.ruta_log = "C:\\Scripts\\descargadores\\P48CIERRE\\"
            
            if self.ruta ==  "E:\\" or  self.ruta_log == "E:\\":
                raise ValueError("self.ruta or self.ruta_log cannot be E:\\ ")
            
        except Exception as e:
             print(f"Error initializing ESIOS: {e}")

    def hora_a_periodo(self, fecha_inicial, fecha, hora):
        """
        Converts a given time into a specific period based on a 15-minute interval.

        This function takes an initial date, a current date, and a time in 'hh:mm' format.
        It calculates the corresponding period for the given time based on the following rules:
        - Each hour is divided into 4 periods of 15 minutes each.
        - The function ensures that the time is valid and falls within the allowed ranges.

        Args:
        fecha_inicial (str): The initial date in 'yyyy-mm-dd' format. This date is used to determine 
                            whether the current date falls before or after it.
        fecha (str): The current date in 'yyyy-mm-dd' format. This date is compared with the initial date.
        hora (str): The time in 'hh:mm' format. The minutes must be one of [0, 15, 30, 45].

        Returns:
        int or str: The calculated period as an integer, or an error message as a string if the input is invalid.

        Raises:
        ValueError: If the minutes are not one of [0, 15, 30, 45] or if the hours are not in the range 0-23.
        IndexError: If the time format is incorrect or missing parts.
        """
        
        if fecha >= fecha_inicial: 
            try:
                partes = hora.split(':')
                horas = int(partes[0])
                minutos = int(partes[1])

                if minutos not in [0, 15, 30, 45]:
                    raise ValueError("Los minutos deben ser 0, 15, 30 o 45.")

                if horas < 0 or horas > 23:
                    raise ValueError("Las horas deben estar entre 0 y 23.")

                periodos_por_hora = horas * 4  # Cada hora tiene 4 periodos (15 minutos cada uno)
                periodos_por_minuto = minutos // 15  # Cantidad de periodos en los minutos

                total_periodos = periodos_por_hora + periodos_por_minuto + 1  # Sumar 1 para empezar desde 1
                
                return total_periodos

            except (ValueError, IndexError) as e:
                return f"Error: {e}. Por favor, introduce una hora válida en formato 'hh:mm'."

        else:
            partes = hora.split(':')
            horas = int(partes[0])
            return horas + 1


    def hora_periodo(self, fecha_inicial, fecha):
        
        if fecha >= fecha_inicial:
            return 0.25
        else:
            return 1

    def utc_to_local(self, utc_dt):
        """
        Converts a UTC datetime to local Madrid time.

        Args:
        utc_dt (datetime): The datetime in UTC to be converted.

        Returns:
        datetime: The datetime converted to local Madrid time.
        """
        local_tz = pytz.timezone('Europe/Madrid')
        local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
        return local_tz.normalize(local_dt)


    def download_precios_secundaria(self, start_date, end_date):
        """
        Downloads price balance data from the ESIOS API for a specified date range.

        Args:
        start_date (str): The start date for the data retrieval in 'yyyy-mm-dd' format.
        end_date (str): The end date for the data retrieval in 'yyyy-mm-dd' format.
        upsert (bool): If True, upserts data into the database (default is True).

        Returns:
        pd.DataFrame: A pandas DataFrame containing the retrieved data.
        """

        registros = []
        for indicador in [634]:

            url = 'https://api.esios.ree.es/indicators/' + str(indicador) + '?start_date=' + start_date + '&end_date=' + end_date
            headers = {}
            headers['x-api-key'] = self.token
            r = requests.get(url, headers=headers)
            datos = json.loads(r.text)
            for d in datos['indicator']['values']:
                rec = {}
                fecha_utc = datetime.strptime(d['datetime_utc'],"%Y-%m-%dT%H:%M:%SZ")
                fecha_local = self.utc_to_local(fecha_utc)
                rec['FECHA'] = fecha_local.strftime("%Y-%m-%d")
                rec['PERIODO'] = int(fecha_local.strftime("%H")) + 1
                rec['PRECIO'] = d['value']
                rec['HORA_PERIODO'] = 1
                registros.append(rec)

        df = pd.DataFrame(registros)

        return df



    def download_precios_terciaria(self, start_date, end_date, upsert = True):
        """
        Downloads price balance data from the ESIOS API for a specified date range.
        Indicador 676: Precio marginal regulación terciaria a bajar de activación programada (AP)
        Indicador 677: Precio marginal regulación terciaria a subir de activación programada (AP)
        Args:
        start_date (str): The start date for the data retrieval in 'yyyy-mm-dd' format.
        end_date (str): The end date for the data retrieval in 'yyyy-mm-dd' format.
        upsert (bool): If True, upserts data into the database (default is True).

        Returns:
        pd.DataFrame: A pandas DataFrame containing the retrieved data.
        """
        registros = []
        for indicador in [677,676]:
            if indicador == 676:
                sentido = "Bajar"
            if indicador == 677:
                sentido = "Subir"

            url = 'https://api.esios.ree.es/indicators/' + str(indicador) + '?start_date=' + start_date + '&end_date=' + end_date
            headers = {}
            headers['x-api-key'] = self.token
            r = requests.get(url, headers=headers)
            datos = json.loads(r.text)
            print(f"datos: {datos['indicator']['values']}")
            for d in datos['indicator']['values']:
                try:
                    rec = {}
                    fecha_utc = datetime.strptime(d['datetime_utc'],"%Y-%m-%dT%H:%M:%SZ")
                    fecha_local = self.utc_to_local(fecha_utc)
                    rec['FECHA'] = fecha_local.strftime("%Y-%m-%d")
                    rec['PERIODO'] = self.hora_a_periodo(fecha_local.strftime("%Y-%m-%d"),fecha_local.strftime("%H:%M"), "00:00") #int(fecha_local.strftime("%H")) + 1
                    rec['SENTIDO'] = sentido
                    rec['PRECIO'] = d['value']
                    rec['HORA_PERIODO'] = self.hora_periodo(fecha_local.strftime("%Y-%m-%d"), rec['FECHA'])
                    registros.append(rec)
                except Exception as error:
                    print(error)

        df = pd.DataFrame(registros)
        df = df.fillna(0)


        return df


    def download_precios_terciaria_media_ponderada(self, start_date, end_date, upsert = True):
        """
        Downloads price balance data from the ESIOS API for a specified date range.
        Indicador 10387: Precio medio ponderado regulación terciaria a bajar 
        Indicador 10386: Precio medio ponderado regulación terciaria a subir 

        Args:
        start_date (str): The start date for the data retrieval in 'yyyy-mm-dd' format.
        end_date (str): The end date for the data retrieval in 'yyyy-mm-dd' format.
        upsert (bool): If True, upserts data into the database (default is True).

        Returns:
        pd.DataFrame: A pandas DataFrame containing the retrieved data.
        """

        registros = []
        for indicador in [10387, 10386]:
            if indicador == 10387:
                sentido = "Bajar"
            if indicador == 10386:
                sentido = "Subir"

            url = 'https://api.esios.ree.es/indicators/' + str(indicador) + '?start_date=' + start_date + '&end_date=' + end_date
            headers = {}
            headers['x-api-key'] = self.token
            r = requests.get(url, headers=headers)
            datos = json.loads(r.text)
            
            print(f"Indicator: {indicador}, Sentido: {sentido}")
            print(f"Number of values: {len(datos['indicator']['values'])}")
            print(f"First value: {datos['indicator']['values'][0]}")
            
            for d in datos['indicator']['values']:
                try:
                    rec = {}
                    fecha_utc = datetime.strptime(d['datetime_utc'],"%Y-%m-%dT%H:%M:%SZ")
                    fecha_local = self.utc_to_local(fecha_utc)
                    rec['FECHA'] = fecha_local.strftime("%Y-%m-%d")
                    rec['HORA'] = self.hora_a_periodo(fecha_local.strftime("%Y-%m-%d"),fecha_local.strftime("%H:%M"), "00:00") #int(fecha_local.strftime("%H")) + 1
                    rec['SENTIDO'] = sentido
                    rec['PRECIO'] = d['value']
                    rec['HORA_PERIODO'] = self.hora_periodo(fecha_local.strftime("%Y-%m-%d"), rec['FECHA'])
                    registros.append(rec)
                    
                    #if len(registros) % 100 == 0:  # Print every 100th record
                       #print(f"Sample record: {rec}")
                    
                except Exception as error:
                    print(f"Error processing record: {error}")

        print(f"Total records collected: {len(registros)}")
        
        df = pd.DataFrame(registros)
        df = df.fillna(0)
        
        print(f"DataFrame shape: {df.shape}")
        print(f"DataFrame columns: {df.columns}")
        print(f"DataFrame head:\n{df.head()}")
        
        return df


    def download_precios_balance_rr(self, start_date, end_date, upsert = True):
        """
        Downloads price balance data from the ESIOS API for a specified date range.

        Args:
        start_date (str): The start date for the data retrieval in 'yyyy-mm-dd' format.
        end_date (str): The end date for the data retrieval in 'yyyy-mm-dd' format.
        upsert (bool): If True, upserts data into the database (default is True).

        Returns:
        pd.DataFrame: A pandas DataFrame containing the retrieved data.
        """

        fecha_inicio_qh = "2020-01-01"

        registros = []
        for indicador in [1782]:

            url = 'https://api.esios.ree.es/indicators/' + str(indicador) + '?start_date=' + start_date + '&end_date=' + end_date
            headers = {}
            headers['x-api-key'] = self.token
            r = requests.get(url, headers=headers)
            datos = json.loads(r.text)       
            for d in datos['indicator']['values']:
                if d['geo_id'] == 3: #España
                    try:
                        rec = {}
                        fecha_utc = datetime.strptime(d['datetime_utc'],"%Y-%m-%dT%H:%M:%SZ")
                        fecha_local = self.utc_to_local(fecha_utc)
                        rec['FECHA'] = fecha_local.strftime("%Y-%m-%d")
                        rec['PERIODO'] = self.hora_a_periodo(fecha_inicio_qh, fecha_local.strftime("%Y-%m-%d"),fecha_local.strftime("%H:%M")) #int(fecha_local.strftime("%H")) + 1
                        rec['PRECIO'] = d['value']
                        #rec['HORA_PERIODO'] = self.hora_periodo(fecha_inicio_qh, fecha_local.strftime("%Y-%m-%d"))
                        registros.append(rec)
                    except Exception as error:
                        print(error)

        df = pd.DataFrame(registros)
        df = df.fillna(0)
        
        #df = df.groupby(["FECHA","PERIODO"]).mean("PRECIO")
        #df = df.reset_index()

        return df




if __name__ == '__main__':
 

    #start_date = "2023-01-01"
    #end_date = "2023-01-02"

    start_date = "2024-10-10"
    end_date = "2024-10-15"


    obj = ESIOS()
    #df_rr = obj.download_precios_balance_rr(start_date, end_date)
    df_mfrr = obj.download_precios_terciaria(start_date, end_date)
    #df_afrr = obj.download_precios_secundaria(start_date, end_date)
    df_mfrr_media = obj.download_precios_terciaria_media_ponderada(start_date, end_date)

    #print(f"df_rr: {df_rr}")
    #print(f"df_afrr: {df_afrr}")
    #print(f"df_mfrr_media: {df_mfrr_media}")
    print(f"df_mfrr: {df_mfrr}")