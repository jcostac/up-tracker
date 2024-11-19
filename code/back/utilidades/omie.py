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
import io
import ssl

ssl._create_default_https_context = ssl._create_unverified_context



class OMIE:
    def __init__(self):
        pass

    def utc_to_local(self, utc_dt):
        local_tz = pytz.timezone('Europe/Madrid')
        local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
        return local_tz.normalize(local_dt)

    def download_precio_diario(self, start_date, end_date):

        frames = []

        delta = timedelta(days=1)
        fecha = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_d = datetime.strptime(end_date, "%Y-%m-%d")


        while fecha <= end_date_d:

            try:

                yyyy = fecha.year
                mm = fecha.month
                dd = fecha.day

                if mm < 10 : mm = "0" + str(mm)
                if dd < 10 : dd = "0" + str(dd)

                url = "https://www.omie.es/informes_mercado/AGNO_" + str(yyyy) + "/MES_" + str(mm) + "/TXT/INT_PBC_EV_H_1_" + str(dd) + "_" + str(mm) + "_" + str(yyyy) + "_" + str(dd) + "_" + str(mm) + "_" + str(yyyy) + ".txt"

                print(url)

                #df = pd.read_csv(url, sep=";", encoding="latin1", skiprows=2)
                resp = requests.get(url)
                csv = resp.content.decode("latin1")
                df_raw = pd.read_csv(io.StringIO(csv), sep=";",skiprows=2)

                df = pd.DataFrame(df_raw.iloc[0])
                df.columns = ['PRECIO']
                df = df.iloc[1:]

                df['FECHA'] = fecha.strftime("%Y-%m-%d")
                df['PERIODO'] = range(1,26)

                df['PRECIO'] = df['PRECIO'].apply(lambda x: float(str(x).replace(",",".")))
                df = df.dropna()

                frames.append(df)

            except Exception as e:
                print(str(e))


            fecha = fecha + delta

        if len(frames)>1:
            df = pd.concat(frames) #concat df in df list
        else: 
            df = frames[0] #return the single element which was appended
        
        return df




    def download_precio_intradiario(self, start_date, end_date):


        frames = []
        fecha_corte = datetime(2024, 7, 14) #ultimo dia  de 7 sesiones intradiarias

        delta = timedelta(days=1)
        fecha = datetime.strptime(start_date, "%Y-%m-%d")
        
        end_date_d = datetime.strptime(end_date, "%Y-%m-%d")



        while fecha <= end_date_d:
            if fecha < fecha_corte: 
                for sesion_input in [1,2,3,4,5,6,7]:

                    if sesion_input == 7:
                        yyyy = (fecha + timedelta(days=1)).year
                        mm = (fecha + timedelta(days=1)).month
                        dd = (fecha + timedelta(days=1)).day
                        sesion = 2
                    else:
                        sesion = sesion_input
                        yyyy = fecha.year
                        mm = fecha.month
                        dd = fecha.day

                    if mm < 10 : mm = "0" + str(mm)
                    if dd < 10 : dd = "0" + str(dd)

                    try:
                        url = "https://www.omie.es//informes_mercado/AGNO_" + str(yyyy) + "/MES_" + str(mm) + "/TXT/INT_PIB_EV_H_1_" + str(sesion) + "_" + str(dd) + "_" + str(mm) + "_" + str(yyyy) + "_" + str(dd) + "_" + str(mm) + "_" + str(yyyy) + ".txt"

                        #df = pd.read_csv(url, sep=";", encoding="latin1", skiprows=2)
                        resp = requests.get(url)
                        csv = resp.content.decode("latin1")
                        df_raw = pd.read_csv(io.StringIO(csv), sep=";",skiprows=2)

                        df = pd.DataFrame(df_raw.iloc[0])
                        df.columns = ['PRECIO']
                        if sesion_input < 7: #if session is not 7 
                            df = df.iloc[5:] #horas del dia actual 
                            df['FECHA'] = fecha.strftime("%Y-%m-%d")
                            df['PERIODO'] = range(1,26)
                            df['PRECIO'] = df['PRECIO'].apply(lambda x: float(str(x).replace(",",".")))
                            df['SESION'] = sesion_input
                        else:
                            df = df.iloc[1:5] #horas del dia anterior si la sesion es 7 
                            df['FECHA'] = fecha.strftime("%Y-%m-%d")
                            df['PERIODO'] = range(21,25)
                            df['PRECIO'] = df['PRECIO'].apply(lambda x: float(str(x).replace(",",".")))
                            df['SESION'] = sesion_input

                        df = df.dropna()

                        frames.append(df)
                    except Exception as e:
                        print(str(e))

                fecha = fecha + delta

            else: 

                for sesion_input in [1,2,3]: 

                    sesion = sesion_input
                    yyyy = fecha.year
                    mm = fecha.month
                    dd = fecha.day

                    if mm < 10 : mm = "0" + str(mm)
                    if dd < 10 : dd = "0" + str(dd)

                    try:
                        url = "https://www.omie.es/sites/default/files/dados/AGNO_" + str(yyyy) + "/MES_" + str(mm) + "/TXT/INT_IDA_PIB_EV_H_1_" + str(sesion) + "_" + str(dd) + "_" + str(mm) + "_" + str(yyyy) + "_" + str(dd) + "_" + str(mm) + "_" + str(yyyy) + ".txt"

                        print(url)
                        #df = pd.read_csv(url, sep=";", encoding="latin1", skiprows=2)
                        resp = requests.get(url)
                        csv = resp.content.decode("latin1")
                        df_raw = pd.read_csv(io.StringIO(csv), sep=";",skiprows=2)

                        #data wrangling

                        df = df_raw.drop(columns=[df_raw.columns[0], df_raw.columns[-1]]) #dropping first and last column
                        df = pd.DataFrame(df.iloc[0]) #primera fila, serie de horas 1-24, que luego se trasnforma a df
                        df.columns = ['PRECIO'] #crear nueva columna de precio que está vacía 
                        df['FECHA'] = fecha.strftime("%Y-%m-%d") #fecha a string
                        df['PERIODO'] = range(1,25) #añadir columna de periodo
                        df['PRECIO'] = df['PRECIO'].apply(lambda x: float(str(x).replace(",","."))) #añadir precio
                        df['SESION'] = sesion_input #populate sesion input

                        df = df.dropna()

                        frames.append(df)

                    except Exception as e:
                        print(str(e))

                fecha = fecha + delta
        
        df = pd.concat(frames)


        return df


if __name__ == '__main__':


    obj = OMIE()

    start_date = "2024-08-06"
    end_date = "2024-08-06"
    df = obj.download_precio_diario(start_date, end_date)
    #df2 = obj.download_precio_intradiario(start_date, end_date)

    #print("#DIARIO#")
    print(df)

   # print("#INTRADIARIO#")
   # print(df2)

    


