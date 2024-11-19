import os

carpeta_actual = os.getcwd()

carpeta_data_lake = carpeta_actual + "\\..\\..\\data"
carpeta_raw = carpeta_data_lake + "\\raw"
carpeta_curated = carpeta_data_lake + "\\curated"
carpeta_daemon_logs = carpeta_data_lake + "\\daemon"

logging = {
    'filename': carpeta_daemon_logs + "\\daemon_descarga_logs.log", #ruta para el log del demonio
    'level': 'INFO', #nivel de mensajes que van a ser loggeados 
    'format': '%(asctime)s %(name)s %(levelname)s:%(message)s'
}

processed_files_log = {
    'filename': carpeta_daemon_logs + "\\processed_files.json", #ruta para el archivo JSON de archivos procesados
    'structure': {
        'i90': {},
        'prices': {
            'diario': {},
            'intradiario': {},
            'rr': {},
            'afrr': {},
            'mfrr': {}
        }
    }
}


#configuration and out  directories that need to be passed to main daemon function
fichero_config = carpeta_actual + "\\utilidades\\config.yml"

#RR
raw_rr = carpeta_raw + "\\ESIOS\\RR"
#carpeta_curated_rr = carpeta_curated + "\\ESIOS\\RR"

#AFRR
raw_afrr = carpeta_raw + "\\ESIOS\\AFRR"
#carpeta_curated_afrr = carpeta_curated + "\\ESIOS\\AFRR"

#MFRR
raw_mfrr = carpeta_raw + "\\ESIOS\\MFRR"
#carpeta_curated_mfrr = carpeta_curated + "\\ESIOS\\MFRR"

#DIARIO
raw_diario = carpeta_raw + "\\OMIE\\DIARIO"
#carpeta_curated_diario = carpeta_curated + "\\OMIE\\DIARIO"

#INTRADIARIO
raw_intradiario = carpeta_raw + "\\OMIE\\INTRADIARIO"
#carpeta_curated_intradiario = carpeta_curated + "\\OMIE\\INTRADIARIO"

#i90
raw_i90 = carpeta_raw + "\\ESIOS\\i90"
#carpeta_curated_i90 = carpeta_curated + "\\ESIOS\\i90"

#creando lista de directorios de cada indicador
raw_dir_lst = [raw_diario, raw_intradiario, raw_rr, raw_afrr, raw_mfrr, raw_i90] 


