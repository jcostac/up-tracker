import os


carpeta_data_lake = "E:\\clientes\\simulart\\UPTracker\\data"
fichero_config = "E:\\clientes\\simulart\\UPTracker\\code\\back\\utilidades\\config.yml"



carpeta_raw = carpeta_data_lake + "\\raw"
carpeta_curated = carpeta_data_lake + "\\curated"
carpeta_logs = carpeta_data_lake + "\\logs"

logging = {
    'filename': 'daemon_raw_curated_logs.log',
    'filedir': carpeta_logs, #ruta para el log del demonio
    'level': 'INFO', #nivel de mensajes que van a ser loggeados 
    'format': '%(asctime)s %(name)s %(levelname)s:%(message)s'
}   

processed_files_log = {
    'filename':  "processed_files.json", #ruta para el archivo JSON de archivos procesados
    'filedir': carpeta_logs,
    'structure': {
        'i90': {},
        'diario': {},
        'intradiario': {},  
        'rr': {},
        'afrr': {},
        'mfrr': {}
    }
}


#RR
raw_rr = carpeta_raw + "\\OMIE\\Rr"
curated_rr = carpeta_curated + "\\OMIE\\Rr"

#AFRR
raw_afrr = carpeta_raw + "\\OMIE\\Afrr"
curated_afrr = carpeta_curated + "\\OMIE\\Afrr"

#MFRR
raw_mfrr = carpeta_raw + "\\OMIE\\Mfrr"
curated_mfrr = carpeta_curated + "\\OMIE\\Mfrr"

#DIARIO
raw_diario = carpeta_raw + "\\OMIE\\Diario"
curated_diario = carpeta_curated + "\\OMIE\\DIARIO"

#INTRADIARIO
raw_intradiario = carpeta_raw + "\\OMIE\\Intradiario"
curated_intradiario = carpeta_curated + "\\OMIE\\INTRADIARIO"

#i90
raw_i90 = carpeta_raw + "\\ESIOS\\i90"
curated_i90 = carpeta_curated + "\\ESIOS\\i90"

#creando lista de directorios de cada indicador
carpeta_raw_dir_lst = [raw_i90, raw_diario, raw_intradiario, raw_rr, raw_afrr, raw_mfrr] 