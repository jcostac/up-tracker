import datetime
from datetime import datetime
from datetime import timedelta
import schedule
import time
import time
import logging
import os
import json
import config 
import negocio.funciones_daemon as funciones_daemon
from typing import List, Dict
import pretty_errors
from apscheduler.schedulers.blocking import BlockingScheduler

def setup_logging(config: Dict[str, str]) -> None:
    """Initialize logging.

    Sets up logging configuration based on the provided configuration dictionary.
    This function reads the log filename, level, and format from the config and
    initializes the logging module accordingly. The logs are written to a specified
    file, allowing for detailed tracking of the application's operations and errors.

    Args:
        config (dict): A dictionary containing logging configuration, including
                       'filename', 'level', and 'format' keys.

    """


    log_filepath = config['filedir'] + "\\" + config['filename']
    
    # Check if the log file already exists
    if os.path.exists(log_filepath):
        pass
    else:
        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(log_filepath), exist_ok=True)
        
        # Create an empty log file
        open(log_filepath, 'a').close()


    #initialize logging
    logging.basicConfig(
        filename=log_filepath, #use logging and filename parameters in yml config
        level=getattr(logging, config['level'].upper()), #INFO level included for logs in the log file
        format='%(asctime)s %(name)s %(levelname)s:%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filemode='a'
    )

    logging.info("Logging initialized")

def setup_processed_files_json(config: dict) -> None:
    """Initialize the processed raw files JSON.

    This function creates a JSON file to track processed raw files for each
    indicator specified in the config. If the file already exists, it's left
    untouched. If not, it creates a new file with an empty structure.

    Args:
        config (dict): A dictionary containing the configuration, including
                       the 'processed_files' key with the filename and the
                       list of indicators to track.
    """
    # Get the path for the processed files JSON from the config
    processed_files_json_path = config['filedir'] + "\\" + config['filename'] #i.e. carpeta_daemon_logs + "\\processed_files.json" in config.py

    # Get the structure for the processed files JSON from the config
    structure = config['structure'] #i.e. { 'i90': {},  'diario': {}, 'intradiario': {}, 'rr': {}, 'afrr': {}, 'mfrr': {} }  in config.py

    # Ensure the directory exists, only useful for when daemon log is not initialized first. 
    os.makedirs(os.path.dirname(processed_files_json_path), exist_ok=True)

    # Check if the processed files JSON already exists
    if os.path.exists(processed_files_json_path):
        pass
    else:
        # If it doesn't exist, create it with the initial structure
        with open(processed_files_json_path, 'w') as f:
            json.dump(structure, f, indent=2)
        
        # Log that we've initialized the tracking file
        logging.info(f"Initialized processed raw files tracking at {processed_files_json_path}")

def raw_process():
    print("Proceso RAW....")
    n = 5
    today =  datetime.now().date()
    funciones_daemon.descargador_precios(config.carpeta_raw, (today - timedelta(days=0)).strftime("%Y-%m-%d"), n, None)
    funciones_daemon.descargador_ultimo_i90(config.fichero_config, config.carpeta_raw, (today - timedelta(days=90)).strftime("%Y-%m-%d"), n, None)

def curated_process():
    print("Proceso CURATED....")
    n = 5
    today =  datetime.now().date()    
    funciones_daemon.process_raw_files(config.fichero_config, config.carpeta_raw_dir_lst, (today - timedelta(days=0)).strftime("%Y-%m-%d"), n, None)

def run():
    sched = BlockingScheduler()
    sched.remove_all_jobs()

    sched.add_job(raw_process,'cron', hour=6, minute=0, misfire_grace_time=60)
    sched.add_job(curated_process,'cron', hour=6, minute=30, misfire_grace_time=60)

    print(sched.get_jobs())    
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        pass  # Detiene el scheduler al recibir una interrupción

if __name__ == "__main__":   
    
    setup_logging(config.logging)
    run()
    #curated_process()
    #raw_process()



    

