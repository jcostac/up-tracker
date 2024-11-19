# UPTracker Daemon

This daemon system is designed to automatically download and process market data on a daily basis. It consists of several Python scripts that work together to fetch, store, and process data from various energy markets.

## General Main Components:

The system is composed of the following main components:

1. `config.py`: Contains configuration settings and paths.
2. `daemon.py`: The main daemon script that schedules and runs the data collection and processing tasks. The daemon has a raw data collection process and a curated data processing process ocurring each day.
3. `funciones_daemon.py`: Contains the core functions for downloading and processing data which are called by the `daemon.py` script.
4. `config.yml`: Configuration file for I90 data processing.

## General Key Features

- Daily automatic download of I90 and market price data
- Processing of raw data into curated formats
- Logging of operations and processed files
- Configurable scheduling of tasks

## Daemon Process Overview

The daemon operates in two main phases: raw data download and curated data processing.


### Raw Data Download

This phase runs at `raw_daemon_time` (default: "8:15") and uses the following functions from `funciones_daemon.py`:

1. `descargador_ultimo_i90(fichero_config, carpeta_raw, dl_dates_lst)`: 
   - Downloads the latest I90 data
   - Parameters:
     - `fichero_config`: Path to the configuration file
     - `carpeta_raw`: Path to store raw data
     - `dl_dates_lst`: List of dates to download

2. `descargador_precios(carpeta_raw, dl_dates_dct)`:
   - Downloads price data for various markets
   - Parameters:
     - `carpeta_raw`: Path to store raw data
     - `dl_dates_dct`: Dictionary of dates to download for each market


### Key Helper Functions for Raw Data Download

Several helper functions support the main processes of downloading raw data:

1. `descargador_ultimo_i90(fichero_config, carpeta_raw, dl_dates)`: 
   - Downloads the latest I90 data
   - Uses the `i90ZIP` class to handle the download
   - Manages directory creation and error handling

2. `descargador_precios(carpeta_raw, dl_dates_dct)`:
   - Downloads price data for various markets (RR, AFRR, MFRR, Diario, Intradiario)
   - Uses `ESIOS` and `OMIE` classes to handle downloads for different markets
   - Manages directory creation and error handling for each market type


These functions are called within the raw data download phase of the daemon process to fetch the latest data from various sources and prepare it for further processing.



### Curated Data Processing

This phase runs at `curated_daemon_time` (default: "23:59") and uses the following function:

1. `process_raw_files(raw_dir)`:
   - Processes raw files for a given indicator
   - Parameter:
     - `raw_dir`: Path to the raw files for a specific indicator

This function is called for each directory in `raw_dir_lst`, which typically includes paths for different types of data (e.g., I90, price data for various markets).

### Key Helper Functions for Curated Data Processing

Several helper functions support the main processes of curating raw data (i.e. raw to curated processing):

1. `save_csv_to_parquet(parquet_filepath, raw_csv_filepath)`: Converts CSV files to Parquet format
2. `save_zip_to_parquet(month_path_curated, month_path_raw, filename)`: Processes zip files (typically I90 data) to Parquet format
3. `check_is_processed(filename, json_processed_files)`: Checks if a file has already been processed
4. `update_json_processed_files(filename, json_processed_files)`: Updates the record of processed files
5. `load_json_processed_files()`:
   - Loads the JSON file that keeps track of processed files
   - Creates the file with a predefined initial structure if it doesn't exist

6. `save_json_processed_files(json_processed_files)`:
   - Saves the updated JSON of processed files back to disk

These functions are called within `process_raw_files` function to handle different file types and maintain processing records.


## Configuration

### Important Configurable Variables

1. In `config.py`:
   - `carpeta_data_lake`: Base path for the data lake for all curated data.
   - `carpeta_raw`: Path for raw data storage
   - `carpeta_curated`: Path for curated data storage
   - `carpeta_daemon_logs`: Path to store daemon logs
   - `logging`: Configuration for logging (filename, level, format) to be used in the daemon logs. Should not be modified unless the logging format changes.
   - `processed_files_log`: Configuration for tracking processed files. Should not be modified unless the processed files format changes.

2. In `daemon.py`:
   - `raw_daemon_time`: Time to run the raw data collection process (default: "8:15")
   - `curated_daemon_time`: Time to run the curated data processing (default: "23:59")

3. In `funciones_daemon.py`:
   - Ensure that the raw paths in download functions like `descargador_ultimo_i90` and `descargador_precios` are correctly specified in the `config.yml` file.

4. In `config.yml`:
   - Configuration of  the settings for I90 data processing, including sheet names, columns, and date ranges. This should not be modified unless the data format changes.

## Setup and Execution

1. Ensure all required Python packages are installed (schedule, pandas, etc.).
2. Set up the directory structure as defined in `config.py`.
3. Configure the variables mentioned above to match your system and requirements.
4. Run `daemon.py` to start the daemon:

   ```
   python daemon.py
   ```

The daemon will automatically run at the specified times each day, downloading new data and processing it according to the configuration.

## Logging and Monitoring

- Check `daemon_descarga_logs.log` for operation logs and any errors.
- The `processed_files.json` keeps track of which files have been processed from raw to curated to avoid redundancy (processing the same file more than once).

## Customization

- To add new data sources or modify existing ones, update the relevant functions in `funciones_daemon.py`.
- Adjust the scheduling in `daemon.py` if you need different run times or frequencies.
- To change folder paths for data download, processing, and logs, modify the variables in the `config.py` file.

## Troubleshooting

- If the daemon isn't running as expected, check the log files for error messages.
- Ensure all paths in the configuration files are correct and accessible.
- Verify that the system has the necessary permissions to read/write in the specified directories.

For any further questions or issues, please contact the system administrator.