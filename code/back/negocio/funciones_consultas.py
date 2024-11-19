import pandas as pd 
import duckdb 
from typing import List, Optional, Union
from datetime import datetime, timedelta
import os
import config_consultas as configc
from bisect import bisect_left
import pretty_errors
import re

class Indicador:
    def __init__(self, indicador_type: str):
        self.indicador_type = indicador_type
        print("Indicador initialized with indicador type: ", self.indicador_type)

    @staticmethod
    def years_between(start_date:str, end_date:str)-> List[str]:
        # Convert string dates to datetime objects if they aren't already
        if not isinstance(start_date, datetime): #object type check
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if not isinstance(end_date, datetime): #object type check
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Extract years
        start_year = start_date.year
        end_year = end_date.year
        
        # Generate list of years
        years_lst = list(range(start_year, end_year + 1)) 
        years_lst = [str(year) for year in years_lst] #convert datetime to string

        return years_lst
    
    @staticmethod
    def binary_search(arr: List[str], x: str) -> bool:
        """
        This function performs a binary search on a sorted list of strings to check if a value is present.
        Args:
            arr (List[str]): A sorted list of strings to search within.
            x (str): The value to search for in the list.
        Returns:
            bool: True if the value is found in the list, False otherwise.
        """
        pos = bisect_left(arr, x) 
        if pos != len(arr) and arr[pos] == x: #if position is the len of the array, it means the value is not in the array (position will always range from 0 to len(arr)-1)
            print(f"Value {x} found in the dataset")
            return True
        else:
            print(f"Value {x} NOT found in the dataset")
            return False

    @staticmethod
    def check_consulta_type(consulta_type: str) -> str:
        """
        This function checks if the consulta_type is valid. And return the corresponding table and path_str needed to execute the query
        Args: 
            program_type (str): The program_type to check.
        Returns: 
            table (str): The table to use in the query.
            path_str (str): The path_str to use in the query.
        """
        if consulta_type == "prog":
            table = "prog"
            path_str = "path_prog"

        elif consulta_type == "prc":
            table = "prc"
            path_str = "path_prc"
        else:
            raise ValueError(f"Invalid consulta_type: {consulta_type}. Must be 'prog' or 'prc'.")

        return table, path_str
    
    @staticmethod
    def filtrar_columnas(df: pd.DataFrame, consulta_type: str, indicador_type: str) -> pd.DataFrame:
        """
        This function filters the dataframe to only include the specified columns based on the consulta_type and indicador_type.
        Args:
            df (pd.DataFrame): The dataframe to filter.
            consulta_type (str): The type of query (e.g., "prog", "prc", "gan").
            indicador_type (str): The type of indicator (e.g., "i90", "p48", "rr", "afrr", "mfrr", "restricciones", "desvios", "diario", "intradiario").
        Returns:
            pd.DataFrame: The filtered dataframe.
        """
        if isinstance(df, str):
            print(f"Received a string instead of a df, meaning no data was found for the given parameters")
            return df

        # Define a mapping of columns for each consulta_type and indicador_type
        column_mapping = configc.column_mapping
        
        if consulta_type not in column_mapping:
            print(f"Invalid consulta_type: {consulta_type}. Returning original dataframe.")
            return df
        
        df = df.copy()

        # Get the default columns for the consulta_type
        columns = column_mapping[consulta_type]["default"]

        # Add specific columns for the given indicador_type if they exist
        if indicador_type in column_mapping[consulta_type]:
            columns += column_mapping[consulta_type][indicador_type]

        unique_columns = list(set(columns))

        # Check if all specified columns are in the dataframe (useful for idnicadores that will have banda and activacion for prc)
        existing_columns = [col for col in unique_columns if col in df.columns]
        missing_columns = set(unique_columns) - set(existing_columns)
        
        if missing_columns:
            print(f"The following columns are not present in the dataframe and will be excluded: {', '.join(missing_columns)}")
        
        # Filter the dataframe to include only the existing columns
        filtered_df = df[existing_columns]
        
        return filtered_df 

    @staticmethod
    def get_summary_stats(df: pd.DataFrame, columnas: Optional[List[str]] = None, dtypes: Optional[List[str]] = None) -> dict:
        """
        This function calculates and returns summary statistics for the specified dataframe.
        Args:
            df (pd.DataFrame): The dataframe for which to compute summary statistics.
            columnas (List[str]): Optional list of columns to include in the summary. If not provided, all columns will be included.
            dtypes (List[str]): Optional list of data types to include in the summary. If not provided, all data types will be included.
        Returns:
            dict: A dictionary containing the summary statistics for the specified dataframe, including count, mean, std, min, 25%, 50%, 75%, and max values.
        """
        if columnas:
            summary_df = df[columnas].describe().to_dict()
    
        if dtypes:
            summary_df = df.describe(include=dtypes).to_dict()
            
        if not columnas and not dtypes:
            summary_df = df.describe().to_dict()

        #round values to to two decimal places
        summary_df = {k: {subk: round(v, 2) for subk, v in vv.items()} for k, vv in summary_df.items()}

        return summary_df  

    def get_path(self, year: str) -> dict:
        """
        This function constructs the path for the given year and indicator.
        Args: 
            year (str): The year to construct the path for.
            indicador (str): The indicator to construct the path for.

        Returns: 
            dict: A dictionary with the paths for the given year and indicator.
            i.e. for RR indicator -> {"path_prog": "C:\\Users\\joaquin.costa\\Escritorio\\UP Tacker\\data\\curated\\ESIOS\\i90\\2023\\PROG_RR.parquet",
            "path_prc": "C:\\Users\\joaquin.costa\\Escritorio\\UP Tacker\\data\\curated\\ESIOS\\RR\\2023\\precios_rr.paquet",
            "path_prc2": "C:\\Users\\joaquin.costa\\Escritorio\\UP Tacker\\data\\curated\\ESIOS\\RR\\2023\\PRE_RR.parquet"}
        
        """

        try:
            #paths_dct= self.config.paths_for_consultas.get(self.indicador_type, {}) #for production environment access paths_for_consultas variable
            paths_dct = configc.test_local_paths.get(self.indicador_type, {}) #for testing purposes in local machine access test_local_paths variable

        except Exception as e:
            print(f"Error al obtener los paths para el indicador {self.indicador_type}: {e}")
            return {}
        
        paths_dct_copy = paths_dct.copy() #copy the paths_dct to avoid modifying the original dictionary

        # Replace the placeholder "year" in the paths
        for key in paths_dct_copy: #for each key in the paths_dct
            paths_dct_copy[key] = paths_dct_copy[key].replace("year", year) #i.e. "...\\data\\curated\\ESIOS\\i90\\year\\PROGRAMAS.parquet" -> "...\\data\\curated\\ESIOS\\i90\\2023\\PROGRAMAS.parquet"
        
        return paths_dct_copy

    def create_program_uprog_filter(self, lista_programas: Optional[List[str]] = None, lista_uprog: Optional[List[str]] = None, table: Optional[str] = None) -> tuple[str, str]:
        """
        This function gets the filter condition for the given lista_programas and lista_uprog.
        Args: 
            lista_programas (List[str]): The lista_programas to get the filter condition for.
            lista_uprog (List[str]): The lista_uprog to get the filter condition for.

        Returns: 
            str: Two strings with the filter conditions for the given lista_programas and lista_uprog. i.e. "prog.PROGRAMA IN ('PBF', 'PVP', 'PHF1')" or "prog.UPROG IN ('ABA1', 'ABA2', 'ACE3')"
        """
        # Initialize default filter conditions
        uprog_filter_condition = "1=1"
        programas_filter_condition = "1=1"

        # Create filter condition for UPROG if lista_uprog is provided
        if lista_uprog: 
            # Join the UPROG values with commas and wrap each in quotes
            lista_uprog_str = ", ".join(f"'{uprog}'" for uprog in lista_uprog)
            # Create the SQL IN clause for UPROG
            if table:
                uprog_filter_condition = f"{table}.UPROG IN ({lista_uprog_str})"
            else:
                uprog_filter_condition = f"UPROG IN ({lista_uprog_str})"
        
        # Create filter condition for PROGRAMA if lista_programas is provided
        if lista_programas: 
            # Join the PROGRAMA values with commas and wrap each in quotes
            lista_programas_str = ", ".join(f"'{programa}'" for programa in lista_programas)
            # Create the SQL IN clause for PROGRAMA
            if table:
                programas_filter_condition = f"{table}.PROGRAMA IN ({lista_programas_str})"
            else:
                programas_filter_condition = f"PROGRAMA IN ({lista_programas_str})"

        # Return both filter conditions
        return programas_filter_condition, uprog_filter_condition

    def create_date_filter(self, year:str, years_lst: List[str], start_date:str, end_date:str, table: Optional[str] = None) -> str:
        """
        This function creates the date filter for a given year that will be used in the SQL query.
        Args: 
            year (str): The year that will be used in the date filter.
            years_lst (List[str]): The total number of years that will be used in the query (useful to check for first, last and middle years).
            start_date (str): The start_date to create the date filter for.
            end_date (str): The end_date to create the date filter for.
        Returns: 
            str: A string with the date filter for the given year.
        """
        if table:
            if len(years_lst) == 1: #for single year queries
                    # If only one year, use the exact date range
                    date_filter = f"TRY_CAST({table}.FECHA AS DATE) BETWEEN '{start_date}' AND '{end_date}'"
                    
            else: #for multi-year queries
                if year == years_lst[0]:  # First year
                    date_filter = f"TRY_CAST({table}.FECHA AS DATE) >= '{start_date}'"
                elif year == years_lst[-1]:  # Last year
                    date_filter = f"TRY_CAST({table}.FECHA AS DATE) <= '{end_date}'"
                else:  # Middle years
                    date_filter = f"TRY_CAST({table}.FECHA AS DATE) BETWEEN '{year}-01-01' AND '{year}-12-31'"
        else:
            if len(years_lst) == 1: #for single year queries
                    # If only one year, use the exact date range
                    date_filter = f"TRY_CAST(FECHA AS DATE) BETWEEN '{start_date}' AND '{end_date}'"  
            else: #for multi-year queries
                if year == years_lst[0]:  # First year
                    date_filter = f"TRY_CAST(FECHA AS DATE) >= '{start_date}'"
                elif year == years_lst[-1]:  # Last year
                    date_filter = f"TRY_CAST(FECHA AS DATE) <= '{end_date}'"
                else:  # Middle years
                    date_filter = f"TRY_CAST(FECHA AS DATE) BETWEEN '{year}-01-01' AND '{year}-12-31'"

        return date_filter
     
    def create_sesion_filter(self, sesion: List[int], table: Optional[str] = None) -> str:
        """
        This function creates the sesion filter for a given sesion.
        Args: 
            sesion (List[int]): The sesion to create the filter for. Has to be list of integers, i.e. [1, 2, 3] or "All" to select all sesions.
            table (str, optional): The table name to prefix the SESION column for the SQL query. Defaults to None.
        Returns: 
            str: A string with the sesion filter for the given sesion. ex: "prog.SESION IN (1, 2, 3)"
        """
        if table:
            if sesion is None:
                return "1=1"
            else:
                try:    
                    if sesion == "All" or len(sesion) == 0 or sesion == list(range(1,8)):
                        return "1=1"
                    else:
                        sesion_str = ", ".join(f"{s}" for s in sesion) #i.e. sesion = [1, 2, 3] -> sesion_str = "'1', '2', '3'"
                        return f"{table}.SESION IN ({sesion_str})"
                except ValueError as e:
                    print(f"Error: {e}")
                    return "1=1"
        
        else:
            if sesion is None:
                return "1=1"
            else:
                try:    
                    if sesion == "All" or len(sesion) == 0:
                        return "1=1"
                    else:
                        sesion_str = ", ".join(f"{s}" for s in sesion) #i.e. sesion = [1, 2, 3] -> sesion_str = "'1', '2', '3'"
                        return f"SESION IN ({sesion_str})"
                except ValueError as e:
                    print(f"Error: {e}")
                    return "1=1"
   
    def create_sentido_filter(self, sentido: List[str], table: str = None) -> str:
        """
        Creates the sentido filter for a given direction (Subir/Bajar).
        
        Args:
             (List[str]): The directions to create the filter for (Subir/Bajar).
            table (str, optional): The table name to prefix the SENTIDO column. Defaults to None.
        
        Returns:
            str: A string with the sentido filter for the given sentido. 
                Ex: "prog.SENTIDO IN ('Subir')" or "1=1" if table is not provided.
        
        Raises:
            ValueError: If an invalid direction is provided.
        """
        valid_sentidos = {"Subir", "Bajar"}
        
        if not sentido:
            print(f"No direction filter provided for {table}")
            return "1=1"
        try:
            invalid_sentidos = set(sentido) - valid_sentidos 
            if invalid_sentidos:
                raise Exception(f"Invalid direction(s): {', '.join(invalid_sentidos)}. Must be ['Subir'] or ['Bajar'] or ['Subir', 'Bajar'].")
            
        except Exception as e:
            print(f"Error: {e}")
            return "1=1"
        
        if set(sentido) == valid_sentidos:
            print(f"All directions selected, no filter needed")
            return "1=1"
        
        sentido_str = ", ".join(f"'{d}'" for d in sentido)
        if table:
            sentido_filter = f"{table}.SENTIDO IN ({sentido_str})"
            print(f"Direction filter for {table}: {sentido_filter}")
        else:
            sentido_filter = f"SENTIDO IN ({sentido_str})"
            print(f"Direction filter: {sentido_filter}")
        
        return sentido_filter

    def check_uprog(self, lista_uprog: List[str], path_prog: str) -> List[str]:
        """
        This function checks if the lista_uprog is valid and returns the lista_uprog with the UPROG values that are found in the dataset.
        Args:   
            lista_uprog (List[str]): The lista_uprog to check.
            path_prog (str): The path to the programa_{program_type}.parquet file.
        Returns: 
            List[str]: A lista_uprog with the UPROG values that are found in the dataset.
        """
        if lista_uprog is None or len(lista_uprog) == 0 :
            lista_uprog = []
            print("All UPROG values will be retrieved")
            return lista_uprog, lista_uprog #this return is needed to avoid errors when lista_uprog is empty (i.e. no filter)

        
        lista_uprog_original = lista_uprog.copy()

        if len(lista_uprog) > 0: #if i am filtering by lista_uprog

            # Load the unique UPROG values directly from the SQL query and sort them alphabetically
            unique_uprog_values = duckdb.sql(f"SELECT DISTINCT UPROG FROM '{path_prog}' ORDER BY UPROG").df()['UPROG'].tolist()

            # Use binary search to filter lista_uprog based on unique UPROG values
            filtered_lista_uprog = [uprog for uprog in lista_uprog if Indicador.binary_search(unique_uprog_values, uprog) == True]

            print(f"{len(filtered_lista_uprog)} out of {len(lista_uprog_original)} UPROG values found in the dataset")
            print(f"UPROG values found in the dataset: {filtered_lista_uprog}")
            
            return filtered_lista_uprog, lista_uprog_original

    def get_programas_precios(self, consulta_type: str, start_date: str, end_date: str, lista_uprog: List[str], programas_i90: List[str], sentido: Optional[List[str]] = None, sesion: Optional[List[int]] = None) -> pd.DataFrame:
        """
        This function gets the programs for the given program_type between two dates - filtered by lista_uprog and programas_i90(optional).

        Args: 
            indicador (str): The indicador to get the program for. i.e. "i90", "P48", "afrr", "mfrr", "rr", "restricciones", "desvios".
            consulta_type (str): The consulta_type to get the program for. i.e. "prog", "prc". Energía = prog, precios = prc.
            start_date (str): The start_date as a string.
            end_date (str): The end_date as a string.
            lista_uprog (List[str]): The list of UPs to get the programs for i.e. "ABA1", "ABA2", "ACE3".
            programas_i90 (Optional[List[str]]): Optional. The program of the i90 i.e.  "PBF", "PVP", "PHF1", "PHF2", "PHF3", "PHF4", "PHF5", "PHF6", "PHF7" or None.
            sentido (Optional[List[str]]): Optional. The sentido to create the filter for. i.e. "Subir" or "Bajar".
            sesion (Optional[List[int]]): Optional. The sesion to create the filter for. Has to be list of integers, i.e. [1, 2, 3] or "All" to select all sesions.

        Returns: 
        pd.DataFrame: A dataframe with the programas_generic for the given program_type, start_date, end_date and lista_uprog.
        """

        years_lst = Indicador.years_between(start_date, end_date)
        master_df = pd.DataFrame()

        #check if program type is prc (precios) or prog (programas) and return the corresponding table and path_str
        table_str, path_str = Indicador.check_consulta_type(consulta_type) 

        for year in years_lst:
            try:
                paths = self.get_path(year) #i.e. 2023, "i90" -> {"path_prog": "C:\\Users\\joaquin.costa\\Escritorio\\UP Tacker\\data\\curated\\ESIOS\\i90\\2023\\PROG_RR.parquet", "path_prc": "C:\\Users\\joaquin.costa\\Escritorio\\UP Tacker\\data\\curated\\ESIOS\\RR\\2023\\precios_rr.paquet"
                path_prog_prc = paths.get(path_str) #i.e. "path_prog" = "C:\\Users\\joaquin.costa\\Escritorio\\UP Tacker\\data\\curated\\ESIOS\\i90\\2023\\PROG_RR.parquet"
                check_path = os.path.exists(path_prog_prc)

                if not check_path:
                    raise FileNotFoundError(f"Path doesn't exist: {path_prog_prc}")
                
                ########################################
                ### START OF FILTER GENERATION BLOCK ###
                date_filter = self.create_date_filter(year, years_lst, start_date, end_date, table_str)
                
                if consulta_type == "prog": #filters valid only for PROG indicadores
                    sesion_filter = "1=1"
                    #check if the lista_uprog is valid (using binary search) and returns the lista_uprog with the UPROG values that are found in the dataset
                    filtered_lista_uprog, lista_uprog_original = self.check_uprog(lista_uprog, path_prog_prc) 

                    if len(filtered_lista_uprog) == 0 and len(lista_uprog_original) > 0: #this would mean that there were not matches for the lista_uprog in the dataset, hence that year can be skipped
                        print(f"No matches for lista_uprog in the dataset for year {year}. Skipping year.")
                        continue #moving on to next iteration of loop (i.e. next year)

                    #Applying data set filters
                    if self.indicador_type in ["pbf", "pvp", "phf"]:
                        programas_filter_condition, uprog_filter_condition = self.create_program_uprog_filter(programas_i90, filtered_lista_uprog, table_str)
                    else:
                        #passing None will return a 1=1 filter condition for programas_filter_condition, which is essentially no filter
                        programas_filter_condition, uprog_filter_condition = self.create_program_uprog_filter(None, filtered_lista_uprog, "prog")
                
                else: #filters valid only for PRC indicadores
                    programas_filter_condition, uprog_filter_condition = "1=1", "1=1" #both filters are set to 1=1 for prc tables, which is essentially no filter

                    if self.indicador_type == "intradiario": #sesion filter is valid only for intradiario indicador
                        sesion_filter = self.create_sesion_filter(sesion, table_str)
                    else:
                        sesion_filter = "1=1"

                if self.indicador_type in ["rr", "afrr", "mfrr", "restricciones", "desvios"]: 
                    sentido_filter = self.create_sentido_filter(sentido, table_str) #table str defaults to None if not provided
                else: 
                    sentido_filter = "1=1"
                ### END OF FILTER GENERATION BLOCK ###
                ######################################

                query = f"""
                SELECT * FROM '{path_prog_prc}' as {table_str}
                WHERE {programas_filter_condition}
                AND {uprog_filter_condition}
                AND {sentido_filter}
                AND {date_filter}
                AND {sesion_filter}
                """
                print(f"Retrieving program {self.indicador_type.upper()} for {year}")
                print(f"query: {query}")

                df = duckdb.sql(query).df()
                if df.empty:
                    if consulta_type == "prog":
                        print(f"No programa  found for the UP {lista_uprog} between {start_date} and {end_date} for the given indicador {self.indicador_type} with parameters sentido: {sentido}")
                    else:
                        print(f"No precios data found for the given indicador {self.indicador_type} with parameters start_date: {start_date}, end_date: {end_date}, sentido: {sentido}")
                    return df

            except Exception as e:
                print(f"Error: {e}")

            else:
                master_df = pd.concat([master_df, df], ignore_index=True)

        return master_df
        
    def get_ganancias(self, config: dict, start_date: str, end_date: str, lista_uprog: List[str], sentido: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Generic function to calculate gains for different indicators.
        
        Args:
            start_date (str): Start date for the query
            end_date (str): End date for the query
            lista_uprog (List[str]): List of UPROGs to filter
            sentido (Optional[List[str]]): Direction filter, if applicable
            config (dict): Configuration dictionary for the indicator
        Returns:
            tuple[pd.DataFrame, pd.DataFrame]: Total gains per UPROG and detailed DataFrame
        """
        
        if not config:
            raise ValueError(f"Invalid indicador: {self.indicador_type}")
        
        # Get programas and precios
        prog_func = config['prog_func']
        prc_func = config['prc_func']

        if self.indicador_type in ["pbf", "pvp", "p48"]:
            #no requieren sentido ni sesion
            prog_df = prog_func(start_date, end_date, lista_uprog) 
            prc_df = prc_func(start_date, end_date)
        elif self.indicador_type in ["rr", "afrr", "mfrr", "restricciones", "desvios"]:
            #requieren sentido (aunque este sea None para banda)
            prog_df = prog_func(start_date, end_date, lista_uprog, sentido)
            prc_df = prc_func(start_date, end_date, sentido)
        else:
            #TODO: requieren sesion (cuando se incorporre ganacias intradiario)
            #prog_df = prog_func(start_date, end_date, lista_uprog, sesion)
            #prc_df = prc_func(start_date, end_date, lista_uprog, sesion)
            print("TODO (wip) BLOCK")


        #debugging statements
        print(f"prc_df: {prc_df}")
        print(f"prog_df: {prog_df}")
        
        if prog_df.empty or prc_df.empty:
            print(f"No ganancias data found for {self.indicador_type} from {start_date} to {end_date}")
            return prog_df, prc_df
        
        # Ensure correct data types
        for df in [prog_df, prc_df]:
            df['FECHA'] = pd.to_datetime(df['FECHA'])
            if 'HORA' in df.columns:
                df['HORA'] = df['HORA'].astype("int64")
            if 'PERIODO' in df.columns:
                df['PERIODO'] = df['PERIODO'].astype("int64")
        
        # Apply pre-merge filter if exists
        if config['filter_func']:
            prog_df = config['filter_func'](prog_df)

        #debugging statements
        #print(prog_df.columns)
        #print(prog_df.head())
        #print(prc_df.columns)
        #print(prc_df.head())

        #before the merge, we need to make sure HORA and PERIODO columns have the same granularity 
        if self.indicador_type in ["desvios",  "afrr"]:
            #for desvios and restricciones, HORA is in 1-96 (15 min granularity) and PERIODO is in 1-24 (hour granularity)
            #we need to convert HORA in prog to  1-24 (hour granularity) to match the PERIODO in prc
            prog_df["HORA"] = prog_df["HORA"]//4

        
        # Merge DataFrames
        merged_df = pd.merge(prog_df, prc_df, left_on=config['merge_prog_cols'], right_on=config['merge_prc_cols'], how='inner')
        #print(f"merged_df: {merged_df}")
        
        # Apply additional filter if exists
        if config['additional_filter']:
            merged_df = config['additional_filter'](merged_df)

        #print(f"merged_df after additional filter: {merged_df}")

        if merged_df.empty:
            print(f"No data found after applying filters for {self.indicador_type}")
            return merged_df, merged_df
        
        # Calculate gains
        merged_df['GANANCIA'] = round((merged_df['ENERGIA'] * merged_df['PRECIO']), 2)

        #print(f"merged_df['GANANCIA']: {merged_df['GANANCIA'].head()}")
        
        # Sum gains per UP
        total_ganancia = merged_df.groupby('UPROG')['GANANCIA'].sum().reset_index()
        total_ganancia['GANANCIA'] = total_ganancia['GANANCIA'].round(2)

        print(f"total_ganancia: {total_ganancia}")
        print(f"ganancia_df: {merged_df}")
        
        return total_ganancia, merged_df

    def agrupar_consulta(self, df: pd.DataFrame, group_by: str, consulta_type: str) -> pd.DataFrame:
        """
        This function groups the dataframe by the given group_by parameter and returns the grouped dataframe.
        Args:
            df (pd.DataFrame): The dataframe to group.
            group_by (str): The parameter to group the dataframe by.
            consulta_type (str): The type of consultation ('prog', 'prc', 'gan').
        Returns:
            pd.DataFrame: The grouped dataframe.
        """
        if isinstance(df, str):
            print(f"Received a string instead of a df, meaning no data was found for the given parameters")
            return df
        
        df = df.copy()  # Create a copy to avoid modifying the original dataframe
     
        # Convert FECHA to datetime and handle HORA/PERIODO columns
        df['FECHA'] = pd.to_datetime(df['FECHA'], errors='coerce')
        if 'PERIODO' in df.columns and 'HORA' not in df.columns: #to avoid duplicating hora col if it already exists
            df.rename(columns={'PERIODO': 'HORA'}, inplace=True)
        if 'HORA' in df.columns:
            df['HORA'] = pd.to_numeric(df['HORA'], errors='coerce')
            df.dropna(subset=['HORA'], inplace=True)

            

            # Handle the HORA column to convert outlier values like "3a" to 3
            def extract_numeric(value):
                if pd.isna(value): #if na return value as is 
                    return value
                if isinstance(value, (int, float)): #if value is already a number return it as is
                    return value
                match = re.search(r'\d+', str(value)) #if value is a string, extract the numeric part
                return int(match.group()) if match else pd.np.nan #return the numeric part as an int, if there is no match, return NaN

            df["HORA"] = df["HORA"].apply(extract_numeric) #apply the extract numeric function
            df["HORA"] = pd.to_numeric(df["HORA"], errors='coerce')
            df.dropna(subset=["HORA"], inplace=True)
            df["HORA"] = df["HORA"] - 1  # Convert to 0-23 range
        else:
            raise KeyError("No HORA column found in the dataframe.")

        # Convert to lower case
        agrupar = group_by.lower()

         # Determine aggregation type
        avg = consulta_type == 'prc'

       # Define aggregation functions
        def get_agg_func(dtype):
            if pd.api.types.is_numeric_dtype(dtype):
                return 'mean' if avg else 'sum' #if avg is true, return mean, otherwise return sum
            return 'first'

        # Create aggregation dictionary
        excluded_columns = ['FECHA', 'HORA', 'SENTIDO']
        agg_dict = {}
        for column in df.columns:
            if column not in excluded_columns:
                agg_func = get_agg_func(df[column].dtype)
                agg_dict[column] = agg_func

        # Define grouping columns
        group_cols = ['FECHA']
        if agrupar == "hora":
            group_cols.append('HORA')
        if "SENTIDO" in df.columns:
            group_cols.append('SENTIDO')

        # Group by the given group_by parameter
        if agrupar == "hora":
            if (self.indicador_type in ['rr', "mfrr"]) or \
               (self.indicador_type in ["afrr", "desvios"] and consulta_type == "prog"):
                df['HORA'] = (df['HORA'] // 4)
        elif agrupar == "dia":
            df['FECHA'] = df['FECHA'].dt.date
        elif agrupar == "mes":
            df['FECHA'] = df['FECHA'].dt.to_period('M')
            print(f"df after mes grouping: {df}")
        elif agrupar == "año":
            df['FECHA'] = df['FECHA'].dt.to_period('Y')
        else:
            print(f"Invalid grouping parameter: {agrupar}. Returning original dataframe.")
            return df

        # Group by the given group_by parameter
        grouped = df.groupby(group_cols).agg(agg_dict).reset_index()
        print(f"grouped: {grouped}")
        
        return grouped.sort_values(by=['FECHA'] + (['HORA'] if 'HORA' in grouped.columns else []))

class PBF(Indicador):
    def __init__(self):
        super().__init__("pbf")
        self.indicador_config = {
            'prog_func': lambda start_date, end_date, lista_uprog: self.get_programas(start_date, end_date, lista_uprog),
            'prc_func': lambda start_date, end_date: Diario().get_precios(start_date, end_date),
            'merge_prog_cols': ['FECHA', 'HORA'],
            'merge_prc_cols': ['FECHA', 'PERIODO'],
            'filter_func': lambda df: df[~df['HORA'].isin(['3a', '3b'])],
            'additional_filter': None
        }

    def get_programas(self, start_date: str, end_date: str, lista_uprog: List[str]) -> pd.DataFrame:
        prog_pbf = super().get_programas_precios("prog", start_date, end_date, lista_uprog, ["PBF"], None, None)
        return prog_pbf

    def get_ganancias(self, start_date: str, end_date: str, lista_uprog: List[str]) -> pd.DataFrame:
        total_ganancias_pbf, ganacias_pbf_df = super().get_ganancias(self.indicador_config, start_date, end_date, lista_uprog)
        return total_ganancias_pbf, ganacias_pbf_df
    
    def agrupar_consulta(self, df: pd.DataFrame, group_by: str, consulta_type: str) -> pd.DataFrame:
        return super().agrupar_consulta(df, group_by, consulta_type) #puede ser prog o gan
    
    def filtrar_columnas(self, df: pd.DataFrame, consulta_type: str) -> pd.DataFrame:
        return super().filtrar_columnas(df, consulta_type, self.indicador_type)

    def get_lista_up(self, start_date: str, end_date: str) -> List[str]:
        """
        Get the list of UPs for the program of a given indicador.
        Args:
            start_date (str): Start date for the query
            end_date (str): End date for the query
        Returns:
            List[str]: A list of unique UPs for the given indicador.
        """
        df = self.get_programas(start_date, end_date, None)
        return df["UPROG"].unique().tolist()

class PVP(Indicador):
    def __init__(self):
        super().__init__("pvp")
        #no need to specify and indicador_config since ganancias are calculated as ganancias_pvp = ganancias_restr + ganancias_pbf

    def get_programas(self, start_date: str, end_date: str, lista_uprog: List[str]) -> pd.DataFrame:
        prog_pvp = super().get_programas_precios("prog", start_date, end_date, lista_uprog, ["PVP"], None, None)
        return prog_pvp

    def get_ganancias(self, start_date: str, end_date: str, lista_uprog: List[str]) -> pd.DataFrame:
        #ingresos restricciones
        _, ganacias_restr_df = Restricciones().get_ganancias(start_date, end_date, lista_uprog, ["Subir", "Bajar"])
        #ingresos pbf
        _, ganacias_pbf_df = PBF().get_ganancias(start_date, end_date, lista_uprog)

        # debugging print sum of ganancias restr and pbf
       # print(f"sum of ganancias restr and pbf: {ganacias_restr_df['GANANCIA'].sum()} and {ganacias_pbf_df['GANANCIA'].sum()}")


        #ingresos pvp df
        ganacias_pvp_df = pd.merge(ganacias_restr_df, ganacias_pbf_df, on=["FECHA", "HORA", "UPROG"], how="outer", suffixes=("_restr", "_pbf"))

        #Fill NaN with 0 in both ganacias restr and pbf
        ganacias_pvp_df["GANANCIA_restr"] = ganacias_pvp_df["GANANCIA_restr"].fillna(0)
        ganacias_pvp_df["GANANCIA_pbf"] = ganacias_pvp_df["GANANCIA_pbf"].fillna(0)
        ganacias_pvp_df["GANANCIA"] = ganacias_pvp_df["GANANCIA_restr"] + ganacias_pvp_df["GANANCIA_pbf"]

        #debugging 
        print(ganacias_pvp_df.columns)
       
        #totalingresos pvp 
        total_ganancias_pvp = ganacias_pvp_df.groupby('UPROG')['GANANCIA'].sum().reset_index()
        total_ganancias_pvp['GANANCIA'] = total_ganancias_pvp['GANANCIA'].round(2)
        print(f"total_ganancias_pvp: {total_ganancias_pvp}")

        return total_ganancias_pvp, ganacias_pvp_df
    
    def agrupar_consulta(self, df: pd.DataFrame, group_by: str, consulta_type: str) -> pd.DataFrame:
        return super().agrupar_consulta(df, group_by, consulta_type) #puede ser prog o gan
    
    def filtrar_columnas(self, df: pd.DataFrame, consulta_type: str) -> pd.DataFrame:
        return super().filtrar_columnas(df, consulta_type, self.indicador_type)
    
    def get_lista_up(self, start_date: str, end_date: str) -> List[str]:
        df = self.get_programas(start_date, end_date, None)
        return df["UPROG"].unique().tolist()

class PHF(Indicador):
    def __init__(self):
        super().__init__("phf")

    def get_programas(self, start_date: str, end_date: str, lista_uprog: List[str], programas_i90: List[str]) -> pd.DataFrame:
        #print("i90 get_programas called with indicador type: ", self.indicador_type)
        for programa in programas_i90:
            if programa not in ["PHF1", "PHF2", "PHF3", "PHF4", "PHF5", "PHF6", "PHF7"]:
                print(f"Try using another class to get programas for: {programa}. This class only works for PHF1 to PHF7.")
                print("Returning empty dataframe.")
                return pd.DataFrame()
        else:
            prog_phf = super().get_programas_precios("prog", start_date, end_date, lista_uprog, programas_i90, None, None)
            return prog_phf
    
    def agrupar_consulta(self, df: pd.DataFrame, group_by: str, consulta_type: str) -> pd.DataFrame:
        consulta_type = "prog" #por ahora esta clase solo acepta prog en consulta type
        return super().agrupar_consulta(df, group_by, consulta_type)
    
    def filtrar_columnas(self, df: pd.DataFrame, consulta_type: str) -> pd.DataFrame:
        consulta_type = "prog" #por ahora esta clase solo acepta prog en consulta type
        return super().filtrar_columnas(df, consulta_type, self.indicador_type)

    def get_lista_up(self, start_date: str, end_date: str) -> List[str]:
        # Since PHF requires programas_i90, we'll use all PHF programs
        df = self.get_programas(start_date, end_date, None, ["PHF1", "PHF2", "PHF3", "PHF4", "PHF5", "PHF6", "PHF7"])
        return df["UPROG"].unique().tolist()

class P48(Indicador):
    def __init__(self):
        super().__init__("p48") #indicador type = "p48" 

    def get_programas(self, start_date: str, end_date: str, lista_uprog: List[str]) -> pd.DataFrame:
        prog_p48 = super().get_programas_precios("prog", start_date, end_date, lista_uprog, None, None, None)
        return prog_p48

    def get_ganancias(self, start_date: str, end_date: str, lista_uprog: List[str]) -> pd.DataFrame:
        pass #TODO: implementar
    
    def agrupar_consulta(self, df: pd.DataFrame, group_by: str, consulta_type: str) -> pd.DataFrame:
        return super().agrupar_consulta(df, group_by, consulta_type) #this generic agrupar parent method will work for both programas and ganancias
    
    def filtrar_columnas(self, df: pd.DataFrame, consulta_type: str) -> pd.DataFrame:
        return super().filtrar_columnas(df, consulta_type, self.indicador_type)
    
    def get_lista_up(self, start_date: str, end_date: str) -> List[str]:
        df = self.get_programas(start_date, end_date, None)
        return df["UPROG"].unique().tolist()

class RR(Indicador):
    def __init__(self):
        super().__init__("rr") #indicador type = "rr"

        #configuracion usada para calcular ganancias
        self.indicador_config = {
            'prog_func': lambda start_date, end_date, lista_uprog, direccion: self.get_programas(start_date, end_date, lista_uprog, direccion),
            'prc_func': lambda start_date, end_date: self.get_precios(start_date, end_date),
            'merge_prog_cols': ['FECHA', 'HORA'],
            'merge_prc_cols': ['FECHA', 'PERIODO'],
            'filter_func': None,
            'additional_filter': lambda df: df[df['REDESPACHO'] == 'RR']
        }

    def get_programas(self, start_date: str, end_date: str, lista_uprog: List[str], sentido: Optional[List[str]] = None) -> pd.DataFrame:
        prog_rr = super().get_programas_precios("prog", start_date, end_date, lista_uprog, None, sentido, None)
        return prog_rr

    def get_precios(self, start_date: str, end_date: str) -> pd.DataFrame:
        prc_rr = super().get_programas_precios("prc", start_date, end_date, None, None, None, None)
        return prc_rr
    
    def get_ganancias(self, start_date: str, end_date: str, lista_uprog: List[str]) -> pd.DataFrame:
        total_ganancias_rr, ganacias_rr_df = super().get_ganancias(self.indicador_config, start_date, end_date, lista_uprog)
        return total_ganancias_rr, ganacias_rr_df
    
    def agrupar_consulta(self, df: pd.DataFrame, group_by: str, consulta_type: str) -> pd.DataFrame:
        """
        This function groups the dataframe by the given group_by parameter and returns the grouped dataframe.
        Args:
            df (pd.DataFrame): The dataframe to group.
            group_by (str): The parameter to group the dataframe by.
        Returns:
            pd.DataFrame: The grouped dataframe.
        """

        if isinstance(df, str):
            print(f"Recieved a string instead of a df, meaning no data was found for the given parameters")
            return df
        
        df  = df.copy() #so that the original df is not modified

        df["FECHA"] = pd.to_datetime(df["FECHA"], errors='coerce') #convert FECHA to datetime YYYY-MM-DD
        
        if "PERIODO" in df.columns:
            df.rename(columns = {"PERIODO": "HORA"}, inplace = True)
         # Convert to lower case
        agrupar = group_by.lower()

        if consulta_type == "prc":
            avg = True
        else:
            avg = False

        # Define aggregation functions based on data types
        def get_agg_func(dtype):
            if pd.api.types.is_numeric_dtype(dtype):
                if avg == True:
                    return 'mean'
                else:
                    return 'sum'
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                return 'first'
            else:
                return 'first'
        
        # Create aggregation dictionary
            #example of the agg_dict where agrupacion is hourly:
            # {'ENERGIA': 'sum', 
            # 'UPROG': 'first', 
            # 'PROGRAMA': 'first'}
            #we are defining the methods to group by for each column in the dataframe
        agg_dict = {} 
    
        #iterate over the columns of the dataframe
        for col in df.columns:
            #if agrupacion is hourly or daily we will NOT define an aggregation method for HORA or FECHA
            if agrupar == "hora" :
                if col != 'FECHA' and col != 'HORA':
                    agg_dict[col] = get_agg_func(df[col].dtype)
            else: #if agrupacion is anything else we will NOTdefine an aggregation method for FECHA
                if col != 'FECHA':
                    agg_dict[col] = get_agg_func(df[col].dtype)

        # Group by the given group_by parameter
        if agrupar == "hora":
            #make sure HORA is an int   
            df["HORA"] = df["HORA"].astype(int)
            #convert HORA from 1-96 (15 min granulairty) to hour granularity
            df["HORA"] = ((df["HORA"]-1)//4) 
            df = df.groupby(["HORA", "FECHA"]).agg(agg_dict).reset_index()
            df = df.sort_values(by=["FECHA", "HORA"])
            return df
        elif agrupar == "dia":
            try:
                # Group by day (i.e. 2023-01-01, 2023-01-02, etc.)
                df["FECHA"] = df["FECHA"].dt.date
                df = df.groupby("FECHA").agg(agg_dict).reset_index()
                return df
            except KeyError:
                print("Error: 'FECHA' column not found in the dataframe.")
                return df 
        elif agrupar == "mes":
            try:
                # Group by month (i.e. 2023-01, 2023-02, etc.)
                df["FECHA"] = df["FECHA"].dt.to_period('M')
                df = df.groupby("FECHA").agg(agg_dict).reset_index()
                return df
            except KeyError:
                print("Error: 'FECHA' column not found in the dataframe.")
                return df
        elif agrupar == "año":
            try:    
                # Group by year (i.e. 2023)
                df["FECHA"] = df["FECHA"].dt.to_period('Y')
                df = df.groupby("FECHA").agg(agg_dict).reset_index()
                return df
            except KeyError:
                print("Error: 'FECHA' column not found in the dataframe.")
                return df
        else:
            print(f"Invalid grouping parameter: {agrupar}. Returning original dataframe.")
            return df
    
    def filtrar_columnas(self, df: pd.DataFrame, consulta_type: str) -> pd.DataFrame:
        return super().filtrar_columnas(df, consulta_type, self.indicador_type)
    
    def get_lista_up(self, start_date: str, end_date: str) -> List[str]:
        df = self.get_programas(start_date, end_date, None)
        return df["UPROG"].unique().tolist()

class Secundaria(Indicador):
    def __init__(self):
        super().__init__("afrr") #indicador type = "afrr"

        #configuracion usada para calcular ganancias
        self.indicador_config = {
            'prog_func': lambda start_date, end_date, lista_uprog, sentido: self.get_programas(start_date, end_date, lista_uprog, sentido),
            'prc_func': lambda start_date, end_date, sentido: self.get_precios(start_date, end_date, sentido),
            'merge_prog_cols': ['FECHA', 'HORA'],
            'merge_prc_cols': ['FECHA', 'PERIODO'],
            'filter_func': None,
            'additional_filter': None
        }

    def get_programas(self, start_date: str, end_date: str, lista_uprog: List[str], sentido: List[str]) -> pd.DataFrame:
        """
        Funcion para obtener los programas de secundaria. 
        Tanto la banda como la energía activada necesita una especificacion de sentido.
        De momento solo se obtienen datos de banda. 
        Args:
            start_date (str): The start date to retrieve the programs.
            end_date (str): The end date to retrieve the programs.
            lista_uprog (List[str]): The list of uprog to retrieve the programs.
            sentido (List[str]): The sentido to retrieve the programs.
        Returns:
            pd.DataFrame: The programs of desvios.
        """
        prog_afrr = super().get_programas_precios("prog", start_date, end_date, lista_uprog, None, sentido, None)
        return prog_afrr
    
    def get_precios(self, start_date: str, end_date: str, sentido: Optional[List[str]] = None) -> pd.DataFrame:
        """
        This function retrieves the precios de activacion or precios de banda depending on the sentido parameter.
        Args:
            start_date (str): The start date of the query.
            end_date (str): The end date of the query.
            sentido (Optional[List[str]]): The sentido of the query.
        Returns:
            pd.DataFrame: The precios dataframe.
        """
        if sentido is not None: #if sentido is not None then retrieve precios activacion
            prc_afrr = super().get_programas_precios("prc", start_date, end_date, None, None, sentido, None) #TODO :implentar precios activacion
        else: #if sentido is None then retrieve precios banda 
            prc_afrr = super().get_programas_precios("prc", start_date, end_date, None, None, None, None)
        return prc_afrr
    
    def get_ganancias(self, start_date: str, end_date: str, lista_uprog: List[str], sentido: List[str]) -> pd.DataFrame:
        #de momento solo se obtiene banda
        total_ganancias_afrr, ganacias_afrr_df = super().get_ganancias(self.indicador_config, start_date, end_date, lista_uprog, sentido)
        return total_ganancias_afrr, ganacias_afrr_df
    
    def agrupar_consulta(self, df: pd.DataFrame, group_by: str, consulta_type: str) -> pd.DataFrame:
        return super().agrupar_consulta(df, group_by, consulta_type)
    
    def filtrar_columnas(self, df: pd.DataFrame, consulta_type: str) -> pd.DataFrame:
        df = super().filtrar_columnas(df, consulta_type, self.indicador_type)
        return df 

    def get_lista_up(self, start_date: str, end_date: str) -> List[str]:
        df = self.get_programas(start_date, end_date, None, ["Subir", "Bajar"])
        return df["UPROG"].unique().tolist()

class Terciaria(Indicador):
    def __init__(self):
        super().__init__("mfrr") #indicador type = "mfrr"

        #configuracion usada para calcular ganancias    
        self.indicador_config = {
            'prog_func': lambda start_date, end_date, lista_uprog, sentido: self.get_programas(start_date, end_date, lista_uprog, sentido),
            'prc_func': lambda start_date, end_date, sentido: self.get_precios(start_date, end_date, sentido),
            'merge_prog_cols': ['FECHA', 'HORA', 'SENTIDO'],
            'merge_prc_cols': ['FECHA', 'PERIODO', 'SENTIDO'],
            'filter_func': None,
            'additional_filter': lambda df: df[df['REDESPACHO'].isin(['TER', 'TERPRO', 'TERMER', "TERDIR"])] #filter for activacion directa, programada, y mecanismo excepcional de terciaria
            #TER y TERPRO son la asignacion de terciaria, mientras que TERMER es la asignacion por mecanismo excepcional de terciaria. 
        }

    def get_programas(self, start_date: str, end_date: str, lista_uprog: List[str], sentido: List[str]) -> pd.DataFrame:
        prog_mfrr = super().get_programas_precios("prog", start_date, end_date, lista_uprog, None, sentido, None)
        return prog_mfrr
    
    def get_precios(self, start_date: str, end_date: str, sentido: List[str]) -> pd.DataFrame:
        #solo precios de acitvacion para el mecanismo de terciaria
        prc_mfrr = super().get_programas_precios("prc", start_date, end_date, None, None, sentido, None)
        return prc_mfrr

    def get_ganancias(self, start_date: str, end_date: str, lista_uprog: List[str], sentido: List[str]) -> pd.DataFrame:
        total_ganancias_mfrr, ganacias_mfrr_df = super().get_ganancias(self.indicador_config, start_date, end_date, lista_uprog, sentido)
        return total_ganancias_mfrr, ganacias_mfrr_df
    
    def agrupar_consulta(self, df: pd.DataFrame, group_by: str, consulta_type: str) -> pd.DataFrame:
        return super().agrupar_consulta(df, group_by, consulta_type)
    
    def filtrar_columnas(self, df: pd.DataFrame, consulta_type: str) -> pd.DataFrame:
        df = super().filtrar_columnas(df, consulta_type, self.indicador_type)
        return df 

    def get_lista_up(self, start_date: str, end_date: str) -> List[str]:
        df = self.get_programas(start_date, end_date, None, ["Subir", "Bajar"])
        return df["UPROG"].unique().tolist()

class Restricciones(Indicador):
    def __init__(self):
        super().__init__("restricciones") #indicador type = "restricciones"

        #configuracion usada para calcular ganancias
        self.indicador_config = {
            'prog_func': lambda start_date, end_date, lista_uprog, sentido: self.get_programas(start_date, end_date, lista_uprog, sentido),
            'prc_func': lambda start_date, end_date, sentido: self.get_precios(start_date, end_date, sentido),
            'merge_prog_cols': ['FECHA', 'HORA', 'SENTIDO', 'UPROG'],
            'merge_prc_cols': ['FECHA', 'HORA', 'SENTIDO', 'UPROG'],
            'filter_func': None,
            'additional_filter': None
        }

    def get_programas(self, start_date: str, end_date: str, lista_uprog: List[str], sentido: List[str]) -> pd.DataFrame:
        prog_restricciones = super().get_programas_precios("prog", start_date, end_date, lista_uprog, None, sentido, None)
        return prog_restricciones

    def get_precios(self, start_date: str, end_date: str, sentido: List[str]) -> pd.DataFrame:
        prc_restricciones = super().get_programas_precios("prc", start_date, end_date, None, None, sentido, None)
        return prc_restricciones
    
    def get_ganancias(self, start_date: str, end_date: str, lista_uprog: List[str], sentido: List[str]) -> pd.DataFrame:
        total_ganancias_restr, ganacias_restr_df = super().get_ganancias(self.indicador_config, start_date, end_date, lista_uprog, sentido)
        return total_ganancias_restr, ganacias_restr_df
    
    def agrupar_consulta(self, df: pd.DataFrame, group_by: str, consulta_type: str) -> pd.DataFrame:
        return super().agrupar_consulta(df, group_by, consulta_type)
    
    def filtrar_columnas(self, df: pd.DataFrame, consulta_type: str) -> pd.DataFrame:
        df = super().filtrar_columnas(df, consulta_type, self.indicador_type)
        return df 

    def get_lista_up(self, start_date: str, end_date: str) -> List[str]:
        df = self.get_programas(start_date, end_date, None, ["Subir", "Bajar"])
        return df["UPROG"].unique().tolist()

class Desvios(Indicador):
    def __init__(self):
        super().__init__("desvios") #indicador type = "desvios"
        
        #configuracion usada para calcular ganancias
        self.indicador_config = {
            'prog_func': lambda start_date, end_date, lista_uprog, sentido: self.get_programas(start_date, end_date, lista_uprog, sentido),
            'prc_func': lambda start_date, end_date, sentido: self.get_precios(start_date, end_date, sentido),
            'merge_prog_cols': ['FECHA', 'HORA', 'SENTIDO'],
            'merge_prc_cols': ['FECHA', 'PERIODO', 'SENTIDO'],
            'filter_func': lambda df: df[~df['HORA'].isin(['3a', '3b'])],
            'additional_filter': lambda df: df[df['REDESPACHO'] == 'DESV']
        }


    def get_programas(self, start_date: str, end_date: str, lista_uprog: List[str], sentido: List[str]) -> pd.DataFrame:
        prog_desvios = super().get_programas_precios("prog", start_date, end_date, lista_uprog, None, sentido, None)
        return prog_desvios

    def get_precios(self, start_date: str, end_date: str, sentido: List[str]) -> pd.DataFrame:
        """
        Funcion para obtener los precios de desvios. Para obtener los precios de ambos sentido se tiene 
        que esecificar sentido como ["Subir", "Bajar"]. }

        Args:
            start_date (str): The start date to retrieve the prices.
            end_date (str): The end date to retrieve the prices.
            sentido (List[str]): The sentido to retrieve the prices as a list of strings. Valid strings are "Subir" and "Bajar".
        Returns:
            pd.DataFrame: The prices of desvios.
        """
        prc_desvios = super().get_programas_precios("prc", start_date, end_date, None, None, sentido, None)
        return prc_desvios
    
    def get_ganancias(self, start_date: str, end_date: str, lista_uprog: List[str], sentido: List[str]) -> pd.DataFrame:
        total_ganancias_desv, ganacias_desv_df = super().get_ganancias(self.indicador_config, start_date, end_date, lista_uprog, sentido)
        return total_ganancias_desv, ganacias_desv_df
    
    def agrupar_consulta(self, df: pd.DataFrame, group_by: str, consulta_type: str) -> pd.DataFrame:
        return super().agrupar_consulta(df, group_by, consulta_type)
    
    def filtrar_columnas(self, df: pd.DataFrame, consulta_type: str) -> pd.DataFrame:
        df = super().filtrar_columnas(df, consulta_type, self.indicador_type)
        return df 

    def get_lista_up(self, start_date: str, end_date: str) -> List[str]:
        df = self.get_programas(start_date, end_date, None, ["Subir", "Bajar"])
        return df["UPROG"].unique().tolist()

class Diario(Indicador):
    def __init__(self):
        super().__init__("diario")

    def get_precios(self, start_date: str, end_date: str) -> pd.DataFrame:
        prc_diario = super().get_programas_precios("prc", start_date, end_date, None, None, None, None)
        return prc_diario

    def agrupar_consulta(self, df: pd.DataFrame, group_by: str, ) -> pd.DataFrame:
        return super().agrupar_consulta(df, group_by, "prc") #this will work for both precios and i90 program ganancias which are calculated in this class

    def filtrar_columnas(self, df: pd.DataFrame) -> pd.DataFrame: 
        return super().filtrar_columnas(df, "prc", self.indicador_type)

class Intradiario(Indicador):
    def __init__(self):
        super().__init__("intradiario") #indicador type = "intradiario"

        #dictionary with the programas for each sesion used to calculate ganancias
        self.sesion_programas = {1: ["PVP", "PHF1"],
                        2: ["PHF1", "PHF2"],
                        3: ["PHF2", "PHF3"],
                        4: ["PHF3", "PHF4"],
                        5: ["PHF4", "PHF5"],
                        6: ["PHF5", "PHF6"],
                        7: ["PHF6", "PHF7"]}
    
    @staticmethod
    def check_sesion(start_date: str, end_date: str, sesion: List[int]) -> Union[List[int], tuple[List[int], List[int]]]:
        """
        This function checks the sessions passed and returns the valid sessions based on a start an end date of data retrieval.
        Args: 
            start_date (str): The start_date to check.
            end_date (str): The end_date to check.
            sesion (List[int]): The sesion to check.
        Returns: 
        Tuple[List[int], List[int]]: If cut off date (14/07/2024) is between the  start_date and end_date then fucntion returns a tuple with two lists.
        The first list contains the sesion values that are found in the dataset before the start date and the second list contains the sesion values that are found in the dataset after the end date.
                                                or 
        List[int]:  Otherwise the function returns a list of the valid sesion values that are found in the dataset given the start and end date.
        """
        fecha_corte = datetime(2024, 7, 14) #ultimo dia  de 7 sesiones intradiaria

        #convert to datetiqtime format
        start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")

        #if  my start and end date is before the fecha corte then return sesion as is 
        if start_date_dt < fecha_corte and end_date_dt < fecha_corte:
            if sesion == "All" or sesion == None:
                return list(range(1, 8)), fecha_corte #i.e. [1, 2, 3, 4, 5, 6, 7]
            else:
                return sesion, fecha_corte #return any combination of sesiones in range 1-7 
            
        #else if my start date and end date are after the fecha de corte then only allow sesiones 1,2,3 (meaning that sesion 4,5,6,7 are filtered if passed)
        elif start_date_dt >= fecha_corte and end_date_dt >= fecha_corte:
            if sesion == "All" or sesion == None:
                return list(range(1, 4)), fecha_corte #i.e. [1, 2, 3]
            else:
                filtered_sesion =  [s for s in sesion if s <=3] #i.e. sesion = [1, 2, 3, 4, 5, 6, 7] -> filtered_sesion = [1, 2, 3]
                excluded_sesion = [s for s in sesion if s > 3] #i.e. sesion = [1, 2, 3, 4, 5, 6, 7] -> excluded_sesion = [4, 5, 6, 7]
                print(f"For the selected dates {start_date} to {end_date} the following sesions are not available: {excluded_sesion}")
                return filtered_sesion, fecha_corte
        
        #if start date is before and end date is after the cut off date
        else:
            if sesion == "All" or sesion == None: 
                before_corte = list(range(1, 8)) #i.e. [1, 2, 3, 4, 5, 6, 7]
                after_corte = list(range(1, 4)) #i.e. [1, 2, 3]
                filtered_sesiones_tuple = (before_corte, after_corte)
                return filtered_sesiones_tuple, fecha_corte
            else: 
                before_corte = [s for s in sesion if s <= 7] #allow sesion in range 1-7
                after_corte = [s for s in sesion if s <=3] #allow sesion in range 1-3
                filtered_sesiones_tuple = (before_corte, after_corte)
                return filtered_sesiones_tuple, fecha_corte

    def get_precios(self, start_date: str, end_date: str, sesion: List[int]) -> pd.DataFrame:
        # Filter sessions based on start and end date
        filtered_sesion, fecha_corte = Intradiario.check_sesion(start_date, end_date, sesion)

        # If filtered_sesion is a tuple, it means that the start and end date are before and after the cut off date respectively
        if isinstance(filtered_sesion, tuple):
            before_corte_sesiones, after_corte_sesiones = filtered_sesion

            # Get intradiario prices before and after the cut off date
            prc_intradiario_before_corte = super().get_programas_precios("prc", start_date, (fecha_corte - timedelta(days=1)).strftime('%Y-%m-%d'), None, None, None, before_corte_sesiones)
            prc_intradiario_after_corte = super().get_programas_precios("prc", fecha_corte.strftime('%Y-%m-%d'), end_date, None, None, None, after_corte_sesiones)

            # Concatenate the two dataframes
            prc_intradiario = pd.concat([prc_intradiario_before_corte, prc_intradiario_after_corte], ignore_index=True)

        else:
            prc_intradiario = super().get_programas_precios("prc", start_date, end_date, None, None, None, filtered_sesion)

        return prc_intradiario

    def get_ganancias(self, start_date: str, end_date: str, lista_uprog: List[str], sesion: Optional[List[int]] = None) -> pd.DataFrame:
        filtered_sesion_lst, fecha_corte = Intradiario.check_sesion(start_date, end_date, sesion)
        prog_df = pd.DataFrame()

        if isinstance(filtered_sesion_lst, tuple):
            before_corte_sesiones = filtered_sesion_lst[0]
            after_corte_sesiones = filtered_sesion_lst[1]

            #get intradiario prices before and after the cut off date
            prc_intradiario_before_corte = self.get_precios(start_date, fecha_corte - timedelta(days=1), None, before_corte_sesiones)
            prc_intradiario_after_corte = self.get_precios(fecha_corte, end_date, None, after_corte_sesiones)

            #concatenate the two dataframes
            prc_intradiario = pd.concat([prc_intradiario_before_corte, prc_intradiario_after_corte], ignore_index=True)
        
            #get programas intradiario before and after the cut off date
            for s in before_corte_sesiones: 
                prog_intradiario_before_corte = PHF().get_programas(start_date, fecha_corte - timedelta(days=1), lista_uprog, self.sesion_programas[s])
                prog_df = pd.concat([prog_df, prog_intradiario_before_corte], ignore_index=True)

            for s in after_corte_sesiones: 
                prog_intradiario_after_corte = PHF().get_programas(fecha_corte, end_date, lista_uprog, self.sesion_programas[s])
                prog_df = pd.concat([prog_df, prog_intradiario_after_corte], ignore_index=True)
        
        else:
            prc_intradiario = self.get_precios(start_date, end_date, filtered_sesion_lst)

            for s in filtered_sesion_lst: 
                prog_intradiario = PHF().get_programas(start_date, end_date, lista_uprog, self.sesion_programas[s])
                prog_df = pd.concat([prog_df, prog_intradiario], ignore_index=True)

        #convert types to datetime and to integer
        prog_df["FECHA"] = pd.to_datetime(prog_df["FECHA"])
        prc_intradiario["FECHA"] = pd.to_datetime(prog_df["FECHA"])

        prog_df["HORA"] = prog_df["HORA"].astype(int)
        prc_intradiario["PERIODO"] = prog_df["HORA"].astype(int)

        #merge the two dataframes on fecha, hora and uprog
        merged_df = pd.merge(prog_df, prc_intradiario, left_on=['FECHA', 'HORA'], right_on=['FECHA', 'PERIODO'], how='left')

        # Calculate the ganancia
        merged_df['GANANCIA'] = round((merged_df['ENERGIA'] * merged_df['PRECIO']), 2)

        # Sum the ganancia per UP and round it
        total_ganancia = merged_df.groupby('UPROG')['GANANCIA'].sum().reset_index()
        total_ganancia['GANANCIA'] = total_ganancia['GANANCIA'].round(2)
        
        return total_ganancia, merged_df
    
    def agrupar_consulta(self, df: pd.DataFrame, group_by: str, consulta_type: str) -> pd.DataFrame:
        df = super().agrupar_consulta(df, group_by, consulta_type)
        return df
    
    def filtrar_columnas(self, df: pd.DataFrame, consulta_type: str) -> pd.DataFrame:
        df = super().filtrar_columnas(df, consulta_type, self.indicador_type)
        return df

class IntradiarioContinuo(Indicador):
    def __init__(self):
        super().__init__("intradiario continuo")

if __name__ == "__main__":

    l = 1 
