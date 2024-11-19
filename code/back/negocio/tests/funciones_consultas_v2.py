import pandas as pd 
import duckdb 
from typing import List, Optional, Union
from datetime import datetime
import os
import negocio.config_consultas as configc
from bisect import bisect_left
from datetime import timedelta

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

def get_path(year:str, indicador:str) -> dict:
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
        #paths_dct= configc.paths_for_consultas.get(indicador, {})
        paths_dct = configc.test_local_paths.get(indicador, {}) #for testing purposes in local machine

    except Exception as e:
        print(f"Error al obtener los paths para el indicador {indicador}: {e}")
        return {}
    
    paths_dct_copy = paths_dct.copy() #copy the paths_dct to avoid modifying the original dictionary

    # Replace the placeholder "year" in the paths
    for key in paths_dct_copy: #for each key in the paths_dct
        paths_dct_copy[key] = paths_dct_copy[key].replace("year", year) #i.e. "...\\data\\curated\\ESIOS\\i90\\year\\PROGRAMAS.parquet" -> "...\\data\\curated\\ESIOS\\i90\\2023\\PROGRAMAS.parquet"
    
    return paths_dct_copy

def create_program_uprog_filter(lista_programas: List[str] = None, lista_uprog: List[str] = None, table: str = None) -> tuple[str, str]:
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

def create_date_filter(year:str, years_lst: List[str], start_date:str, end_date:str, table: str = None) -> str:
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

def create_sesion_filter(sesion: List[int], table: str = None) -> str:
    """
    This function creates the sesion filter for a given sesion.
    Args: 
        sesion (List[int]): The sesion to create the filter for. Has to be list of integers, i.e. [1, 2, 3] or "All" to select all sesions.
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
                if sesion == "All" or len(sesion) == 0 or sesion == list(range(1,8)):
                    return "1=1"
                else:
                    sesion_str = ", ".join(f"{s}" for s in sesion) #i.e. sesion = [1, 2, 3] -> sesion_str = "'1', '2', '3'"
                    return f"SESION IN ({sesion_str})"
            except ValueError as e:
                print(f"Error: {e}")
                return "1=1"

def create_direccion_filter(direccion: List[str], table: str = None) -> str:
    """
    Creates the sentido filter for a given direction (Subir/Bajar).
    
    Args:
        direccion (List[str]): The directions to create the filter for (Subir/Bajar).
        table (str, optional): The table name to prefix the SENTIDO column. Defaults to None.
    
    Returns:
        str: A string with the direccion filter for the given direccion. 
             Ex: "prog.SENTIDO IN ('Subir')" or "1=1" if table is not provided.
    
    Raises:
        ValueError: If an invalid direction is provided.
    """
    valid_direcciones = {"Subir", "Bajar"}
    
    if not direccion:
        print(f"No direction filter provided for {table}")
        return "1=1"
    
    invalid_direcciones = set(direccion) - valid_direcciones 
    if invalid_direcciones:
        raise ValueError(f"Invalid direction(s): {', '.join(invalid_direcciones)}. Must be 'Subir' or 'Bajar'.")
    
    if set(direccion) == valid_direcciones:
        print(f"All directions selected, no filter needed")
        return "1=1"
    
    direccion_str = ", ".join(f"'{d}'" for d in direccion)
    if table:
        direccion_filter = f"{table}.SENTIDO IN ({direccion_str})"
    else:
        direccion_filter = f"SENTIDO IN ({direccion_str})"
    
    print(f"Direction filter for {table}: {direccion_filter}")
    return direccion_filter

def check_uprog(lista_uprog: List[str], path_prog: str) -> List[str]:
    """
    This function checks if the lista_uprog is valid and returns the lista_uprog with the UPROG values that are found in the dataset.
    Args:   
        lista_uprog (List[str]): The lista_uprog to check.
        path_prog (str): The path to the programa_{program_type}.parquet file.
    Returns: 
        List[str]: A lista_uprog with the UPROG values that are found in the dataset.
    """
    if lista_uprog is None:
        lista_uprog = []
        print("All UPROG values will be retrieved")
    
    lista_uprog_original = lista_uprog

    if len(lista_uprog) > 0: #if i am filtering by lista_uprog

        # Load the unique UPROG values directly from the SQL query and sort them alphabetically
        unique_uprog_values = duckdb.sql(f"SELECT DISTINCT UPROG FROM '{path_prog}' ORDER BY UPROG").df()['UPROG'].tolist()

        # Use binary search to filter lista_uprog based on unique UPROG values
        filtered_lista_uprog = [uprog for uprog in lista_uprog if binary_search(unique_uprog_values, uprog) == True]

        print(f"{len(filtered_lista_uprog)} out of {len(lista_uprog_original)} UPROG values found in the dataset")
        print(f"UPROG values found in the dataset: {filtered_lista_uprog}")

    return filtered_lista_uprog, lista_uprog_original

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

def check_sesion(start_date: str, end_date: str, sesion: List[int]) -> Union[List[int], tuple[List[int], List[int]]]:
    """
    This function checks the sessions passed and returns the valid sessions based on a start an end date of data retrieval.
    Args: 
        start_date (str): The start_date to check.
        end_date (str): The end_date to check.
        sesion (List[int]): The sesion to check.
    Returns: 
    if cut off date (14/07/2024) is between the  start_date and end_date then fucntion returns a tuple with two lists:
        tuple: A tuple with the sesion values that are found in the dataset before the start date and the sesion values that are found in the dataset after the end date.
    else the function returns a list of the valid sesion values that are found in the dataset given the start and end date.
        List[int]: A sesion with the sesion values that are found in the dataset given the start and end date.
        fecha_corte (datetime): The fecha_corte as a datetime object.
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
        
    #else if my start date and end date is after fecha de corte then only allow sesiones 1,2,3 (meaning that sesion 4,5,6,7 are filtered if passed)
    elif start_date_dt >= fecha_corte and end_date_dt >= fecha_corte:
        if sesion == "All" or sesion == None:
            return list(range(1, 4)) #i.e. [1, 2, 3]
        else:
            filtered_sesion =  [s for s in sesion if s <=3] #i.e. sesion = [1, 2, 3, 4, 5, 6, 7] -> filtered_sesion = [1, 2, 3]
            excluded_sesion = [s for s in sesion if s > 3] #i.e. sesion = [1, 2, 3, 4, 5, 6, 7] -> excluded_sesion = [4, 5, 6, 7]
            print(f"For the selected dates {start_date} to {end_date} the following sesions are not available: {excluded_sesion}")
            return filtered_sesion, fecha_corte
    
    else:
        if sesion == "All" or sesion == None:
            before_corte = list(range(1, 8))
            after_corte = list(range(1, 4)) 
            filtered_sesiones_tuple = (before_corte, after_corte)
            return filtered_sesiones_tuple, fecha_corte
        else: 
            before_corte = [s for s in sesion if s <= 7] 
            after_corte = [s for s in sesion if s <=3] 
            filtered_sesiones_tuple = (before_corte, after_corte)
            return filtered_sesiones_tuple, fecha_corte

def get_programas_precios(indicador: str, consulta_type: str, start_date: str, end_date: str, lista_uprog: List[str], programas_i90: List[str], direccion: List[str], sesion: List[int]) -> pd.DataFrame:
    """
    This function gets the programs for the given program_type between two dates - filtered by lista_uprog and programas_i90(optional).

    Args: 
        indicador (str): The indicador to get the program for. i.e. "i90", "P48", "afrr", "mfrr", "rr", "restricciones", "desvios".
        consulta_type (str): The consulta_type to get the program for. i.e. "prog", "prc". Energía = prog, precios = prc.
        start_date (str): The start_date as a string.
        end_date (str): The end_date as a string.
        lista_uprog (List[str]): The list of UPs to get the programs for i.e. "ABA1", "ABA2", "ACE3".
        programas_i90 (Optional[List[str]]): Optional. The program of the i90 i.e.  "PBF", "PVP", "PHF1", "PHF2", "PHF3", "PHF4", "PHF5", "PHF6", "PHF7" or None.
        direccion (Optional[List[str]]): Optional. The direccion to create the filter for. i.e. "Subir" or "Bajar".
        sesion (Optional[List[int]]): Optional. The sesion to create the filter for. Has to be list of integers, i.e. [1, 2, 3] or "All" to select all sesions.

    Returns: 
        pd.DataFrame: A dataframe with the programas_generic for the given program_type, start_date, end_date and lista_uprog.
    """

    years_lst = years_between(start_date, end_date)
    master_df = pd.DataFrame()

    #check if program type is prc (precios) or prog (programas) and return the corresponding table and path_str
    table_str, path_str = check_consulta_type(consulta_type) 

    for year in years_lst:
        try:
            paths = get_path(year, indicador) #i.e. 2023, "i90" -> {"path_prog": "C:\\Users\\joaquin.costa\\Escritorio\\UP Tacker\\data\\curated\\ESIOS\\i90\\2023\\PROG_RR.parquet", "path_prc": "C:\\Users\\joaquin.costa\\Escritorio\\UP Tacker\\data\\curated\\ESIOS\\RR\\2023\\precios_rr.paquet"
            path_prog_prc = paths.get(path_str) #i.e. "path_prog" = "C:\\Users\\joaquin.costa\\Escritorio\\UP Tacker\\data\\curated\\ESIOS\\i90\\2023\\PROG_RR.parquet"
            check_path = os.path.exists(path_prog_prc)

            if not check_path:
                raise FileNotFoundError(f"Path doesn't exist: {path_prog_prc}")
            
            ########################################
            ### START OF FILTER GENERATION BLOCK ###
            date_filter = create_date_filter(year, years_lst, start_date, end_date, table_str)
            
            if consulta_type == "prog": #filters valid only for PROG indicadores
                sesion_filter = "1=1"
                #check if the lista_uprog is valid (using binary search) and returns the lista_uprog with the UPROG values that are found in the dataset
                filtered_lista_uprog, lista_uprog_original = check_uprog(lista_uprog, path_prog_prc) 

                if len(filtered_lista_uprog) == 0 and len(lista_uprog_original) > 0: #this would mean that there were not matches for the lista_uprog in the dataset, hence that year can be skipped
                    print(f"No matches for lista_uprog in the dataset for year {year}. Skipping year.")
                    continue #moving on to next iteration of loop (i.e. next year)

                #Applying data set filters
                if indicador == "i90":
                    programas_filter_condition, uprog_filter_condition = create_program_uprog_filter(programas_i90, filtered_lista_uprog, table_str)
                else:
                    #passing None will return a 1=1 filter condition for programas_filter_condition, which is essentially no filter
                    programas_filter_condition, uprog_filter_condition = create_program_uprog_filter(None, filtered_lista_uprog, "prog")
            
            else: #filters valid only for PRC indicadores
                programas_filter_condition, uprog_filter_condition = "1=1", "1=1" #both filters are set to 1=1 for prc tables, which is essentially no filter

                if indicador == "intradiario": #sesion filter is valid only for intradiario indicador
                    sesion_filter = create_sesion_filter(sesion, table_str)
                else:
                    sesion_filter = "1=1"

            if indicador in ["rr", "afrr", "mfrr", "restricciones", "desvios"]: 
                direccion_filter = create_direccion_filter(direccion, table_str)
            else: 
                direccion_filter = "1=1"
            ### END OF FILTER GENERATION BLOCK ###
            ######################################

            query = f"""
            SELECT * FROM '{path_prog_prc}' as {table_str}
            WHERE {programas_filter_condition}
            AND {uprog_filter_condition}
            AND {direccion_filter}
            AND {date_filter}
            AND {sesion_filter}
            """
            print(f"Retrieving program {indicador.upper()} for {year}")
            print(f"query: {query}")

            df = duckdb.sql(query).df()
            if df.empty:
                raise ValueError(f"Data frame is empty for {indicador} for {year}")

        except Exception as e:
            print(f"Error: {e}")

        else:
            master_df = pd.concat([master_df, df], ignore_index=True)

    return master_df

def get_programas_i90(start_date: str, end_date: str, lista_uprog: List[str], programas_i90: List[str]) -> pd.DataFrame:
    prog_i90 = get_programas_precios("i90", "prog", start_date, end_date, lista_uprog, programas_i90, None, None)
    return prog_i90

def get_programas_p48(start_date: str, end_date: str, lista_uprog: List[str]) -> pd.DataFrame:
    prog_p48 = get_programas_precios("p48", "prog", start_date, end_date, lista_uprog, None, None, None)
    return prog_p48

def get_programas_rr(start_date: str, end_date: str, lista_uprog: Optional[List[str]] = None, direccion: Optional[List[str]] = None) -> pd.DataFrame:
    prog_rr = get_programas_precios("rr", "prog", start_date, end_date, lista_uprog, None, direccion, None)
    return prog_rr

def get_programas_secundaria(start_date: str, end_date: str, lista_uprog: Optional[List[str]] = None, direccion: Optional[List[str]] = None) -> pd.DataFrame:
    prog_secundaria = get_programas_precios("afrr", "prog", start_date, end_date, lista_uprog, None, direccion, None)
    return prog_secundaria

def get_programas_terciaria(start_date: str, end_date: str, lista_uprog: Optional[List[str]] = None, direccion: Optional[List[str]] = None) -> pd.DataFrame:
    prog_terciaria = get_programas_precios("mfrr", "prog", start_date, end_date, lista_uprog, None, direccion, None)
    return prog_terciaria

def get_programas_restricciones(start_date: str, end_date: str, lista_uprog: Optional[List[str]] = None, direccion: Optional[List[str]] = None) -> pd.DataFrame:
    prog_restricciones = get_programas_precios("restricciones", "prog", start_date, end_date, lista_uprog, None, direccion, None)
    return prog_restricciones

def get_programas_desvios(start_date: str, end_date: str, lista_uprog: Optional[List[str]] = None, direccion: Optional[List[str]] = None) -> pd.DataFrame:
    prog_desvios = get_programas_precios("desvios", "prog", start_date, end_date, lista_uprog, None, direccion, None)
    return prog_desvios

def get_precios_intradiario(start_date: str, end_date: str, sesion: Optional[List[int]] = None) -> pd.DataFrame:

    #filter sesions based on start and end date
    filtered_sesion, fecha_corte = check_sesion(start_date, end_date, sesion)

    #if filtered_sesion is a tuple, then it means that the start and end date are before and after the cut off date respectively, hence we would have to convert the sesion parameter to fit the data retrieval 
    if isinstance(filtered_sesion, tuple):
        before_corte_sesiones = filtered_sesion[0]
        after_corte_sesiones = filtered_sesion[1]

        #get intradiario prices before and after the cut off date
        prc_intradiario_before_corte = get_programas_precios("intradiario", "prc", start_date, fecha_corte - timedelta(days=1), None, None, None, before_corte_sesiones)
        prc_intradiario_after_corte = get_programas_precios("intradiario", "prc", fecha_corte, end_date, None, None, None, after_corte_sesiones)

        #concatenate the two dataframes
        prc_intradiario = pd.concat([prc_intradiario_before_corte, prc_intradiario_after_corte], ignore_index=True)

    else:
        prc_intradiario = get_programas_precios("intradiario", "prc", start_date, end_date, None, None, None, filtered_sesion)

    return prc_intradiario

def get_precios_diario(start_date: str, end_date: str) -> pd.DataFrame:
    prc_diario = get_programas_precios("diario", "prc", start_date, end_date, None, None, None, None)
    return prc_diario

def get_precios_rr(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Precio de banda de RR. No se necesita direccion (i.e None) porque es un único precio horario.
    """
    prc_rr = get_programas_precios("rr", "prc", start_date, end_date, None, None, None, None)
    return prc_rr

def get_precios_secundaria(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Estos son precios horarios de la banda secundaria. Es decir pagos por capacidad. Un precio único horario cada hora.

    No se necesita direccion (i.e None) porque es un único precio horario.
    """
    prc_secundaria = get_programas_precios("afrr", "prc", start_date, end_date, None, None, None, None)
    return prc_secundaria

def get_precios_terciaria(start_date: str, end_date: str, direccion: Optional[List[str]] = None) -> pd.DataFrame:
    prc_terciaria = get_programas_precios("mfrr", "prc", start_date, end_date, None, None, direccion, None)
    return prc_terciaria

def get_precios_restricciones(start_date: str, end_date: str, direccion: Optional[List[str]] = None) -> pd.DataFrame:
    """
    A fucntion that gets the prices associated with the technical restrictions program. Restricciones = RRTT .

    Args: 
        start_date (str): The start_date as a string.
        end_date (str): The end_date as a string.
        direccion (Optional[List[str]]): Optional. The direccion to create the filter for. i.e. "Subir" or "Bajar".
    Returns: 
        pd.DataFrame: A dataframe with the price asssociated with the technical restrictions program.
    """
    prc_restricciones = get_programas_precios("restricciones", "prc", start_date, end_date, None, None, direccion, None)

    return prc_restricciones

def get_precios_desvios(start_date: str, end_date: str, direccion: Optional[List[str]] = None) -> pd.DataFrame:
    prc_desvios = get_programas_precios("desvios", "prc", start_date, end_date, None, None, direccion, None)
    return prc_desvios

def get_ganancias_mercado_diario(start_date: str, end_date: str, lista_uprog: List[str]) -> tuple[pd.DataFrame, pd.DataFrame]:

    # Get precios diario
    prc_df = get_precios_diario(start_date, end_date)
    
    # Get programas i90 with programa PBF and uprog in lista_uprog
    prog_df = get_programas_i90(start_date, end_date, lista_uprog, ["PBF"])

    if prog_df.empty:
        print(f"No programas i90 found for {start_date} to {end_date} for the following UPs: {lista_uprog}")
        print(f"Returning empty dataframes")
        return pd.DataFrame(), pd.DataFrame()
    
    # Calculate the sum of ENERGIA for each element in lista_uprog
    energia_sum_per_uprog = prog_df[prog_df['UPROG'].isin(lista_uprog)].groupby('UPROG')['ENERGIA'].sum().reset_index()
    #print("Sum of ENERGIA for each element in lista_uprog:")
    #print(energia_sum_per_uprog)


    # Filter out rows where HORA is '3a' or '3b'
    prog_df_filtered = prog_df[~prog_df['HORA'].isin(['3a', '3b'])] #exclude daylights saving hours
    #print(prog_df_filtered.shape)


   # Ensure both columns are of the same type (integer and datetime) before merging
    prog_df_filtered['HORA'] = prog_df_filtered['HORA'].astype(int)
    prc_df['PERIODO'] = prc_df['PERIODO'].astype(int)
    prog_df_filtered['FECHA'] = pd.to_datetime(prog_df_filtered['FECHA'])
    prc_df['FECHA'] = pd.to_datetime(prc_df['FECHA'])

    # Merge the two DataFrames on FECHA and HORA/PERIODO
    merged_df = pd.merge(prog_df_filtered, prc_df, left_on=['FECHA', 'HORA'], right_on=['FECHA', 'PERIODO'], how='left')
    #print(merged_df.shape)

    # Calculate the ganancia
    merged_df['GANANCIA'] = round((merged_df['ENERGIA'] * merged_df['PRECIO']), 2)

    # Sum the ganancia per UP and round it
    total_ganancia = merged_df.groupby('UPROG')['GANANCIA'].sum().reset_index()
    total_ganancia['GANANCIA'] = total_ganancia['GANANCIA'].round(2)

    return total_ganancia, merged_df

def get_ganancias_intradiario(start_date: str, end_date: str, lista_uprog: List[str], sesion: Optional[List[int]] = None) -> tuple[pd.DataFrame, pd.DataFrame]:

    sesion_programas = {1: ["PVP", "PHF1"],
                        2: ["PHF1", "PHF2"],
                        3: ["PHF2", "PHF3"],
                        4: ["PHF3", "PHF4"],
                        5: ["PHF4", "PHF5"],
                        6: ["PHF5", "PHF6"],
                        7: ["PHF6", "PHF7"]}
    
    filtered_sesion_lst, fecha_corte = check_sesion(start_date, end_date, sesion)
    prog_df = pd.DataFrame()

    if isinstance(filtered_sesion_lst, tuple):
        before_corte_sesiones = filtered_sesion_lst[0]
        after_corte_sesiones = filtered_sesion_lst[1]

        #get intradiario prices before and after the cut off date
        prc_intradiario_before_corte = get_precios_intradiario(start_date, fecha_corte - timedelta(days=1), None, before_corte_sesiones)
        prc_intradiario_after_corte = get_precios_intradiario(fecha_corte, end_date, None, after_corte_sesiones)

        #concatenate the two dataframes
        prc_intradiario = pd.concat([prc_intradiario_before_corte, prc_intradiario_after_corte], ignore_index=True)

        #get programas intradiario before and after the cut off date
        for s in before_corte_sesiones: 
            prog_intradiario_before_corte = get_programas_i90(start_date, fecha_corte - timedelta(days=1), lista_uprog, sesion_programas[s])
            prog_df = pd.concat([prog_df, prog_intradiario_before_corte], ignore_index=True)

        for s in after_corte_sesiones: 
            prog_intradiario_after_corte = get_programas_i90(fecha_corte, end_date, lista_uprog, sesion_programas[s])
            prog_df = pd.concat([prog_df, prog_intradiario_after_corte], ignore_index=True)
    
    else:
        prc_intradiario = get_precios_intradiario(start_date, end_date, filtered_sesion_lst)

        for s in filtered_sesion_lst: 
            prog_intradiario = get_programas_i90(start_date, end_date, lista_uprog, sesion_programas[s])
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

def get_ganancias_restricciones(start_date: str, end_date: str, lista_uprog: List[str], direccion: Optional[List[str]] = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    # get precio restricciones prc 
    prc_df = get_precios_restricciones(start_date, end_date, direccion)
    
    #get programas restricciones prog
    prog_df = get_programas_restricciones(start_date, end_date, lista_uprog, direccion)

    if prog_df.empty:
        print(f"No programas restricciones found for {start_date} to {end_date} for the following UPs: {lista_uprog} and direccion: {direccion}")
        print(f"Returning empty dataframes")
        return pd.DataFrame(), pd.DataFrame()
    
    # Ensure both FECHA and HORA columns are of the same type (integer) before merging
    prog_df['FECHA'] = pd.to_datetime(prog_df["FECHA"])
    prc_df['FECHA'] = pd.to_datetime(prc_df["FECHA"])
    prog_df['HORA'] = prog_df['HORA'].astype(int)
    prc_df['HORA'] = prc_df['HORA'].astype(int)
    
    #left join on fecha, hora sentido y uprog
    merged_df = pd.merge(prog_df, prc_df, left_on=['FECHA', 'HORA', 'SENTIDO', 'UPROG'], right_on=['FECHA', 'PERIODO', 'SENTIDO', 'UPROG'], how='left')
    
    # Calculate the ganancia
    merged_df['GANANCIA'] = round((merged_df['ENERGIA'] * merged_df['PRECIO']), 2)

    # Sum the ganancia per UP and round it
    total_ganancia = merged_df.groupby('UPROG')['GANANCIA'].sum().reset_index()
    total_ganancia['GANANCIA'] = total_ganancia['GANANCIA'].round(2)

    return total_ganancia, merged_df

def get_ganancias_rr(start_date: str, end_date: str, lista_uprog: List[str], direccion: Optional[List[str]] = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    #get programas rr prog
    prog_df = get_programas_rr(start_date, end_date, lista_uprog, direccion)
    print(prog_df.columns)

    #get precios rr prc
    prc_df = get_precios_rr(start_date, end_date)
    print(prc_df.columns)

    if prog_df.empty:
        print(f"No programas restricciones found for {start_date} to {end_date} for the following UPs: {lista_uprog} and direccion: {direccion}")
        print(f"Returning empty dataframes")
        return pd.DataFrame(), pd.DataFrame()
    
    # Ensure both FECHA and HORA columns are of the same type (integer) before merging
    prog_df['FECHA'] = pd.to_datetime(prog_df["FECHA"])
    prc_df['FECHA'] = pd.to_datetime(prc_df["FECHA"])
    prog_df['HORA'] = prog_df['HORA'].astype(int)
    prc_df['PERIODO'] = prc_df['PERIODO'].astype(int)

    #merge on fecha, hora, sentido y uprog
    merged_df = pd.merge(prog_df, prc_df, left_on=['FECHA', 'HORA'], right_on=['FECHA', 'PERIODO'], how='left')
    
    #filter for only RR in REDESPACHO column 
    filtered_df= merged_df[merged_df['REDESPACHO'] == 'RR']

    if filtered_df.empty:
        print(f"No programas rr found for REDESPACHO = RR")
        print(f"Returning empty dataframes")
        return pd.DataFrame(), pd.DataFrame()

    # Calculate the ganancia
    filtered_df['GANANCIA'] = round((filtered_df['ENERGIA'] * filtered_df['PRECIO']), 2)

    # Sum the ganancia per UP and round it
    total_ganancia = filtered_df.groupby('UPROG')['GANANCIA'].sum().reset_index()
    total_ganancia['GANANCIA'] = total_ganancia['GANANCIA'].round(2)

    return total_ganancia, merged_df

def get_ganancias_secundaria(start_date: str, end_date: str, lista_uprog: List[str], direccion: Optional[List[str]] = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Get programas secundaria (afrr) 
    prog_df = get_programas_secundaria(start_date, end_date, lista_uprog, direccion)
    
    # Get precios secundaria (afrr) prc
    prc_df = get_precios_secundaria(start_date, end_date)
    
    if prog_df.empty:
        print(f"No programas secundaria found for {start_date} to {end_date} for the following UPs: {lista_uprog} and direccion: {direccion}")
        print(f"Returning empty dataframes")
        return pd.DataFrame(), pd.DataFrame()
    
    # Ensure both FECHA and HORA columns are of the same type before merging
    prog_df['FECHA'] = pd.to_datetime(prog_df["FECHA"])
    prc_df['FECHA'] = pd.to_datetime(prc_df["FECHA"])
    prog_df['HORA'] = prog_df['HORA'].astype(int)
    prc_df['PERIODO'] = prc_df['PERIODO'].astype(int)

    # Merge on fecha and hora
    merged_df = pd.merge(prog_df, prc_df, left_on=['FECHA', 'HORA'], right_on=['FECHA', 'PERIODO'], how='left')
    
    # Calculate the ganancia
    merged_df['GANANCIA'] = round((merged_df['ENERGIA'] * merged_df['PRECIO']), 2)

    # Sum the ganancia per UP and round it
    total_ganancia = merged_df.groupby('UPROG')['GANANCIA'].sum().reset_index()
    total_ganancia['GANANCIA'] = total_ganancia['GANANCIA'].round(2)

    return total_ganancia, merged_df

def get_ganancias_terciaria(start_date: str, end_date: str, lista_uprog: List[str], direccion: Optional[List[str]] = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Get programas terciaria (mfrr) 
    prog_df = get_programas_terciaria(start_date, end_date, lista_uprog, direccion)
    print(prog_df.columns)
    print(prog_df.head())
    
    # Get precios terciaria (mfrr) prc
    prc_df = get_precios_terciaria(start_date, end_date, direccion)
    print(prc_df.columns)
    print(prc_df.head())

    if prog_df.empty:
        print(f"No programas terciaria found for {start_date} to {end_date} for the following UPs: {lista_uprog} and direccion: {direccion}")
        print(f"Returning empty dataframes")
        return pd.DataFrame(), pd.DataFrame()
    
    elif prc_df.empty:
        print(f"No precios terciaria found for {start_date} to {end_date} in  {direccion} direccion")
        print(f"Returning empty dataframes")
        return pd.DataFrame(), pd.DataFrame()
    
    # Ensure both FECHA and HORA columns are of the correct types before merging
    prog_df['FECHA'] = pd.to_datetime(prog_df['FECHA'])
    prc_df['FECHA'] = pd.to_datetime(prc_df['FECHA'])
    prog_df['HORA'] = prog_df['HORA'].astype(int)
    prc_df['PERIODO'] = prc_df['PERIODO'].astype(int)

    # Perform the left join
    merged_df = prog_df.merge(prc_df, 
                              how='left',
                              left_on=['FECHA', 'HORA', 'SENTIDO'],
                              right_on=['FECHA', 'PERIODO', 'SENTIDO'])

    # Apply the filters
    filtered_df = merged_df[(merged_df['REDESPACHO'] == 'TER')]

    if filtered_df.empty:
        print(f"No programas terciaria found for REDESPACHO = TER")
        print(f"Returning empty dataframes")
        return pd.DataFrame(), pd.DataFrame()

    # Calculate the ganancia
    filtered_df['GANANCIA'] = round((filtered_df['ENERGIA'] * filtered_df['PRECIO']), 2)

    # Sum the ganancia per UP and round it
    total_ganancia = filtered_df.groupby('UPROG')['GANANCIA'].sum().reset_index()
    total_ganancia['GANANCIA'] = total_ganancia['GANANCIA'].round(2)

    return total_ganancia, filtered_df

def get_ganancias_desvios(start_date: str, end_date: str, lista_uprog: List[str], direccion: Optional[List[str]] = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Get programas desvios
    prog_df = get_programas_desvios(start_date, end_date, lista_uprog, direccion)
    
    # Get precios desvios
    prc_df = get_precios_desvios(start_date, end_date, direccion)
    
    if prog_df.empty:
        print(f"No programas desvios found for {start_date} to {end_date} for the following UPs: {lista_uprog} and direccion: {direccion}")
        print(f"Returning empty dataframes")
        return pd.DataFrame(), pd.DataFrame()
    
    elif prc_df.empty:
        print(f"No precios desvios found for {start_date} to {end_date} in  {direccion} direccion")
        print(f"Returning empty dataframes")
        return pd.DataFrame(), pd.DataFrame()
    
    # Ensure both FECHA and HORA columns are of the correct types before merging
    prog_df['FECHA'] = pd.to_datetime(prog_df['FECHA'])
    prc_df['FECHA'] = pd.to_datetime(prc_df['FECHA'])
    prog_df['HORA'] = prog_df['HORA'].astype(str)
    prc_df['PERIODO'] = prc_df['PERIODO'].astype(str)

    # Apply the 3a, 3b filter before the merge
    prog_df = prog_df[~prog_df['HORA'].isin(['3a', '3b'])]

    # Perform the left join
    merged_df = prog_df.merge(prc_df, 
                              how='left',
                              left_on=['FECHA', 'HORA', 'SENTIDO'],
                              right_on=['FECHA', 'PERIODO', 'SENTIDO'])

    # Apply the DESV filter after the merge
    filtered_df = merged_df[merged_df['REDESPACHO'] == 'DESV']

    if filtered_df.empty:
        print(f"No programas desvios found for REDESPACHO = DESV")
        print(f"Returning empty dataframes")
        return pd.DataFrame(), pd.DataFrame()

    # Calculate the ganancia
    filtered_df['GANANCIA'] = round((filtered_df['ENERGIA'] * filtered_df['PRECIO']), 2)

    # Sum the ganancia per UP and round it
    total_ganancia = filtered_df.groupby('UPROG')['GANANCIA'].sum().reset_index()
    total_ganancia['GANANCIA'] = total_ganancia['GANANCIA'].round(2)

    return total_ganancia, filtered_df

def get_ganancias(indicador: str, start_date: str, end_date: str, lista_uprog: List[str], direccion: Optional[List[str]] = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Generic function to calculate gains for different indicators.
    
    Args:
        indicator (str): The indicator type ('mercado_diario', 'restricciones', 'rr', 'secundaria', 'terciaria', 'desvios')
        start_date (str): Start date for the query
        end_date (str): End date for the query
        lista_uprog (List[str]): List of UPROGs to filter
        direccion (Optional[List[str]]): Direction filter, if applicable
    
    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: Total gains per UPROG and detailed DataFrame
    """
    # Define indicator-specific functions and parameters

    
    config = configc.indicador_config.get(indicador)
    if not config:
        raise ValueError(f"Invalid indicator: {indicador}")
    
    # Get programas and precios
    prog_func = config['prog_func'] 
    prc_func = config['prc_func']

    if indicador in ["diario"]: #diario does not use direccion in either programas or precios
        prog_df = prog_func(start_date, end_date, lista_uprog)
        prc_df = prc_func(start_date, end_date)

    elif indicador in ["rr", "afrr"]: #rr and aafrr do not use direccion in precios
        prog_df = prog_func(start_date, end_date, lista_uprog, direccion)
        prc_df = prc_func(start_date, end_date)

    else: #all other indicators use direccion in both programas and precios
        prog_df = prog_func(start_date, end_date, lista_uprog, direccion)
        prc_df = prc_func(start_date, end_date, direccion)
    
    if prog_df.empty:
        print(f"No programas {indicador} found for {start_date} to {end_date} for the following UPs: {lista_uprog} and direccion: {direccion}")
        return pd.DataFrame(), pd.DataFrame()
    
    elif prc_df.empty:
        print(f"No precios {indicador} found for {start_date} to {end_date} in  {direccion} direccion")
        print(f"Returning empty dataframes")
        return pd.DataFrame(), pd.DataFrame()
    
    # Ensure correct data types
    for df in [prog_df, prc_df]:
        df['FECHA'] = pd.to_datetime(df['FECHA'])
        if 'HORA' in df.columns:
            df['HORA'] = df['HORA'].astype(int)
        if 'PERIODO' in df.columns:
            df['PERIODO'] = df['PERIODO'].astype(int)
    
    # Apply pre-merge filter if exists
    if config['filter_func']:
        prog_df = config['filter_func'](prog_df)
    
    # Merge DataFrames
    merged_df = pd.merge(prog_df, prc_df, left_on=config['merge_prog_cols'], right_on=config['merge_prc_cols'], how='left')
    
    # Apply additional filter if exists
    if config['additional_filter']:
        merged_df = config['additional_filter'](merged_df)


    #someitmes the additional filter will yield an empty df, in that case return two empty dfs
    if merged_df.empty: 
        if indicador in configc.indicador_config: 
            print(f"No programas {indicador} found when applying filter REDESPACHO == {indicador}")
            return pd.DataFrame(), pd.DataFrame()

    
    # Calculate gains
    merged_df['GANANCIA'] = round((merged_df['ENERGIA'] * merged_df['PRECIO']), 2)
    
    # Sum gains per UP
    total_ganancia = merged_df.groupby('UPROG')['GANANCIA'].sum().reset_index()
    total_ganancia['GANANCIA'] = total_ganancia['GANANCIA'].round(2)
    
    return total_ganancia, merged_df


def main( programas_i90: List[str], start_date:str, end_date:str, lista_uprog ): 
    pass

if __name__ == "main": 
    start_date = "" 
    end_date = ""

    programas_i90 = ["PBF", "PVP", "PHF1", "PHF2", "PHF3", "PHF4", "PHF5", "PHF6", "PHF7"]  