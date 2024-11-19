import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../negocio'))

from flask import Blueprint, jsonify, Response, request, Flask
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
from hashlib import sha1
from datetime import datetime, timedelta
import io
from utilidades.common import token_required, jsend_response_maker
from negocio.funciones_consultas import PBF, PVP, PHF, P48, RR, Secundaria, Terciaria, Restricciones, Desvios, Diario, Intradiario, IntradiarioContinuo
import pandas as pd
from typing import Optional, List

def get_indicador_class(mercado: str) -> object:
    """
    Get the indicator class based on the program type.

    This function returns the indicator class object based on the provided program type.
    It uses a dictionary to map program types to their corresponding classes.

    Args:
        mercado (str): The type of mercado to retrieve the indicator class for. 
    Returns:
        The indicator class object.
    """
    
    # Create a comprehensive map of all indicator classes
    # This map associates program types with their corresponding classes      
    indicador_map = {
        #diario
        ##energia
        'pbf': PBF,
        'pvp': PVP,
        ##precio
        "diario": Diario,

        #intradiario sesiones
        ##programa
        'phf1': PHF,
        'phf2': PHF,
        'phf3': PHF,
        'phf4': PHF,
        'phf5': PHF,
        'phf6': PHF,
        'phf7': PHF, 

        ##precio 
        "mi1": Intradiario,
        "mi2": Intradiario,
        "mi3": Intradiario,
        "mi4": Intradiario,
        "mi5": Intradiario,
        "mi6": Intradiario,
        "mi7": Intradiario,

        #intradiario continuo
        "intradiariocontinuo": IntradiarioContinuo, #TODO: implementar

        #restricciones 
        'restricciones': Restricciones, #needs sentido specification by user

        #final
        'p48': P48,
        
        #mercados ajuste
        ##programa
        # affr banda and asignacion are commented out for programas to avoid redunacy
        #'afrr banda': Secundaria,  #needs sentido specification only for programas
        #"afrr asignacion": Secundario #needs specification
        'mfrr': Terciaria, #needs sentido specification for programas

        ##precio de banda y asignacion  
        'afrrbanda': Secundaria,  #does not require sentido specification
        'afrrasignacion': Secundaria, #needs sentido specification by user for both precio and programas
        'mfrrasignacion': Terciaria, #needs sentido specification by user for both precio and programas


        # desvios --> RR 
        ##programa
        'desvios': Desvios, #needs sentido specification by user
        "rr": RR, #needs sentido specification by user

        #precios banda y asignacion
        'rrasignacion': RR, #needs sentido specification
        'desviosasignacion': Desvios, #needs sentido specification
        
    }  

    try:
        mercado = mercado.lower().strip()
        #print(f"mercado: {mercado}")

        indicador_class = indicador_map.get(mercado, None)
        #print(f"indicador_class: {indicador_class}")

        if indicador_class is None:
            raise Exception(f"Invalid mercado: {mercado}")
    
    except Exception as e:
        print(f"Error: {e}")
    
    return indicador_class

def extract_request_data(data: dict, consulta_type: str) -> tuple:  
    """
    Extract and validate data from the incoming JSON request.

    Args:
        data (dict): The incoming JSON request data.
        consulta_type (str): The type of consulta to extract data for, can take the different values: "programas", "precios", "ganancias"

    Returns:
        tuple: A tuple containing the extracted and validated data.
        (fecha_inicial, fecha_final, up, programa, sentido, agrupar)

    Raises:
    Exception: If required fields are missing or invalid.
    """

    try:
        # Extract data from the incoming JSON request
        fecha_inicial = data.get('fecha_inicial')
        fecha_final = data.get('fecha_final')
        up = data.get('up', None) #irrelevant for price
        mercado = data.get('mercado')
        sentido = data.get('sentido', None) #optional
        agrupar = data.get('agrupar')

        if consulta_type != "precios": #for programas and ganancias there is a need to specify up
            if not all([fecha_inicial, fecha_final, up, mercado, agrupar]):
                raise Exception("The following fields are required: fecha_inicial, fecha_final, UP, mercado, and agrupar.")
        else: #for precios there is no need to specify up
            if not all([fecha_inicial, fecha_final, mercado, agrupar]):
                raise Exception("The following fields are required: fecha_inicial, fecha_final, mercado, and agrupar.")
        
        if consulta_type == "programasUP" or consulta_type == "gananciasUP":
        # Validate 'sentido' if the program type is not in the  'diario', 'intradiario' or "final" group in my indicador map
            if mercado.lower() not in ["pbf", "pvp", "phf1", "phf2", "phf3", "phf4", "phf5", "phf6", "phf7", "p48"] and not sentido:
                raise Exception(f"Sentido is required for mercado: {mercado}")
            
        if consulta_type == "precios":   # Validate 'sentido' if the program type is not 'diario' or intradiario (sesion 1-7) or 'afrrbanda'
            if mercado.lower() not in ["diario", "mi1", "mi2", "mi3", "mi4", "mi5", "mi6", "mi7", "afrrbanda"] and not sentido:
                raise Exception(f"Sentido is required for mercado: {mercado}")
        
    except Exception as e:
        print(f"Error: {e}")
            
    return fecha_inicial, fecha_final, up, mercado, sentido, agrupar

def obtener_programas(indicador: object, fecha_inicial: str, fecha_final: str, mercado: str,  up: list[str], sentido: list[str]) -> pd.DataFrame:
    """
    Get the programas by based on the selected mercado and by inputting the fecha_inicial, fecha_final, up, and sentido (where applicable)

    Args:
        indicador (object): The indicator class instance.
        fecha_inicial (str): The initial date for the program.
        fecha_final (str): The final date for the program.
        up (list[str]): The UP for the program.
        mercado (str): The market for the program.
        sentido (list[str]): The direction for the program.
        agrupar (str): The grouping for the program.

    Returns:
        pd.DataFrame: A DataFrame containing the programs.
    """

    try: 
        print(f"Obtaining programs for indicador: {type(indicador).__name__}")

        #handle pbf, pvp and phf1-7 programas
        if isinstance(indicador, PBF) or isinstance(indicador, PVP)  or isinstance(indicador, P48): 
            result = indicador.get_programas(fecha_inicial, fecha_final, up)
            
        if isinstance(indicador, PHF):
            mercado = mercado.upper()
            programas_i90 = [mercado] #convert to uppercase since the programas are stored in uppercase in the database
            result  = indicador.get_programas(fecha_inicial, fecha_final, up, programas_i90)
        
        #handle programas de ajuste, desvios y restrcciones que requieren sentido
        elif isinstance(indicador, Restricciones) or isinstance(indicador, Desvios) or isinstance(indicador, RR) or isinstance(indicador, Secundaria) or isinstance(indicador, Terciaria):
            #print(f"Sentido: {sentido}")
            #eliminate any spaces from sentido
            sentido = [sentido.strip() for sentido in sentido]
            result = indicador.get_programas(fecha_inicial, fecha_final, up, sentido)

        if result is None or result.empty:
            print("No data found for the given parameters")
            return pd.DataFrame()
    
    except Exception as e:
        print(f"Error: {e}")

    return result

def obtener_precios(indicador: object, fecha_inicial: str, fecha_final: str, mercado: str, sentido: str, sesion: Optional[int] = None) -> pd.DataFrame:
    """
    Get the precios by based on the selected mercado and by inputting the fecha_inicial, fecha_final, sentido (where applicable), sesion (where applicable)

    Args:
        indicador (object): The indicator class instance.
        fecha_inicial (str): The initial date for the program.
        fecha_final (str): The final date for the program.
        mercado (str): The market for the program.
        sentido (str): The direction for the program.
        sesion (str): The session for the program.

    Returns:
        pd.DataFrame: A DataFrame containing the programs.
    """

    print(indicador)


    # Check if the mercado requires sentido specification   
    requires_sentido = mercado in ['rrasignacion', 'desviosasignacion', 'afrrasignacion', 'mfrrasignacion']

    # Check if the mercado does not require sentido
    not_requires_sentido = mercado in ['diario', 'afrrbanda']


    try:
        if not_requires_sentido: #diario, afrr capacidad, mi1-7 do not require sentido
            result = indicador.get_precios(fecha_inicial, fecha_final)
    
        elif requires_sentido: #restricciones, desvios, rr, afrr activacion y mfrr activacion requieren sentido
            result = indicador.get_precios(fecha_inicial, fecha_final, sentido)
            print(result)

        elif isinstance(indicador, Intradiario):
            sesion = [int(mercado[3:]) for mercado in mercado] #extract number after "mi", convert to int [int]
            result = indicador.get_precios(fecha_inicial, fecha_final, sesion) 

    except Exception as e:
        print(f"Error: {e}")
    
    return result

def obtener_ganancias(indicador: object, fecha_inicial: str, fecha_final: str, mercado: str, up: str, sentido: str) -> pd.DataFrame:
    """
    Get the ganancias by based on the selected mercado and by inputting the fecha_inicial, fecha_final, up, and sentido (where applicable)

    Args:
        indicador_class (object): The indicator class.
        fecha_inicial (str): The initial date for the program.
        fecha_final (str): The final date for the program.
        up (str): The UP for the program.
        sentido (str): The direction for the program.

    Returns:
        total_ganancias (float): The total ganancias fo the selected UP(s).
        ganacias_df (pd.DataFrame): A DataFrame containing the ganancias of the selected UP(s).
    """
    try:
        print(indicador)

        #PBF and PVP ganancias do not require sentido
        if isinstance(indicador, PBF) or isinstance(indicador, PVP):
            total_ganancias, ganacias_df = indicador.get_ganancias(fecha_inicial, fecha_final, [up])

        #restricciones, desvios, rr, afrr activacion and banda (None sentido for banda) and mfrr activacion require sentido
        elif isinstance(indicador, Restricciones) or isinstance(indicador, Desvios) or isinstance(indicador, RR)  or isinstance(indicador, Secundaria) or isinstance(indicador, Terciaria):
            sentido = sentido.replace(" ", "")
            
            if sentido == "Subir,Bajar":
                total_ganancias, ganacias_df = indicador.get_ganancias(fecha_inicial, fecha_final, [up], ["Subir", "Bajar"])
        
            else:
                total_ganancias, ganacias_df = indicador.get_ganancias(fecha_inicial, fecha_final, [up], [sentido])
        
        #intradiario earnings require sesion
        elif isinstance(indicador, Intradiario):
            sesion = int(mercado[3:]) #extract number after "mi", convert to int
            total_ganancias, ganacias_df = indicador.get_ganancias(fecha_inicial, fecha_final, [up], sesion)

        elif isinstance(indicador, P48):
            #TODO: implementar
            total_ganancias, ganacias_df = indicador.get_ganancias(fecha_inicial, fecha_final, [up])
        else:
            raise Exception(f"Invalid program type: {mercado}")
        
        if ganacias_df is None or ganacias_df.empty:
            print("No data found for the given parameters")
            return pd.DataFrame(), pd.DataFrame()
        
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame(), pd.DataFrame()
    
    return total_ganancias, ganacias_df

def obtener_lista_up(indicador: object, fecha_inicial: str, fecha_final: str) -> List[str]:
    """
    Get the list of UPs for the given UOF.

    Args:
        indicador (object): The indicador class.
        fecha_inicial (str): The initial date for the program.
        fecha_final (str): The final date for the program.

    Returns:
        List[str]: A list of UPs for the given UOF.
    """

    print(f"Obtaining UP list for indicador: {type(indicador).__name__}")

    try:
        result = indicador.get_lista_up(fecha_inicial, fecha_final)
        return result

    except Exception as e:
        print(f"Error: {e}")
        return []

def handle_multiple_up(up: str) -> List[str]:
    """
    Reformat input string for more than one UP to appropriate format for passing onto the indicador methods
    """
    pass
   
    
if __name__ == "__main__":
    print(get_indicador_class("pbf"))
    print(obtener_programas(Secundaria(), "2023-06-21", "2023-09-28", "afrrbanda", "ACE3", "Subir"))
