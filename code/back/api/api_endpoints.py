import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../negocio'))
from flask import Blueprint, jsonify, Response, request, Flask
from werkzeug.security import generate_password_hash, check_password_hash
import jwt
from hashlib import sha1
from datetime import datetime, timedelta
from utilidades.common import token_required, jsend_response_maker
import api.api_funciones as af  # Import the entire module
import pandas as pd
import json
from flask_cors import CORS
from io import StringIO
from negocio.funciones_consultas import Indicador, PBF, PVP, PHF, P48, RR, Secundaria, Terciaria, Restricciones, Desvios, Diario, Intradiario, IntradiarioContinuo

#Defining blueprint instances
#----------------------------------------------------------------------
#General
general_bp = Blueprint('general', __name__) #create blueprint for the general route

#summary statistics
summary_bp = Blueprint('summary', __name__) #create blueprint for the summary route

#UPs
up_bp = Blueprint('up', __name__, url_prefix='/up') #create blueprint for the up route

#Programas y ganancias por UP
programasUP_bp = Blueprint('programasUP', __name__, url_prefix='/up/programas') #create blueprint for the programas route per UP
gananciasUP_bp = Blueprint('gananciasUP', __name__, url_prefix='/up/ganancias') #create blueprint for the ganancias route per UP

#Programas y ganancias por UOF
programasUOF_bp = Blueprint('programasUOF', __name__, url_prefix='/uof/programas') #create blueprint for the programas route per UOF
gananciasUOF_bp = Blueprint('gananciasUOF', __name__, url_prefix='/uof/ganancias') #create blueprint for the ganancias route per UOF

#Precios de los diferentes mercados
precios_bp = Blueprint('precios', __name__, url_prefix='/precios') #create blueprint for the precios route

#Endpoint para obtener lista de UPs
#----------------------------------------------------------------------
@up_bp.route('/get-list', methods=['GET']) #url = /up/get-list
def get_up_list():
    """
    Endpoint to retrieve a list of UPs for a given market and date range.

    Query parameters:
    - fecha_inicial: The start date for the data retrieval.
    - fecha_final: The end date for the data retrieval.
    - mercado: The market for which to retrieve the UP list.

    Returns:
    - A JSON response with the list of UPs for the given market and date range.
    - On success: The requested UP data or a message if no data is found.
    - On error: An error message describing the issue.
    """

    # Retrieve query parameters
    fecha_inicial= request.args.get('fecha_inicial')
    fecha_final = request.args.get('fecha_final')
    mercado = request.args.get('mercado')

    # Check for missing parameters
    if not all([fecha_inicial, fecha_final, mercado]):
        return jsend_response_maker(status="fail", message="Missing required parameters", data={}), 400

    try:
        # Process the request
        indicador_class = af.get_indicador_class(mercado)
        indicador = indicador_class()
        up_list = indicador.get_lista_up(fecha_inicial, fecha_final)
        return jsend_response_maker(status="success", message="Data retrieved successfully", data={"up_list": up_list}), 200
    except Exception as e:
        return jsend_response_maker(status="error", message=f"An error occurred: {str(e)}", data={}), 500


#Endpoints para tarjeta de  summary statistics
#----------------------------------------------------------------------
@summary_bp.route('/summary', methods=["POST"]) #url = /summary
def handle_consulta_summary_statistics():
    """
    Endpoint to retrieve summary statistics for a given set of data.

    This endpoint processes incoming GET requests to fetch summary statistics for a provided dataset.
    It validates the input data, retrieves the relevant summary statistics, and returns the data in JSON format.

    Query parameters:
    - data (str): The data for which to retrieve summary statistics.

    Returns:
        JSON response containing the requested summary statistics.
        example:
        {
            "status": "success",
            "data": {
                "summary_stats": "summary_statistics"
            }
        }
    """
    data_json = request.json.get('data', '{}')

    if data_json == '{}':
        return jsend_response_maker(status="fail", message="No data provided", data={}), 400

    try:
    
        #our json data will always have one of the following keys: ganancias, programas or precios, retrieving all
        # and where there is no data for a key, we will pass None
        ganancias_json = data_json.get('ganancias', None)
        programas_json = data_json.get('programas', None)
        precios_json = data_json.get('precios', None)

        #filtering to only process valid keys, there will always be one valid key (either ganancias, programas or precios)
        valid_key = [json_data for json_data in [ganancias_json, programas_json, precios_json] if json_data is not None]

        #since there will always be one valid key, we can access the first element of the list
        df = pd.read_json(StringIO(valid_key[0])) #wrapped json in string io since read_json will de deprecated in future versions

        print(df.dtypes)

        #get summary stats of the dataframe
        summary_stats = Indicador.get_summary_stats(df)

        return jsend_response_maker(status="success", message="Data retrieved successfully", data={"summary_stats": summary_stats}), 200

    except Exception as e:
        return jsend_response_maker(status="error", message=f"An error occurred: {str(e)}", data={}), 500


#Endpoints para ganancias y programas por UP
#----------------------------------------------------------------------
@programasUP_bp.route('/<mercado>', methods=['POST']) #url = /up/programas/<mercado>
def handle_consulta_programa_up(mercado):
    """
    Handle requests for program data based on the specified market type.

    This endpoint processes incoming POST requests to fetch program data based on 
    user-provided parameters. It validates the input data, retrieves the relevant 
    program information, and returns the data in JSON format.

    Args:
        mercado (str): The market type for which to retrieve program data.

    Expected JSON payload structure:
    {
        "entradaAPI": {
            "fecha_inicial": "YYYY-MM-DD",
            "fecha_final": "YYYY-MM-DD",
            "up": "unit_name",
            "sentido": "direction",
            "agrupar": "grouping_option"
        }
    }

    Returns:
        JSON response containing:
        - On success: The requested program data or a message if no data is found.
        - On error: An error message describing the issue.

    Response structure follows JSend specification.
    example of a succesful response:    
    {
        "status": "success",
        "data": {
            "mercado": "mercado",
            "programas": "programas"
        },
        "message": "Successfully retrieved program data"
    }

    HTTP Status Codes:
        200: Successful request (including when no data is found)
        400: Bad request (invalid input data)
        500: Internal server error
    """
    # Extract data from the incoming JSON request
    data = request.json.get('entradaAPI', {})
    
    try:
        # Use the extract_request_data function for 'programasUP' to get the input parameters
        fecha_inicial, fecha_final, up, mercado, sentido, group_by = af.extract_request_data(data, "programasUP")
    except Exception as e:
        return jsend_response_maker(status="fail", message=f"Validation error: {str(e)}", data={}), 400

    try:
        # Get the indicator class based on the mercado
        mercado = mercado.lower().replace(" ", "") #make sure mercado is lowercase
        indicador_class = af.get_indicador_class(mercado)
        indicador = indicador_class()

        if indicador is None:
            return jsend_response_maker(status="fail", message=f"Validation error: No indicador class found for mercado: {mercado}", data={}), 400

        # Get the relevant program data based on the indicator class and input parameters
        result = af.obtener_programas(indicador, fecha_inicial, fecha_final, mercado, up, sentido)

        print(f"Result: {result}")

        # Check if result is empty
        if result is None or result.empty:
            return jsend_response_maker(status="success", message="No data found for the given parameters", data={"mercado": mercado, "programas": []}), 200

        # Apply grouping and filtering
        result_grouped = indicador.agrupar_consulta(result, group_by, "prog")

        print(f"Result grouped: {result_grouped}")

        filtered_result = indicador.filtrar_columnas(result_grouped, "prog")
        #conver fecha to str otherwise we get an error when converting to json if dtype period[ns]
        filtered_result ["FECHA"] = filtered_result["FECHA"].astype(str)

        print(f"Filtered result: {filtered_result}")
        print(filtered_result.info())
        print(filtered_result.dtypes)

        # Convert DataFrame to JSON format for the response
        json_result = filtered_result.to_json(orient='records', date_format='iso')

        print(f"JSON result: {json_result}")

        return jsend_response_maker(status="success", message="Data retrieved successfully", data={"mercado": mercado, "programas": json_result}), 200

    except Exception as e:
        # Log the exception for debugging
        app.logger.error(f"Error in handle_consulta_programa: {str(e)}")
        return jsend_response_maker(status="error", message=f"An unexpected error occurred: {str(e)}", data={}), 500

@gananciasUP_bp.route('/<mercado>', methods=['POST']) #url = /up/ganancias/<mercado>
def handle_consulta_ganancias_up(mercado):
    """
    Endpoint to retrieve ganancias data based on the specified program type.

    This function processes incoming requests to fetch ganancias data based on user provided
    parameters in the frontend dropdowns. It validates the input data which includes the following:

      - programa_type
      - the dates (fecha_inicial, fecha_final)
      - the up
      - the programa
      - the sentido (if the programa_type is not 'diario' or 'intradiario')

    It then retrieves the relevant ganancias information using the appropriate class methods based on the program type, 
    and returns the data in JSON format.

    Args:
        programa_type (str): The type of program to retrieve ganancias data for (e.g., 'rr', 'afrr', etc.).

    Returns:
        JSON response containing the requested ganancias data or an error message.
        example:
        {
            "status": "success",
            "data": {
                "mercado": "mercado",
                "ganancias": "ganancias",
                "ganancias_totales": "ganancias_totales"
            }
        }
    """
    data = request.json.get('entradaAPI', {})

    try:
        # Use the extract_request_data function for 'gananciasUP' to get the input parameters
        fecha_inicial, fecha_final, up, mercado, sentido, group_by = af.extract_request_data(data, "gananciasUP")
    except ValueError as e:
        return jsend_response_maker(status="fail", message=f"Validation error: {str(e)}", data={}), 400

    try:
        mercado = mercado.lower().replace(" ", "")
        # Get the indicator class based on the mercado
        indicador_class = af.get_indicador_class(mercado)
        indicador = indicador_class()

        if indicador is None:
            return jsend_response_maker(status="fail", message=f"Validation error: No indicator class found for mercado: {mercado}", data={}), 400

        # Get the relevant ganancias data based on the indicator class and input parameters
        total_ganancia, ganancia_df = af.obtener_ganancias(indicador, fecha_inicial, fecha_final, mercado, up, sentido)

        # Check if result is empty
        if ganancia_df is None or ganancia_df.empty:
            return jsend_response_maker(status="success", message="No data found for the given parameters", data={"mercado": mercado, "ganancias": [], "ganancias_totales": []}), 200

        # Apply grouping and filtering
        print(f"Group by: {group_by}")
        ganancia_df_grouped = indicador.agrupar_consulta(ganancia_df, group_by, "gan")
        print(f"Ganancia df grouped: {ganancia_df_grouped}")
        filtered_ganancia_df = indicador.filtrar_columnas(ganancia_df_grouped, "gan")
        print(f"Filtered ganancia df: {filtered_ganancia_df}")
        filtered_ganancia_df["FECHA"] = filtered_ganancia_df["FECHA"].astype(str)

        # Convert DataFrame to JSON format for the response
        ganancias_df_json_result = filtered_ganancia_df.to_json(orient='records', date_format='iso')

        #convert ganancias totales per UP to json
        ganancias_totales_json_result = total_ganancia.to_json(orient='records', date_format='iso')

        return jsend_response_maker(status="success", message="Data retrieved successfully", data={"mercado": mercado, "ganancias": ganancias_df_json_result, "ganancias_totales": ganancias_totales_json_result}), 200

    except Exception as e:
        # Log the exception for debugging
        app.logger.error(f"Error in handle_consulta_ganancias_up: {str(e)}")
        return jsend_response_maker(status="error", message=f"An unexpected error occurred: {str(e)}", data={}), 500

#Endpoints para precios
#----------------------------------------------------------------------
@precios_bp.route('/<mercado>', methods=['POST']) #url = /precios/<mercado>
def handle_consulta_precios(mercado):
    """
    Endpoint to get prices for a specific market.

    This endpoint handles POST requests to retrieve price data for a given market.
    It processes the incoming JSON request, extracts and validates the required parameters,
    and returns the price data for the specified market and time range.

    The endpoint supports various markets including diario, intradiario, restricciones,
    desvios, RR, AFRR, and MFRR. Some markets may require additional parameters like
    'sentido' (direction) or 'sesion' (session).

    Request JSON format:
    {
        "entradaAPI": {
            "fecha_inicial": "YYYY-MM-DD",
            "fecha_final": "YYYY-MM-DD",
            "mercado": "market_name",
            "sentido": "Subir" or "Bajar" (optional, required for some markets),
            "sesion": "session_number" (optional, required for intradiario market),
            "agrupar": "grouping_option"
        }
    }

    Returns:
        A tuple containing a JSON response and an HTTP status code.
        example of a succesful response:
        {
            "status": "success",
            "data": {
                "mercado": "mercado",
                "precios": "precios"
            }
        }
    Raises:
        400 Bad Request: If required parameters are missing or invalid.
        500 Internal Server Error: For any unexpected errors during processing.

    Note:
        This function relies on helper functions from the 'af' module for data extraction,
        indicator class retrieval, and price data fetching.
    """
    # Extract data from the incoming JSON request
    data = request.json.get('entradaAPI', {})
    
    try:
        # Use the extract_request_data function for 'precios' to get the input parameters
        fecha_inicial, fecha_final, _,  mercado, sentido, group_by = af.extract_request_data(data, "precios") #up is irrelevant for price hence "_"
    except ValueError as e:
        return jsend_response_maker(status="fail", message=f"Validation error: {str(e)}", data={}), 400

    try:
        mercado = mercado.lower().replace(" ", "")
        # Get the indicador class based on the mercado
        indicador_class = af.get_indicador_class(mercado)
        indicador = indicador_class()

        if indicador is None:
            return jsend_response_maker(status="fail", message=f"Validation error: No indicador class found for mercado: {mercado}", data={}), 400

        # Get the relevant price data based on the indicador class and input parameters
        result = af.obtener_precios(indicador, fecha_inicial, fecha_final, mercado, sentido)

        # Check if result is empty
        if  (result is None) or (result.empty):
            return jsend_response_maker(status="success", message="No data found for the given parameters", data={"mercado": mercado, "precios": []}), 200

        # Apply grouping and filtering
        result_grouped = indicador.agrupar_consulta(result, group_by, "prc")
        filtered_result = indicador.filtrar_columnas(result_grouped, "prc")
        filtered_result["FECHA"] = filtered_result["FECHA"].astype(str)

        # Convert DataFrame to JSON format for the response
        json_result = filtered_result.to_json(orient='records', date_format='iso')

        return jsend_response_maker(status="success", message="Data retrieved successfully", data={"mercado": mercado, "precios": json_result}), 200

    except Exception as e:
        # Log the exception for debugging
        app.logger.error(f"Error in handle_consulta_precios: {str(e)}")
        return jsend_response_maker(status="error", message=f"An unexpected error occurred: {str(e)}", data={}), 500

#Endpoints para programas y ganancias por UOF
#----------------------------------------------------------------------
@programasUOF_bp.route('/<mercado>', methods=['POST']) #url = /uof/programas/<mercado>
def handle_consulta_programa_uof(mercado):
    pass
    

@gananciasUOF_bp.route('/<mercado>', methods=['POST']) #url = /uof/ganancias/<mercado>
def handle_consulta_ganancias_uof(mercado):
    pass

# Endpoint de prueba
#----------------------------------------------------------------------
@general_bp.route('/', methods=['GET'])
def test():
    out = "API funcionando!"  
    return jsonify(out)


"""
#
# Listar simulaciones del usuario
#----------------------------------------------------------------------
@bp.route('/simulaciones', methods=['GET'])
def obtener_lista_simulaciones():
    modelo = request.args.get('modelo')
    simulaciones = negocio.obtener_lista_simulaciones(modelo)
    return jsonify(simulaciones)
"""

#Creacion de la app
#----------------------------------------------------------------------
#create the Flask app instance
app = Flask(__name__)  
CORS(app)

# Register blueprints within the app
app.register_blueprint(general_bp)
app.register_blueprint(programasUP_bp)
app.register_blueprint(gananciasUP_bp)
app.register_blueprint(programasUOF_bp)
app.register_blueprint(gananciasUOF_bp)
app.register_blueprint(precios_bp)
app.register_blueprint(up_bp)
app.register_blueprint(summary_bp)

if __name__ == '__main__':
    app.run(debug=True, port=5000)

