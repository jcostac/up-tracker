# API Documentation

## Table of Contents
1. [Introduction](#introduction)
2. [Project Structure](#project-structure)
3. [Module Interactions](#module-interactions)
4. [API Endpoints](#api-endpoints)
5. [Key Components](#key-components)
6. [Setup and Installation](#setup-and-installation)
7. [Usage](#usage)
8. [Contributing](#contributing)

## Introduction

This API is designed to handle various operations related to energy market data, including programs, prices, and earnings calculations. It provides endpoints for retrieving and processing data for different market types and time periods.

## Project Structure

The API consists of two main Python files:
1. `api_endpoints.py`: Defines the Flask application and API routes.
2. `api_funciones.py`: Contains helper functions used by the API endpoints.

These files interact with other modules in the project, particularly those in the `negocio` directory.

## Module Interactions

### api_endpoints.py
- Imports necessary modules and classes from Flask and the project's custom modules.
- Defines blueprints for different API routes.
- Creates endpoint functions that handle incoming requests.
- Uses functions from `api_funciones.py` to process data and generate responses.

### api_funciones.py
- Imports classes from `negocio/funciones_consultas.py`.
- Provides helper functions used by the API endpoints, such as:
  - `get_indicador_class()`: Maps market types to their corresponding indicator classes.
  - `extract_request_data()`: Validates and extracts data from incoming requests.
  - `obtener_programas()`, `obtener_precios()`, `obtener_ganancias()`: Retrieve and process data based on input parameters.

### negocio/funciones_consultas.py
- Defines classes for different market indicators (e.g., PBF, PVP, RR, Secundaria, etc.).
- Each class implements methods for retrieving and processing specific types of market data.

## API Endpoints

The API provides the following main endpoint groups:

1. General: `/`
   - Test endpoint to check if the API is running.

2. Programs by UP (Unidad de Programación):
   - `/up/programas/<mercado>` (POST)
   - Retrieves program data for a specific market and UP.

3. Earnings by UP:
   - `/up/ganancias/<mercado>` (POST)
   - Calculates earnings for a specific market and UP.

4. Prices:
   - `/precios/<mercado>` (POST)
   - Retrieves price data for a specific market.

5. Programs by UOF (Unidad Operativa Física):
   - `/uof/programas/<mercado>` (POST)
   - Retrieves program data for a specific market and UOF.

6. Earnings by UOF:
   - `/uof/ganancias/<mercado>` (POST)
   - Calculates earnings for a specific market and UOF.

## Key Components

### Indicator Classes
The API uses various indicator classes (e.g., PBF, PVP, RR, Secundaria) to handle different types of market data. These classes are defined in `negocio/funciones_consultas.py` and are instantiated based on the requested market type.

### Request Data Extraction
The `extract_request_data()` function in `api_funciones.py` validates and extracts data from incoming requests, ensuring that all required fields are present and correctly formatted.

### Data Retrieval and Processing
Functions like `obtener_programas()`, `obtener_precios()`, and `obtener_ganancias()` in `api_funciones.py` handle the retrieval and initial processing of data based on the input parameters and the specific indicator class being used.

### Response Formatting
The API uses a custom `jsend_response_maker()` function to format responses according to the JSend specification, providing consistent and clear response structures.

Response always has the following structure:

{
  "status": "success" | "error" | "fail",
  "data": {...} | [...] | null,
  "message": "..."
}

## Setup and Installation

1. The most important thing is to have the `config_consultas.py` file with the correct paths to the databases, otherwise the API will not work since it will not be able to retrieve the data.


## Usage
Example request to the programs endpoint:


  "entradaAPI": {
    "fecha_inicial": "2023-06-21",
    "fecha_final": "2023-09-28",
    "up": "ACE3",
    "mercado": "MFRR",
    "sentido": "Subir, Bajar",
    "agrupar": "mes"
  }

Example response:

{
  "status": "success",
  "message": "Data retrieved successfully",
  "data": {
    "programas": pd.DataFrame
  }
  }



## Contributing
When contributing to this project:
1. Ensure any new endpoints follow the existing structure and naming conventions.
2. Update the `get_indicador_class()` function in `api_funciones.py` if adding new market types.
3. `funciones_consultas.py` is the basis of the API, any change here should be reflected in the API. The next important file to change is `api_funciones.py` to match the changes. 
Finally, the file `api_endpoints.py` should also be changed to match any changes in the other files that could affect the API.
4. Update this README file with any significant changes or additions to the API functionality.
