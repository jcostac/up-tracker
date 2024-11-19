import sys
import os

# Add the parent directory to the system path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config  # Ensure config.py is in the parent directory

"""
paths_for_consultas = {
    "rr": {
        "path_prog": f"{config.curated_i90}\\year\\PROG_RR.parquet",
        "path_prc": f"{config.curated_rr}\\year\\precios_rr.paquet",
        "path_prc2": f"{config.curated_i90}\\year\\PRE_RR.parquet"
    },
    "afrr": {
        "path_prog": f"{config.curated_i90}\\year\\PROG_SEC.parquet",
        "path_prc": f"{config.curated_afrr}\\year\\precios_secundaria.parquet"
    },
    "mfrr": {
        "path_prog": f"{config.curated_i90}\\year\\PROG_TERC.parquet",
        "path_prc": f"{config.curated_mfrr}\\year\\precios_terciaria.parquet",
        "path_prc2": f"{config.curated_i90}\\year\\PRE_TER_DES_TR.parquet"
    },
    "diario": {
        "path_prog": f"{config.curated_i90}\\year\\PROGRAMAS.parquet",
        "path_prc": f"{config.curated_diario}\\year\\precios_diario.parquet"
    },
    "intradiario": {
        "path_prog": f"{config.curated_i90}\\year\\PROGRAMAS.parquet",
        "path_prc": f"{config.curated_intradiario}\\year\\precios_intradiariio.parquet"
    },
    "restricciones": {
        "path_prog": f"{config.curated_i90}\\year\\RESULT_RES.parquet",
        "path_prc": f"{config.curated_i90}\\year\\PRE_RES_MD.parquet"
    },
    "desvios": {
        "path_prog": f"{config.curated_i90}\\year\\PROG_GES_DEV.parquet",
        "path_prc": f"{config.curated_i90}\\year\\precios_gestion_desvios.parquet"
    },
    "pbf": {
        "path_prog": f"{config.curated_i90}\\year\\PROGRAMAS.parquet",
    },
    "pvp": {
        "path_prog": f"{config.curated_i90}\\year\\PROGRAMAS.parquet",
    },
    "phf": {
        "path_prog": f"{config.curated_i90}\\year\\PROGRAMAS.parquet",
    },
    "p48": {
        "path_prog": f"{config.curated_i90}\\year\\P48.parquet"
    }
}
"""


test_local_paths = {
    "pbf": {
        "path_prog": "C:\\Users\\joaquin.costa\\Downloads\\parquets-i90-2023\\year\\PROGRAMAS.parquet"
    },  
    "pvp": {
        "path_prog": "C:\\Users\\joaquin.costa\\Downloads\\parquets-i90-2023\\year\\PROGRAMAS.parquet"
    }, 
    "phf": {
        "path_prog": "C:\\Users\\joaquin.costa\\Downloads\\parquets-i90-2023\\year\\PROGRAMAS.parquet"
    },                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
    "p48": {
        "path_prog": "C:\\Users\\joaquin.costa\\Downloads\\parquets-i90-2023\\year\\P48.parquet"
    },
    "diario": {
        "path_prc": "C:\\Users\\joaquin.costa\\Downloads\\OMIE\\precios\\year\\precios_diario.parquet"
    },
    "intradiario": {
        "path_prc": "C:\\Users\\joaquin.costa\\Downloads\\OMIE\\precios\\year\\precios_intradiario.parquet"
    },
    "rr": {
        "path_prog": "C:\\Users\\joaquin.costa\\Downloads\\parquets-i90-2023\\year\\PROG_RR.parquet",
        "path_prc": "C:\\Users\\joaquin.costa\\Downloads\\OMIE\\precios\\year\\precios_balance_rr.parquet",
        "path_prc2": "C:\\Users\\joaquin.costa\\Downloads\\parquets-i90-2023\\year\\PRE_RR.parquet"
    }, 
    "afrr": {
        "path_prog": "C:\\Users\\joaquin.costa\\Downloads\\parquets-i90-2023\\year\\PROG_SEC.parquet",
        "path_prc": "C:\\Users\\joaquin.costa\\Downloads\\OMIE\\precios\\year\\precios_banda_secundaria.parquet"
    },
    "mfrr": {
        "path_prog": "C:\\Users\\joaquin.costa\\Downloads\\parquets-i90-2023\\year\\PROG_TERC.parquet",
        "path_prc": "C:\\Users\\joaquin.costa\\Downloads\\OMIE\\precios\\year\\precios_terciaria.parquet",
        "path_prc2": "C:\\Users\\joaquin.costa\\Downloads\\parquets-i90-2023\\year\\PRE_TER_DES_TR.parquet"
    }, 
    "restricciones": {
        "path_prog": "C:\\Users\\joaquin.costa\\Downloads\\parquets-i90-2023\\year\\RESULT_RES.parquet",
        "path_prc": "C:\\Users\\joaquin.costa\\Downloads\\parquets-i90-2023\\year\\PRE_RES_MD.parquet"
    },
    "desvios": {
        "path_prog": "C:\\Users\\joaquin.costa\\Downloads\\parquets-i90-2023\\year\\PROG_GES_DESV.parquet",
        "path_prc": "C:\\Users\\joaquin.costa\\Downloads\\OMIE\\precios\\year\\precios_gestion_desvios.parquet"  # de momento no se descargan precios de desvios
    }
}

column_mapping = { #used in funciones_consultas.py to filter columns in the dataframes
            "prog": {
                "default": ["FECHA", "HORA", "ENERGIA", "UPROG"],
                "rr": ["SENTIDO"],
                "afrr": ["SENTIDO"],
                "mfrr": ["SENTIDO"],
                "restricciones": ["SENTIDO"],
                "desvios": ["SENTIDO"],
                "pbf": ["PROGRAMA"],
                "pvp": ["PROGRAMA"],
                "phf": ["PROGRAMA"],
                "p48": ["PROGRAMA"],
            },
            "prc": {
                "default": ["FECHA", "HORA", "PRECIO"],
                "rr": ["SENTIDO"], #TODO: add precios de subida y bajada
                "afrr": ["SENTIDO"],
                "mfrr": ["SENTIDO"],
                "restricciones": ["SENTIDO"],
                "desvios": ["SENTIDO"],
                "intradiario": ["SESION"],
                "diario": [],
            },
            "gan": {
                "default": ["FECHA", "HORA", "UPROG", "GANANCIA"],
                "rr": ["SENTIDO"],
                "afrr": ["SENTIDO"],
                "mfrr": ["SENTIDO"],
                "restricciones": ["SENTIDO"],
                "desvios": ["SENTIDO"],
                "intradiario": ["SESION"],
                "pbf": ["PROGRAMA"],
                "pvp": ["PROGRAMA"],
                "phf": ["PROGRAMA"],
                "p48": ["PROGRAMA"],
            }
        }

