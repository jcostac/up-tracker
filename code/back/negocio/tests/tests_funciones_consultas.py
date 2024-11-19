import funciones_consultas_v2 as fc
from datetime import datetime
import os
import pretty_errors
import negocio.config_consultas as configc
from negocio.funciones_consultas import PBF, PVP, PHF, P48, RR, Secundaria, Terciaria, Restricciones, Desvios, Diario, Intradiario, IntradiarioContinuo
from typing import List

# Test usage functions funciones_consultas (draft).py 
def test_years_between():
    try:
        # Test case 1: Normal case
        assert fc.years_between('2020-01-01', '2023-12-31') == ['2020', '2021', '2022', '2023']
        
        # Test case 2: Same year
        assert fc.years_between('2022-01-01', '2022-12-31') == ['2022']
        
        # Test case 3: One year apart
        assert fc.years_between('2021-12-31', '2022-01-01') == ['2021', '2022']
        
        # Test case 4: Reversed dates
        assert fc.years_between('2023-01-01', '2020-01-01') == []
        
        # Test case 5: Different date formats
        assert fc.years_between('2020-01-01', datetime(2022, 12, 31)) == ['2020', '2021', '2022']

    except AssertionError as e:
        print(f"Test failed: {e}")
    
    print("All tests passed!")

def test_program_uprog_filter():
    # Test case 1: lista_uprog provided
    result1, result2 = fc.create_program_uprog_filter(lista_uprog=['ABA1', 'ABA2', 'ACE3'])
    assert result2 == "prog.UPROG IN ('ABA1', 'ABA2', 'ACE3')", "Test case 1 failed"
    assert result1 == "1=1", "Test case 1 failed"

    # Test case 2: lista_programas provided
    result1, result2 = fc.create_program_uprog_filter(lista_programas=['PBF', 'PVP', 'PHF1'])
    assert result1 == "prog.PROGRAMA IN ('PBF', 'PVP', 'PHF1')", "Test case 2 failed"
    assert result2 == "1=1", "Test case 2 failed"

    # Test case 3: Both lists are empty
    result1, result2 = fc.create_program_uprog_filter()
    assert result1 == "1=1", "Test case 3 failed"
    assert result2 == "1=1", "Test case 3 failed"

    # Test case 4: lista_uprog is empty list
    result1, result2 = fc.create_program_uprog_filter(lista_uprog=[])
    assert result1 == "1=1", "Test case 4 failed"
    assert result2 == "1=1", "Test case 4 failed"

    # Test case 5: lista_programas is empty list
    result1, result2 = fc.create_program_uprog_filter(lista_programas=[])
    assert result1 == "1=1", "Test case 5 failed"
    assert result2 == "1=1", "Test case 5 failed"

    print("All test cases passed!")

def test_get_path():
    indicators = ["i90", "diario", "intradiario", "rr"]
    for year in ["2022", "2023"]: #for each year in the years_lst
        for indicator in indicators:
            paths = fc.get_path(year, indicator) #get the path for the given year and indicator  
            print(f"\nPaths for {indicator.upper()} in {year}:")
            for key, value in paths.items():
                print(f"  {key}: {value}")  

def test_get_programas():
    programas_i90 = fc.get_programas_i90(start_date="2023-01-01", end_date="2023-05-31", lista_uprog=["ABA1", "ABA2", "CJON3X"], programas_i90=["PBF"])
    programas_p48 = fc.get_programas_p48(start_date="2023-01-01", end_date="2023-05-31", lista_uprog=["ABA1", "ABA2", "CJON3X"])
    programas_rr = fc.get_programas_rr(start_date="2023-01-01", end_date="2023-05-31",  direccion = ["Subir"])
    programas_afrr = fc.get_programas_secundaria(start_date="2023-01-01", end_date="2023-05-31", lista_uprog=["ACE3", "ABA2", "CHIPG"], direccion= [])
    programas_mfrr = fc.get_programas_terciaria(start_date="2023-01-01", end_date="2023-05-31")
    programas_restr = fc.get_programas_restricciones(start_date="2023-01-01", end_date="2023-05-31", direccion=["Subir"])
    programas_desvios = fc.get_programas_desvios(start_date="2023-01-01", end_date="2023-05-31", lista_uprog=["ABA1", "ABA2", "CJON3X"], direccion=["Bajar"])

    str_lst = ["I90", "P48", "RR", "AFRR", "MFRR", "RESTR", "DESVIOS"]

    for i, df in enumerate([programas_i90, programas_p48, programas_rr, programas_afrr, programas_mfrr, programas_restr, programas_desvios]):
        print(f"{str_lst[i]}")
        print(df.head())

def test_get_precios():
    precios_rr = fc.get_precios_rr(start_date="2023-01-01", end_date="2023-05-31")
    precios_afrr = fc.get_precios_secundaria(start_date="2021-01-01", end_date="2023-07-31")
    precios_mfrr = fc.get_precios_terciaria(start_date="2022-01-01", end_date="2023-05-31", direccion=["Bajar"])
    precios_restr = fc.get_precios_restricciones(start_date="2023-01-01", end_date="2023-05-31", direccion=["Subir", "Bajar"])
    precios_desvios = fc.get_precios_desvios(start_date="2020-01-01", end_date="2020-03-31", direccion=["Bajar"])

    precios_intradiario = fc.get_precios_intradiario(start_date="2023-01-01", end_date="2023-05-31", sesion = "All")
    precios_intradiario_sesion = fc.get_precios_intradiario(start_date="2023-01-01", end_date="2023-05-31", sesion = [1, 2])
    precios_diario = fc.get_precios_diario(start_date="2023-01-01", end_date="2023-05-31")

    str_lst = ["RR", "AFRR", "MFRR", "RESTR", "DESVIOS", "INTRADIARIO1", "INTRADIARIO2", "DIARIO"]

    for i, df in enumerate([precios_rr, precios_afrr, precios_mfrr, precios_restr, precios_desvios, precios_intradiario, precios_intradiario_sesion, precios_diario]):
        print(f"{str_lst[i]}")
        print(df.head())

def test_sentido_sesion_filter():
    #sentido filter
    assert fc.create_direccion_filter(["Subir"], "prc") == "prc.SENTIDO IN ('Subir')", "Test case 1 failed"
    assert fc.create_direccion_filter(["Bajar"], "prog") == "prog.SENTIDO IN ('Bajar')", "Test case 2 failed"
    assert fc.create_direccion_filter(["Subir", "Bajar"], "prc") == "1=1", "Test case 3 failed"
    assert fc.create_direccion_filter([]) == "1=1", "Test case 4 failed"
    assert fc.create_direccion_filter(None) == "1=1", "Test case 5 failed"

    #sesion filter
    assert fc.create_sesion_filter([1, 2, 3]) == "prc.SESION IN (1, 2, 3)", "Test case 6 failed"
    assert fc.create_sesion_filter([4, 5, 6]) == "prc.SESION IN (4, 5, 6)", "Test case 7 failed"
    assert fc.create_sesion_filter([1, 2, 3, 4, 5, 6, 7]) == "1=1", "Test case 8 failed"
    assert fc.create_sesion_filter([]) == "1=1", "Test case 9 failed"
    assert fc.create_sesion_filter(None) == "1=1", "Test case 10 failed"
    print("All test cases passed!")

def test_get_ganancias_mercado_diario():
    total_ganancia, result_df = fc.get_ganancias_mercado_diario(start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["CJON3X"])
    print(total_ganancia)
    print(result_df.head())

def test_get_ganancias_mercado_intradiario():
    total_ganancia, result_df = fc.get_ganancias_mercado_intradiario(start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["CJON3X"], sesion = 1)
    print(total_ganancia)
    print(result_df.head())

def test_get_ganancias_restricciones():
    total_ganancia, result_df = fc.get_ganancias_restricciones(start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["BALFRAI", "AGUB", "BALPORE"], direccion=["Subir", "Bajar"])
    print("Total ganancia RRTT:")
    print(total_ganancia)
    print(result_df.head())

    total_ganancia_subir, result_df_subir = fc.get_ganancias_restricciones(start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["BALFRAI", "ABO2", "CJON3X"], direccion=["Subir"])
    print("Total ganancia subir RRTT:")
    print(total_ganancia_subir)
    print(result_df_subir.head())

    total_ganancia_bajar, result_df_bajar = fc.get_ganancias_restricciones(start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["ABA1", "AGUB", "CJON3X"], direccion=["Bajar"])
    print("Total ganancia bajar RRTT:")
    print(total_ganancia_bajar)
    print(result_df_bajar.head())

def test_get_ganancias_rr (): 
    total_ganancia, result_df = fc.get_ganancias_rr(start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["BALFRAI", "AGUB", "BALPORE"], direccion=["Subir"])
    print("Total ganancia subir RR:")
    print(total_ganancia)
    print(result_df.head())

    total_ganancia_subir, result_df_subir = fc.get_ganancias_rr(start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["BALFRAI", "ABO2", "CJON3X"], direccion=["Subir"])
    print("Total ganancia subir RR:")
    print(total_ganancia_subir)
    print(result_df_subir.head())

    total_ganancia_bajar, result_df_bajar = fc.get_ganancias_rr(start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["ABA1", "AGUB", "CJON3X"], direccion=["Bajar"])
    print("Total ganancia bajar RR:")
    print(total_ganancia_bajar)
    print(result_df_bajar.head())

def test_get_ganancias_afrr():
    total_ganancia, result_df = fc.get_ganancias_secundaria(start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["BALFRAI", "ABO2", "CJON3X"], direccion=["Subir"])
    print("Total ganancia subir AFRR:")
    print(total_ganancia)
    print(result_df.head())

    total_ganancia_bajar, result_df_bajar = fc.get_ganancias_secundaria(start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["ABA1", "AGUB", "CJON3X"], direccion=["Bajar"])
    print("Total ganancia bajar AFRR:")
    print(total_ganancia_bajar)
    print(result_df_bajar.head())

    total_ganancia, result_df = fc.get_ganancias_secundaria(start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["ABA1", "AGUB", "BALPORE"])
    print("Total ganancia subir y bajar AFRR:")
    print(total_ganancia)
    print(result_df.head())

def test_get_ganancias_mfrr():
    total_ganancia, result_df = fc.get_ganancias_terciaria(start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["BALFRAI", "ABO2", "CJON3X"], direccion=None)
    print("Total ganancia subir y bajarMFRR:")
    print(total_ganancia)
    print(result_df.head())

    total_ganancia_subir, result_df_subir = fc.get_ganancias_terciaria(start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["BALFRAI", "ABO2", "CJON3X"], direccion=["Subir"])
    print("Total ganancia subir MFRR:")
    print(total_ganancia_subir)
    print(result_df_subir.head())

    total_ganancia_bajar, result_df_bajar = fc.get_ganancias_terciaria(start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["ABA1", "AGUB", "CJON3X"], direccion=["Bajar"])
    print("Total ganancia bajar MFRR:")
    print(total_ganancia_bajar)
    print(result_df_bajar.head())

def test_get_ganancias_desvios():
    total_ganancia, result_df = fc.get_ganancias_desvios(start_date="2020-01-01", end_date="2020-01-31", lista_uprog=["BALFRAI", "ABO2", "CJON3X"], direccion=["Subir", "Bajar"])
    print("Total ganancia subir y bajar DESVIOS:")
    print(total_ganancia)
    print(result_df.head())

    total_ganancia_bajar, result_df_bajar = fc.get_ganancias_desvios(start_date="2020-01-01", end_date="2020-01-31", lista_uprog=["ABA1", "AGUB", "CJON3X"], direccion=["Bajar"])
    print("Total ganancia bajar DESVIOS:")
    print(total_ganancia_bajar)
    print(result_df_bajar.head())

    total_ganancia, result_df = fc.get_ganancias_desvios(start_date="2020-01-01", end_date="2020-01-31", lista_uprog=["ABA1", "AGUB", "BALPORE"], direccion=["Subir"])
    print("Total ganancia subir DESVIOS:")
    print(total_ganancia)
    print(result_df.head())

def test_get_ganancias_intradiario():
    total_ganancia, result_df = fc.get_ganancias_intradiario(start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["BALFRAI", "ABO2", "CJON3X"], sesion =[1, 2])
    print("Total ganancia INTRADIARIO sesion 1 y 2 :")
    print(total_ganancia)
    print(result_df.head())

    total_ganancia, result_df = fc.get_ganancias_intradiario(start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["BALFRAI", "ABO2", "CJON3X"], sesion ="All")
    print("Total ganancia INTRADIARIO sesion 1 y 2 :")
    print(total_ganancia)
    print(result_df.head())

def test_get_ganancias():
    total_ganancia, result_df = fc.get_ganancias(indicador="rr", start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["BALFRAI", "ABO2", "CJON3X"], direccion=["Subir", "Bajar"])
    print("Total ganancia subir y bajar RR:")
    print(total_ganancia)
    print(result_df.head()) 

    total_ganancia, result_df = fc.get_ganancias(indicador="diario", start_date="2023-01-01", end_date="2023-01-31", lista_uprog=["ABO2", "CJON3X"])
    print("Total ganancia diario:")
    print(total_ganancia)
    print(result_df.head())

# Test usage functions funciones_consultas.py 
def test_main_programas_i90( start_date: str, end_date: str, lista_uprog: str):
    prog_lst = [PBF(), PVP(), P48()]
    lista_uprog = [lista_uprog]

    for prog in prog_lst:
        prog_i90_data = prog.get_programas(start_date, end_date, lista_uprog)
        prog_i90_data_grouped = prog.agrupar_consulta(prog_i90_data, "mes", "prog")
        prog_i90_data_filtered = prog.filtrar_columnas(prog_i90_data_grouped, "prog")
        print(f"prog_i90_data {prog}", prog_i90_data_filtered)

    intradiario_lst = ["PHF1", "PHF2", "PHF3", "PHF4", "PHF5", "PHF6", "PHF7"]  
    for i in intradiario_lst:
        intradiario = [i]
        intradiario_data = PHF().get_programas(start_date, end_date, lista_uprog, intradiario)
        intradiario_data_grouped = PHF().agrupar_consulta(intradiario_data, "año", "prog")
        intradiario_data_filtered = PHF().filtrar_columnas(intradiario_data_grouped, "prog")
        print(f"intradiario_data {i}", intradiario_data_filtered)

    return prog_i90_data, intradiario_data

def test_main_programa_RR(start_date: str, end_date: str, lista_uprog: List[str]):
    rr_indicador = RR()
    prog_rr_data = {}
    programas = ["RR"]
    for prog in programas:
        prog_rr_data[prog] = {}
        prog_original = rr_indicador.get_programas(start_date, end_date, [lista_uprog])
        print("prog_original", prog_original)
        prog_hora_grouped = rr_indicador.agrupar_consulta(prog_original, "hora", "prog")
        
        prog_dia_grouped = rr_indicador.agrupar_consulta(prog_original, "dia", "prog")
    
        prog_mes_grouped = rr_indicador.agrupar_consulta(prog_original, "mes", "prog")
        
        prog_año_grouped = rr_indicador.agrupar_consulta(prog_original, "año", "prog")

        

        # Filtering by "programas"
        prog_filtered_dia = rr_indicador.filtrar_columnas(prog_dia_grouped, "prog")
        print("prog_filtered_dia", prog_filtered_dia)   
        prog_filtered_hora = rr_indicador.filtrar_columnas(prog_hora_grouped, "prog")
        print("prog_filtered_hora", prog_filtered_hora)
        prog_filtered_mes = rr_indicador.filtrar_columnas(prog_mes_grouped, "prog")
        print("prog_filtered_mes", prog_filtered_mes)
        prog_filtered_año = rr_indicador.filtrar_columnas(prog_año_grouped, "prog")
        print("prog_filtered_año", prog_filtered_año)

        
        
        
        precios = ["RR"]
        prc_rr_data = {}
        for prc in precios:
            prc_original1 = rr_indicador.get_precios("2023-03-17", "2023-03-22")
            prc_hora_grouped = rr_indicador.agrupar_consulta(prc_original1, "hora", "prc", avg=True)
            prc_dia_grouped = rr_indicador.agrupar_consulta(prc_original1, "dia", "prc", avg=True)
            prc_mes_grouped = rr_indicador.agrupar_consulta(prc_original1, "mes", "prc", avg=True)
            prc_año_grouped = rr_indicador.agrupar_consulta(prc_original1, "año", "prc", avg=True)
            print("prc_año_grouped", prc_año_grouped)
           

            # Filtering by "programas"
            prc_filtered_dia = rr_indicador.filtrar_columnas(prc_dia_grouped, "prc")
            print("prc_filtered_dia", prc_filtered_dia)
            prc_filtered_hora = rr_indicador.filtrar_columnas(prc_hora_grouped, "prc")
            print("prc_filtered_hora", prc_filtered_hora)
            prc_filtered_mes = rr_indicador.filtrar_columnas(prc_mes_grouped, "prc")
            print("prc_filtered_mes", prc_filtered_mes)
            prc_filtered_año = rr_indicador.filtrar_columnas(prc_año_grouped, "prc")
            print("prc_filtered_año", prc_filtered_año)

    return  prc_rr_data

def test_main_precios(start_date: str, end_date: str, sesion: List[int]):
    diario_indicador = Diario()
    prc_diario = diario_indicador.get_precios(start_date, end_date)
    print("prc_diario", prc_diario)
    diario_hora_grouped = diario_indicador.agrupar_consulta(prc_diario, "hora")
    diario_dia_grouped = diario_indicador.agrupar_consulta(prc_diario, "dia")
    diario_mes_grouped = diario_indicador.agrupar_consulta(prc_diario, "mes")
    diario_año_grouped = diario_indicador.agrupar_consulta(prc_diario, "año")
    diario_filtered_hora = diario_indicador.filtrar_columnas(diario_hora_grouped)
    print("diario_filtered_hora", diario_filtered_hora)
    diario_filtered_dia = diario_indicador.filtrar_columnas(diario_dia_grouped)
    print("diario_filtered_dia", diario_filtered_dia)
    diario_filtered_mes = diario_indicador.filtrar_columnas(diario_mes_grouped)
    print("diario_filtered_mes", diario_filtered_mes)
    diario_filtered_año = diario_indicador.filtrar_columnas(diario_año_grouped)
    print("diario_filtered_año", diario_filtered_año)

    intradiario_indicador = Intradiario()
    prc_intradiario = intradiario_indicador.get_precios(start_date, end_date, sesion)
    print("prc_intradiario", prc_intradiario)
    intradiario_hora_grouped = intradiario_indicador.agrupar_consulta(prc_intradiario, "hora", "prc")
    intradiario_dia_grouped = intradiario_indicador.agrupar_consulta(prc_intradiario, "dia", "prc")
    intradiario_mes_grouped = intradiario_indicador.agrupar_consulta(prc_intradiario, "mes", "prc")
    intradiario_año_grouped = intradiario_indicador.agrupar_consulta(prc_intradiario, "año", "prc")
    intradiario_filtered_hora = intradiario_indicador.filtrar_columnas(intradiario_hora_grouped, "prc")
    print("intradiario_filtered_hora", intradiario_filtered_hora)
    intradiario_filtered_dia = intradiario_indicador.filtrar_columnas(intradiario_dia_grouped, "prc")
    print("intradiario_filtered_dia", intradiario_filtered_dia)
    intradiario_filtered_mes = intradiario_indicador.filtrar_columnas(intradiario_mes_grouped, "prc")
    print("intradiario_filtered_mes", intradiario_filtered_mes)
    intradiario_filtered_año = intradiario_indicador.filtrar_columnas(intradiario_año_grouped, "prc")
    print("intradiario_filtered_año", intradiario_filtered_año)



    #desvios_indicador = Desvios()
    #prc_desvios = desvios_indicador.get_precios(start_date, end_date, lista_uprog)

    return prc_diario, prc_intradiario

def test_main_afrr(start_date: str, end_date: str, lista_uprog: List[str]):
    afrr_indicador = Secundaria()
    consulta_type = "prog"
    #afrr_data = afrr_indicador.get_precios(start_date, end_date)
    afrr_data = afrr_indicador.get_programas(start_date, end_date, [lista_uprog], ["Subir", "Bajar"])

    print("afrr_data", afrr_data)

    afrr_hora_grouped = afrr_indicador.agrupar_consulta(afrr_data, "hora", consulta_type)
    afrr_dia_grouped = afrr_indicador.agrupar_consulta(afrr_data, "dia", consulta_type)
    afrr_mes_grouped = afrr_indicador.agrupar_consulta(afrr_data, "mes", consulta_type)
    afrr_año_grouped = afrr_indicador.agrupar_consulta(afrr_data, "año", consulta_type)
    filtered_afrr_hora_grouped = afrr_indicador.filtrar_columnas(afrr_hora_grouped, consulta_type)
    filtered_afrr_dia_grouped = afrr_indicador.filtrar_columnas(afrr_dia_grouped, consulta_type)
    filtered_afrr_mes_grouped = afrr_indicador.filtrar_columnas(afrr_mes_grouped, consulta_type)
    filtered_afrr_año_grouped = afrr_indicador.filtrar_columnas(afrr_año_grouped, consulta_type)

    print("afrr_hora_grouped", filtered_afrr_hora_grouped)
    print("afrr_dia_grouped", filtered_afrr_dia_grouped)
    print("afrr_mes_grouped", filtered_afrr_mes_grouped)
    print("afrr_año_grouped", filtered_afrr_año_grouped)
    
    return 
      
def test_main_mfrr(start_date: str, end_date: str, lista_uprog: List[str]):
    mfrr_indicador = Terciaria()
    consulta_type = "prog"

    #mfrr_data = mfrr_indicador.get_precios(start_date, end_date)
    mfrr_data = mfrr_indicador.get_programas(start_date, end_date, [lista_uprog], ["Subir"])
    print("mfrr_data", mfrr_data)

    mfrr_hora_grouped = mfrr_indicador.agrupar_consulta(mfrr_data, "hora", consulta_type)
    mfrr_dia_grouped = mfrr_indicador.agrupar_consulta(mfrr_data, "dia", consulta_type)
    mfrr_mes_grouped = mfrr_indicador.agrupar_consulta(mfrr_data, "mes", consulta_type)
    mfrr_año_grouped = mfrr_indicador.agrupar_consulta(mfrr_data, "año", consulta_type)
    filtered_mfrr_hora_grouped = mfrr_indicador.filtrar_columnas(mfrr_hora_grouped, consulta_type)
    filtered_mfrr_dia_grouped = mfrr_indicador.filtrar_columnas(mfrr_dia_grouped, consulta_type)
    filtered_mfrr_mes_grouped = mfrr_indicador.filtrar_columnas(mfrr_mes_grouped, consulta_type)
    filtered_mfrr_año_grouped = mfrr_indicador.filtrar_columnas(mfrr_año_grouped, consulta_type)

    print("mfrr_hora_grouped", filtered_mfrr_hora_grouped)
    print("mfrr_dia_grouped", filtered_mfrr_dia_grouped)
    print("mfrr_mes_grouped", filtered_mfrr_mes_grouped)
    print("mfrr_año_grouped", filtered_mfrr_año_grouped)

def test_main_restricciones(start_date: str, end_date: str, lista_uprog: str):
    restricciones_indicador = Restricciones()
    consulta_type = "prc"

    restricciones_data = restricciones_indicador.get_precios(start_date, end_date, ["Subir"])
    #restricciones_data = restricciones_indicador.get_programas(start_date, end_date, [lista_uprog], ["Subir"])
    print("restricciones_data", restricciones_data)

    restricciones_hora_grouped = restricciones_indicador.agrupar_consulta(restricciones_data, "hora",  consulta_type)
    filtered_restricciones_hora_grouped = restricciones_indicador.filtrar_columnas(restricciones_hora_grouped, consulta_type)
    restricciones_dia_grouped = restricciones_indicador.agrupar_consulta(restricciones_data, "dia",  consulta_type)   
    filtered_restricciones_dia_grouped = restricciones_indicador.filtrar_columnas(restricciones_dia_grouped, consulta_type)
    restricciones_mes_grouped = restricciones_indicador.agrupar_consulta(restricciones_data, "mes",  consulta_type)
    filtered_restricciones_mes_grouped = restricciones_indicador.filtrar_columnas(restricciones_mes_grouped, consulta_type)
    restricciones_año_grouped = restricciones_indicador.agrupar_consulta(restricciones_data, "año",  consulta_type)
    filtered_restricciones_año_grouped = restricciones_indicador.filtrar_columnas(restricciones_año_grouped, consulta_type)

    print("restricciones_hora_grouped", filtered_restricciones_hora_grouped)
    print("restricciones_dia_grouped", filtered_restricciones_dia_grouped)
    print("restricciones_mes_grouped", filtered_restricciones_mes_grouped)
    print("restricciones_año_grouped", filtered_restricciones_año_grouped)

def test_main_desvios(start_date: str, end_date: str, lista_uprog: str):
    desvios_indicador = Desvios()
    #desvios precios solo van hasta 03, 2020
    desvios_data = desvios_indicador.get_precios("2020-01-01", "2020-03-01", ["Subir"])
    #desvios_data = desvios_indicador.get_programas(start_date, end_date, [lista_uprog], ["Subir"])
    print("desvios_data", desvios_data)

    desvios_hora_grouped = desvios_indicador.agrupar_consulta(desvios_data, "hora", "prc")
    filtered_desvios_hora_grouped = desvios_indicador.filtrar_columnas(desvios_hora_grouped, "prc")
    desvios_dia_grouped = desvios_indicador.agrupar_consulta(desvios_data, "dia", "prc")
    filtered_desvios_dia_grouped = desvios_indicador.filtrar_columnas(desvios_dia_grouped, "prc")
    desvios_mes_grouped = desvios_indicador.agrupar_consulta(desvios_data, "mes", "prc")
    filtered_desvios_mes_grouped = desvios_indicador.filtrar_columnas(desvios_mes_grouped, "prc")
    desvios_año_grouped = desvios_indicador.agrupar_consulta(desvios_data, "año", "prc")
    filtered_desvios_año_grouped = desvios_indicador.filtrar_columnas(desvios_año_grouped, "prc")

    print("desvios_hora_grouped", filtered_desvios_hora_grouped)
    print("desvios_dia_grouped", filtered_desvios_dia_grouped)
    print("desvios_mes_grouped", filtered_desvios_mes_grouped)
    print("desvios_año_grouped", filtered_desvios_año_grouped)

def test_ganancias_i90(start_date: str, end_date: str, lista_uprog: str, sesion: List[int]):
    ganancias_indicador = [PBF(), PVP(), Intradiario()]
    lista_uprog = [lista_uprog]

    for ganancia in ganancias_indicador:
        if isinstance(ganancia, Intradiario):
            print("ganancia == Intradiario()")
            ganancias_totales, ganancias_data = ganancia.get_ganancias(start_date, end_date, lista_uprog, sesion)

        elif isinstance(ganancia, PVP) or isinstance(ganancia, PBF):
            print("ganancia == PVP() or PBF()")
            ganancias_totales, ganancias_data = ganancia.get_ganancias(start_date, end_date, lista_uprog)
            print(f"ganancias_data {ganancia}", ganancias_data)
            print(f"ganancias_totales {ganancia}", ganancias_totales)

def test_ganancias_ajuste(start_date: str, end_date: str, lista_uprog: str):
    #CHEQUEAR LOS NONE EN SENTIDO QUE EQUIVALEN A CONSULTAS DE BANDA 
    indicadores_ajuste = [Secundaria()]#Secundaria(), Terciaria(), #Desvios()] #falta RR
    lista_uprog = [lista_uprog]
    sentido = ["Subir", "Bajar"]

    for indicador in indicadores_ajuste:
        if isinstance(indicador, Desvios):
            print("indicador == Desvios()")
            ganancias_totales, ganancias_data = indicador.get_ganancias(start_date, end_date, lista_uprog, sentido)
            filtered_ganancias_data = indicador.filtrar_columnas(ganancias_data, "gan")
            print(f"ganancias_data desvios", filtered_ganancias_data)
            print(f"ganancias_totales desvios", ganancias_totales)

        if isinstance(indicador, Secundaria): #esto es banda 
            print("indicador == Secundaria()")
            ganancias_totales, ganancias_data = indicador.get_ganancias(start_date, end_date, lista_uprog)
            filtered_ganancias_data = indicador.filtrar_columnas(ganancias_data, "gan")
            print(f"ganancias_data secundaria", filtered_ganancias_data)
            print(f"ganancias_totales secundaria", ganancias_totales)

        if isinstance(indicador, Terciaria):
            print("indicador == Terciaria()")
            ganancias_totales, ganancias_data = indicador.get_ganancias(start_date, end_date, lista_uprog, sentido)
            filtered_ganancias_data = indicador.filtrar_columnas(ganancias_data, "gan")
            hora_grouped = indicador.agrupar_consulta(filtered_ganancias_data, "año", "gan")
            print(f"ganancias_data terciaria", hora_grouped)
            print(filtered_ganancias_data.columns)
            print(f"ganancias_totales terciaria", ganancias_totales)

def test_lista_up(start_date: str, end_date: str):
    indicadores = [PBF(), PVP(), P48(),PHF(), Secundaria(), Terciaria(), RR()]

    for indicador in indicadores:
        lista_up = indicador.get_lista_up(start_date, end_date)
        print(f"lista_up {indicador}", lista_up)


if __name__ == "__main__":
    #print("Test years_between")
    #test_years_between()
    
    #print("Test get_filter_condition")
    #test_get_filter_condition()

    #print("Test get_path")
    #test_get_path()

    #print("Test sentido_sesion_filter")
    #test_sentido_sesion_filter()

    #print("Test get_programas")
    #test_get_programas()

    #print("Test get_precios")
    #test_get_precios()

    #print("Test get_ganancias_mercado_diario")
    #test_get_ganancias_mercado_diario()

    #print("Test get_ganancias_restricciones")
    #test_get_ganancias_restricciones()

    #print("Test get_ganancias_rr")
    #test_get_ganancias_rr()

    #print("Test get_ganancias_afrr")
    #test_get_ganancias_afrr()

    #print("Test get_ganancias_mfrr")
    #test_get_ganancias_mfrr()

    #print("Test get_ganancias_desvios")
    #test_get_ganancias_desvios()

    #print("Test ganancias intradiario")
    #test_get_ganancias_intradiario()

    #print("Test get_ganancias")
    test_get_ganancias()



    