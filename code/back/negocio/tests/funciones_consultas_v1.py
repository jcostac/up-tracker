import duckdb
import pandas as pd

def ganancia_mercado_diario(anio, unidad):

    consulta = """
    SELECT round(sum(prog.ENERGIA * prc.PRECIO)/1000000,2) as ganancia 
    from 'E:\\clientes\\simulyde\\analizador_unidades\\data\\output\\i90\\""" + str(anio) + """\\PROGRAMAS.parquet' as prog
    left join 'E:\\clientes\\simulyde\\analizador_unidades\\data\\output\\precios\\precios_diario.parquet' as prc 
    on prog.FECHA = prc.FECHA
    and prog.HORA = prc.PERIODO
    where prog.HORA NOT IN ('3a','3b')
    and prog.UPROG = '""" + unidad + """'
    and prog.PROGRAMA = 'PBF'
    """
    df = duckdb.sql(consulta).df()
    gu = round(df['ganancia'].iloc[0],2)
    return gu


def ganancia_restricciones(anio, unidad):

    consulta = """
    SELECT round(sum(prog.ENERGIA * prc.PRECIO)/1000000,2) as ganancia 
    FROM 'E:\\clientes\\simulyde\\analizador_unidades\\data\\output\\i90\\""" + str(anio) + """\\RESULT_RES.parquet' as prog
    left join 'E:\\clientes\\simulyde\\analizador_unidades\\data\\output\\i90\\""" + str(anio) + """\\PRE_RES_MD.parquet' as prc 
    on prog.FECHA = prc.FECHA
    and prog.HORA = prc.HORA
    and prog.SENTIDO = prc.SENTIDO
    and prog.UPROG = prc.UPROG
    where prog.UPROG = '""" + unidad + """'
    """

    df = duckdb.sql(consulta).df()
    gu = round(df['ganancia'].iloc[0],2)
    return gu


def ganancia_intradiario(anio, unidad):

    gu = 0
    for s in range(1,8):

        if s == 1: prog = ["PVP","PHF1"]
        if s == 2: prog = ["PHF1","PHF2"]
        if s == 3: prog = ["PHF2","PHF3"]
        if s == 4: prog = ["PHF3","PHF4"]
        if s == 5: prog = ["PHF4","PHF5"]
        if s == 6: prog = ["PHF5","PHF6"]
        if s == 7: prog = ["PHF6","PHF7"]


        consulta = """
        select round(sum(ene.DIF_PROG * prc.PRECIO)/1000000,2) as ganancia 
        from 
            (
            select prog.FECHA, prog.HORA, prog.ENERGIA - prog_ante.ENERGIA DIF_PROG
            from 'E:\\clientes\\simulyde\\analizador_unidades\\data\\output\\i90\\""" + str(anio) + """\\PROGRAMAS.parquet' as prog
            left join 'E:\\clientes\\simulyde\\analizador_unidades\\data\\output\\i90\\""" + str(anio) + """\\PROGRAMAS.parquet' as prog_ante
            on prog.FECHA = pr  og_ante.FECHA
            and prog.HORA = prog_ante.HORA
            and prog_ante.PROGRAMA = '""" + prog[0] + """'
            and prog_ante.UPROG = '""" + unidad + """'
            where prog.HORA NOT IN ('3a','3b')
            and prog.UPROG = '""" + unidad + """'
            and prog.PROGRAMA = '""" + prog[1] + """'	
            ) as ene
        left join 'E:\\clientes\\simulyde\\analizador_unidades\\data\\output\\precios\\precios_intradiario.parquet' as prc
        on ene.FECHA = prc.FECHA
        and ene.HORA = prc.PERIODO
        and prc.SESION = """ + str(s) + """
        """

        df = duckdb.sql(consulta).df()
        gu = gu + round(df['ganancia'].iloc[0],2)
    return gu


def ganancia_banda_secundaria(anio, unidad):

    consulta = """
    SELECT round(sum(abs(prog.ENERGIA) * prc.PRECIO)/1000000,2) as ganancia
    FROM 'E:\\clientes\\simulyde\\analizador_unidades\\data\\output\\i90\\""" + str(anio) + """\\PROG_SEC.parquet' AS prog
    LEFT JOIN 'E:\\clientes\\simulyde\\analizador_unidades\\data\\output\\precios\\precios_banda_secundaria.parquet' AS prc
    ON  prog.FECHA = prc.FECHA
    AND prog.HORA = prc.PERIODO
    WHERE prog.uprog = '""" + unidad + """'
    """

    df = duckdb.sql(consulta).df()
    gu = round(df['ganancia'].iloc[0],2)
    return gu


def ganancia_terciaria(anio, unidad):

    consulta = """
    SELECT round(sum(prog.ENERGIA * prc.PRECIO)/1000000,2) as ganancia 
    FROM 'E:\\clientes\\simulyde\\analizador_unidades\\data\\output\\i90\\""" + str(anio) + """\\PROG_TERC.parquet' as prog
    left join 'E:\\clientes\\simulyde\\analizador_unidades\\data\\output\\i90\\""" + str(anio) + """\PRE_TER_DES_TR.parquet' as prc 
    on prog.FECHA = prc.FECHA
    and prog.HORA = prc.HORA
    and prog.SENTIDO = prc.SENTIDO
    and prog.UPROG = prc.UPROG
    where prog.UPROG = '""" + unidad + """'
    and prog.REDESPACHO = 'TER'
    and prc.TIPO = 'TER'
    """

    df = duckdb.sql(consulta).df()
    gu = round(df['ganancia'].iloc[0],2)
    return gu



def ganancia_gestion_desvios(anio, unidad):

    consulta = """
    SELECT round(sum(prog.ENERGIA * prc2.PRECIO)/1000000,2) as ganancia
    FROM 'E:\\clientes\\simulyde\\analizador_unidades\\data\\output\\i90\\""" + str(anio) + """\\PROG_GES_DESV.parquet' AS prog
    LEFT JOIN (
        SELECT *
        FROM (
            SELECT FECHA, PERIODO, SENTIDO, PRECIO 
            FROM 'E:\\clientes\\simulyde\\analizador_unidades\\data\\output\\precios\\precios_gestion_desvios.parquet' AS prc
            WHERE SENTIDO = 'Bajar'
            UNION
            SELECT FECHA, PERIODO, SENTIDO, PRECIO 
            FROM 'E:\\clientes\\simulyde\\analizador_unidades\\data\\output\\precios\\precios_gestion_desvios.parquet' AS prc
            WHERE SENTIDO = 'Subir'
        )
    ) AS prc2 
    ON prog.FECHA = prc2.FECHA AND prog.HORA = prc2.PERIODO AND prog.SENTIDO = prc2.SENTIDO
    WHERE prog.uprog = '""" + unidad + """' AND prog.REDESPACHO = 'DESV'
    AND prog.HORA NOT IN ('3a','3b')
    """

    df = duckdb.sql(consulta).df()
    gu = round(df['ganancia'].iloc[0],2)
    return gu


def ganancia_balance_rr(anio, unidad):
    consulta = """
    SELECT round(sum(prog.ENERGIA * prc.PRECIO)/1000000,2) as ganancia
    FROM 'E:\\clientes\\simulyde\\analizador_unidades\\data\\output\\i90\\""" + str(anio) + """\\PROG_RR.parquet' AS prog
    LEFT JOIN 'E:\\clientes\\simulyde\\analizador_unidades\\data\\output\\precios\\precios_balance_rr.parquet' AS prc
    ON  prog.FECHA = prc.FECHA
    AND prog.HORA = prc.PERIODO
    WHERE prog.uprog = '""" + unidad + """' AND prog.REDESPACHO = 'RR'
    AND prog.HORA NOT IN ('3a','3b')
    """

    df = duckdb.sql(consulta).df()
    gu = round(df['ganancia'].iloc[0],2)
    return gu