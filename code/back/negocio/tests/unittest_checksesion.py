import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from funciones_consultas_v2 import check_sesion, get_precios_intradiario
import pandas as pd

class TestCheckSesion(unittest.TestCase):

    def test_before_fecha_corte(self):
        start_date = "2024-01-01"
        end_date = "2024-06-30"
        sesion = [1, 2, 3]
        result = check_sesion(start_date, end_date, sesion)
        self.assertEqual(result, sesion)

    def test_after_fecha_corte(self):
        start_date = "2024-08-01"
        end_date = "2024-12-31"
        sesion = [1, 2, 3, 4, 5, 6, 7]
        result = check_sesion(start_date, end_date, sesion)
        self.assertEqual(result, [1, 2, 3])

    def test_across_fecha_corte(self):
        start_date = "2024-06-01"
        end_date = "2024-08-01"
        sesion = [1, 2, 3, 4, 5, 6, 7]
        result = check_sesion(start_date, end_date, sesion)
        self.assertEqual(result, ([1, 2, 3, 4, 5, 6, 7], [1, 2, 3]))

    def test_all_sessions_before_fecha_corte(self):
        start_date = "2024-01-01"
        end_date = "2024-06-30"
        sesion = "All"
        result = check_sesion(start_date, end_date, sesion)
        self.assertEqual(result, [1, 2, 3, 4, 5, 6, 7])

    def test_all_sessions_after_fecha_corte(self):
        start_date = "2024-08-01"
        end_date = "2024-12-31"
        sesion = "All"
        result = check_sesion(start_date, end_date, sesion)
        self.assertEqual(result, [1, 2, 3])

    def test_all_sessions_across_fecha_corte(self):
        start_date = "2024-06-01"
        end_date = "2024-08-01"
        sesion = "All"
        result = check_sesion(start_date, end_date, sesion)
        self.assertEqual(result, ([1, 2, 3, 4, 5, 6, 7], [1, 2, 3]))

    def test_no_sessions_before_fecha_corte(self):
        start_date = "2024-01-01"
        end_date = "2024-06-30"
        sesion = None
        result = check_sesion(start_date, end_date, sesion)
        self.assertEqual(result, [1, 2, 3, 4, 5, 6, 7])

    def test_no_sessions_after_fecha_corte(self):
        start_date = "2024-08-01"
        end_date = "2024-12-31"
        sesion = None
        result = check_sesion(start_date, end_date, sesion)
        self.assertEqual(result, [1, 2, 3])

    def test_no_sessions_across_fecha_corte(self):
        start_date = "2024-06-01"
        end_date = "2024-08-01"
        sesion = None
        result = check_sesion(start_date, end_date, sesion)
        self.assertEqual(result, ([1, 2, 3, 4, 5, 6, 7], [1, 2, 3]))

if __name__ == '__main__':
    unittest.main()