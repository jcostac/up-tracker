import unittest
from back.negocio.funciones_daemon import extract_year_from_filename

class TestExtractYearFromFilename(unittest.TestCase):

    def test_extract_year_from_valid_filenames(self):
        # Test cases with expected results
        test_cases = {
            "2023-04-15_precios_diario.csv": "2023",
            "I90DIA_20230315.zip": "2023",
            "2022-01-01_precios_intradiario.csv": "2022",
            "2021-12-31_precios_rr.csv": "2021",
            "data_2022.zip": "2022",
            "file_without_year.txt": None,
            "2023_data_file.csv": "2023",
            "random_2022_data.zip": "2022",
        }

        for filename, expected_year in test_cases.items():
            with self.subTest(filename=filename):
                self.assertEqual(extract_year_from_filename(filename), expected_year)

if __name__ == '__main__':
    unittest.main()