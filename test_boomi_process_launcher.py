#test_boomi_process_launcher.py

from boomi_process_launcher import BoomiAPI
import datetime
import unittest as ut1
from unittest.mock import patch

class ScriptExitException(Exception):
    pass

def identify(func):
    def wrapper(*args, **kwargs):
        print(f"\nTEST: {func.__name__}", end="")
        return func(*args, **kwargs)
    return wrapper

class TestMain1(ut1.TestCase):
    def setUp(self):
        self.boomi = BoomiAPI()

    @identify
    def test_convert_from_iso_to_local(self):
        date = datetime.datetime.strptime("2024-01-01T08:01:02Z","%Y-%m-%dT%H:%M:%SZ")
        test_date = self.boomi.convert_from_iso_to_local_datetime(date)
        constant_date = datetime.datetime.fromisoformat('2024-01-01 00:01:02-08:00')
        self.assertEqual(constant_date, test_date)
        
    @identify
    def test_delay_execution(self):
        time = 1
        new_time = self.boomi.delay_execution(time)
        self.assertEqual(2, new_time)

class TestMain2(ut1.TestCase):
    def setUp(self):
        self.boomi = BoomiAPI()

    @patch('datetime.datetime', autospec=True)
    @patch('boomi_process_launcher.datetime', autospec=True)
    @identify
    
    def test_format_log_message(self, mock_datetime, mock_boomi_api):
        mock_datetime.now.return_value = datetime.datetime.strptime("2024-01-01 08:01:02", "%Y-%m-%d %H:%M:%S")
        mock_boomi_api.now.return_value = datetime.datetime.strptime("2024-01-01 08:01:02", "%Y-%m-%d %H:%M:%S")
        groups = [
            ('1only', '12345', None, None, None),
            ('1only', '12345678901234567890123456789012345', None, None, None),
            ('1+2', '12345', 'section2', None, None),
            ('1+2', '12345678901234567890123456789012345', 'section2', None, None),
            ('1+2+n', '12345', 'section2', 'section3', None),
            ('1+2+n', '12345', 'section2', 'section4', None),
            ('4', '12345', 'section2', 'section3', 'section4'),
        ]

        for lines, group1, group2, group3, group4 in groups:
            with self.subTest(lines=lines, group1=group1, group2=group2, group3=group3, group4=group4):
                log = self.boomi.format_log_message(group1, group2, group3, group4)
                if lines == '1only':
                    self.assertEqual(f"{datetime.datetime.now()}\t{group1}", log)
                elif lines == '2':
                    self.assertEqual(f"{datetime.datetime.now()}\t{group1.ljust(self.boomi.GROUP1_LENGTH)}", log)
                elif lines == '1+2+n':
                    self.assertEqual(f"{datetime.datetime.now()}\t{group1.ljust(self.boomi.GROUP1_LENGTH)}{group2}\n\t\t\t\t{group3}", log)
                elif lines == '4':
                    self.assertEqual(f"{datetime.datetime.now()}\t{group1.ljust(self.boomi.GROUP1_LENGTH)}{group2}\n\t\t\t\t{group3}\n\t\t\t\t{group4}", log)

class TestMain3(ut1.TestCase):
    def setUp(self):
        self.boomi = BoomiAPI()
        self.python_script_path = ''

    @identify
    def test_retrieve_api_settings(self):
        self.boomi.retrieve_api_settings()
        self.assertEqual("api.boomi.com", self.boomi.api_url)
