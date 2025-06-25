#test_boomi_process_launcher_with_config.py

from boomi_process_launcher import (
    BoomiAPI,
)
import unittest as ut1

def identify(func):
    def wrapper(*args, **kwargs):
        print(f"\nTEST: {func.__name__}")
        return func(*args, **kwargs)
    return wrapper

class TestConfigFile(ut1.TestCase):
    def setUp(self):
        self.boomi = BoomiAPI("api.boomi.com", "/api/rest/v1/BOOMI_ACCOUNT_NAME", "BOOMI_USERNAME", "BOOMI_PASSWORD", "atom_name", "process_name", False, None, False)
        self.python_script_path = ''

    @identify
    def test_retrieve_api_settings(self):
        # test retrieving api settings from external config (needs to read Mock file instead)
        self.boomi.retrieve_api_settings()
        self.assertEqual("api.boomi.com", self.boomi.api_url)
        self.assertEqual("/api/rest/v1/BOOMI_ACCOUNT_NAME", self.boomi.path_url)
        self.assertEqual("BOOMI_USERNAME", self.boomi.username)
        self.assertEqual("BOOMI_PASSWORD", self.boomi.password)
