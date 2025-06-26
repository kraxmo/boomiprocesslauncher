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
    @identify
    def test_retrieve_api_settings(self):
        # test retrieving api settings from external config (needs to read Mock file instead)
        api_url, path_url, username, password = BoomiAPI.retrieve_api_settings()
        self.assertEqual("api.boomi.com", api_url)
        self.assertEqual("/api/rest/v1/BOOMI_ACCOUNT_NAME", path_url)
        self.assertEqual("BOOMI_USERNAME", username)
        self.assertEqual("BOOMI_PASSWORD", password)
