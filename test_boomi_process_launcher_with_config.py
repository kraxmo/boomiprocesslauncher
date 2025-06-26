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
    def setUp(self) -> None:
        self.api_url, self.path_url, self.username, self.password = BoomiAPI.retrieve_api_settings()   

    @identify
    def test_retrieve_api_settings(self):
        # test retrieving api settings from external config (needs to read Mock file instead)
        self.assertEqual("api.boomi.com", self.api_url)

    @identify
    def test_retrieve_path_settings(self):
        self.assertEqual("/api/rest/v1/BOOMI_ACCOUNT_NAME", self.path_url)

    @identify
    def test_username_settings(self):
        self.assertEqual("BOOMI_USERNAME", self.username)

    @identify
    def test_retrieve_password_settings(self):
        self.assertEqual("BOOMI_PASSWORD", self.password)
