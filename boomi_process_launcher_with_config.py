#boomi_process_launcher_with_config.py
"""
Launch Boomi Process via API calls using configuration file settings

This script initiates a request to execute a Boomi atom integration process 
- references with configuration file settings to prevent hardcoding of API settings and user credentials
- uses published Boomi API calls 
- can reference dynamical process properties (optional) 
- can wait (optionally) for execution completion (either SUCCESS or FAILURE).

External Dependencies:
- boomi_process_launcher.ini
- boomi_process_launcher.py

Command Line Execution:
- Windows     : py boomi_process_launcher_with_config.py "atom_name" "process_name" -d "property_key1:property_value1;property_key2:property_value2"
- Windows wait: py boomi_process_launcher_with_config.py "atom_name" "process_name" -d "property_key1:property_value1;property_key2:property_value2" -w
- Unix        : py boomi_process_launcher_with_config.py 'atom_name' 'process_name' -d 'property_key1:property_value1;property_key2:property_value2'
- Unix    wait: py boomi_process_launcher_with_config.py 'atom_name' 'process_name' -d 'property_key1:property_value1;property_key2:property_value2' -w
"""

import argparse                         # parses command-line arguments
import sys

from boomi_process_launcher import BoomiAPI
from configparser import ConfigParser   # parse configuration file elements
from os import path
from typing import Tuple                # type hinting for function return values

# def retrieve_api_settings() -> Tuple[str, str, str, str]:
#     """Read Boomi API configuration file settings"""
#     config_file = path.dirname(path.realpath(sys.argv[0]))+r'\boomi_process_launcher.ini'
#     try:
#         config   = ConfigParser()
#         config.read(config_file)
#         key      = "connection"
#         api_url  = config.get(key, "api_url")
#         path_url = config.get(key, "path_url")
#         username = config.get(key, "username")
#         password = config.get(key, "password")

#     except Exception as ex:
#         print(f"Reading configuration file {config_file}\n{ex}")
#         exit(1)  # script exit point
        
#     return api_url, path_url, username, password

DEBUG = False   # set to True to enable debug mode, False for production mode
HELP_EPILOG = '''

This script initiates a request to execute a Boomi atom process with dynamical process properties (optional) and can wait (optionally) for execution completion (either SUCCESS or FAILURE).

'''
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Execute a Boomi process and wait for completion",
        epilog=HELP_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("atom_name", help="Boomi Atom name where process will run")
    parser.add_argument("process_name", help="Boomi Process name that will executon on atom")
    parser.add_argument("-w", "--wait", help='Indicates if the script should wait for the job to complete (Default: No Wait)', action="store_true")
    parser.add_argument("-d", "--dynamicprops", help='Key:pair Boomi dynamic process properties seperated by a semicolon.\n\n\tIf the property values contain spaces, wrap the entire sequence in double quotes.\n\n\tExample: "DPP_1:abc123;DPP_2:xyz 321"', default='')
    if DEBUG:
        args = parser.parse_args(args = [
            'atom_name',
            'process_name',
            '-w',
            '-d', 'key1:value1;key2:value2',
        ])
        verbose = True
    else:
        args = parser.parse_args()
        verbose = False
        
    atom_name    = args.atom_name
    process_name = args.process_name
    wait         = args.wait
    dynamic_properties = args.dynamicprops
    api_url, path_url, username, password = BoomiAPI.retrieve_api_settings()
    launcher = BoomiAPI(api_url, path_url, username, password, atom_name, process_name, wait, dynamic_properties, verbose)
    launcher.run_process()
