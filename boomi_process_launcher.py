#boomi_process_launcher.py
"""
Launch Boomi Process via API calls

This script initiates a request to execute a Boomi atom integration process 
- uses published Boomi API calls 
- can reference dynamical process properties (optional) 
- can wait (optionally) for execution completion (either SUCCESS or FAILURE).

External Dependencies:
- None

Command Line Execution:
- Windows     : py boomi_process_launcher.py "api_url" "path_url" "username" "password" "atom_name" "process_name" -d "property_key1:property_value1;property_key2:property_value2"
- Windows wait: py boomi_process_launcher.py "api_url" "path_url" "username" "password" "atom_name" "process_name" -d "property_key1:property_value1;property_key2:property_value2" -w
- Unix        : py boomi_process_launcher.py 'api_url' 'path_url' 'username' 'password' 'atom_name' 'process_name' -d 'property_key1:property_value1;property_key2:property_value2'
- Unix    wait: py boomi_process_launcher.py 'api_url' 'path_url' 'username' 'password' 'atom_name' 'process_name' -d 'property_key1:property_value1;property_key2:property_value2' -w
"""

import argparse                         # parses command-line arguments
from base64 import b64encode
from configparser import ConfigParser   # parse configuration file elements
from datetime import(
    datetime,
    timezone,
)
import http.client
import inspect                          # inspects call stack to determine currently executing function name
import json
from os import path
import sys
import time
from typing import Tuple                # type hinting for function return values

class BoomiAPI():
    """Call Boomi API to execute process using api, path, username, password, atom name, process name and optional dynamic process properties"""
    CONFIGURATION_FILENAME     = r'\boomi_process_launcher.ini'
    CONNECTION_TIMEOUT         = 30
    EXECUTION_STATUS           = {
        'KNOWN': ['ABORTED', 'COMPLETE', 'COMPLETE_WARN', 'DISCARDED', 'ERROR', 'STARTED'],
        'SUCCESS': ['COMPLETE', 'COMPLETE_WARN'],
        'PROCESSING': ['INPROCESS'],
        'TERMINATED': ['UNKNOWN', 'ABORTED', 'DISCARDED', 'ERROR']
    }
    EXIT_CODE_SUCCESS          = 0
    EXIT_CODE_ERROR            = 1
    GROUP1_LENGTH              = 32
    MAX_WAIT_SECONDS           = 60
    NEWLINE_TABBED_INSERT      = "\n\t\t\t\t"
    RESPONSE_CODE_200_OK       = 200
    RESPONSE_CODE_202_ACCEPTED = 202
    TOTAL_ATTEMPTS             = 1440
    TOTAL_ERRORS               = 3
    TOTAL_TRIES                = 3
    VALID_RESPONSE_CODES       = {RESPONSE_CODE_200_OK, RESPONSE_CODE_202_ACCEPTED}

    @staticmethod
    def retrieve_api_settings() -> Tuple[str, str, str, str]:
        """Read Boomi API configuration file settings"""
        config_file = path.dirname(path.realpath(sys.argv[0]))+r'\boomi_process_launcher.ini'
        try:
            config   = ConfigParser()
            config.read(config_file)
            key      = "connection"
            api_url  = config.get(key, "api_url")
            path_url = config.get(key, "path_url")
            username = config.get(key, "username")
            password = config.get(key, "password")

        except Exception as ex:
            print(f"Reading configuration file {config_file}\n{ex}")
            exit(1)  # script exit point
            
        return api_url, path_url, username, password
    
    def __init__(self, api_url: str, path_url: str, username: str, password: str, atom_name: str, process_name: str, wait: bool = False, dynamic_properties: str = "", verbose: bool = False):
        self.exit_code = self.EXIT_CODE_ERROR   # set script return exit code to FAILURE
        try:
            self.api_url            = api_url
            self.path_url           = path_url
            self.username           = username
            self.password           = password
            if atom_name:
                self.atom_name      = atom_name.strip()
            else:
                print(self.format_log_message("ERROR Atom name cannot be blank"))
                raise ScriptExitException   # exit script
            
            if process_name:
                self.process_name   = process_name.strip()
            else:
                print(self.format_log_message("ERROR Process name cannot be blank"))
                raise ScriptExitException   # exit script

            self.wait               = wait
            self.dynamic_properties = dynamic_properties
            self.verbose            = verbose
            
            self.connection         = None
            self.headers            = None 
            self.response           = None
            self.execution_status   = ''
            self.execution_completed_timestamp = ''
            self.atom_id            = None
            self.deployment_id      = None
            self.execution_id       = None
            self.component_id       = None
            self.environment_id     = None
            
        except ScriptExitException:
            exit(self.exit_code)

    def connect_to_api(self) -> None:
        """Establish Boomi API connection with headers

        Args:
            api_url (str): Boomi API url
            username (str): Boomi user name
            password (str): Boomi password
        """
        # Setup API connection
        self.connection = http.client.HTTPSConnection(self.api_url, timeout=self.CONNECTION_TIMEOUT)
        
        # Setup API headers
        login           = f"{self.username}:{self.password}".encode("utf-8")
        authorization   = f"Basic {b64encode(login).decode('utf-8')}"
        self.headers    = {'Accept':'application/json','Content-Type':'application/json','Authorization':authorization}

    def convert_from_iso_to_local_datetime(self, iso_date: str) -> datetime:
        """Convert date from ISO to local datetime
        
        Args: 
            iso_date (str): datetime in ISO date format

        Returns: 
            (datetime): datetime in datetime format
        """
        return iso_date.replace(tzinfo=timezone.utc).astimezone(tz=None)

    def delay_execution(self, wait_seconds: int) -> int:
        """Delay execution for wait_seconds up to maximum wait period

        Args:
            wait_seconds (int): time to wait in seconds

        Returns: 
            (int): updated wait time in seconds
        """
        time.sleep(wait_seconds)
        return min(wait_seconds * 2, self.MAX_WAIT_SECONDS)

    def format_log_message(self, section1: str, *args) -> str:
        """Format message into OpCon log output format using time complexity O(n)
        
        Args:
            section1 (str): log section 1
            args:
                0 (str, optional): log section 2. Defaults to None.
                1 (str, optional): log section 3. Defaults to None.
                2 (str, optional): log section 4. Defaults to None.
            
        Returns: 
            log (str): formatted log message
        """
        if section1 is None:
            if len(args) < 1:
                return ""
            
            section1 = ""
            
        log = [str(datetime.now())+"\t"]
        if len(args) >= 1 and args[0] is not None:
            log.append(section1.ljust(self.GROUP1_LENGTH)[:self.GROUP1_LENGTH] + args[0])
        else:
            log.append(section1)

        for ctr, value in enumerate(args[1:]):
            if len(args) >= ctr and value is not None:
                log.append(self.NEWLINE_TABBED_INSERT + value)
            
        return "".join(log)

    def get_requested_id(self, action: str, endpoint: str, body: str, status_codes: set, name: str, description: str, value: str):
        """Retrieve requested id using API endpoint and body

        Args:
            action (str): 'query' or 'execution'
            endpoint(str): API endpoint
            body (str): HTTP API body url
            status_codes(set): valid HTTP Response Codes
            name (str): API requested id name 
            description (str): requested component name
            value (str): requested value
        
        Returns: API requested id(s) (str)
        """
        method_signature = f"{__class__.__name__}.{inspect.stack()[0][3]}('{action}', '{endpoint}', '{body}', '{status_codes}', '{name}', '{description}', '{value}')"
        try:
            execution_status = 'SENDING'
            for _ in range(self.TOTAL_ATTEMPTS):
                self.response, status, message = self.make_api_request('POST', endpoint, body, status_codes)
                if action == "query":
                    print(self.format_log_message(f"POST {action.title()} {description} ID:", f"{status} {message}"))
                else:
                    print(self.format_log_message(f"POST {action.title()} {description} ID:", f"{status} {message} ({execution_status})"))

                if status == self.RESPONSE_CODE_200_OK:
                    if action =='query':   # query (lookup) requested id
                        results = 0
                        if 'numberOfResults' in self.response:
                            results: int = self.response['numberOfResults']
                            if results == 1:
                                if 'status' in self.response['result'][0]:
                                    execution_status = self.response['result'][0]['status']

                                if description == 'Deployment':
                                    names = name.split(',')
                                    deployment_id = self.response['result'][0][names[0]]
                                    component_id  = self.response['result'][0][names[1]]
                                    print(self.format_log_message(f"{names[0]}:", f"{deployment_id}"))
                                    print(self.format_log_message(f"{names[1]}:", f"{component_id}"))
                                    return deployment_id, component_id
                                
                                print(self.format_log_message(f"{description} ID:", self.response['result'][0][name]))
                                return self.response['result'][0][name]  # return requested id
                            
                        print(self.format_log_message(f"{results} {description} found with name '{value}'", None, method_signature if self.verbose==True else None))
                        raise ScriptExitException   # exit script
                    else:
                        if action == 'execution':   # execute requested process
                            requested_id = self.response.get(name)
                            print(self.format_log_message("Request ID:", requested_id))
                            return requested_id
                        else:
                            raise ValueError
                            
                execution_status = 'UNKNOWN'
                print(self.format_log_message(f"Failed: {description}.", f"Retrying in {self.MAX_WAIT_SECONDS} seconds ({execution_status})"))
                time.sleep(self.MAX_WAIT_SECONDS)

            print(self.format_log_message(f"No {description} found", None, method_signature if self.verbose==True else None))
            
        except ScriptExitException:
            raise   # if execution comes here, re-raise it to exit script
            
        except Exception as err:
            print(self.format_log_message("Processing POST request", None, err , method_signature if self.verbose==True else None))

        raise ScriptExitException   # force exit

    def initiate_atom_process(self) -> None:
        """Get Boomi Execution Id for monitoring Process ID execution"""
        endpoint = self.path_url + "/ExecutionRequest"
        payload  = {
            '@type':'ExecutionRequest',
            "atomId": self.atom_id,
            "processId": self.component_id
        }
        payload.update(self.parse_dynamic_properties())
        body = json.dumps(payload)
        print(self.format_log_message("Starting process:", self.process_name))
        self.execution_id = self.get_requested_id('execution', endpoint, body, {self.RESPONSE_CODE_200_OK}, 'requestId', f"Request", self.atom_name+'|'+self.process_name)
        if self.execution_id == '':
            print(self.format_log_message("Failed to start process", "Check Boomi for details"))
            raise ScriptExitException   # exit script

    def make_api_request(self, method: str, endpoint: str, body: str, status_codes: set):
        """Make an HTTP request with retry logic
        
        Args:
            method (str): GET or POST
            endpoint(str): HTTP API body url
            body (str): API request body
            status_codes(set): a set of valid HTTP Response Codes
            
        Returns Tuple:
            response (str): json-formatted API response
            status (int): API response status code
            reason (str): API response reason
        """
        method_signature = f"{__class__.__name__}.{inspect.stack()[0][3]}('{method}', '{endpoint}', '{body}', '{status_codes}')"
        if status_codes is None:
            status_codes = {self.RESPONSE_CODE_200_OK}
        
        try:
            for _ in range(self.TOTAL_TRIES):
                self.connection.request(method, endpoint, body=body, headers=self.headers)
                response = self.connection.getresponse()
                status = response.status
                message = response.reason
                if status in status_codes:
                    return json.loads(response.read().decode("utf-8")), status, message

                print(self.format_log_message("Failed to start process", f"Retrying in {self.MAX_WAIT_SECONDS} seconds"))
                time.sleep(self.MAX_WAIT_SECONDS)

        except Exception as err:
            print(self.format_log_message(f"ERROR Processing HTTP {method} request", None, err, method_signature if self.verbose==True else None))
            raise ScriptExitException   # exit script

        finally:
            self.connection.close()

    def monitor_process(self, wait: bool, wait_seconds: int) -> int:
        """Monitor process execution status until completion or timeout

        Returns: wait_seconds (int): wait_seconds in seconds
        """
        method_signature      = f"{__class__.__name__}.{inspect.stack()[0][3]}({wait}, {wait_seconds})"
        endpoint              = self.path_url + f"/ExecutionRecord/async/{self.execution_id}"
        self.execution_status = 'STATUS PENDING'
        self.execution_completed_timestamp = ''
        errors   = 0
        attempts = 0
        try:
            while   attempts < self.TOTAL_ATTEMPTS and \
                    errors   < self.TOTAL_ERRORS   and \
                    self.execution_status not in self.EXECUTION_STATUS["KNOWN"]:
                self.response, status, message = self.make_api_request('GET', endpoint, None, self.VALID_RESPONSE_CODES)
                if status == self.RESPONSE_CODE_202_ACCEPTED:
                    print(self.format_log_message(f"GET Execution Status:", f"{status} {message} ({self.execution_status})"))
                    wait_seconds = self.delay_execution(wait_seconds)
                    continue
                
                elif status == self.RESPONSE_CODE_200_OK:
                    if 'result' in self.response:
                        if 'status' in self.response['result'][0]:
                            self.execution_status = self.response['result'][0]['status']
                            print(self.format_log_message(f"GET Execution Status:", f"{status} {message} ({self.execution_status})"))

                            # If wait indicated and process executing, wait and check again
                            if wait and self.execution_status in self.EXECUTION_STATUS["PROCESSING"]:
                                attempts += 1
                                print(self.format_log_message(None, f"Retry attempt #{attempts}: retrying in {wait_seconds} seconds"))
                                wait_seconds = self.delay_execution(wait_seconds)
                                continue                                
                                
                            if 'recordedDate' in self.response['result'][0]:
                                recorded_date = datetime.strptime(self.response['result'][0]['recordedDate'],"%Y-%m-%dT%H:%M:%SZ")
                                self.execution_completed_timestamp = self.convert_from_iso_to_local_datetime(recorded_date)

                            break

                    print(self.format_log_message(f"{status}: Process aborted"))
                    raise ScriptExitException   # exit script

                attempts += 1
                errors += 1
                self.execution_status = 'UNKNOWN'
                print(self.format_log_message("GET Execution Status:", f"{status} {message} ({self.execution_status})", f"Retry attempt #{attempts}: retrying in {self.MAX_WAIT_SECONDS} seconds"))
                time.sleep(self.MAX_WAIT_SECONDS)

        except Exception as err:
            print(self.format_log_message("Processing HTTP GET request", None, err, method_signature if self.verbose==True else None))
            raise ScriptExitException   # exit script

        finally:
            return wait_seconds

    def parse_dynamic_properties(self) -> dict:
        """format dynamic process properties json payload
        
        Returns: (dict): json-formatted Boomi API Dynamic Process Properties
        """
        method_signature = f"{__class__.__name__}.{inspect.stack()[0][3]}()"
        if not self.dynamic_properties.strip():
            return {}
        
        try:
            dynamic_properties_list = []
            pairs = self.dynamic_properties.strip().split(";")
            for pair in pairs:
                if pair:    # pair is not blank (multiple/leading/trailing ;)
                    if ":" not in pair:
                        print(self.format_log_message(f"Invalid key:pair data format for pair '{pair}'", None, method_signature if self.verbose==True else None))
                        raise ScriptExitException   # exit script
                    else:
                        name, value = pair.split(":", 1)
                        dynamic_properties_list.append({"name": name, "value": value})
                        print(self.format_log_message("Dynamic Prop:", pair))

            return {"DynamicProcessProperties": {"DynamicProcessProperty": dynamic_properties_list}}
        
        except ScriptExitException: # if execution comes here, re-raise it to exit script
            raise
        
        except Exception as err:
            print(self.format_log_message("Parsing dynamic process properties", None, err, method_signature if self.verbose==True else None))
            raise ScriptExitException   # exit script

    def run_process(self) -> None:
        """Run Boomi atom process"""
        try:
            if self.atom_name is None or not self.atom_name.strip():
                print(self.format_log_message("ERROR Atom name cannot be blank"))
                raise ScriptExitException   # exit script

            self.atom_name          = self.atom_name.strip()
            if self.process_name is None or not self.process_name.strip():
                print(self.format_log_message("ERROR Process name cannot be blank"))
                raise ScriptExitException   # exit script

            self.process_name       = self.process_name.strip()
            # self.wait               = wait
            # self.dynamic_properties = dynamic_properties

            # self.retrieve_api_settings()
            self.connect_to_api()
            self.verify_atom_exists()
            self.verify_atom_environment_exists()
            self.verify_process_exists_in_environment()
            self.initiate_atom_process()
            
            # check to see if process is running
            self.execution_status = ""
            self.execution_completed_timestamp = ''
            wait_seconds = 1
            wait_seconds = self.monitor_process(False, wait_seconds)
            
            # exit script if waiting for Boomi process to finish is *NOT* required
            if not self.wait:
                if self.execution_status in self.EXECUTION_STATUS["TERMINATED"]:
                    print(self.format_log_message("Process failed to start", None, f"{self.execution_status}"))
                else:                    
                    print(self.format_log_message(f"Process successfully sent to Boomi Atom {self.atom_name}"))
                    self.exit_code = self.EXIT_CODE_SUCCESS # set execution for successful processing status
                
                raise ScriptExitException                   # exit script

            self.delay_execution(wait_seconds)

            # if execution status is currently processing, monitor process for known completion status
            if self.execution_status in self.EXECUTION_STATUS['PROCESSING']:
                print(self.format_log_message("Waiting for completion:", self.process_name))
                self.monitor_process(True, wait_seconds)

            if self.execution_status == 'COMPLETE':         # report complete status
                print(self.format_log_message(f"Process completed successfully at {self.execution_completed_timestamp}"))
                self.exit_code = self.EXIT_CODE_SUCCESS  # set execution for successful processing status
                raise ScriptExitException                   # exit script

            # report incomplete status (if exists)
            if 'message' in self.response['result'][0]:
                print(self.format_log_message(f"ERROR: {self.response['result'][0]['message']}"))
            else:
                print(self.format_log_message(f"ERROR: Unable to determine status of process {self.process_name} execution"))
                
        except ScriptExitException:
            pass    # if execution comes here at any time, pass to script exit point

        except Exception as err:
            print(self.format_log_message("ERROR occurred while executing Boomi API Process steps", None, err))
        
        finally:
            exit(self.exit_code) # script exit point

    def verify_atom_exists(self) -> None:
        """Get Boomi Atom Id to verify Atom Name is valid"""
        endpoint = self.path_url + "/Atom/query"
        body     = json.dumps({'QueryFilter':{'expression':{'argument':[self.atom_name],'operator':'EQUALS','property':'name'}}})
        self.atom_id = self.get_requested_id('query', endpoint, body, {self.RESPONSE_CODE_200_OK}, 'id', "Atom", self.atom_name)

    def verify_process_exists_in_environment(self) -> None:
        """Get Boomi Deployment Id to verify Process Id is deployed within Environment ID"""
        endpoint = self.path_url + f"/DeployedPackage/query"
        body     = json.dumps({'QueryFilter':{'expression':{'operator':'and','nestedExpression':[{'argument':[self.environment_id],'operator':'EQUALS','property':'environmentId'},{'argument':['process'],'operator':'EQUALS','property':'componentType'},{'argument':[True],'operator':'EQUALS','property':'active'},{'argument':[self.process_name],'operator':'EQUALS','property':'componentName'}]}}})
        self.deployment_id, self.component_id = self.get_requested_id('query', endpoint, body, {self.RESPONSE_CODE_200_OK}, 'deploymentId,componentId', "Deployment", self.process_name)

    def verify_atom_environment_exists(self) -> None:
        """Get Boomi Environment Id to verify Atom Id is valid"""
        endpoint = self.path_url + f"/EnvironmentAtomAttachment/query"
        body     = json.dumps({'QueryFilter':{'expression':{'argument':[self.atom_id],'operator':'EQUALS','property':'atomId'}}})
        self.environment_id = self.get_requested_id('query', endpoint, body, {self.RESPONSE_CODE_200_OK}, 'environmentId', "Environment", self.atom_name)

class ScriptExitException(Exception):
    """Placeholder exception to allow for programmatically graceful exits from the script
    
    Instead of issuing an immediate exit(#) function, raising this exception does the following:
    1. Breaks out of the current code
    2. Allows exception to be passed to common end-of-routine exit (one-entry/one-exit) in the BoomiProcessLauncher.run() method
    """
    pass

DEBUG = False   # set to True to enable debug mode, False for production mode
HELP_EPILOG = '''

This script initiates a request to execute a Boomi atom process with dynamical process properties (optional) and can wait (optionally) for execution completion (either SUCCESS or FAILURE).

'''
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Execute a Boomi process and wait for completion",
        epilog=HELP_EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        exit_on_error=True,
    )
    parser.add_argument("username", type=str, help="Boomi API Username")
    parser.add_argument("password", type=str, help="Boomi API Password")
    parser.add_argument("atom_name", help="Boomi Atom name where process will run")
    parser.add_argument("process_name", help="Boomi Process name that will executon on atom")
    parser.add_argument("-w", "--wait", help='Indicates if the script should wait for the job to complete (Default: No Wait)', action="store_true")
    parser.add_argument("-d", "--dynamicprops", help='Key:pair Boomi dynamic process properties seperated by a semicolon.\n\n\tIf the property values contain spaces, wrap the entire sequence in double quotes.\n\n\tExample: "DPP_1:abc123;DPP_2:xyz 321"', default='')
    if DEBUG:
        args = parser.parse_args(args = [
            'username',
            'password',
            'atom_name',
            'process_name',
            '-w',
            '-d', 'key1:value1;key2:value2',
        ])
        verbose = True
    else:
        args = parser.parse_args()
        verbose = False

    api_url      = "Boomi API URL"
    path_url     = "Boomi API Path URL"
    username     = args.username
    password     = args.password        
    atom_name    = args.atom_name
    process_name = args.process_name
    wait         = args.wait
    verbose      = False
    dynamic_properties = args.dynamicprops
    launcher = BoomiAPI(api_url, path_url, username, password, atom_name, process_name, wait, dynamic_properties, verbose)
    launcher.run_process()
