#boomi_process_launcher.py
"""
Launch Boomi Process via API calls

Created by Jim Kraxberger on 1-9-2025

Use Python Standard Libraries

External Dependencies:
- boomi_process_launcher.ini

Command Line Execution:
- Windows     : py boomi_process_launcher "atom_name" "process_name" -d "property_key1:property_value1;property_key2:property_value2"
- Windows wait: py boomi_process_launcher "atom_name" "process_name" -d "property_key1:property_value1;property_key2:property_value2" -w
- Unix        : py boomi_process_launcher 'atom_name' 'process_name' -d 'property_key1:property_value1;property_key2:property_value2'
- Unix    wait: py boomi_process_launcher 'atom_name' 'process_name' -d 'property_key1:property_value1;property_key2:property_value2' -w
"""

import argparse
from base64 import b64encode
from configparser import ConfigParser
from datetime import(
    datetime,
    timezone,
)
import http.client as hc1
import inspect as i1
import json as j1
from os import path
import sys as s1
import time as t1

DEBUG = False
HELP_EPILOG = '''

This script initiates a request to execute a Boomi atom process with dynamical process properties (optional) and can wait (optionally) for execution completion (either SUCCESS or FAILURE).

'''

class BoomiAPI():
    """Call Boomi API to execute process using atom name, process name and optional dynamic process properties"""

    DAILY_SECONDS              = 86400
    CONFIGURATION_FILENAME     = r'\boomi_process_launcher.ini'
    EXECUTION_STATUS           = {
        'KNOWN': ['ABORTED', 'COMPLETE', 'COMPLETE_WARN', 'DISCARDED', 'ERROR', 'INPROCESS', 'STARTED'],
        'SUCCESS': ['COMPLETE', 'COMPLETE_WARN'],
        'TERMINATED': ['UNKNOWN', 'ABORTED', 'DISCARDED', 'ERROR']
    }
    GROUP1_LENGTH              = 32
    MAX_WAIT_SECONDS           = 60
    RESPONSE_CODE_200_OK       = 200
    RESPONSE_CODE_202_ACCEPTED = 202
    TOTAL_ATTEMPTS             = 1440
    TOTAL_ERRORS               = 3
    TOTAL_TRIES                = 3
    VALID_RESPONSE_CODES       = {RESPONSE_CODE_200_OK, RESPONSE_CODE_202_ACCEPTED}

    def connect_to_api(self) -> None:
        """_summary_

        Args:
            api_url (str): Boomi API url
            username (str): Boomi user name
            password (str): Boomi password
        """
        # Setup API connection
        self.connection = hc1.HTTPSConnection(self.api_url)
        
        # Setup API headers
        login           = f"{self.username}:{self.password}".encode("utf-8")
        authorization   = f"Basic {b64encode(login).decode('utf-8')}"
        self.headers = {'Accept':'application/json','Content-Type':'application/json','Authorization':authorization}

    def convert_from_iso_to_local_datetime(self, iso_date:str) -> datetime:
        """Convert date from ISO to local datetime
        
        Args: 
            iso_date (str): datetime in ISO date format

        Returns: 
            (datetime): datetime in datetime format
        """
        utc_time = iso_date.replace(tzinfo=timezone.utc)
        local_time = utc_time.astimezone()
        return local_time

    def delay_execution(self, wait_seconds: int) -> int:
        """Delay execution for wait_seconds seconds up to maximum wait period

        Args:
            wait_seconds (int): time to wait in seconds

        Returns: 
            (int): updated wait time in seconds
        """
        t1.sleep(wait_seconds)
        return min(wait_seconds * 2, self.MAX_WAIT_SECONDS)

    def format_log_message(self, section1: str, section2: str=None, section3: str=None, section4: str=None) -> str:
        """Format message into OpCon log output format
        
        Args:
            section1 (str): log section 1
            section2 (str, optional): log section 2. Defaults to None.
            section3 (str, optional): log section 3. Defaults to None.
            section4 (str, optional): log section 4. Defaults to None.
            
        Returns: log (str): formatted OpCon log message
        """
        log = f"{datetime.now()}\t"
        if section2 is None:
            log += section1
        else:
            log += section1.ljust(self.GROUP1_LENGTH)+section2

        if section3 is not None:
            log+=f"\n\t\t\t\t{section3}"

        if section4 is not None:
            log+=f"\n\t\t\t\t{section4}"

        return log

    def run_process(self, atom_name: str, process_name: str, wait: bool, dynamic_properties: str=None, verbose: bool=False) -> None:
        """Run Boomi atom process"""
        EXIT_CODE_SUCCESSFUL = 0
        EXIT_CODE_ERROR      = 1
        
        exit_code = EXIT_CODE_ERROR         # set execution to error status
        try:
            if atom_name is None or len(atom_name.strip()) == 0:
                print(self.format_log_message("ERROR Atom name cannot be blank"))
                raise ScriptExitException   # exit script

            self.atom_name          = atom_name.strip()
            if process_name is None or len(process_name.strip()) == 0:
                print(self.format_log_message("ERROR Process name cannot be blank"))
                raise ScriptExitException   # exit script

            self.process_name       = process_name.strip()
            self.wait               = wait
            self.dynamic_properties = dynamic_properties

            self.retrieve_api_settings()    # read sensitive information from external configuration file
            self.connect_to_api()           # connect to Boomi api
            self.verify_atom_name()         # verify atom name exists
            self.verify_environment()       # verify atom environment exists
            self.verify_process_name()      # verify process name exists
            self.verify_deployed_process()  # verify process is deployed in atom environment
            self.initiate_process()         # start executing process name in atom name
            
            # check to see if process is running
            wait_seconds = 1
            wait_seconds = self.monitor_process(wait_seconds)
            if not self.wait:                           # exit script if waiting for Boomi process to finish is not required
                if self.execution_status in self.EXECUTION_STATUS["TERMINATED"]:
                    print(self.format_log_message(f"Process {self.process_name} failed to start", None, f"{self.execution_status}"))
                else:                    
                    print(self.format_log_message(f"Process {self.process_name} successfully sent to Boomi Atom {self.atom_name}"))
                    exit_code = EXIT_CODE_SUCCESSFUL    # set execution for successful processing status
                
                raise ScriptExitException               # exit script

            self.delay_execution(wait_seconds)          # pause between initial check and subsequent checking

            # if execution status not successful, monitor process for some status
            if self.execution_status not in self.EXECUTION_STATUS['SUCCESS']:
                self.monitor_process(wait_seconds)

            if self.execution_status == 'COMPLETE':     # report complete status
                print(self.format_log_message(f"Process completed successfully at {self.execution_completed_timestamp}"))
                exit_code = EXIT_CODE_SUCCESSFUL        # set execution for successful processing status
                raise ScriptExitException               # exit script

            if 'result' in self.response['result'][0]:  # report incomplete or unknown status
                if 'message' in self.response['result'][0]:
                    print(self.format_log_message(f"{self.response['result'][0]['message']}"))
            else:
                print(self.format_log_message(f"WARNING: Unable to determine status of process {self.process_name} execution"))
                
        except ScriptExitException:
            pass            # if execution comes here at any time, pass to script exit point

        except Exception as err:
            print(self.format_log_message("ERROR Executing Boomi API Process steps", None, err))
        
        finally:
            exit(exit_code) # script exit point

    def get_requested_id(self, action: str, endpoint: str, body: str, status_codes: set, name: str, description: str, value: str) -> str:
        """Retrieve requested id using API endpoint and body

        Args:
            action (str): query or execution
            endpoint(str): API endpoint
            body (str): HTTP API body url
            status_codes(set): valid HTTP Response Codes
            name (str): API requested id name 
            description (str): requested component name
            value (str): requested value
        
        Returns: API requested id (str)
        """
        method_signature = f"{__class__.__name__}.{i1.stack()[0][3]}('{action}', '{endpoint}', '{body}', '{status_codes}', '{name}', '{description}')"
        try:
            execution_status = 'SENDING'
            for _ in range(self.TOTAL_ATTEMPTS):
                response, status, message = self.make_api_request('POST', endpoint, body, status_codes)
                if action == "query":
                    print(self.format_log_message(f"POST {action.title()} {description} ID:", f"{status} {message}"))
                else:
                    print(self.format_log_message(f"POST {action.title()} {description} ID:", f"{status} {message} ({execution_status})"))
                if status == self.RESPONSE_CODE_200_OK:
                    if action == 'query':
                        if 'numberOfResults' in response:
                            results = response['numberOfResults']
                            if results == 1:
                                if 'status' in response['result'][0]:
                                    execution_status = response['result'][0]['status']

                                print(self.format_log_message(f"{description} ID:", response['result'][0][name]))
                                return response['result'][0][name]  # return requested id
                            
                        print(self.format_log_message(f"{results} {description} found with name '{value}'", None, method_signature if verbose==True else None))
                        raise ScriptExitException   # exit script
                    else:
                        if action == 'execution':
                            requested_id = response.get(name)
                            print(self.format_log_message("Request ID:", requested_id))
                            return requested_id
                        else:
                            raise ValueError
                            
                execution_status = 'UNKNOWN'
                print(self.format_log_message(f"Failed: {description}.", f"Retrying in {self.MAX_WAIT_SECONDS} seconds. ({execution_status})"))
                t1.sleep(self.MAX_WAIT_SECONDS)

            print(self.format_log_message(f"No {description} found", None, method_signature if verbose==True else None))
            
        except ScriptExitException:
            # if execution comes here, re-raise it to exit script
            raise
            
        except Exception as err:
            print(self.format_log_message("Processing POST request", None, err , method_signature if verbose==True else None))

        raise ScriptExitException   # force exit as TOTAL_ATTEMPTS exceeded

    def initiate_process(self):
        """Get Boomi Execution Id for monitoring Process ID execution"""
        endpoint = self.path_url + "/ExecutionRequest"
        payload  = {
            '@type':'ExecutionRequest',
            "atomId": self.atom_id,
            "processId": self.process_id
        }
        payload.update(self.parse_dynamic_properties())
        body     = j1.dumps(payload)
        self.execution_id = self.get_requested_id('execution', endpoint, body, {self.RESPONSE_CODE_200_OK}, 'requestId', f"Request", self.atom_name+'|'+self.process_name)
        if self.execution_id == '':
            print(self.format_log_message(f"Failed to start atom {self.atom_name} process {self.process_name}. Check Boomi for details"))
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
        method_signature = f"{__class__.__name__}.{i1.stack()[0][3]}('{method}', '{endpoint}', '{body}', '{status_codes}')"
        if status_codes is None:
            status_codes = {self.RESPONSE_CODE_200_OK}
        
        try:
            for _ in range(self.TOTAL_TRIES):
                self.connection.request(method, endpoint, body=body, headers=self.headers)
                response = self.connection.getresponse()
                status = response.status
                message = response.reason
                if status in status_codes:
                    return j1.loads(response.read().decode("utf-8")), status, message

                print(self.format_log_message(f"Failed to start process. Retrying in {self.MAX_WAIT_SECONDS} seconds."))
                t1.sleep(self.MAX_WAIT_SECONDS)

        except Exception as err:
            print(self.format_log_message(f"Processing HTTP {method} request", None, err, method_signature if verbose==True else None))
            raise ScriptExitException   # exit script

        finally:
            self.connection.close()

    def monitor_process(self, wait_seconds: int):
        """Monitor process execution status until completion or timeout

        Returns: wait_seconds (int): wait_seconds in seconds
        """
        method_signature = f"{__class__.__name__}.{i1.stack()[0][3]}('{self.execution_id}')"
        endpoint = self.path_url + f"/ExecutionRecord/async/{self.execution_id}"
        execution_status = 'STATUS PENDING'
        execution_completed_timestamp = ''
        errors = 0
        attempts = 0
        try:
            while   attempts < self.TOTAL_ATTEMPTS and \
                    errors   < self.TOTAL_ERRORS   and \
                    execution_status not in self.EXECUTION_STATUS["KNOWN"]:
                response, status, message = self.make_api_request('GET', endpoint, None, self.VALID_RESPONSE_CODES)
                if status == self.RESPONSE_CODE_202_ACCEPTED:
                    print(self.format_log_message(f"GET Execution Status:", f"{status} {message} ({execution_status})"))
                    wait_seconds = self.delay_execution(wait_seconds)
                    continue
                
                elif status == self.RESPONSE_CODE_200_OK:
                    if 'result' in response:
                        if 'status' in response['result'][0]:
                            execution_status = response['result'][0]['status']
                            print(self.format_log_message(f"GET Execution Status:", f"{status} {message} ({execution_status})"))
                            
                        if 'recordedDate' in response['result'][0]:
                            recordedDate = datetime.strptime(response['result'][0]['recordedDate'],"%Y-%m-%dT%H:%M:%SZ")
                            execution_completed_timestamp = self.convert_from_iso_to_local_datetime(recordedDate)

                        self.response = response
                        self.execution_status = execution_status
                        self.execution_completed_timestamp = execution_completed_timestamp
                        return wait_seconds

                    print(self.format_log_message(f"{status}: Process aborted"))
                    raise ScriptExitException   # exit script

                attempts += 1
                errors += 1
                execution_status = 'UNKNOWN'
                print(self.format_log_message(f"GET Execution Status:", f"{status} {message} ({execution_status})", f"Retrying in {wait_seconds} seconds"))
                wait_seconds = self.delay_execution(wait_seconds)

        except Exception as err:
            print(self.format_log_message("Processing HTTP GET request", None, err, method_signature if verbose==True else None))
            raise ScriptExitException   # exit script

    def parse_dynamic_properties(self) -> dict:
        """format dynamic process properties json payload
        
        Returns:
            dynamic_properties_json (dict): json-formatted Boomi API Dynamic Process Properties
        """
        method_signature = f"{__class__.__name__}.{i1.stack()[0][3]}('{self.dynamic_properties}')"
        if len(self.dynamic_properties.strip()) == 0:
            return ''
        
        try:
            dynamic_properties_list = []
            pairs = self.dynamic_properties.strip().split(";")
            for pair in pairs:
                if pair:    # pair is not blank (multiple/leading/trailing ;)
                    if ":" not in pair:
                        print(self.format_log_message(f"Invalid key:pair data format for pair '{pair}'", None, method_signature if verbose==True else None))
                        raise ScriptExitException   # exit script
                    else:
                        name, value = pair.split(":", 1)
                        dynamic_properties_list.append({"name": name, "value": value})
                        print(self.format_log_message("Dynamic Prop:", pair))

            dynamic_properties_json = {"DynamicProcessProperties": {"DynamicProcessProperty": dynamic_properties_list}}
            return dynamic_properties_json
        
        except ScriptExitException:
            # if execution comes here, re-raise it to exit script
            raise
        
        except Exception as err:
            print(self.format_log_message("Parsing dynamic process properties", None, err, method_signature))
            raise ScriptExitException   # exit script

    def retrieve_api_settings(self):
        """Read api configuration file settings"""
        method_signature = f"{__class__.__name__}.{i1.stack()[0][3]}()"
        try:
            config_file = path.dirname(path.realpath(s1.argv[0]))+self.CONFIGURATION_FILENAME
            config   = ConfigParser()
            config.read(config_file)
            key      = "connection"
            api_url  = config.get(key, "api_url")
            path_url = config.get(key, "path_url")
            username = config.get(key, "username")
            password = config.get(key, "password")

        except Exception as err:
            print(self.format_log_message(f"Reading configuration file {config_file}", None, err, method_signature))
            raise ScriptExitException   # exit script
            
        self.api_url, self.path_url, self.username, self.password = api_url, path_url, username, password

    def verify_atom_name(self) -> None:
        """Get Boomi Atom Id to verify Atom Name is valid"""
        endpoint = self.path_url + f"/Atom/query"
        body     = j1.dumps({'QueryFilter':{'expression':{'argument':[self.atom_name],'operator':'EQUALS','property':'name'}}})
        self.atom_id = self.get_requested_id('query', endpoint, body, {self.RESPONSE_CODE_200_OK}, 'id', f"Atom", self.atom_name)

    def verify_deployed_process(self) -> None:
        """Get Boomi Deployment Id to verify Process Id is deployed within Environment ID"""
        endpoint = self.path_url + f"/DeployedPackage/query"
        body     = j1.dumps({'QueryFilter':{'expression':{'operator':'and','nestedExpression':[{'argument':[self.environment_id],'operator':'EQUALS','property':'environmentId'},{'argument':['process'],'operator':'EQUALS','property':'componentType'},{'argument':[True],'operator':'EQUALS','property':'active'},{'argument':[self.process_id],'operator':'EQUALS','property':'componentId'}]}}})
        self.deployment_id = self.get_requested_id('query', endpoint, body, {self.RESPONSE_CODE_200_OK}, 'deploymentId', f"Deployment", self.process_name)

    def verify_environment(self) -> None:
        """Get Boomi Environment Id to verify Atom Id is valid"""
        endpoint = self.path_url + f"/EnvironmentAtomAttachment/query"
        body     = j1.dumps({'QueryFilter':{'expression':{'argument':[self.atom_id],'operator':'EQUALS','property':'atomId'}}})
        self.environment_id = self.get_requested_id('query', endpoint, body, {self.RESPONSE_CODE_200_OK}, 'environmentId', f"Environment", self.atom_name)

    def verify_process_name(self) -> None:
        """Get Boomi Process Id to verify Process Name is valid
        
        Args: process_name (str): name of Boomi process
        """
        endpoint = self.path_url + f"/Process/query"
        body     = j1.dumps({'QueryFilter':{'expression':{'argument':[self.process_name],'operator':'EQUALS','property':'name'}}})
        self.process_id = self.get_requested_id('query', endpoint, body, {self.RESPONSE_CODE_200_OK}, 'id', f"Process", self.process_name)

class ScriptExitException(Exception):
    """Placeholder exception to allow for programmatically graceful exits from the script
    
    Instead of issuing an immediate exit(#) function, raising this exception does the following:
    1. Breaks out of the current code
    2. Allows exception to be passed to common end-of-routine exit (one-entry/one-exit) in the BoomiProcessLauncher.run() method
    """
    pass

if __name__ == "__main__":
    verbose = False
    if DEBUG:
        atom_name = "Test Analytics"
        process_name = "Jim_Test"
        dynamic_properties = "Jim_Test_EmailTo:jkraxberger@schoolsfirstfcu.org;Jim_Test_WaitPeriod:5;Jim_Test_SendEmailYN:Y"
        wait = True
        verbose = True
        # atom_name = '1'
        # process_name = '1'
        # dynamic_properties = ";;;" # "bif"
    else:
        parser = argparse.ArgumentParser(
            description="Execute a Boomi process and wait for completion",
            epilog=HELP_EPILOG,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        parser.add_argument("atom_name", help="Boomi Atom name where process will run")
        parser.add_argument("process_name", help="Boomi Process name that will executon on atom")
        parser.add_argument("-w", "--wait", help='Indicates if the script should wait for the job to complete (Default: No Wait)', action="store_true")
        parser.add_argument("-d", "--dynamicprops", help='Key:pair Boomi dynamic process properties seperated by a semicolon.\n\n\tIf the property values contain spaces, wrap the entire sequence in double quotes.\n\n\tExample: "DPP_1:abc123;DPP_2:xyz 321"', default='')
        args = parser.parse_args()
        
        atom_name = args.atom_name.strip()
        process_name = args.process_name.strip()
        wait = args.wait
        if len(dynamic_properties := args.dynamicprops):
            dynamic_properties = args.dynamicprops.strip()

    launcher = BoomiAPI()
    launcher.run_process(atom_name, process_name, wait, dynamic_properties, verbose)
