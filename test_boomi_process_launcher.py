#test_boomi_process_launcher.py

from boomi_process_launcher import (
    BoomiAPI,
    ScriptExitException,
)
import datetime
import unittest as ut1
from unittest.mock import (
    patch,
    Mock,
)

def identify(func):
    def wrapper(*args, **kwargs):
        print(f"\nTEST: {func.__name__}")
        return func(*args, **kwargs)
    return wrapper

class TestUtility(ut1.TestCase):
    def setUp(self):
        self.boomi = BoomiAPI("atom_name", "process_name", False, None, False)

    @identify
    def test_convert_from_iso_to_local(self):
        # test proper datetime conversion logic
        date = datetime.datetime.strptime("2024-01-01T08:01:02Z","%Y-%m-%dT%H:%M:%SZ")
        test_date = self.boomi.convert_from_iso_to_local_datetime(date)
        constant_date = datetime.datetime.fromisoformat('2024-01-01 00:01:02-08:00')
        self.assertEqual(constant_date, test_date)
        
    @identify
    def test_delay_execution(self):
        # test programmed delay logic
        time = 1
        new_time = self.boomi.delay_execution(time)
        self.assertEqual(2, new_time)

    @identify
    def test_parse_dynamic_properties_valid(self):
        # test parsing validly formatted dynamic process properties
        self.boomi = BoomiAPI("atom_name", "process_name", False, "key1:value1;key2:value2", True)
        self.boomi.parse_dynamic_properties()

    @identify
    def test_parse_dynamic_properties_invalid(self):
        # test parsing invalidly formatted dynamic process properties
        self.boomi = BoomiAPI("atom_name", "process_name", False, "key1:value1;key2", True)
        with self.assertRaises(ScriptExitException):
            self.boomi.parse_dynamic_properties()

class TestPrintLogMessage(ut1.TestCase):
    def setUp(self):
        self.boomi = BoomiAPI("atom_name", "process_name", False, None, False)

    @patch('datetime.datetime', autospec=True)
    @patch('boomi_process_launcher.datetime', autospec=True)
    @identify
    def test_format_log_message(self, mock_datetime, mock_boomi_api):
        # test various formatting combinations of log message formatter
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

class TestConfigFile(ut1.TestCase):
    def setUp(self):
        self.boomi = BoomiAPI("atom_name", "process_name", False, None, False)
        self.python_script_path = ''

    @identify
    def test_retrieve_api_settings(self):
        # test retrieving api settings from external config (needs to read Mock file instead)
        self.boomi.retrieve_api_settings()
        self.assertEqual("api.boomi.com", self.boomi.api_url)

class TestRequestResponse(ut1.TestCase):
    def setUp(self):
        self.boomi = BoomiAPI("atom_name", "process_name", False, None, False)
        self.boomi.api_url = "jsonplaceholder.typicode.com"
        self.boomi.path_url = "/posts"
        self.boomi.username = "username"
        self.boomi.password = "password"
        self.boomi.MAX_WAIT_SECONDS = 1
        self.boomi.TOTAL_ATTEMPTS = 3
        self.boomi.connect_to_api()
        self.original_make_api_request = self.boomi.make_api_request
        self.boomi.make_api_request = Mock()

    def tearDown(self):
        self.boomi.make_api_request = self.original_make_api_request
        
    @identify
    def test_verify_atom_name_exists_200(self):
        # test receiving an HTTP response 200 when verifying atom name exists on Boomi server
        self.boomi.make_api_request.return_value = {
            '@type': 'QueryResult', 
            'result': 
                [
                    {
                        '@type': 'Atom', 
                        'capabilities': [],
                        'id': '12345',
                        'name': 'myatom', 
                        'status': 'ONLINE', 
                        'type': 'any', 
                        'hostName': 'xxx.org', 
                        'dateInstalled': '2021-04-08T23:36:15Z', 
                        'currentVersion': '25.01.0', 
                        'purgeHistoryDays': 5, 
                        'purgeImmediate': False, 
                        'forceRestartTime': 0
                    }
                ],
            'numberOfResults': 1
        }, 200, "OK"
        self.boomi.atom_name = "myatom"
        self.boomi.verify_atom_exists()
        self.assertTrue("12345", self.boomi.atom_id)

    @identify
    def test_verify_atom_name_exists_200_multiple_atoms(self):
        # test receiving an HTTP response 200 when verifying atom name with multiple names on Boomi server
        self.boomi.make_api_request.return_value = {
            '@type': 'QueryResult', 
            'result': 
                [
                    {
                        '@type': 'Atom', 
                        'capabilities': [],
                        'id': '12345',
                        'name': 'myatom', 
                        'status': 'ONLINE', 
                        'type': 'any', 
                        'hostName': 'xxx.org', 
                        'dateInstalled': '2021-04-08T23:36:15Z', 
                        'currentVersion': '25.01.0', 
                        'purgeHistoryDays': 5, 
                        'purgeImmediate': False, 
                        'forceRestartTime': 0
                    }
                ],
            'numberOfResults': 3
        }, 200, "OK"
        self.boomi.atom_name = "myatom"
        with self.assertRaises(ScriptExitException):
            self.boomi.verify_atom_exists()

    @identify
    def test_verify_atom_name_exists_400(self):
        # test receiving an HTTP response 400 when verifying atom name does not exist on Boomi server
        self.boomi.make_api_request.return_value = {
            '@type': 'QueryResult', 
            'result': 
                [
                    {
                        '@type': 'Atom', 
                        'capabilities': [],
                        'id': '12345',
                        'name': 'myatom', 
                        'status': 'ONLINE', 
                        'type': 'any', 
                        'hostName': 'xxx.org', 
                        'dateInstalled': '2021-04-08T23:36:15Z', 
                        'currentVersion': '25.01.0', 
                        'purgeHistoryDays': 5, 
                        'purgeImmediate': False, 
                        'forceRestartTime': 0
                    }
                ],
            'numberOfResults': 1
        }, 400, "ABORTED"
        self.boomi.atom_name = "myatom"
        with self.assertRaises(ScriptExitException):
            self.boomi.verify_atom_exists()

    @identify
    def test_verify_atom_environment_exists_200(self):
        # test receiving an HTTP response 200 when verifying atom environment exists on Boomi server
        self.boomi.make_api_request.return_value = {
            '@type': 'QueryResult', 
            'result': 
                [
                    {
                        '@type': 'Environment', 
                        'capabilities': [],
                        'environmentId': '23456',
                        'name': 'myenvironment', 
                        'status': 'ONLINE', 
                        'type': 'environment', 
                        'hostName': 'xxx.org', 
                        'dateInstalled': '2021-04-08T23:36:15Z', 
                        'currentVersion': '25.01.0', 
                        'purgeHistoryDays': 5, 
                        'purgeImmediate': False, 
                        'forceRestartTime': 0
                    }
                ],
            'numberOfResults': 1
        }, 200, "OK"
        self.boomi.atom_id = "12345"
        self.boomi.verify_atom_environment_exists()
        self.assertTrue("23456", self.boomi.environment_id)

    @identify
    def test_verify_process_exists_in_environment_200(self):
        # test receiving an HTTP response 200 when verifying process exists in environment on Boomi server
        self.boomi.make_api_request.return_value = {
            '@type': 'QueryResult', 
            'result': 
                [
                    {
                        '@type': 'Deployment', 
                        'capabilities': [],
                        'deploymentId': '45678',
                        'componentId': '67890',
                        'name': 'mydeployment', 
                        'status': 'ONLINE', 
                        'type': 'deployment', 
                        'hostName': 'xxx.org', 
                        'dateInstalled': '2021-04-08T23:36:15Z', 
                        'currentVersion': '25.01.0', 
                        'purgeHistoryDays': 5, 
                        'purgeImmediate': False, 
                        'forceRestartTime': 0
                    }
                ],
            'numberOfResults': 1
        }, 200, "OK"
        self.boomi.environment_id = "23456"
        self.boomi.process_id = "34567"
        self.boomi.verify_process_exists_in_environment()
        self.assertTrue("45678", self.boomi.deployment_id)
        self.assertTrue("67890", self.boomi.component_id)

    @identify
    def test_initiate_atom_process_200(self):
        # test receiving an HTTP response 200 when starting a process name on Boomi server
        self.boomi.make_api_request.return_value = {
                '@type': 'ExecutionRequest',
                'DynamicProcessProperties': 
                {
                    '@type': 'ExecutionRequestDynamicProcessProperties', 
                    'DynamicProcessProperty': 
                        [
                            {
                                '@type': '',
                                'name': 'key1', 
                                'value': 'value1'
                            }, 
                            {
                                '@type': '',
                                'name': 'key2', 
                                'value': 'value2'
                            }, 
                        ]
                }, 
                'processId': '56789', 
                'atomId': '12345', 
                'requestId': 'executionrecord-6dd73fca-81ee-4f21-b1c1-9a6e0ec79d48', 
                'recordUrl': 'https://platform.boomi.com/api/rest/v1/schoolsfirstfederalcredit-P1B39S/ExecutionRecord/async/executionrecord-6dd73fca-81ee-4f21-b1c1-9a6e0ec79d48'
            }, 200, "OK"
        self.boomi.atom_id = "12345"
        self.boomi.component_id = "56789"
        self.boomi.dynamic_properties = "key1:value1;key2:value2"
        self.boomi.initiate_atom_process()
        self.assertTrue("56789", self.boomi.execution_id)

    @identify
    def test_monitor_process_200_nowait_complete(self):
        # test receiving an HTTP response 200 OK COMPLETE when monitoring a process name on Boomi server with no waiting
        self.boomi.make_api_request.return_value = {
            '@type': 'QueryResult', 
            'result': 
                [
                    {
                        '@type': 'ExecutionRecord',
                        'executionId': '56789',
                        'account': 'account',
                        'executionTime': '2025-01-14T18:49:42Z',
                        'status': 'COMPLETE',
                        'executionType': 'exec_manual',
                        'processName': 'myprocess',
                        'processId': '34567',
                        'atomName': 'myatom',
                        'atomId': '12345',
                        'inboundDocumentCount': 1,
                        'inboundErrorDocumentCount': 0,
                        'outboundDocumentCount': 1,
                        'executionDuration': ['Long', 0],
                        'inboundDocumentSize': ['Long', 0],
                        'outboundDocumentSize': ['Long', 00],
                        'recordedDate': '2025-01-14T18:49:47Z'
                    }
                ]
        }, 200, "OK"
        self.boomi.execution_id = "56789"
        self.boomi.monitor_process(False, 1)
        self.assertEqual("COMPLETE", self.boomi.execution_status)

    @identify
    def test_monitor_process_200_nowait_inprocess(self):
        # test receiving an HTTP response 200 OK INPROCESS when monitoring a process name on Boomi server with no waiting
        self.boomi.make_api_request.return_value = {
            '@type': 'QueryResult', 
            'result': 
                [
                    {
                        '@type': 'ExecutionRecord',
                        'executionId': '56789',
                        'account': 'account',
                        'executionTime': '2025-01-14T18:49:42Z',
                        'status': 'INPROCESS',
                        'executionType': 'exec_manual',
                        'processName': 'myprocess',
                        'processId': '34567',
                        'atomName': 'myatom',
                        'atomId': '12345',
                        'inboundDocumentCount': 1,
                        'inboundErrorDocumentCount': 0,
                        'outboundDocumentCount': 1,
                        'executionDuration': ['Long', 0],
                        'inboundDocumentSize': ['Long', 0],
                        'outboundDocumentSize': ['Long', 00],
                        'recordedDate': '2025-01-14T18:49:47Z'
                    }
                ]
        }, 200, "OK"
        self.boomi.execution_id = "56789"
        self.boomi.monitor_process(False, 1)
        self.assertEqual("INPROCESS", self.boomi.execution_status)

    @identify
    def test_monitor_process_200_wait_complete(self):
        # test receiving an HTTP response 200 OK COMPLETE when monitoring a process name on Boomi server while waiting for process completion
        self.boomi.make_api_request.return_value = {
            '@type': 'QueryResult', 
            'result': 
                [
                    {
                        '@type': 'ExecutionRecord',
                        'executionId': '56789',
                        'account': 'account',
                        'executionTime': '2025-01-14T18:49:42Z',
                        'status': 'COMPLETE',
                        'executionType': 'exec_manual',
                        'processName': 'myprocess',
                        'processId': '34567',
                        'atomName': 'myatom',
                        'atomId': '12345',
                        'inboundDocumentCount': 1,
                        'inboundErrorDocumentCount': 0,
                        'outboundDocumentCount': 1,
                        'executionDuration': ['Long', 0],
                        'inboundDocumentSize': ['Long', 0],
                        'outboundDocumentSize': ['Long', 00],
                        'recordedDate': '2025-01-14T18:49:47Z'
                    }
                ]
        }, 200, "OK"
        self.boomi.execution_id = "56789"
        self.boomi.monitor_process(True, 1)
        self.assertEqual("COMPLETE", self.boomi.execution_status)

    @identify
    def test_monitor_process_200_wait_inprocess(self):
        # test receiving an HTTP response 200 OK INPROCESS when monitoring a process name on Boomi server while waiting for process completion
        self.boomi.make_api_request.return_value = {
            '@type': 'QueryResult', 
            'result': 
                [
                    {
                        '@type': 'ExecutionRecord',
                        'executionId': '56789',
                        'account': 'account',
                        'executionTime': '2025-01-14T18:49:42Z',
                        'status': 'INPROCESS',
                        'executionType': 'exec_manual',
                        'processName': 'myprocess',
                        'processId': '34567',
                        'atomName': 'myatom',
                        'atomId': '12345',
                        'inboundDocumentCount': 1,
                        'inboundErrorDocumentCount': 0,
                        'outboundDocumentCount': 1,
                        'executionDuration': ['Long', 0],
                        'inboundDocumentSize': ['Long', 0],
                        'outboundDocumentSize': ['Long', 00],
                        'recordedDate': '2025-01-14T18:49:47Z'
                    }
                ]
        }, 200, "OK"
        self.boomi.execution_id = "56789"
        self.boomi.monitor_process(True, 1)
        self.assertEqual("INPROCESS", self.boomi.execution_status)
