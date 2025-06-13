# boomi_process_launcher.py
This script runs a Boomi integration process via Boomi API call.

It verifies the following:
1. api_url
2. path_url
3. username
4. password
5. Valid atom name
6. Valid atom environment
7. Valid process name
8. Valid process deployed in atom environment

It executes the process and verifies it has been queued for execution

If optional -w or -wait parameter is specified, script will attempt to wait for process to complete (successful or fail)

# test_boomi_process_launcher.py
This script runs a unittest vs. boomi_process_launcher.py

# boomi_process_launcher_from_config.py
This script runs a Boomi integration process via Boomi API call using a configuration file.

It verifies the following:
1. Valid atom name
2. Valid atom environment
3. Valid process name
4. Valid process deployed in atom environment

It executes the process and verifies it has been queued for execution

If optional -w or -wait parameter is specified, script will attempt to wait for process to complete (successful or fail)

# test_boomi_process_launcher_with_config.py
This script runs a unittest vs. boomi_process_launcher_with_config.py

*Known issue*
class TestPrintLogMessage function test_format_log_message returns errors even though when calling the same function within test_boomi_process_launcher.py executes successfully.
