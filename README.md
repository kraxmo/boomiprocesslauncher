# boomi_process_launcher.py
This script runs a Boomi integration process via Boomi API call.

It verifies the following Boomi information:
1. api_url
2. path_url
3. username
4. password
5. atom name
6. atom environment
7. process name
8. process deployed in atom environment

It executes the process and verifies it has been queued for execution.
If optional -w or -wait parameter is specified, script will attempt to wait for process to complete (successful or fail).

# boomi_process_launcher_with_config.py
This script reads sensitive execution information from external file boomi_process_launcher.ini and uses it as input to run boomi_process_launcher.py 

# test_boomi_process_launcher.py
This script runs a unittest vs. boomi_process_launcher.py
C:\> python -m unittest test_boomi_process_launcher.py

# test_boomi_process_launcher_with_config.py
This script runs a unittest vs. boomi_process_launcher_with_config.py
C:\> python -m unittest test_boomi_process_launcher_with_config.py