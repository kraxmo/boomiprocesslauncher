# boomiprocesslauncher
This pythonic script runs a Boomi integration process via Boomi API call.

It does the following:
1. Valid atom name
2. Valid atom environment
3. Valid process name
4. Valid process deployed in atom environment
5. If optional -w or -wait parameter is specified, 
   script will attempt to wait for process to complete (successful or fail)

UNIT TESTING:

test_boomi_process_launcher.py mock tests all API calls *without* communicating with Boomi Atom API layer:

>py -m unittest test_boomi_process_launcher.py