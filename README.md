# boomiprocesslauncher
Launch process via Boomi API call

This script runs a Boomi integration process via Boomi API call.

It verifies the following:
1. Valid atom name
2. Valid atom environment
3. Valid process name
4. Valid process deployed in atom environment

It executes the process and verifies it has been queued for execution

If optional -w or -wait parameter is specified, script will attempt to wait for process to complete (successful or fail)
