/*
=============================================================
Create Database
=============================================================
Script Purpose:
    This script creates a new database named 'datawarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script checks for any
    active connections to the database and terminates them if present.
WARNING:
	It is important to run each step separately, and you must be connected to 
    a different database when executing this script. Running this script will 
    drop the entire 'datawarehouse' database if it exists. All data in the 
    database will be permanently deleted. Proceed with caution and ensure you 
    have proper backups before running this script.
*/

-- Step 1: Terminate connections
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'datawarehouse'
  AND pid <> pg_backend_pid();

-- Step 2: Drop 'datawarehouse' database if exists
DROP DATABASE IF EXISTS datawarehouse;

-- Step 3: Recreate Database
CREATE DATABASE datawarehouse;
