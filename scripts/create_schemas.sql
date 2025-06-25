/*
=============================================================
Create Schemas
=============================================================
Script Purpose:
    This script sets up three schemas within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    In PostgreSQL, you must be connected to the target database before creating schemas.
    Make sure the database has been created first. You can use the script provided 
    in 'init_database.sql' to create the database.
*/

CREATE SCHEMA bronze;

CREATE SCHEMA silver;

CREATE SCHEMA gold;
