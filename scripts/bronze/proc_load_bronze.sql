/*
=============================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=============================================================

Script Purpose:
    This procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
      - Truncates the bronze tables before loading data.
      - Uses the 'COPY' command to load data from CSV files into the bronze tables.

    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze();

WARNING:
    Ensure that the database, schemas, and tables have been created beforehand.
    If using this script, don't forget to update the data directory path.
*/


CREATE OR REPLACE PROCEDURE bronze.load_bronze()
AS $$
DECLARE
	start_time TIMESTAMP;
	end_time TIMESTAMP;

	start_time_overall TIMESTAMP;
	end_time_overall TIMESTAMP;
BEGIN
	BEGIN
		start_time_overall := clock_timestamp();
		RAISE NOTICE '========================================================================';
		RAISE NOTICE 'Loading Bronze Layer';
		RAISE NOTICE '========================================================================';
	
		RAISE NOTICE '------------------------------------------------------------------------';
		RAISE NOTICE 'Loading CRM Tables';
		RAISE NOTICE '------------------------------------------------------------------------';

		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
	
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
		COPY bronze.crm_cust_info
		FROM '/Users/mbent/Desktop/data/datawarehousepostgresproject/source_crm/cust_info.csv'
		WITH (
		    FORMAT csv,
		    HEADER,
		    DELIMITER ','
		);
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
	
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
		COPY bronze.crm_prd_info
		FROM '/Users/mbent/Desktop/data/datawarehousepostgresproject/source_crm/prd_info.csv'
		WITH (
		    FORMAT csv,
		    HEADER,
		    DELIMITER ','
		);
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		
		RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
		COPY bronze.crm_sales_details
		FROM '/Users/mbent/Desktop/data/datawarehousepostgresproject/source_crm/sales_details.csv'
		WITH (
		    FORMAT csv,
		    HEADER,
		    DELIMITER ','
		);
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
	
		RAISE NOTICE '------------------------------------------------------------------------';
		RAISE NOTICE 'Loading ERP Tables';
		RAISE NOTICE '------------------------------------------------------------------------';

		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
	
		RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
		COPY bronze.erp_cust_az12
		FROM '/Users/mbent/Desktop/data/datawarehousepostgresproject/source_erp/CUST_AZ12.csv'
		WITH (
		    FORMAT csv,
		    HEADER,
		    DELIMITER ','
		);
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
	
		RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
		COPY bronze.erp_loc_a101
		FROM '/Users/mbent/Desktop/data/datawarehousepostgresproject/source_erp/LOC_A101.csv'
		WITH (
		    FORMAT csv,
		    HEADER,
		    DELIMITER ','
		);
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);

		start_time := clock_timestamp();
		RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	
		RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		COPY bronze.erp_px_cat_g1v2
		FROM '/Users/mbent/Desktop/data/datawarehousepostgresproject/source_erp/PX_CAT_G1V2.csv'
		WITH (
		    FORMAT csv,
		    HEADER,
		    DELIMITER ','
		);
		end_time := clock_timestamp();
		RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
		end_time_overall := clock_timestamp();
		RAISE NOTICE '>> OVERALL DURATION: % seconds', EXTRACT(EPOCH FROM end_time_overall - start_time_overall);
	EXCEPTION
        WHEN OTHERS THEN
			RAISE NOTICE '========================================================================';
            RAISE NOTICE 'An error occurred: %', SQLERRM;
			RAISE NOTICE 'An error occurred: %', SQLSTATE;
			RAISE NOTICE '========================================================================';
    END;
END;
$$ LANGUAGE plpgsql;
