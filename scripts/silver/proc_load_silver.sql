/*
=============================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
=============================================================

Script Purpose:
    This procedure performs the ETL (Extract, Transform, Load) process to
    populate the 'silver' schema tables from the 'bronze' schema.
It performs the following actions:
    - Truncate Silver Tables,
    - Inserts transformed and cleansed data from Bronze into Silver Tables.

    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL silver.load_silver();

WARNING:
    Ensure that the database, schemas, and tables have been created beforehand.
    You also need to run bronze.load_bronze() procedure before.
*/



CREATE OR REPLACE PROCEDURE silver.load_silver()
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
            RAISE NOTICE 'Loading silver Layer';
            RAISE NOTICE '========================================================================';

            RAISE NOTICE '------------------------------------------------------------------------';
            RAISE NOTICE 'Loading CRM Tables';
            RAISE NOTICE '------------------------------------------------------------------------';

		        start_time := clock_timestamp();
            RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
            TRUNCATE TABLE silver.crm_cust_info;

            RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
            INSERT INTO silver.crm_cust_info(
                                            cst_id,
                                            cst_key,
                                            cst_firstname,
                                            cst_lastname,
                                            cst_marital_status,
                                            cst_gndr,
                                            cst_create_date)
            SELECT
                cst_id,
                cst_key,
                TRIM(cst_firstname) AS cst_firstname,
                TRIM(cst_lastname) AS cst_lastname,
                CASE
                    WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                    WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                    ELSE 'n/a'
                    END AS cst_marital_status,
                CASE
                    WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                    ELSE 'n/a'
                END AS cst_gndr,
                cst_create_date
                FROM (
                    SELECT
                    *,
                    ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
                    FROM bronze.crm_cust_info
                    WHERE cst_id IS NOT NULL
                )t
              WHERE flag = 1;

            end_time := clock_timestamp();
            RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
  
            start_time := clock_timestamp();
            RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
            TRUNCATE TABLE silver.crm_prd_info;
  
            RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
            INSERT INTO silver.crm_prd_info(
                                            prd_id,
                                            cat_id,
                                            prd_key,
                                            prd_nm,
                                            prd_cost,
                                            prd_line,
                                            prd_start_dt,
                                            prd_end_dt)
            SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5),'-', '_') AS cat_id,
            SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
            prd_nm,
            COALESCE(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            prd_start_dt,
            LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt
            FROM bronze.crm_prd_info;
  
            end_time := clock_timestamp();
            RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
  
            start_time := clock_timestamp();
            RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
            TRUNCATE TABLE silver.crm_sales_details;
  
            RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
            INSERT INTO silver.crm_sales_details(
                                            sls_ord_num,
                                            sls_prd_key,
                                            sls_cust_id,
                                            sls_order_dt,
                                            sls_ship_dt,
                                            sls_due_dt,
                                            sls_sales,
                                            sls_quantity,
                                            sls_price)
            SELECT sls_ord_num,
                   sls_prd_key,
                   sls_cust_id,
                   CASE
                        WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS varchar)) !=8 THEN NULL
                        ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
                   END AS sls_order_dt,
                   CASE
                        WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS varchar)) !=8
                            THEN NULL
                        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
                   END AS sls_ship_dt,
                   CASE
                        WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS varchar)) !=8
                            THEN NULL
                        ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
                   END AS sls_due_dt,
                    CASE
                        WHEN
                            sls_sales IS NULL OR
                            sls_sales <= 0  OR
                            sls_sales != sls_quantity * ABS(sls_price)
                            THEN sls_quantity * ABS(sls_price)
                        ELSE sls_sales
                    END AS sls_sales,
                   sls_quantity,
                    CASE
                        WHEN
                            sls_price IS NULL OR
                            sls_price <= 0
                        THEN sls_sales / NULLIF(sls_quantity, 0)
                        ELSE ABS(sls_price)
                    END AS sls_price
            FROM bronze.crm_sales_details;
  
            end_time := clock_timestamp();
            RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
  
            RAISE NOTICE '------------------------------------------------------------------------';
            RAISE NOTICE 'Loading ERP Tables';
            RAISE NOTICE '------------------------------------------------------------------------';
  
            start_time := clock_timestamp();
            RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
            TRUNCATE TABLE silver.erp_cust_az12;
  
            RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
            INSERT INTO silver.erp_cust_az12 (
                                              cid,
                                              bdate,
                                              gen)
            SELECT
                CASE WHEN cid LIKE 'NAS%'
                    THEN SUBSTRING(cid, 4, LENGTH(cid))
                    ELSE cid
                    END AS cid,
                CASE
                    WHEN bdate > current_date THEN NULL
                    ELSE bdate
                END AS bdate,
                CASE
                    WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                    WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                    ELSE 'n/a'
                END AS gen
            FROM bronze.erp_cust_az12;
  
            end_time := clock_timestamp();
            RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
  
            start_time := clock_timestamp();
            RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
            TRUNCATE TABLE silver.erp_loc_a101;
  
            RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
            INSERT INTO silver.erp_loc_a101(
                                            cid,
                                            cntry)
            SELECT
                REPLACE(cid,'-','') AS cid,
                CASE
                    WHEN UPPER(TRIM(cntry)) IN ('DE', 'GERMANY') THEN 'Germany'
                    WHEN UPPER(TRIM(cntry)) IN ('US', 'USA', 'UNITED STATES') THEN 'United States'
                    WHEN UPPER(TRIM(cntry)) IN ('AUS', 'AUSTRALIA') THEN 'Australia'
                    WHEN UPPER(TRIM(cntry)) IN ('CAN', 'CANADA') THEN 'Canada'
                    WHEN UPPER(TRIM(cntry)) IN ('UK', 'UNITED KINGDOM') THEN 'United Kingdom'
                    WHEN UPPER(TRIM(cntry)) IN ('FR', 'FRANCE') THEN 'France'
                    ELSE 'n/a'
                END AS cntry
            FROM bronze.erp_loc_a101;
  
            end_time := clock_timestamp();
            RAISE NOTICE '>> Load Duration: % seconds', EXTRACT(EPOCH FROM end_time - start_time);
  
            start_time := clock_timestamp();
            RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
            TRUNCATE TABLE silver.erp_px_cat_g1v2;
  
            RAISE NOTICE '>> Inserting Data: silver.erp_px_cat_g1v2';
            INSERT INTO silver.erp_px_cat_g1v2(id,
                                               cat,
                                               subcat,
                                               maintenance)
            SELECT
                id,
                cat,
                subcat,
                maintenance
            FROM bronze.erp_px_cat_g1v2;
  
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




