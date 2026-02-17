/* 
====================================================================
STORED PROCEDURE: Load Silver Layer (Bronze -> Silver)

-- Execute Stored Procedure
execute silver.load_silver;
====================================================================
*/

create or alter procedure silver.load_silver as
begin
	declare @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	begin try
		set @batch_start_time = GETDATE();
		print '==============================================';
		print 'Loading Silver Layer...';
		print '==============================================';
		print '';
		print '-------------------------';
		print 'Loading CRM Tables...';
		print '-------------------------';

		-- silver.crm_cust_info
		set @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_cust_info'
		truncate table silver.crm_cust_info;

		print '>> Inserting Data into silver.crm_cust_info'
		insert into silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date)
		select 
			cst_id,
			cst_key,
			trim(cst_firstname) as cst_firstname,
			trim(cst_lastname) as cst_lastname,
			case when upper(trim(cst_marital_status)) = 'S' then 'Single' 
				 when upper(trim(cst_marital_status)) = 'M' then 'Married'
				 else 'n/a'
			end cst_marital_status, -- normalize marital status values to readable format
			case when upper(trim(cst_gndr)) = 'F' then 'Female' 
				 when upper(trim(cst_gndr)) = 'M' then 'Male'
				 else 'n/a'
			end cst_gndr, -- noemalize gender values to readable format
			cst_create_date
		FROM (
			SELECT 
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			where cst_id is not null
		)t 
		where flag_last = 1 -- select the most recent record per customer
		
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.';
		print '>> ---------------------------------------------------'

		-- silver.crm_prd_info
		set @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_prd_info'
		truncate table silver.crm_prd_info;

		print '>> Inserting Data into silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost, 
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		select
			prd_id,
			replace (SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id, -- Extract category ID
			SUBSTRING(prd_key, 7, len(prd_key)) as prd_key, -- Extract product key
			prd_nm,
			isnull(prd_cost, 0) as prd_cost,
			case upper(trim(prd_line)) 
				when 'M' then 'Mountain'
				when 'R' then 'Road'
				when 'S' then 'Other Sales'
				when 'T' then 'Touring'
				else 'n/a'
			end as prd_line, -- Map product line codes to descriptive values
			cast(prd_start_dt as date) as prd_start_dt,
			cast(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 as date) 
				as prd_end_dt -- Calculate end date as one day before the next start date
		from bronze.crm_prd_info

		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.';
		print '>> ---------------------------------------------------'

		-- silver.crm_sales_details
		set @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_sales_details'
		truncate table silver.crm_sales_details;

		print '>> Inserting Data into silver.crm_sales_details'
		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		select
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case 
				when sls_order_dt = 0 or len(sls_order_dt) != 8 then null -- Transformation: handling invalid data
				else cast(cast(sls_order_dt as varchar) as date) -- data type casting to DATE
			end as sls_order_dt,
			case 
				when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null 
				else cast(cast(sls_ship_dt as varchar) as date) 
			end as sls_ship_dt,
			case 
				when sls_due_dt = 0 or len(sls_due_dt) != 8 then null 
				else cast(cast(sls_due_dt as varchar) as date) 
			end as sls_due_dt,
			case 
				when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price) -- handling missing data, invalid data by deriving the column from already existing one
					then sls_quantity * abs(sls_price) -- converted to positive using ABS()
				else sls_sales -- recalculated using quantity x price
			end as sls_sales,
			sls_quantity,
			case 
				when sls_price is null or sls_price <= 0 -- handling invalid data by deriving it from specific calcualtion 
					then sls_sales / nullif(sls_quantity, 0)
				else sls_price
			end as sls_price -- recalculated using sales/quantity
		from bronze.crm_sales_details

		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.';
		print '>> ---------------------------------------------------'

		print '';
		print '-------------------------';
		print 'Loading ERP Tables...';
		print '-------------------------';

		-- silver.erp_cust_az12
		set @start_time = GETDATE();
		print '>> Truncating Table: silver.erp_cust_az12'
		truncate table silver.erp_cust_az12;
		
		print '>> Inserting Data into silver.erp_cust_az12'
		insert into silver.erp_cust_az12(
			cid,
			bdate,
			gen
		)
		select
		case when cid like 'NAS%' then substring(cid, 4, len(cid)) -- remove 'NAS' prefix if present
			else cid
		end as cid,
		case when bdate > getdate() then null
			else bdate
		end as bdate, -- set future birthdates to NULL
		case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
			when upper(trim(gen)) in ('M', 'MALE') then 'Male'
			else 'n/a'
		end as gen -- normalize gender values and handle unknown cases
		from bronze.erp_cust_az12

		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.';
		print '>> ---------------------------------------------------'

		-- silver.erp_loc_a101
		set @start_time = GETDATE();
		print '>> Truncating Table: silver.erp_loc_a101'
		truncate table silver.erp_loc_a101;

		print '>> Inserting Data into silver.erp_loc_a101'
		insert into silver.erp_loc_a101(
			cid,
			cntry
		)
		select 
		replace(cid, '-', '') cid, -- Handling invalid values
		case 
			when trim(cntry) = 'DE' then 'Germany'
			when trim(cntry) in ('US', 'USA') then 'United States'
			when trim(cntry) = '' or cntry is null then 'n/a'
			else trim(cntry)
		end as cntry -- Normalize and handle missing or blank country codes
		from bronze.erp_loc_a101

		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.';
		print '>> ---------------------------------------------------'

		-- erp_px_cat_g1v2
		set @start_time = GETDATE();
		print '>> Truncating Table: silver.erp_px_cat_g1v2'
		truncate table silver.erp_px_cat_g1v2;

		print '>> Inserting Data into silver.erp_px_cat_g1v2'
		insert into silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		select
			id,
			cat,
			subcat,
			maintenance
		from bronze.erp_px_cat_g1v2
		
		SET @end_time = GETDATE();
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR) + ' seconds.';
		print '>> ---------------------------------------------------'

		set @batch_end_time = GETDATE();
		print '==============================================';
		print 'Loading Silver Layer is Completed.';
		print '>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) as NVARCHAR) + ' seconds.';
		print '==============================================';
	end try

	begin catch
		print '==============================================';
		print 'ERROR OCCURED DURING LOADING SILVER LAYER';
		print 'Error Message' + ERROR_MESSAGE();
		print 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		print 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		print '==============================================';
	end catch
end 
