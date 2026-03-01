/*
Data Cleansing, Transformation, and Loading to Silver Layer
===============================================================================
Script Purpose:
    This script performs data cleansing, transformation, validation, and loading 
    processes from the Bronze layer into the Silver layer for CRM and ERP datasets. 
    The goal is to ensure data quality, consistency, and readiness for downstream 
    analytics or Gold layer modeling.

    The script includes:
    - Data quality checks (NULL values, duplicates, unwanted spaces).
    - Data standardization (gender, marital status, product line, country).
    - Handling invalid or missing values.
    - Date validation and conversion (INT → DATE).
    - Derivation of calculated fields (sales, price recalculation).
    - Slowly Changing Dimension (SCD Type 2) handling for product history.
    - Key normalization and referential integrity validation between tables.
    - Loading cleaned data into Silver schema tables.

Processed Tables:
    CRM:
        - crm_cust_info
        - crm_prd_info
        - crm_sales_details
    ERP:
        - erp_cust_az12
        - erp_loc_a101
        - erp_px_cat_g1v2

Data Quality Rules Applied:
    - Remove NULL or duplicate primary keys.
    - Trim unwanted spaces from string columns.
    - Normalize categorical values into standardized formats.
    - Replace invalid numeric values with derived or default values.
    - Validate chronological correctness of date fields.
    - Recalculate inconsistent financial metrics when necessary.
    - Ensure referential integrity across related datasets.
    - Handle invalid identifiers and formatting inconsistencies.

Usage Notes:
    - Execute after Bronze layer ingestion is completed.
    - Recommended to run table-by-table validation queries before inserts.
/*

============== crm_cust_info ==============
*/

-- Checking for Nulls or Duplicate Data in Primary key
SELECT cst_id, 
COUNT(*)
FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 or cst_id is null
-- There's 1 null

-- Check Unwanted Spaces
-- Expectation: No Result
SELECT cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)
-- No unwanted spaces

SELECT cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname)
-- No unwanted spaces

SELECT cst_gndr
from bronze.crm_cust_info
where cst_gndr != trim(cst_gndr)
-- No unwanted spaces

-- Check consistency of values in low cardinality number (cst_marital_status, cst_gndr)
select distinct cst_gndr
from bronze.crm_cust_info

select distinct cst_marital_status
from bronze.crm_cust_info

select * from bronze.crm_cust_info

DELETE FROM bronze.crm_cust_info
WHERE cst_id IS NULL;

-- Final fixed
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

-- Insert into DDL
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

-- Check data quality
  
SELECT 
    cst_id,
    COUNT(*) 
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    cst_key 
FROM silver.crm_cust_info
WHERE cst_key != TRIM(cst_key);

-- Data Standardization & Consistency
SELECT DISTINCT 
    cst_marital_status 
FROM silver.crm_cust_info;
  
/*
============== crm_prd_info ==============
*/

-- Check unwanted spaces
select prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm)
-- No unwanted spaces

-- check for null or negative numbers
select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null
-- there's null
-- Handling: replace NULL with 0

-- check prd_line
select distinct prd_line
from bronze.crm_prd_info

-- prd_start_dt
select * from bronze.crm_prd_info
where prd_end_dt < prd_start_dt

-- recalculated prd_end_dt using LEAD() window function based on the next row’s prd_start_dt to correctly construct SCD Type 2 time periods 
SELECT
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    DATEADD(day, -1,
        LEAD(prd_start_dt) OVER (
            PARTITION BY prd_key
            ORDER BY prd_start_dt
        )
    ) AS prd_end_dt
FROM bronze.crm_prd_info;

select
	prd_id,
	prd_key,
	replace (SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
from bronze.crm_prd_info
where replace (SUBSTRING(prd_key, 1, 5), '-', '_') not in
(select distinct id from bronze.erp_px_cat_g1v2)

select
	prd_id,
	prd_key,
	replace (SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id,
	SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
	prd_nm,
	isnull(prd_cost, 0) as prd_cost,
	case upper(trim(prd_line)) 
		when 'M' then 'Mountain'
		when 'R' then 'Road'
		when 'S' then 'Other Sales'
		when 'T' then 'Touring'
		else 'n/a'
	end as prd_line,
	cast(prd_start_dt as date) as prd_start_dt,
	cast(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 as date) as prd_end_dt
	from bronze.crm_prd_info

-- Insert into DDL
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

-- Checking Data quality
SELECT prd_id, 
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 or prd_id is null

-- Expectation: No Result
SELECT prd_nm
from silver.crm_prd_info
where prd_nm != trim(prd_nm)
-- No unwanted spaces

SELECT prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null
-- No negative number or null

-- Data standardization & consistency
select distinct prd_line
from silver.crm_prd_info

-- Check for invalid date orders
select *
from silver.crm_prd_info
where prd_end_dt < prd_start_dt
-- No issues

select * from silver.crm_prd_info

/*
============== crm_sales_details ==============
*/

select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details

-- Checking unwanted spaces
select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num)
-- No unwanted spaces

-- Check if the key in each table can be mergered 

select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info)
-- sls_prd_key from sales_details can be used and connected with crm_prd_info

-- Chech integrity in cust_id
select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info)
-- can connect sls_cust_id with cst_id

-- turn sls_order_dt from INT to DATE
select sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0
-- there's 0 values in sls_order_dt

-- replace it with NULL 
select 
nullif(sls_order_dt, 0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0

-- Check invalid dates

-- sls_order_dt
select 
nullif(sls_order_dt, 0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0
or len(sls_order_dt) !=8
or sls_order_dt > 20500101 
or sls_order_dt < 19000101

-- sls_ship_dt
select 
nullif(sls_ship_dt, 0) sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0
or len(sls_ship_dt) !=8
or sls_ship_dt > 20500101 
or sls_ship_dt < 19000101

-- sls_due_dt
select 
nullif(sls_due_dt, 0) sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0
or len(sls_due_dt) !=8
or sls_due_dt > 20500101 
or sls_due_dt < 19000101

-- fix it
select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null 
		else cast(cast(sls_order_dt as varchar) as date) 
	end as sls_order_dt,
	case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null 
		else cast(cast(sls_ship_dt as varchar) as date) 
	end as sls_ship_dt,
	case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null 
		else cast(cast(sls_due_dt as varchar) as date) 
	end as sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
from bronze.crm_sales_details

-- Checking invalid dates (order, ship, due)
select *
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt
-- No invalid dates

-- Check data consistency
select distinct 
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0

-- solution
select distinct 
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
		then sls_quantity * abs(sls_price) -- converted to positive using ABS()
	else sls_sales -- recalculated using quantity x price
end as sls_sales,
case when sls_price is null or sls_price <= 0
		then sls_sales / nullif(sls_quantity, 0)
	else sls_price
end as sls_price -- recalculated using sales/quantity
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price

-- final query
select
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null 
		else cast(cast(sls_order_dt as varchar) as date) 
	end as sls_order_dt,
	case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null 
		else cast(cast(sls_ship_dt as varchar) as date) 
	end as sls_ship_dt,
	case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null 
		else cast(cast(sls_due_dt as varchar) as date) 
	end as sls_due_dt,
	case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
		then sls_quantity * abs(sls_price) -- converted to positive using ABS()
	else sls_sales -- recalculated using quantity x price
	end as sls_sales,
	sls_quantity,
	case when sls_price is null or sls_price <= 0
		then sls_sales / nullif(sls_quantity, 0)
	else sls_price
end as sls_price -- recalculated using sales/quantity
from bronze.crm_sales_details

-- insert into ddl silver 
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

-- Check invalid date orders
select * from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt
-- No invalid data

select distinct 
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price
-- No issues

select * from silver.crm_sales_details

/*
============== erp_cust_az12 ==============
*/

select
cid,
bdate,
gen
from bronze.erp_cust_az12

select * from [silver].[crm_cust_info]

-- check id
select
cid,
bdate,
gen
from bronze.erp_cust_az12
where cid like '%AW00011000%'

-- clean up id
select
cid,
case when cid like 'NAS%' then substring(cid, 4, len(cid))
	else cid
end as cid,
bdate,
gen
from bronze.erp_cust_az12
where case when cid like 'NAS%' then substring(cid, 4, len(cid))
	else cid
end not in (select distinct cst_key from silver.crm_cust_info)

-- final fixed for cid
select
case when cid like 'NAS%' then substring(cid, 4, len(cid))
	else cid
end as cid,
bdate,
gen
from bronze.erp_cust_az12

-- Identify out-of-range dates 
select distinct
bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate()

-- fix the birth date
select
case when cid like 'NAS%' then substring(cid, 4, len(cid))
	else cid
end as cid,
case when bdate > getdate() then null
	else bdate
end as bdate,
gen
from bronze.erp_cust_az12

-- data standardization & consistency: gen
select distinct gen
from bronze.erp_cust_az12

-- make it only mle, Female, NULL
select distinct 
case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
	when upper(trim(gen)) in ('M', 'MALE') then 'Male'
	else 'n/a'
end as gen
from bronze.erp_cust_az12

-- fial fixed
select
case when cid like 'NAS%' then substring(cid, 4, len(cid))
	else cid
end as cid,
case when bdate > getdate() then null
	else bdate
end as bdate,
case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
	when upper(trim(gen)) in ('M', 'MALE') then 'Male'
	else 'n/a'
end as gen
from bronze.erp_cust_az12

-- insert into silver layer
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

-- Check data quality in silver
select distinct
bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate()

select distinct gen
from silver.erp_cust_az12

select * from silver.erp_cust_az12

/*
============== erp_loc_a101 ==============
*/

select 
cid,
cntry
from bronze.erp_loc_a101

-- check cid to connect cid
select cst_key from silver.crm_cust_info

-- replace - in cid
select 
replace(cid, '-', '') cid,
cntry
from bronze.erp_loc_a101

-- find cid from erp bronze that do not exist in crm silver
select 
replace(cid, '-', '') cid,
cntry
from bronze.erp_loc_a101 where replace(cid, '-', '') not in
(select cst_key from silver.crm_cust_info)
-- no unmatching data

-- check consistency in country column
select distinct cntry 
from bronze.erp_loc_a101
order by cntry	

-- fix cntry values
select 
replace(cid, '-', '') cid,
case when trim(cntry) = 'DE' then 'Germany'
	when trim(cntry) in ('US', 'USA') then 'United States'
	when trim(cntry) = '' or cntry is null then 'n/a'
	else trim(cntry)
end as cntry
from bronze.erp_loc_a101

-- check the result
select distinct 
cntry as old_cntry,
case when trim(cntry) = 'DE' then 'Germany'
	when trim(cntry) in ('US', 'USA') then 'United States'
	when trim(cntry) = '' or cntry is null then 'n/a'
	else trim(cntry)
end as cntry
from bronze.erp_loc_a101
order by cntry	

-- insert into ddl: silver.erp_loc_a101
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

-- Check data quality
select distinct cntry 
from silver.erp_loc_a101
order by cntry	

select * from silver.erp_loc_a101

/*
============== erp_px_cat_g1v2 ==============
*/

select
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2

-- Checking unwanted spaces
select * from bronze.erp_px_cat_g1v2
where cat != trim(cat)
-- No unwanted spaces 

select * from bronze.erp_px_cat_g1v2
where subcat != trim(subcat)
-- No unwanted spaces

select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)
-- No unwanted spaces

-- Data standardization & Consistency
select distinct 
cat
from bronze.erp_px_cat_g1v2

select distinct 
subcat
from bronze.erp_px_cat_g1v2

select distinct 
maintenance
from bronze.erp_px_cat_g1v2

-- Insert into ddl
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

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
   OR subcat != TRIM(subcat) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;
	
-- Check data quality
select * from silver.erp_px_cat_g1v2








