CREATE OR ALTER PROCEDURE silver.load_silver as
begin
declare @start_time datetime , @end_time datetime , @batch_start_time datetime , @batch_end_time datetime;
begin try
set @batch_start_time = getdate();
print'-----------------------'
print 'LOADING SILVER TABLES'
PRINT '----------------------'
print'-----------------------'
print 'LOADING CRM TABLES'
PRINT '----------------------'
--lodingsilver------------
set @start_time = getdate();
print 'TRUNCATING THE TABLE silver.crm_cust_info'
TRUNCATE TABLE silver.crm_cust_info;
print 'INSERTING THE VALUES IN silver.crm_cust_info'
insert into silver.crm_cust_info(
 cst_id,
 cst_key,
 cst_firstname,
 cst_lastname,
 cst_marital_status,
 cst_gndr,
 cst_create_date
 )


select
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
CASE 
	WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
	WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
	ELSE 'n/a'
END AS cst_marital_status,
case when upper(trim(cst_gndr)) = 'M' then 'Male'
     when upper(trim(cst_gndr)) ='F' then 'Female'
	 else 'n/a'
end cst_gndr,
cst_create_date


from
(
select
*,
ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flagvalue
from brozen.crm_cust_info
where cst_id is not null
)t where flagvalue = 1
set @end_time = getdate()
PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'

-----------------------------silver prd-----------------------------------------------------------------------
print 'TRUNCATING THE TABLE silver.crm_prd_info'
TRUNCATE TABLE silver.crm_prd_info;
print 'INSERTING THE VALUES IN silver.crm_prd_info'

set @start_time = getdate();
print 'TRUNCATING THE TABLE silver.crm_prd_info'
TRUNCATE TABLE silver.crm_prd_info;
print 'INSERTING THE VALUES IN silver.crm_prd_info'
INSERT INTO silver.crm_prd_info (
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
replace(substring(prd_key ,1,5),'-','_') as cat_id,
substring(prd_key ,7,len(prd_key)) as prd_key,
prd_nm,
isnull (prd_cost , 0) as prd_cost,
case
  when upper(trim(prd_line)) = 'M' then 'Mountain'
  when upper(trim(prd_line)) = 'R' then 'Road'
  when upper(trim(prd_line)) = 'S' then 'Other Sales'
  when upper(trim(prd_line)) = 'T' then 'Tourining'
  else 'n/a'
end prd_line,
cast(prd_start_dt as date) as prd_start_dt,
cast(lead(prd_start_dt) over(partition by prd_key  order by prd_start_dt)-1 as date) as prd_end_dt
from brozen.crm_prd_info
set @end_time = getdate()
print 'LOAD DURATION' + cast(datediff(second, @start_time , @end_time)as varchar) + 'seconds'
print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'

------------------silver sales details-------------------------------------------------------------------
print 'TRUNCATING THE TABLE silver.crm_sales_details'
TRUNCATE TABLE silver.crm_sales_details;
print 'INSERTING THE VALUES IN ssilver.crm_sales_details'

set @start_time = getdate();
print 'TRUNCATING THE TABLE silver.crm_sales_details'
TRUNCATE TABLE silver.crm_sales_details;
print 'INSERTING THE VALUES IN silver.crm_sales_details'
insert into silver.crm_sales_details(
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
   when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
   else cast(cast(sls_order_dt as varchar)as date)
   end sls_order_dt,
case 
   when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
   else cast(cast(sls_ship_dt as varchar)as date)
   end sls_ship_dt,
case 
   when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
   else cast(cast(sls_due_dt as varchar)as date)
   end sls_due_dt,
case
    when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity*abs(sls_price)
	then sls_quantity*abs(sls_price)
	else sls_sales
end sls_sales,
sls_quantity,
case 
    when sls_price is null or sls_price<= 0 
	then sls_sales / nullif(sls_quantity,0)
	else sls_price
end sls_price
from brozen.crm_sales_details
set @end_time = getdate()
print 'LOAD DURATION' + cast(datediff(second , @start_time , @end_time)as varchar) + 'seconds'
print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'

----------------------------------silver cust az12------------------------------------------------------
print 'TRUNCATING THE TABLE silver.erp_cust_az12'
TRUNCATE TABLE silver.erp_cust_az12;
print 'INSERTING THE VALUES IN silver.erp_cust_az12'

set @start_time = getdate();
print 'TRUNCATING THE TABLE silver.erp_cust_az12'
TRUNCATE TABLE silver.erp_cust_az12;
print 'INSERTING THE VALUES IN silver.erp_cust_az12'
insert into silver.erp_cust_az12(
cid,
bdate,
gen
)

select 
case 
   when cid like 'NAS%' then substring(cid,4,len(cid))
   else cid
end cid,
case when bdate > getdate() then null
else bdate
end bdate,
case 
    when  upper(trim(gen)) in ('M','Male') then 'Male'
	when  upper(trim(gen)) in ('F','Female') then 'Female'
	else 'n/a'
end gen
from brozen.erp_cust_az12
set @end_time = getdate()
print 'LOAD DURATION' + cast(datediff(second, @start_time , @end_time)as varchar) + 'seconds'
print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'


---------------------silevr ero loc a101---------------------------------
print 'TRUNCATING THE TABLE silver.erp_loc_a101'
TRUNCATE TABLE silver.erp_loc_a101;
print 'INSERTING THE VALUES IN silver.erp_loc_a101'

set @start_time = getdate();
print 'TRUNCATING THE TABLE silver.erp_loc_a101'
TRUNCATE TABLE silver.erp_loc_a101;
print 'INSERTING THE VALUES IN silver.erp_loc_a101'
insert into silver.erp_loc_a101(
cid,
cntry
)


select
replace (cid,'-','_') as cid,
case
    when trim(cntry) = 'DE' then 'Den Mark'
	when trim(cntry) in ('US','USA') then 'United States Of America'
    when trim(cntry) = ' ' or cntry is null then 'N/A'
	else trim(cntry)
end cntry
from brozen.erp_loc_a101
set @end_time = getdate()
print 'LOAD DURATION' + cast(datediff(second , @start_time , @end_time)as varchar) + 'seconds'
print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'


-----------------------------------silver.erp_px_cat_g1v2---------------------------------
print 'TRUNCATING THE TABLE silver.erp_px_cat_g1v2'
TRUNCATE TABLE silver.erp_px_cat_g1v2;
print 'INSERTING THE VALUES IN silver.erp_px_cat_g1v2'

set @start_time = getdate();
print 'TRUNCATING THE TABLE silver.erp_px_cat_g1v2'
TRUNCATE TABLE silver.erp_px_cat_g1v2;
print 'INSERTING THE VALUES IN silver.erp_px_cat_g1v2'
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
from brozen.erp_px_cat_g1v2
set @end_time = getdate()
print 'LOAD DURATION' + cast(datediff(second , @start_time , @end_time)as varchar) + 'seconds'
print '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>'

set @batch_end_time = getdate()
end try
----
begin catch
print '-----------------------------'
print 'ERROR OCCURED DURINING LOADING '
PRINT 'Error Message' + ERROR_MESSAGE();
PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR)
PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR)
print '----------------------------'
end catch
end
