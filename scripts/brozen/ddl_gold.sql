--------- creating view for the  diminsiion gold table products-------------------- 

create view dim_goald_products as
select
ROW_NUMBER() over(order by prd_key) as Product_key, 
cp.prd_id as product_id,
cp.prd_key as product_number,
cp.prd_nm as product_name,
cp.prd_cost as product_cost,
cp.prd_line as product_line,
cp.prd_start_dt as product_start_date,
cp.prd_end_dt as product_end_date,
cp.cat_id as category_id,
ec.cat as category,
ec.subcat as sub_category,
ec.maintenance
from silver.crm_prd_info as cp
left join silver.erp_px_cat_g1v2 as ec
on cp.prd_key =ec.id
where prd_end_dt is null

------------------------creating view for the  diminsiion gold table customers -------------------------------------------


CREATE VIEW dim_goald_customers as
select
ROW_NUMBER() over(order by cst_id) as customer_key,
ci.cst_id as customer_id,
ci.cst_key as customer_number, 
ci.cst_firstname as customer_first_name,
ci.cst_lastname as customer_last_name,
ci.cst_marital_status as customer_material_status,
case 
    when ci.cst_gndr != 'n/a' then ci.cst_gndr 
	else coalesce(ca.gen,'n/a')
end new_gender,
ci.cst_create_date as customer_create_date,
ca.bdate as date_of_birth,
el.cntry
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
on ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 AS el
  ON upper(TRIM(ci.cst_key)) = upper(TRIM(el.cid))

  ------------------------create fact table goald sales cystomers---------------------------

  create  view fact_sales as
select
cs.sls_cust_id as sales_customers_id,
pd.Product_key,
gc.customer_key,
cs.sls_order_dt as sales_order_date,
cs.sls_ship_dt as sales_ship_date,
cs.sls_due_dt as sales_due_date,
cs.sls_sales as sales,
cs.sls_quantity as quantity,
cs.sls_price as price
from silver.crm_sales_details as cs
left join dim_goald_products as pd 
on cs.sls_prd_key = pd.product_number
left join dim_goald_customers as gc
on cs.sls_cust_id = gc.customer_id
