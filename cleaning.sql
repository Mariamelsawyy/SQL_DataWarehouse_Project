--------crm_cust_info TABLE-----------
--check for nulls or duplicates in pk

select cst_id,count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) >1 or cst_id is null




-------check unwanted spaces
select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim (cst_firstname)


--------check the data standardization and consistency
select distinct cst_gndr
from bronze.crm_cust_info

select distinct cst_material_status
from bronze.crm_cust_info

---------------------after cleaning---------


print '>>Truncating Table : silver.crm_cust_info'
truncate table silver.crm_cust_info
print '>>Inserting Data into: silver.crm_cust_info'

insert into silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_material_status,
cst_gndr,
cst_create_date
)

select 
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,

case when upper(trim(cst_material_status)) ='S' then 'Single'
when upper(trim(cst_material_status))='M' then 'Married'
else 'N/A'
end cst_material_status,

case when upper(trim(cst_gndr)) ='F' then 'Female'
when upper(trim(cst_gndr))='M' then 'Male'
else 'N/A'
end cst_gndr,
cst_create_date
from(
select *,
ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
from bronze.crm_cust_info
where cst_id is not null

)t where flag_last=1


select * from silver.crm_cust_info



--------------crm_prd_info TABLE-------------
--check for nulls or duplicates in pk

select prd_id,count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) >1 or prd_id is null


------Spliting table


--where 
--replace(SUBSTRING(prd_key,1,5),'-','_') 
--not in (select distinct id from bronze.erp_px_cat_g1v2)-- trying to find anh cat_id that doesn'r exist in the other table

--select distinct id from bronze.erp_px_cat_g1v2

select sls_prd_key from bronze.crm_sales_details


-------check unwanted spaces
select prd_nm
from bronze.crm_prd_info
where prd_nm != trim (prd_nm)


-----check nulls or -ve numbers
select prd_cost
from bronze.crm_prd_info
where prd_cost<0 or prd_cost is null


--------check the data standardization and consistency
select distinct prd_line
from bronze.crm_prd_info

-----check for invalid date orders
select * from bronze.crm_prd_info
where prd_end_dt<prd_start_dt




-----after cleaning------
print '>>Truncating Table : silver.crm_prd_info'
truncate table silver.crm_prd_info
print '>>Inserting Data into: silver.crm_prd_info'

insert into silver.crm_prd_info(
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
replace(SUBSTRING(prd_key,1,5),'-','_')as  cat_id,---a way to extract info
SUBSTRING(prd_key,7,len(prd_key))as prd_key,
prd_nm,
isnull(prd_cost,0)as prd_cost,
case upper(trim(prd_line))
when 'M' then 'Mountain'
when 'R' then 'Road'
when 'S' then 'other sales'
when 'T' then 'Touring'
else 'N/A'
end as prd_line,
prd_start_dt,
dateadd (day,-1,lead(prd_start_dt)over (partition by prd_key order by prd_start_dt)) as prd_end_dt
from bronze.crm_prd_info


select * from silver.crm_prd_info




------------crm_sales_details TABLE---------------
-------------after cleaning

print '>>Truncating Table : silver.crm_sales_details'
truncate table silver.crm_sales_details
print '>>Inserting Data into: silver.crm_sales_details '
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

case when sls_order_dt= 0 or len(sls_order_dt) !=8 then null
else cast(cast(sls_order_dt as varchar )as date)---in order to convert from in to date we need to cnvert int to varchar first
end sls_order_dt,
case when sls_ship_dt= 0 or len(sls_ship_dt) !=8 then null
else cast(cast(sls_ship_dt as varchar )as date)---in order to convert from in to date we need to cnvert int to varchar first
end sls_ship_dt,
case when sls_due_dt= 0 or len(sls_due_dt) !=8 then null
else cast(cast(sls_due_dt as varchar )as date)---in order to convert from in to date we need to cnvert int to varchar first
end sls_due_dt,
case when sls_sales is null or sls_sales < = 0 or sls_sales != sls_quantity* abs(sls_price)
then sls_quantity* abs(sls_price)
else sls_sales
end as sls_sales,
sls_quantity,
case when sls_price is null or sls_price<= 0
then sls_sales/nullif(sls_quantity,0)
else sls_price
end as sls_price
from bronze.crm_sales_details


-------converting from intgers to date


--check for invalid date
select 
nullif(sls_due_dt,0) as sls_due_dt ---replace the zero values to nulls
from bronze.crm_sales_details
where sls_due_dt<=0
or  len(sls_ship_dt)!=8
or sls_due_dt>20500101
or sls_due_dt<19000101

--checeking for invalid date orders 

select *
from bronze.crm_sales_details
where sls_order_dt>sls_ship_dt or sls_order_dt>sls_due_dt

-- check data consistency between sales (s), quantity (q), price (p)
--s = q *p
--values must not be null , 0, or -ve
select distinct
sls_sales as old_sls_sales,
sls_quantity,
sls_price as old_sls_price,
case when sls_sales is null or sls_sales < = 0 or sls_sales != sls_quantity* abs(sls_price)
then sls_quantity* abs(sls_price)
else sls_sales
end as sls_sales,
case when sls_price is null or sls_price<= 0
then sls_sales/nullif(sls_quantity,0)
else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales != sls_price * sls_quantity
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales<= 0 or sls_quantity <=0 or sls_price<=0
order by sls_sales, sls_quantity, sls_price


select * from silver.crm_sales_details


----------------erp_cust_az12 TABLE--------------
select * from bronze.erp_cust_az12
select * from silver.crm_cust_info


---since we have found extra charcters in the cid column while comparing it to the cst_key column


select 
cid,
case when cid like 'NAS%' then substring(cid,4,len(cid))
else cid
end cid,
bdate,
gen
from bronze.erp_cust_az12
where 
case when cid like 'NAS%' then substring(cid,4,len(cid))
else cid
end not in (select distinct cst_key from silver.crm_cust_info) --to check that there is not any missing data


------checking for out of range dates

select distinct 
bdate
from bronze.erp_cust_az12
where bdate < '1924-01-01' or bdate >getdate()



----Data Standarization & consistency

select distinct gen
from bronze.erp_cust_az12

------------after cleaning
print '>>Truncating Table : silver.erp_cust_az12'
truncate table silver.erp_cust_az12
print '>>Inserting Data into: silver.erp_cust_az122 '
insert into silver.erp_cust_az12(cid,bdate,gen)

select

case when cid like 'NAS%' then substring(cid,4,len(cid))
else cid
end cid,

case when bdate>getdate () then null
else bdate 
end as bdate,

case when upper(trim(gen)) in ('F','Female') then 'Female'
when upper(trim(gen)) in ('M','Male') then 'Male'
else 'N/A'
end as gen
from bronze.erp_cust_az12


select * from silver.erp_cust_az12




--------------bronze.erp_loc_a101 TABLE

select * from bronze.erp_loc_a101

select * from silver.crm_cust_info

---to check we havn't missed any value while modifiyng the cid column:
select

replace(cid,'-','') cid,    -- we need to remove '-' from the cid column 
cntry
from bronze.erp_loc_a101
where replace(cid,'-','')  not in 
(select cst_key from silver.crm_cust_info)

--data standarization & consistency
select distinct cntry
from bronze.erp_loc_a101

-----after cleaning
print '>>Truncating Table : silver.erp_loc_a101'
truncate table silver.erp_loc_a101
print '>>Inserting Data into: silver.erp_loc_a101 '
insert into silver.erp_loc_a101(cid,cntry)
select

replace(cid,'-','') cid,    -- we need to remove '-' from the cid column 

case when trim(cntry) = 'DE' then 'Germany'
when trim(cntry)  in('US','USA')then 'United States'
when trim(cntry) = '' or cntry is null  then 'N/A'
else trim(cntry)
end as cntry

from bronze.erp_loc_a101


select * from silver.erp_loc_a101


---------bronze.erp_px_cat_g1v2 TABLE
select * from bronze.erp_px_cat_g1v2

select * from silver.crm_prd_info


---check for unwanted spaces
select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim (subcat) or maintenance!=trim(maintenance)


---Data Standarization & consistency 
select distinct maintenance 
from bronze.erp_px_cat_g1v2



---the table is clean already we will just insert it 
print '>>Truncating Table : silver.erp_px_cat_g1v2'
truncate table silver.erp_px_cat_g1v2
print '>>Inserting Data into: silver.erp_px_cat_g1v2 '
insert into silver.erp_px_cat_g1v2(id, cat, subcat,maintenance)
select 
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_g1v2

select * from silver.erp_px_cat_g1v2

