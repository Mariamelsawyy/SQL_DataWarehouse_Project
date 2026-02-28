--create database
create database DataWarehouse

----create schemas
create schema bronze
create schema silver
go
create schema gold
go


----DDL of the  bronze layer
create table bronze.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date

)

create table bronze.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date

)

create table bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int

)

create table bronze.erp_cust_az12(

cid nvarchar(50),
bdate date,
gen nvarchar(50)
)

create table bronze.erp_loc_a101(

cid nvarchar(50),
cntry nvarchar(50)
)

create table bronze.erp_px_cat_g1v2(

id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50)
)

exec bronze.load_bronze


-------------creating stored procedure

create or alter procedure bronze.load_bronze as begin


begin try 
print '===========================================================';
print 'Loading Bronze Layer';
print '===========================================================';

print '-----------------------------------------------------------';
print 'Loading CRM Tables';
print '-----------------------------------------------------------';
-------loading the table from csv files into the  database table
truncate table bronze.crm_cust_info;
bulk insert bronze.crm_cust_info
from 'ur_file_path.csv'
with (
firstrow = 2,
fieldterminator=',',
tablock
);


truncate table bronze.crm_prd_info;
bulk insert bronze.crm_prd_info
from 'ur_file_path.csv'
with (
firstrow = 2,
fieldterminator=',',
tablock
);


truncate table bronze.crm_sales_details;
bulk insert bronze.crm_sales_details
from 'ur_file_path.csv'
with (
firstrow = 2,
fieldterminator=',',
tablock
);




print '-----------------------------------------------------------';
print 'Loading ERP Tables';
print '-----------------------------------------------------------';


truncate table bronze.erp_cust_az12;
bulk insert bronze.erp_cust_az12
from 'ur_file_path.csv'
with (
firstrow = 2,
fieldterminator=',',
tablock
);



truncate table bronze.erp_loc_a101;
bulk insert bronze.erp_loc_a101
from 'ur_file_path.csv'
with (
firstrow = 2,
fieldterminator=',',
tablock
);


truncate table bronze.erp_px_cat_g1v2;
bulk insert bronze.erp_px_cat_g1v2
from 'ur_file_path.csv'
with (
firstrow = 2,
fieldterminator=',',
tablock
);

end try
begin catch
print '==================';
print 'ERR OCCURED';
print '==================';
end catch

end

----------SILVER LAYER-----------


------DDL SILVER---
----DDL of the  silver layer
create table silver.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date,
dwh_create_date datetime2 default getdate()

)
use DataWarehouse

if OBJECT_ID('silver.crm_prd_info','U') is not null
drop table silver.crm_prd_info;
create table silver.crm_prd_info(
prd_id int,
cat_id nvarchar(50),
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date datetime2 default getdate()

)



if OBJECT_ID('silver.crm_sales_details','U') is not null
drop table silver.crm_sales_details;
create table silver.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date datetime2 default getdate()

)

create table silver.erp_cust_az12(

cid nvarchar(50),
bdate date,
gen nvarchar(50),
dwh_create_date datetime2 default getdate()
)

create table silver.erp_loc_a101(

cid nvarchar(50),
cntry nvarchar(50),
dwh_create_date datetime2 default getdate()
)

create table silver.erp_px_cat_g1v2(

id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50),
dwh_create_date datetime2 default getdate()
)

