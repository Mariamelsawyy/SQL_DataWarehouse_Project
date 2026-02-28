# SQL_DataWarehouse_Project
🏗️ SQL Data Warehouse Project (Bronze–Silver–Gold Architecture)
    
  This project implements a modern Data Warehouse solution using SQL Server, following the Medallion Architecture (Bronze, Silver, Gold layers) to transform raw CRM and ERP data into clean, structured, and analytics-ready datasets.
      
  The solution demonstrates end-to-end data engineering concepts including data ingestion, transformation, cleansing, standardization, and layered architecture design.
      
📌 Project Overview

  The data warehouse integrates data from multiple source systems (CRM and ERP) and processes it through structured layers:

🥉 Bronze Layer – Raw Data Ingestion
    
    Stores raw data exactly as received from CSV source files.
    
    Uses BULK INSERT for efficient loading.
    
    Preserves original structure for traceability.
    
    Implemented via stored procedure: bronze.load_bronze.

🥈 Silver Layer – Data Cleansing & Transformation
    
    Cleans and standardizes raw data.
    
    Removes duplicates using ROW_NUMBER().
    
    Fixes inconsistent values (gender, marital status, country codes).
    
    Handles nulls and invalid dates.
    
    Derives new columns and corrects calculated fields.
    
    Adds dwh_create_date for audit tracking.
    
    Implemented via stored procedure: silver.load_silver.

🥇 Gold Layer – (Upcoming)
    
    Will contain business-level, analytics-ready tables.
    
    Designed for reporting, dashboards, and BI tools.

⚙️ Key Concepts Applied
    
    Medallion Architecture (Bronze / Silver / Gold)
    
    Data Cleansing & Standardization
    
    Data Quality Validation
    
    Window Functions (ROW_NUMBER, LEAD)
    
    Derived & Calculated Columns
    
    ETL using Stored Procedures
    
    Schema Separation for Layer Isolation

🛠️ Technologies Used

    SQL Server
    
    T-SQL
    
    Stored Procedures
    
    Bulk Insert
    
    Window Functions

🎯 Learning Outcomes
    
    Designing layered data warehouse architecture
    
    Implementing ETL pipelines in SQL
    
    Handling real-world data quality issues
    
    Applying transformation logic systematically
    
    Building scalable and maintainable warehouse structures
