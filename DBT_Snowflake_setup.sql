-- first we need to set up a DBT repo for snowflake
-- useful link for setting up dbt for snowflake: https://docs.getdbt.com/docs/core/connect-data-platform/snowflake-setup
-- install and set up DBT core with a snowflake data warehouse:
-- https://www.youtube.com/watch?v=ZbLzOgAMAwk

-- installation instructions for DBT, with snowflake (briefly)
-- 1) obtain a snowflake account and run the following commands in snowsight: CREATE DATABASE DBT_ETL; CREATE SCHEMA STAGING;
-- 2) install dbt core and dbt snowflake
-- 3) from whichever working folder you want to create your DBT repo on your local machine, run dbt init from the command line
--    i chose the folder: cd C:\Users\User\NHS_2005_2006_NWL\DBT
--    i had this error: ModuleNotFoundError: No module named 'urllib3.packages.six.moves', so I had to run the following code:
--    pip install --upgrade urllib3 six
--    i called my project this: Enter a name for your project (letters, digits, underscore): dbt_incremental
--    then I had a runtime error telling me that no adapters were available - I think I may be using a version of python
--    which is too new for DBT - python version 3.10 is fully supported so i uninstalled 3.13 and installed 3.10 instead
--    I received this message upon installing 3.10:
--    WARNING: You are using pip version 21.2.3; however, version 25.1.1 is available.
--    You should consider upgrading via the 'C:\Program Files\Python310\python.exe -m pip install --upgrade pip' command.
--    but i don't think that is anything to worry about
-- 4) select '1' for snowflake, my user account as at 16/06 is 'etnsirn-hc61829' and my username is 'garthajon'
--    i selected 'password' for authentication, i entered my snowflake password and also selected ACCOUNTADMIN as the role
--    and COMPUTE_WH as the warehouse, DBT_ETL as the database and STAGING  as the schema
--    08:58:22  Profile dbt_incremental written to C:\Users\User\.dbt\profiles.yml using target's profile_template.yml and your supplied values.
-- 5) now change directory to the DBT project file which you have set up: cd C:\Users\User\NHS_2005_2006_NWL\DBT\dbt_incremental
-- 6) and  Run 'dbt debug' to validate the connection. Note that the emphasis for the whole time is on 'dev'
--    the DBT set up is expressly for your 'dev' environment
  
-- The snowSQL for the DBT incrementatal transform demonstration
-- from: https://www.youtube.com/watch?v=MgSO6458c_4
-- (DBT Incremental Models Made Easy with Snowflake Data Warehouse) CK Data Tech

CREATE DATABASE DBT_ETL;
USE DATABASE DBT_ETL;
CREATE SCHEMA STAGING;

CREATE TABLE sales (
    id INT AUTOINCREMENT PRIMARY KEY,
    order_id STRING NOT NULL,
    product_id STRING NOT NULL,
    customer_id STRING NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    sale_date TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

--1st run -> initial run
INSERT INTO sales (order_id, product_id, customer_id, quantity, price, sale_date, updated_at)
VALUES
('ORD001', 'PROD001', 'CUST001', 3, 29.99, '2025-01-01 10:15:00', '2025-01-01 10:15:00'),
('ORD002', 'PROD002', 'CUST002', 1, 49.99, '2025-01-02 12:30:00', '2025-01-02 12:30:00'),
('ORD003', 'PROD003', 'CUST003', 5, 15.99, '2025-01-03 15:45:00', '2025-01-03 15:45:00'),
('ORD004', 'PROD004', 'CUST004', 2, 99.99, '2025-01-04 09:20:00', '2025-01-04 09:20:00'),
('ORD005', 'PROD005', 'CUST005', 4, 19.99, '2025-01-05 11:05:00', '2025-01-05 11:05:00'),
('ORD006', 'PROD001', 'CUST006', 1, 29.99, '2025-01-06 15:50:00', '2025-01-06 15:50:00'),
('ORD007', 'PROD002', 'CUST007', 1, 49.99, '2025-01-07 08:25:00', '2025-01-07 08:25:00'),
('ORD008', 'PROD003', 'CUST008', 7, 15.99, '2025-01-08 10:40:00', '2025-01-08 10:40:00'),
('ORD009', 'PROD004', 'CUST009', 2, 99.99, '2025-01-09 14:00:00', '2025-01-09 14:00:00'),
('ORD010', 'PROD005', 'CUST010', 6, 19.99, '2025-01-10 16:30:00', '2025-01-10 16:30:00');


-- 2nd run (additional inserts)
INSERT INTO sales (order_id, product_id, customer_id, quantity, price, sale_date, updated_at)
VALUES
('ORD011', 'PROD006', 'CUST011', 8, 34.99, '2025-01-11 10:00:00', '2025-01-11 10:00:00'),
('ORD012', 'PROD007', 'CUST012', 2, 59.99, '2025-01-12 12:15:00', '2025-01-12 12:15:00'),
('ORD013', 'PROD008', 'CUST013', 3, 24.99, '2025-01-13 14:30:00', '2025-01-13 14:30:00'),
('ORD014', 'PROD009', 'CUST014', 1, 79.99, '2025-01-14 16:45:00', '2025-01-14 16:45:00'),
('ORD015', 'PROD010', 'CUST015', 5, 12.99, '2025-01-15 18:00:00', '2025-01-15 18:00:00');



-- 3rd run -> updated records
INSERT INTO sales (id, order_id, product_id, customer_id, quantity, price, sale_date, updated_at)
VALUES
  (1, 'ORD001', 'PROD001', 'CUST001', 4, 27.99, '2025-01-01 10:15:00', '2025-01-16 10:00:00'), -- Modified quantity and price
  (2, 'ORD002', 'PROD002', 'CUST002', 2, 47.99, '2025-01-02 12:30:00', '2025-01-16 11:00:00'), -- Modified quantity and price
  (3, 'ORD003', 'PROD003', 'CUST003', 6, 14.99, '2025-01-03 15:45:00', '2025-01-16 12:00:00'), -- Modified quantity and price
  (4, 'ORD004', 'PROD004', 'CUST004', 3, 97.99, '2025-01-04 09:20:00', '2025-01-16 13:00:00'), -- Modified quantity and price
  (5, 'ORD005', 'PROD005', 'CUST005', 5, 18.99, '2025-01-05 11:05:00', '2025-01-16 14:00:00'), -- Modified quantity and price
  (6, 'ORD006', 'PROD001', 'CUST006', 4, 28.99, '2025-01-06 13:50:00', '2025-01-16 15:00:00'), -- Modified quantity and price
  (7, 'ORD007', 'PROD002', 'CUST007', 2, 48.99, '2025-01-07 08:25:00', '2025-01-16 16:00:00'), -- Modified quantity and price
  (8, 'ORD008', 'PROD003', 'CUST008', 8, 14.49, '2025-01-08 10:40:00', '2025-01-16 17:00:00'); -- Modified quantity and price


select * from sales;



select load_date, count(*) from sales group by 1;

select * from sales
where load_date = '2025-01-03 15:45:00' 
order by order_id;


