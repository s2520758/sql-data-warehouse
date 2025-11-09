-- Switch to master database
Use master;
GO

-- Drop existing DataWarehouse database if it exists
--    - Force to SINGLE_USER mode to disconnect active sessions
--    - Drop the database to allow clean recreation
IF EXISTS(SELECT 1 FROM sys.databases where name='DataWarehouse')
BEGIN 
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
 DROP DATABASE DataWarehouse;
END;
GO
--Create Database'DataWarehouse'  
CREATE DATABASE DataWarehouse;
GO
Use DataWarehouse;
GO
--Create Schemas
	CREATE SCHEMA bronze;
	GO
	CREATE SCHEMA silver;
	GO
	CREATE SCHEMA gold;
	GO

