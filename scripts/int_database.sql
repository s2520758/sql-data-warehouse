--Create Database'DataWarehouse'
Use master;
GO
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

